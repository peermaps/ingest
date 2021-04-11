use peermaps_ingest::{encode_osmpbf,encode_o5m,decode,id_key,ID_PREFIX,BACKREF_PREFIX,
  varint,Decoded,DecodedWay,DecodedRelation};
use leveldb::database::{Database,batch::{Batch,Writebatch}};
use leveldb::options::{Options,WriteOptions,ReadOptions};
use leveldb::iterator::{LevelDBIterator,Iterable};
use leveldb::kv::KV;
use std::collections::HashMap;
use async_std::{prelude::*,sync::Arc,io,fs::File};

type Error = Box<dyn std::error::Error+Send+Sync>;
type NodeDeps = HashMap<u64,(f32,f32)>;
type WayDeps = HashMap<u64,Vec<u64>>;
type LDB = Arc<Database<Key>>;
type S = random_access_disk::RandomAccessDisk;
type P = (eyros::Coord<f32>,eyros::Coord<f32>);
type V = Vec<u8>;
type T = eyros::Tree2<f32,f32,V>;
type EDB = eyros::DB<S,T,P,V>;

#[derive(Clone)]
struct Key { pub data: Vec<u8> }
impl Key { fn from(key: Vec<u8>) -> Self { Key { data: key } } }
impl db_key::Key for Key {
  fn from_u8(key: &[u8]) -> Self { Key { data: key.into() } }
  fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T { f(&self.data) }
}

#[async_std::main]
async fn main() -> Result<(),Error> {
  let args: Vec<String> = std::env::args().collect();
  let cmd = &args[1];
  if cmd == "ingest" {
    let pbf_file = &args[2];
    let ldb_dir = &args[3];
    let edb_dir = &args[4];
    let ldb = Arc::new(open(std::path::Path::new(&ldb_dir))?);
    phase0(ldb.clone(), pbf_file)?;
    let mut edb = eyros::open_from_path2(&std::path::Path::new(&edb_dir)).await?;
    phase1(ldb.clone(), &mut edb).await?;
  } else if cmd == "phase0" {
    let pbf_file = &args[2];
    let ldb_dir = &args[3];
    let ldb = Arc::new(open(std::path::Path::new(&ldb_dir))?);
    phase0(ldb, pbf_file)?;
  } else if cmd == "phase1" {
    let ldb_dir = &args[2];
    let edb_dir = &args[3];
    let ldb = Arc::new(open(std::path::Path::new(&ldb_dir))?);
    let mut edb = eyros::open_from_path2(&std::path::Path::new(&edb_dir)).await?;
    phase1(ldb, &mut edb).await?;
  } else if cmd == "changeset" {
    let o5c_file = &args[2];
    let ldb_dir = &args[3];
    let edb_dir = &args[4];
    let ldb = Arc::new(open(std::path::Path::new(&ldb_dir))?);
    let mut edb = eyros::open_from_path2(&std::path::Path::new(&edb_dir)).await?;
    changeset(ldb, &mut edb, o5c_file).await?;
  }
  Ok(())
}

fn open(path: &std::path::Path) -> Result<Database<Key>,Error> {
  let mut options = Options::new();
  options.create_if_missing = true;
  options.compression = leveldb_sys::Compression::Snappy;
  Database::open(path, options).map_err(|e| e.into())
}

// write the pbf into leveldb
fn phase0(db: LDB, pbf_file: &str) -> Result<(),Error> {
  let start = std::time::Instant::now();
  let mut batch = Writebatch::new();
  let batch_size = 10_000;
  let mut n = 0;
  let mut write_count = 0;
  osmpbf::ElementReader::from_path(pbf_file)?.for_each(|element| {
    let (key,value) = encode_osmpbf(&element).unwrap();
    batch.put(Key::from(key), &value);
    n += 1;
    write_count += 1;
    if n >= batch_size {
      db.write(WriteOptions::new(), &batch).unwrap();
      batch.clear();
      n = 0;
    }
  })?;
  if n >= 0 {
    db.write(WriteOptions::new(), &batch)?;
    batch.clear();
  }
  eprintln!["phase 0: wrote {} records in {} seconds",
    write_count, start.elapsed().as_secs_f64()];
  Ok(())
}

// loop over the db, denormalize the records, georender-pack the data,
// store into eyros, and write backrefs into leveldb
async fn phase1(ldb: LDB, edb: &mut EDB) -> Result<(),Error> {
  let start = std::time::Instant::now();
  let gt = Key::from(vec![ID_PREFIX]);
  let lt = Key::from(vec![ID_PREFIX,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff]);
  let mut iter = ldb.iter(ReadOptions::new()).from(&gt).to(&lt);
  let mut count = 0;
  let mut ebatch_count = 0;
  let place_other = *georender_pack::osm_types::get_types().get("place.other").unwrap();
  let ebatch_size = 10_000;
  let esync_interval = 20;
  let mut ebatch = Vec::with_capacity(ebatch_size);
  let mut lbatch_count = 0;
  let lbatch_size = 10_000;
  let mut lbatch = Writebatch::new();
  while let Some((key,value)) = iter.next() {
    match decode(&key.data,&value)? {
      Decoded::Node(node) => {
        if node.feature_type == place_other { continue }
        let encoded = georender_pack::encode::node_from_parsed(
          node.id, (node.lon,node.lat), node.feature_type, &node.labels
        )?;
        let point = (eyros::Coord::Scalar(node.lon),eyros::Coord::Scalar(node.lat));
        ebatch.push(eyros::Row::Insert(point, encoded));
      },
      Decoded::Way(way) => {
        let mut deps = HashMap::with_capacity(way.refs.len());
        get_way_deps(ldb.clone(), &way, &mut deps)?;
        let mut bbox = (f32::INFINITY,f32::INFINITY,f32::NEG_INFINITY,f32::NEG_INFINITY);
        let mut pcount = 0;
        for (lon,lat) in deps.values() {
          bbox.0 = bbox.0.min(*lon);
          bbox.1 = bbox.1.min(*lat);
          bbox.2 = bbox.2.max(*lon);
          bbox.3 = bbox.3.max(*lat);
          pcount += 1;
        }
        if pcount <= 1 { continue }
        let encoded = georender_pack::encode::way_from_parsed(
          way.id, way.feature_type, way.is_area, &way.labels, &way.refs, &deps
        )?;
        let point = (
          eyros::Coord::Interval(bbox.0,bbox.2),
          eyros::Coord::Interval(bbox.1,bbox.3),
        );
        ebatch.push(eyros::Row::Insert(point, encoded));
        for r in deps.keys() {
          // node -> way backref
          lbatch.put(Key::from(backref_key(*r*3+0, way.id*3+1)?), &vec![]);
          lbatch_count += 1;
        }
      },
      Decoded::Relation(relation) => {
        let mut node_deps = HashMap::new();
        let mut way_deps = HashMap::with_capacity(relation.members.len());
        get_relation_deps(ldb.clone(), &relation, &mut node_deps, &mut way_deps)?;
        let mut bbox = (f32::INFINITY,f32::INFINITY,f32::NEG_INFINITY,f32::NEG_INFINITY);
        let mut pcount = 0;
        for refs in way_deps.values() {
          for r in refs.iter() {
            if let Some(p) = node_deps.get(r) {
              bbox.0 = bbox.0.min(p.0);
              bbox.1 = bbox.1.min(p.1);
              bbox.2 = bbox.2.max(p.0);
              bbox.3 = bbox.3.max(p.1);
              pcount += 1;
            }
          }
        }
        if pcount <= 1 { continue }
        let members = relation.members.iter().map(|m| {
          georender_pack::Member::new(
            m/2,
            match m%2 {
              0 => georender_pack::MemberRole::Outer(),
              _ => georender_pack::MemberRole::Inner(),
            },
            georender_pack::MemberType::Way()
          )
        }).collect::<Vec<_>>();
        let encoded = georender_pack::encode::relation_from_parsed(
          relation.id, relation.feature_type, relation.is_area,
          &relation.labels, &members, &node_deps, &way_deps
        )?;
        let point = (
          eyros::Coord::Interval(bbox.0,bbox.2),
          eyros::Coord::Interval(bbox.1,bbox.3),
        );
        ebatch.push(eyros::Row::Insert(point, encoded));

        for way_id in way_deps.keys() {
          // way -> relation backref
          lbatch.put(Key::from(backref_key(way_id*3+1,relation.id*3+2)?), &vec![]);
          lbatch_count += 1;
        }
      },
    }
    count += 1;
    if ebatch.len() >= ebatch_size {
      edb.batch(&ebatch).await?;
      ebatch.clear();
      ebatch_count += 1;
      if ebatch_count % esync_interval == 0 {
        edb.sync().await?;
      }
    }
    if lbatch_count >= lbatch_size {
      ldb.write(WriteOptions::new(), &lbatch)?;
      lbatch.clear();
      lbatch_count = 0;
    }
  }
  if ebatch.len() > 0 {
    edb.batch(&ebatch).await?;
    ebatch.clear();
  }
  edb.sync().await?;
  if lbatch_count > 0 {
    ldb.write(WriteOptions::new(), &lbatch)?;
    lbatch.clear();
  }
  eprintln!["phase 1: processed and stored {} records in {} seconds",
    count, start.elapsed().as_secs_f64()];
  Ok(())
}

// import changes from an o5c changeset file
async fn changeset(ldb: LDB, edb: &mut EDB, o5c_file: &str) -> Result<(),Error> {
  type R = Box<dyn io::Read+Unpin>;
  let infile: R = match &o5c_file {
    &"-" => Box::new(io::stdin()),
    x => Box::new(File::open(x).await?),
  };
  let mut stream = o5m_stream::decode(infile);
  while let Some(result) = stream.next().await {
    let dataset = result?;
    let m = match dataset {
      o5m_stream::Dataset::Node(node) => {
        match &node.data {
          Some(data) => { // modify or create
            let pt = (
              eyros::Coord::Scalar(data.get_longitude()),
              eyros::Coord::Scalar(data.get_latitude())
            );
            Some((false,pt,node.id*3+0))
          },
          None => { // delete
            let key = Key::from(id_key(node.id*3+0)?);
            match ldb.get(ReadOptions::new(), key.clone())? {
              Some(buf) => {
                if let Decoded::Node(decoded) = decode(&key.data,&buf)? {
                  let pt = (
                    eyros::Coord::Scalar(decoded.lon),
                    eyros::Coord::Scalar(decoded.lat),
                  );
                  Some((true,pt,node.id*3+0))
                } else {
                  return Err(Box::new(failure::err_msg(
                    "expected node, got unexpected decoded type"
                  ).compat()));
                }
              },
              None => None,
            }
          },
        }
      },
      o5m_stream::Dataset::Way(way) => {
        unimplemented![]
      },
      o5m_stream::Dataset::Relation(relation) => {
        unimplemented![]
      },
      _ => None,
    };
    if let Some((is_rm,pt,ex_id)) = m {
      //println!["{} {:?}", is_rm, pt];
      let backrefs = get_backrefs(ldb.clone(), ex_id)?;
      if !is_rm {
        //let encoded = encode_o5m(dataset);
      }
    }
  }
  Ok(())
}

fn get_way_deps(db: LDB, way: &DecodedWay, deps: &mut NodeDeps) -> Result<(),Error> {
  let mut key_data = [ID_PREFIX,0,0,0,0,0,0,0,0];
  key_data[0] = ID_PREFIX;
  for r in way.refs.iter() {
    let s = varint::encode(r*3+0,&mut key_data[1..])?;
    let key = Key::from(key_data[0..1+s].to_vec());
    if let Some(buf) = db.get(ReadOptions::new(), &key)? {
      match decode(&key.data,&buf)? {
        Decoded::Node(node) => {
          deps.insert(*r,(node.lon,node.lat));
        },
        _ => {},
      }
    }
  }
  Ok(())
}

fn get_relation_deps(db: LDB, relation: &DecodedRelation,
  node_deps: &mut NodeDeps, way_deps: &mut WayDeps
) -> Result<(),Error> {
  let mut key_data = [ID_PREFIX,0,0,0,0,0,0,0,0];
  key_data[0] = ID_PREFIX;
  for m in relation.members.iter() {
    let s = varint::encode((m/2)*3+1,&mut key_data[1..])?;
    let key = Key::from(key_data[0..1+s].to_vec());
    if let Some(buf) = db.get(ReadOptions::new(), &key)? {
      match decode(&key.data,&buf)? {
        Decoded::Way(way) => {
          //assert![way.id == m/2, "{} != {}", way.id, m/2];
          way_deps.insert(way.id, way.refs.clone());
          get_way_deps(db.clone(), &way, node_deps)?;
        },
        _ => {},
      }
    }
  }
  Ok(())
}

fn backref_key(a: u64, b: u64) -> Result<Vec<u8>,Error> {
  // both a and b are extended ids
  let mut key = vec![0u8;1+varint::length(a)+varint::length(b)];
  key[0] = BACKREF_PREFIX;
  let s = varint::encode(a, &mut key[1..])?;
  varint::encode(b, &mut key[1+s..])?;
  Ok(key)
}

fn get_backrefs(ldb: LDB, ex_id: u64) -> Result<Vec<u64>,Error> {
  let ex_id_len = varint::length(ex_id);
  let gt = Key::from({
    let mut key = vec![0u8;1+ex_id_len];
    key[0] = BACKREF_PREFIX;
    varint::encode(ex_id, &mut key[1..])?;
    key
  });
  let lt = Key::from({
    let mut key = vec![0u8;1+ex_id_len+8];
    key[0] = BACKREF_PREFIX;
    let s = varint::encode(ex_id, &mut key[1..])?;
    for i in 0..8 { key[1+s+i] = 0xff }
    key
  });
  let mut results = vec![];
  let mut iter = ldb.keys_iter(ReadOptions::new());
  iter.seek(&gt);
  while let Some(key) = iter.next() {
    if key.data > lt.data { break }
    let (_,r_id) = varint::decode(&key.data[1+ex_id_len..])?;
    results.push(r_id);
  }
  Ok(results)
}
