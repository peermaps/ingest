use peermaps_ingest::{
  encode_osmpbf,encode_o5m,decode,id_key,ID_PREFIX,BACKREF_PREFIX,
  varint,Decoded,DecodedWay,DecodedRelation,
  Key,EStore,LStore,
};
use leveldb::{database::Database,options::Options};
use leveldb::iterator::{LevelDBIterator,Iterable};
use leveldb::kv::KV;
use std::collections::HashMap;
use async_std::{prelude::*,sync::{Arc,Mutex},io,fs::File};

type Error = Box<dyn std::error::Error+Send+Sync>;
type NodeDeps = HashMap<u64,(f32,f32)>;
type WayDeps = HashMap<u64,Vec<u64>>;

type LS = Arc<Mutex<LStore>>;
type ES = Arc<Mutex<EStore>>;

#[async_std::main]
async fn main() -> Result<(),Error> {
  let args: Vec<String> = std::env::args().collect();
  let cmd = &args[1];
  if cmd == "ingest" {
    let pbf_file = &args[2];
    let ldb_dir = &args[3];
    let edb_dir = &args[4];
    let lstore = Arc::new(Mutex::new(
      LStore::new(open(std::path::Path::new(&ldb_dir))?)
    ));
    phase0(lstore.clone(), pbf_file).await?;
    let estore = Arc::new(Mutex::new(EStore::new(
      eyros::open_from_path2(&std::path::Path::new(&edb_dir)).await?
    )));
    phase1(lstore.clone(), estore.clone()).await?;
  } else if cmd == "phase0" {
    let pbf_file = &args[2];
    let ldb_dir = &args[3];
    let lstore = Arc::new(Mutex::new(
      LStore::new(open(std::path::Path::new(&ldb_dir))?)
    ));
    phase0(lstore, pbf_file).await?;
  } else if cmd == "phase1" {
    let ldb_dir = &args[2];
    let edb_dir = &args[3];
    let lstore = Arc::new(Mutex::new(
      LStore::new(open(std::path::Path::new(&ldb_dir))?)
    ));
    let estore = Arc::new(Mutex::new(EStore::new(
      eyros::open_from_path2(&std::path::Path::new(&edb_dir)).await?
    )));
    phase1(lstore, estore).await?;
  } else if cmd == "changeset" {
    let o5c_file = &args[2];
    let ldb_dir = &args[3];
    let edb_dir = &args[4];
    let lstore = Arc::new(Mutex::new(
      LStore::new(open(std::path::Path::new(&ldb_dir))?)
    ));
    let estore = Arc::new(Mutex::new(EStore::new(
      eyros::open_from_path2(&std::path::Path::new(&edb_dir)).await?
    )));
    changeset(lstore, estore, o5c_file).await?;
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
async fn phase0(mlstore: LS, pbf_file: &str) -> Result<(),Error> {
  let mut lstore = mlstore.lock().await;
  let start = std::time::Instant::now();
  osmpbf::ElementReader::from_path(pbf_file)?.for_each(|element| {
    let (key,value) = encode_osmpbf(&element).unwrap();
    lstore.put(Key::from(&key), &value);
  })?;
  lstore.flush()?;
  eprintln!["phase 0: wrote {} records in {} seconds",
    lstore.count, start.elapsed().as_secs_f64()];
  Ok(())
}

// loop over the db, denormalize the records, georender-pack the data,
// store into eyros, and write backrefs into leveldb
async fn phase1(mlstore: LS, mestore: ES) -> Result<(),Error> {
  let mut estore = mestore.lock().await;
  let start = std::time::Instant::now();
  let gt = Key::from(&vec![ID_PREFIX]);
  let lt = Key::from(&vec![ID_PREFIX,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff]);
  let mlstorec = mlstore.clone();
  let mut lstorec = mlstorec.lock().await;
  let mut iter = lstorec.iter(&lt, &gt);
  let place_other = *georender_pack::osm_types::get_types().get("place.other").unwrap();
  while let Some((key,value)) = iter.next() {
    //if key.data > lt.data { break }
    match decode(&key.data,&value)? {
      Decoded::Node(node) => {
        if node.feature_type == place_other { continue }
        let encoded = georender_pack::encode::node_from_parsed(
          node.id, (node.lon,node.lat), node.feature_type, &node.labels
        )?;
        let point = (eyros::Coord::Scalar(node.lon),eyros::Coord::Scalar(node.lat));
        estore.create(point, encoded.into());
      },
      Decoded::Way(way) => {
        let mut deps = HashMap::with_capacity(way.refs.len());
        get_way_deps(mlstore.clone(), &way, &mut deps).await?;
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
        estore.create(point, encoded.into());
        let mut lstore = mlstore.lock().await;
        for r in deps.keys() {
          // node -> way backref
          lstore.put(Key::from(&backref_key(*r*3+0, way.id*3+1)?), &vec![]);
        }
      },
      Decoded::Relation(relation) => {
        let mut node_deps = HashMap::new();
        let mut way_deps = HashMap::with_capacity(relation.members.len());
        get_relation_deps(mlstore.clone(), &relation, &mut node_deps, &mut way_deps).await?;
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
        estore.create(point, encoded.into());

        let mut lstore = mlstore.lock().await;
        for way_id in way_deps.keys() {
          // way -> relation backref
          lstore.put(Key::from(&backref_key(way_id*3+1,relation.id*3+2)?), &vec![]);
        }
      },
    }
  }
  estore.flush().await?;
  estore.sync().await?;
  let mut lstore = mlstore.lock().await;
  lstore.flush()?;
  eprintln!["phase 1: processed and stored {} records in {} seconds",
    lstore.count, start.elapsed().as_secs_f64()];
  Ok(())
}

// import changes from an o5c changeset file
async fn changeset(mlstore: LS, mestore: ES, o5c_file: &str) -> Result<(),Error> {
  let mut lstore = mlstore.lock().await;
  let mut estore = mestore.lock().await;
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
            let key = Key::from(&id_key(node.id*3+0)?);
            match lstore.get(&key.clone())? {
              Some(buf) => {
                let decoded = decode(&key.data,&buf)?;
                if let Decoded::Node(node) = decoded {
                  let pt = (
                    eyros::Coord::Scalar(node.lon),
                    eyros::Coord::Scalar(node.lat),
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
      let backrefs = get_backrefs(mlstore.clone(), ex_id).await?;
      if is_rm {
        // delete the record but don't bother dealing with backrefs
        // because the referred-to elements *should* be deleted too.
        // not the job of this ingest script to verify changeset integrity
        // TODO: use ex_id directly once georender-pack is updated
        estore.delete(pt, ex_id/3);
        lstore.put(Key::from(&id_key(ex_id)?), &vec![]);
        for r in backrefs.iter() {
          lstore.put(Key::from(&backref_key(ex_id, *r)?), &vec![]);
        }
      } else {
        //let encoded = encode_o5m(dataset);
        for r in backrefs.iter() {
          update_ref(*r, mlstore.clone(), mestore.clone())
        }
      }
    }
  }
  lstore.flush()?;
  estore.flush().await?;
  estore.sync().await?;
  Ok(())
}

fn update_ref(ex_id: u64, lstore: LS, estore: ES) -> () {
  /*
  let encoded = georender_pack::encode::way_from_parsed(
    way.id, way.feature_type, way.is_area, &way.labels, &way.refs, &deps
  )?;
  if let Some(buf) = ldb.get(Key::from(id_key(*r)?))? {
    match decode(&key.data,&buf)? {
      Decoded::Node(node) => {},
      Decoded::Way(way) => {
        let (nway,nbuf) = update_way(&way, &pt);
        let wbackrefs = get_backrefs(ldb.clone(), *r)?;
        // TODO: use backrefs for way to update relations
      },
      Decoded::Relation(relation) => {
        let nrelation = update_relation(&relation, &prev);
      },
    }
  }
  */
}

async fn get_way_deps(mlstore: LS, way: &DecodedWay, deps: &mut NodeDeps) -> Result<(),Error> {
  let mut lstore = mlstore.lock().await;
  let mut key_data = [ID_PREFIX,0,0,0,0,0,0,0,0];
  key_data[0] = ID_PREFIX;
  for r in way.refs.iter() {
    let s = varint::encode(r*3+0,&mut key_data[1..])?;
    let key = Key::from(&key_data[0..1+s]);
    if let Some(buf) = lstore.get(&key)? {
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

async fn get_relation_deps(mlstore: LS, relation: &DecodedRelation,
  node_deps: &mut NodeDeps, way_deps: &mut WayDeps
) -> Result<(),Error> {
  let mut lstore = mlstore.lock().await;
  let mut key_data = [ID_PREFIX,0,0,0,0,0,0,0,0];
  key_data[0] = ID_PREFIX;
  for m in relation.members.iter() {
    let s = varint::encode((m/2)*3+1,&mut key_data[1..])?;
    let key = Key::from(&key_data[0..1+s]);
    if let Some(buf) = lstore.get(&key)? {
      match decode(&key.data,&buf)? {
        Decoded::Way(way) => {
          //assert![way.id == m/2, "{} != {}", way.id, m/2];
          way_deps.insert(way.id, way.refs.clone());
          get_way_deps(mlstore.clone(), &way, node_deps).await?;
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

async fn get_backrefs(mlstore: LS, ex_id: u64) -> Result<Vec<u64>,Error> {
  let mut lstore = mlstore.lock().await;
  let ex_id_len = varint::length(ex_id);
  let gt = Key::from(&{
    let mut key = vec![0u8;1+ex_id_len];
    key[0] = BACKREF_PREFIX;
    varint::encode(ex_id, &mut key[1..])?;
    key
  });
  let lt = Key::from(&{
    let mut key = vec![0u8;1+ex_id_len+8];
    key[0] = BACKREF_PREFIX;
    let s = varint::encode(ex_id, &mut key[1..])?;
    for i in 0..8 { key[1+s+i] = 0xff }
    key
  });
  let mut results = vec![];
  let mut iter = lstore.keys_iter(lt, gt);
  while let Some(key) = iter.next() {
    //if key.data > lt.data { break }
    let (_,r_id) = varint::decode(&key.data[1+ex_id_len..])?;
    results.push(r_id);
  }
  Ok(results)
}
