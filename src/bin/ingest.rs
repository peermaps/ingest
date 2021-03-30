use peermaps_ingest::{encode,decode,ID_PREFIX,varint,
  Decoded,DecodedNode,DecodedWay,DecodedRelation};
use leveldb::database::{Database,batch::{Batch,Writebatch}};
use leveldb::options::{Options,WriteOptions,ReadOptions};
use leveldb::iterator::{LevelDBIterator,Iterable};
use leveldb::kv::KV;
use std::collections::HashMap;
use async_std::sync::Arc;

type Error = Box<dyn std::error::Error+Send+Sync>;
type NodeDeps = HashMap<u64,(f32,f32)>;
type WayDeps = HashMap<u64,Vec<u64>>;
type LDB = Arc<Database<Key>>;

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
    let db_dir = &args[3];
    let db = Arc::new(open(std::path::Path::new(&db_dir))?);
    phase0(db.clone(), pbf_file)?;
    phase1(db)?;
  } else if cmd == "phase0" {
    let pbf_file = &args[2];
    let db_dir = &args[3];
    let db = Arc::new(open(std::path::Path::new(&db_dir))?);
    phase0(db, pbf_file)?;
  } else if cmd == "phase1" {
    let db_dir = &args[2];
    let db = Arc::new(open(std::path::Path::new(&db_dir))?);
    phase1(db)?;
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
    let (key,value) = encode(&element).unwrap();
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
  eprintln!["phase 0: wrote {} records in {} seconds", n, start.elapsed().as_secs_f64()];
  Ok(())
}

// loop over the db, denormalize the records, georender-pack the data,
// store into eyros, and write backrefs into leveldb
fn phase1(db: LDB) -> Result<(),Error> {
  let start = std::time::Instant::now();
  let gt = Key::from(vec![ID_PREFIX]);
  let lt = Key::from(vec![ID_PREFIX,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff]);
  let mut iter = db.iter(ReadOptions::new()).from(&gt).to(&lt);
  let mut count = 0;
  while let Some((key,value)) = iter.next() {
    match decode(&key.data,&value)? {
      Decoded::Node(_node) => {
        // ...
      },
      Decoded::Way(way) => {
        let mut deps = HashMap::with_capacity(way.refs.len());
        get_way_deps(db.clone(), &way, &mut deps)?;
        //eprintln!["{} deps={:?}", way.id, &deps];
        /*
        georender_pack::encode::way_from_feature_type(
          way.id, way.feature_type, way.is_area, &way.refs, &deps
        )
        */
      },
      Decoded::Relation(relation) => {
        let mut node_deps = HashMap::new();
        let mut way_deps = HashMap::with_capacity(relation.members.len());
        get_relation_deps(db.clone(), &relation, &mut node_deps, &mut way_deps)?;
        //eprintln!["{} node_deps={:?} way_deps={:?}", relation.id, &node_deps, &way_deps];
      },
    }
    count += 1;
  }
  eprintln!["phase 1: processed and stored {} records in {} seconds",
    count, start.elapsed().as_secs_f64()];
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
          way_deps.insert(way.id, way.refs.clone());
          get_way_deps(db.clone(), &way, node_deps)?;
        },
        _ => {},
      }
    }
  }
  Ok(())
}
