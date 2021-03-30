use peermaps_ingest::{encode,ID_PREFIX};
use leveldb::database::{Database,batch::{Batch,Writebatch}};
use leveldb::options::{Options,WriteOptions,ReadOptions};
use leveldb::iterator::{LevelDBIterator,Iterable};

type Error = Box<dyn std::error::Error+Send+Sync>;

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
    let mut db = open(std::path::Path::new(&db_dir))?;
    phase0(&mut db, pbf_file)?;
    phase1(&mut db)?;
  } else if cmd == "phase0" {
    let pbf_file = &args[2];
    let db_dir = &args[3];
    let mut db = open(std::path::Path::new(&db_dir))?;
    phase0(&mut db, pbf_file)?;
  } else if cmd == "phase1" {
    let db_dir = &args[2];
    let mut db = open(std::path::Path::new(&db_dir))?;
    phase1(&mut db)?;
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
fn phase0(db: &mut Database<Key>, pbf_file: &str) -> Result<(),Error> {
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
fn phase1(db: &mut Database<Key>) -> Result<(),Error> {
  let start = std::time::Instant::now();
  let gt = Key::from(vec![ID_PREFIX]);
  let lt = Key::from(vec![ID_PREFIX,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff]);
  let mut iter = db.iter(ReadOptions::new()).from(&gt).to(&lt);
  let mut count = 0;
  while let Some((key,value)) = iter.next() {
    //eprintln!["{:?} => {:?}", &key.data, &value];
    count += 1;
  }
  eprintln!["phase 1: processed and stored {} records in {} seconds",
    count, start.elapsed().as_secs_f64()];
  Ok(())
}
