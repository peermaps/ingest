use peermaps_ingest_leveldb::encode;
type Error = Box<dyn std::error::Error+Send+Sync>;

use leveldb::database::{Database,batch::{Batch,Writebatch}};
use leveldb::options::{Options,WriteOptions};

struct Key { data: Vec<u8> }
impl Key {
  fn from(key: Vec<u8>) -> Self { Key { data: key } }
}
impl db_key::Key for Key {
  fn from_u8(key: &[u8]) -> Self { Key { data: key.into() } }
  fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T { f(&self.data) }
}

#[async_std::main]
async fn main() -> Result<(),Error> {
  let args: Vec<String> = std::env::args().collect();
  let cmd = &args[1];
  if cmd == "ingest" {
    let pbf = &args[2];
    let db_dir = &args[3];
    let db = open(std::path::Path::new(&db_dir))?;
    let mut batch = Writebatch::new();
    let batch_size = 10_000;
    let mut n = 0;
    osmpbf::ElementReader::from_path(pbf)?.for_each(|element| {
      let (key,value) = encode(&element).unwrap();
      batch.put(Key::from(key), &value);
      n += 1;
      if n >= batch_size {
        db.write(WriteOptions::new(), &batch).unwrap();
        batch.clear();
        n = 0;
      }
      //eprintln!["{:?} => {:?}", &key, &value];
    })?;
    if n >= 0 {
      db.write(WriteOptions::new(), &batch)?;
      batch.clear();
    }
  }
  Ok(())
}

fn open(path: &std::path::Path) -> Result<Database<Key>,Error> {
  let mut options = Options::new();
  options.create_if_missing = true;
  options.compression = leveldb_sys::Compression::Snappy;
  Database::open(path, options).map_err(|e| e.into())
}
