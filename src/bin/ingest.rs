use peermaps_ingest::{Ingest,Key,EStore,LStore};
use leveldb::{database::Database,options::Options};
use leveldb::iterator::{LevelDBIterator,Iterable};
use leveldb::kv::KV;
use std::collections::HashMap;
use async_std::{prelude::*,sync::{Arc,Mutex},io,fs::File};

type Error = Box<dyn std::error::Error+Send+Sync>;

#[async_std::main]
async fn main() -> Result<(),Error> {
  let args: Vec<String> = std::env::args().collect();
  let cmd = &args[1];
  if cmd == "ingest" {
    let pbf_file = &args[2];
    let ldb_dir = &args[3];
    let edb_dir = &args[4];
    let ingest = Ingest::new(
      LStore::new(open(std::path::Path::new(&ldb_dir))?),
      EStore::new(eyros::open_from_path2(&std::path::Path::new(&edb_dir)).await?)
    );
    ingest.load_pbf(pbf_file).await?;
    ingest.process().await?;
  } else if cmd == "phase0" {
    let pbf_file = &args[2];
    let ldb_dir = &args[3];
    let edb_dir = &args[4];
    let ingest = Ingest::new(
      LStore::new(open(std::path::Path::new(&ldb_dir))?),
      EStore::new(eyros::open_from_path2(&std::path::Path::new(&edb_dir)).await?)
    );
    ingest.load_pbf(pbf_file).await?;
    ingest.process().await?;
  } else if cmd == "phase1" {
    let ldb_dir = &args[2];
    let edb_dir = &args[3];
    let ingest = Ingest::new(
      LStore::new(open(std::path::Path::new(&ldb_dir))?),
      EStore::new(eyros::open_from_path2(&std::path::Path::new(&edb_dir)).await?)
    );
    ingest.process().await?;
  } else if cmd == "changeset" {
    let o5c_file = &args[2];
    let ldb_dir = &args[3];
    let edb_dir = &args[4];
    let ingest = Ingest::new(
      LStore::new(open(std::path::Path::new(&ldb_dir))?),
      EStore::new(eyros::open_from_path2(&std::path::Path::new(&edb_dir)).await?)
    );
    let o5c_stream: Box<dyn io::Read+Unpin> = match o5c_file.as_str() {
      "-" => Box::new(io::stdin()),
      x => Box::new(File::open(x).await?),
    };
    ingest.changeset(o5c_stream).await?;
  }
  Ok(())
}

fn open(path: &std::path::Path) -> Result<Database<Key>,Error> {
  let mut options = Options::new();
  options.create_if_missing = true;
  options.compression = leveldb_sys::Compression::Snappy;
  Database::open(path, options).map_err(|e| e.into())
}
