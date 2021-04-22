use peermaps_ingest::{Ingest,Key,EStore,LStore};
use leveldb::{database::Database,options::Options};
use async_std::fs::File;
use tempfile::Builder as Tmpfile;

type Error = Box<dyn std::error::Error+Send+Sync>;

#[async_std::test]
async fn ingest() -> Result<(),Error> {
  let dir = Tmpfile::new().prefix("peermaps-ingest").tempdir()?;
  let mut ldb_dir = std::path::PathBuf::from(&dir.path());
  ldb_dir.push("ldb");
  let mut edb_dir = std::path::PathBuf::from(&dir.path());
  edb_dir.push("edb");

  let mut pbf_file = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
  pbf_file.push("tests/data/ingest.pbf");
  let mut o5c_file = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
  o5c_file.push("tests/data/changeset.o5c");

  let mut ingest = Ingest::new(
    LStore::new(open(std::path::Path::new(&ldb_dir))?),
    EStore::new(eyros::open_from_path2(&std::path::Path::new(&edb_dir)).await?)
  );
  eprintln!["load_pbf"];
  ingest.load_pbf(pbf_file.to_str().unwrap()).await?;
  eprintln!["process"];
  ingest.process().await?;

  // TODO: check db results here

  eprintln!["changeset"];
  ingest.changeset(Box::new(File::open(&o5c_file).await?)).await?;

  // TODO: check db results here

  Ok(())
}

fn open(path: &std::path::Path) -> Result<Database<Key>,Error> {
  let mut options = Options::new();
  options.create_if_missing = true;
  options.compression = leveldb_sys::Compression::Snappy;
  Database::open(path, options).map_err(|e| e.into())
}
