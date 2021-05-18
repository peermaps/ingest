use peermaps_ingest::{Ingest,Key,EStore,LStore};
use leveldb::{database::Database,options::Options};
use async_std::{io,fs::File};

type Error = Box<dyn std::error::Error+Send+Sync>;

#[async_std::main]
async fn main() -> Result<(),Error> {
  let (args,argv) = argmap::new()
    .booleans(&["help","h"])
    .parse(std::env::args());
  if argv.contains_key("help") || argv.contains_key("h") {
    print!["{}", usage(&args)];
    return Ok(());
  }
  match args.get(1).map(|x| x.as_str()) {
    None => print!["{}", usage(&args)],
    Some("help") => print!["{}", usage(&args)],
    Some("ingest") => {
      let pbf_file = argv.get("pbf").and_then(|x| x.first());
      let ldb_dir = argv.get("ldb").and_then(|x| x.first());
      let edb_dir = argv.get("edb").and_then(|x| x.first());
      if pbf_file.is_none() || ldb_dir.is_none() || edb_dir.is_none() {
        print!["{}", usage(&args)];
        std::process::exit(1);
      }
      let ingest = Ingest::new(
        LStore::new(open(std::path::Path::new(&ldb_dir.unwrap()))?),
        EStore::new(eyros::open_from_path2(&std::path::Path::new(&edb_dir.unwrap())).await?)
      );
      ingest.load_pbf(pbf_file.unwrap()).await?;
      ingest.process().await?;
    },
    Some("phase0") => {
      let pbf_file = argv.get("pbf").and_then(|x| x.first());
      let ldb_dir = argv.get("ldb").and_then(|x| x.first());
      let edb_dir = argv.get("edb").and_then(|x| x.first());
      if pbf_file.is_none() || ldb_dir.is_none() || edb_dir.is_none() {
        eprint!["{}", usage(&args)];
        std::process::exit(1);
      }
      let ingest = Ingest::new(
        LStore::new(open(std::path::Path::new(&ldb_dir.unwrap()))?),
        EStore::new(eyros::open_from_path2(&std::path::Path::new(&edb_dir.unwrap())).await?)
      );
      ingest.load_pbf(pbf_file.unwrap()).await?;
      ingest.process().await?;
    },
    Some("phase1") => {
      let ldb_dir = argv.get("ldb").and_then(|x| x.first());
      let edb_dir = argv.get("edb").and_then(|x| x.first());
      if ldb_dir.is_none() || edb_dir.is_none() {
        eprint!["{}", usage(&args)];
        std::process::exit(1);
      }
      let ingest = Ingest::new(
        LStore::new(open(std::path::Path::new(&ldb_dir.unwrap()))?),
        EStore::new(eyros::open_from_path2(&std::path::Path::new(&edb_dir.unwrap())).await?)
      );
      ingest.process().await?;
    },
    Some("changeset") => {
      let o5c_file = argv.get("o5c").and_then(|x| x.first());
      let ldb_dir = argv.get("ldb").and_then(|x| x.first());
      let edb_dir = argv.get("edb").and_then(|x| x.first());
      if o5c_file.is_none() || ldb_dir.is_none() || edb_dir.is_none() {
        eprint!["{}",usage(&args)];
        std::process::exit(1);
      }
      let mut ingest = Ingest::new(
        LStore::new(open(std::path::Path::new(&ldb_dir.unwrap()))?),
        EStore::new(eyros::open_from_path2(&std::path::Path::new(&edb_dir.unwrap())).await?)
      );
      let o5c_stream: Box<dyn io::Read+Unpin> = match o5c_file.unwrap().as_str() {
        "-" => Box::new(io::stdin()),
        x => Box::new(File::open(x).await?),
      };
      ingest.changeset(o5c_stream).await?;
    },
    Some(cmd) => {
      eprint!["unrecognized command {}", cmd];
      std::process::exit(1);
    },
  }
  Ok(())
}

fn open(path: &std::path::Path) -> Result<Database<Key>,Error> {
  let mut options = Options::new();
  options.create_if_missing = true;
  options.compression = leveldb_sys::Compression::Snappy;
  Database::open(path, options).map_err(|e| e.into())
}

fn usage(args: &[String]) -> String {
  format![indoc::indoc![r#"usage: {} COMMAND {{OPTIONS}}

    ingest - runs phases 0 and 1
      --pbf  osm pbf file to ingest
      --ldb  level db dir to write normalized data
      --edb  eyros db dir to write spatial data

    phase0 - write normalized data to level db
      --pbf  osm pbf file to ingest
      --ldb  level db dir to write normalized data
      --edb  eyros db dir to write spatial data

    phase1 - write georender-pack data to eyros db
      --ldb  level db dir to read normalized data
      --edb  eyros db dir to write spatial data

    changeset - ingest data from an o5c changeset
      --o5c  o5c changeset file or "-" for stdin (default)
      --ldb  level db dir to read/write normalized data
      --edb  eyros db dir to write spatial data

  "#], args.get(0).unwrap_or(&"???".to_string())]
}
