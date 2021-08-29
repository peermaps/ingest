#![feature(backtrace)]
use peermaps_ingest::{Ingest,IngestOptions,EDB,Progress};
use async_std::{prelude::*,sync::{Arc,RwLock},task,stream};
use std::io::{Write,Read};
use desert::{ToBytes,FromBytes};

type Error = Box<dyn std::error::Error+Send+Sync>;
use osmpbf_parser::ScanTable;

#[async_std::main]
async fn main() -> Result<(),Error> {
  if let Err(err) = run().await {
    match err.backtrace().map(|bt| (bt,bt.status())) {
      Some((bt,std::backtrace::BacktraceStatus::Captured)) => {
        eprint!["{}\n{}", err, bt];
      },
      _ => eprintln!["{}", err],
    }
    std::process::exit(1);
  }
  Ok(())
}

async fn run() -> Result<(),Error> {
  let (args,argv) = argmap::new()
    .booleans(&["help","h","defaults","d","no-monitor"])
    .parse(std::env::args());
  if argv.contains_key("help") || argv.contains_key("h") {
    print!["{}", usage(&args)];
    return Ok(());
  }
  if argv.contains_key("version") || argv.contains_key("v") {
    println!["{}", get_version()];
    return Ok(());
  }

  match args.get(1).map(|x| x.as_str()) {
    None => print!["{}", usage(&args)],
    Some("help") => print!["{}", usage(&args)],
    Some("version") => print!["{}", get_version()],
    Some("scan") => {
      let scan_file = argv.get("scan_file").or_else(|| argv.get("scan-file"))
        .and_then(|x| x.first())
        .cloned()
        .or_else(|| {
          argv.get("outdir").or_else(|| argv.get("o"))
            .and_then(|x| x.first())
            .and_then(|d| {
              let mut p = std::path::PathBuf::from(d);
              p.push("scan");
              p.to_str().map(|s| s.to_string())
            })
        })
        .expect("could not infer --scan_file")
      ;
      let o_pbf_file = argv.get("pbf").or_else(|| argv.get("f"))
        .and_then(|x| x.first());
      if o_pbf_file.is_none() {
        println!["--pbf or -f option required\n"];
        print!["{}", usage(&args)];
        std::process::exit(1);
      }
      let pbf_file = o_pbf_file.unwrap();
      let mut ingest = Ingest::new(&["scan"]);
      let scan_table = {
        if argv.contains_key("no-monitor") {
          ingest.scan(&pbf_file).await
        } else {
          let mut p = Monitor::open(ingest.progress.clone());
          let scan_table = ingest.scan(&pbf_file).await;
          p.end().await;
          scan_table
        }
      };
      let mut file = std::fs::File::create(scan_file)?;
      file.write_all(&scan_table.to_bytes()?)?;
    },
    Some("ingest_from_scan") | Some("ingest-from-scan") => {
      let scan_file = argv.get("scan_file").or_else(|| argv.get("scan-file"))
        .and_then(|x| x.first())
        .cloned()
        .or_else(|| {
          argv.get("outdir").or_else(|| argv.get("o"))
            .and_then(|x| x.first())
            .and_then(|d| {
              let mut p = std::path::PathBuf::from(&*d);
              p.push("scan");
              p.to_str().map(|s| s.to_string())
            })
        })
        .expect("could not infer --scan_file")
      ;
      let scan_table = {
        let mut file = std::fs::File::open(scan_file)?;
        let mut buf = vec![];
        file.read_to_end(&mut buf)?;
        ScanTable::from_bytes(&buf)?.1
      };
      let o_pbf_file = argv.get("pbf").or_else(|| argv.get("f"))
        .and_then(|x| x.first());
      if o_pbf_file.is_none() {
        println!["--pbf or -f option required\n"];
        print!["{}", usage(&args)];
        std::process::exit(1);
      }
      let pbf_file = o_pbf_file.unwrap();
      let ingest_options = get_ingest_options(&argv);
      let edb_dir = get_dirs(&argv);
      if edb_dir.is_none() {
        print!["{}", usage(&args)];
        std::process::exit(1);
      }
      let mut ingest = Ingest::new(&["ingest"]);
      if argv.contains_key("no-monitor") {
        ingest.ingest(
          open_eyros(&std::path::Path::new(&edb_dir.unwrap())).await?,
          &pbf_file, scan_table,
          &ingest_options
        ).await;
      } else {
        let mut p = Monitor::open(ingest.progress.clone());
        ingest.ingest(
          open_eyros(&std::path::Path::new(&edb_dir.unwrap())).await?,
          &pbf_file, scan_table, &ingest_options
        ).await;
        p.end().await;
      }
    },
    Some("ingest") => {
      let o_pbf_file = argv.get("pbf").or_else(|| argv.get("f"))
        .and_then(|x| x.first());
      if o_pbf_file.is_none() {
        println!["--pbf or -f option required\n"];
        print!["{}", usage(&args)];
        std::process::exit(1);
      }
      let pbf_file = o_pbf_file.unwrap();
      let ingest_options = get_ingest_options(&argv);
      let edb_dir = get_dirs(&argv);
      if edb_dir.is_none() {
        print!["{}", usage(&args)];
        std::process::exit(1);
      }
      let mut ingest = Ingest::new(&["scan","ingest"]);
      if argv.contains_key("no-monitor") {
        let scan_table = ingest.scan(&pbf_file).await;
        ingest.ingest(
          open_eyros(&std::path::Path::new(&edb_dir.unwrap())).await?,
          &pbf_file, scan_table, &ingest_options
        ).await;
      } else {
        let mut p = Monitor::open(ingest.progress.clone());
        let scan_table = ingest.scan(&pbf_file).await;
        ingest.ingest(
          open_eyros(&std::path::Path::new(&edb_dir.unwrap())).await?,
          &pbf_file, scan_table, &ingest_options
        ).await;
        p.end().await;
      }
    },
    Some("changeset") => {
      unimplemented![]
    },
    Some(cmd) => {
      eprintln!["unrecognized command {}", cmd];
      std::process::exit(1);
    },
  }
  Ok(())
}

async fn open_eyros(file: &std::path::Path) -> Result<EDB,Error> {
  eyros::Setup::from_path(&std::path::Path::new(&file))
    .build().await
}

fn usage(args: &[String]) -> String {
  format![indoc::indoc![r#"usage: {} COMMAND {{OPTIONS}}

    ingest - scans and processes a pbf
      -f, --pbf     osm pbf file to ingest or "-" for stdin (default)
      -e, --edb     eyros db dir to write spatial data
      -o, --outdir  write eyros db in this dir in edb/

    scan - scans a pbf, outputting a scan file
      -f, --pbf     osm pbf file to ingest or "-" for stdin (default)
      -o, --outdir  write a scan file in this dir
      --scan_file   write scan file with explicit path

    ingest-from-scan - process a pbf from an existing scan
      -f, --pbf     osm pbf file to ingest or "-" for stdin (default)
      -e, --edb     eyros db dir to write spatial data
      -o, --outdir  write eyros db in this dir in edb/ and read scan file
      --scan_file   read scan file with explicit path

    -h, --help     Print this help message
    -v, --version  Print the version string ({})

  "#], args.get(0).unwrap_or(&"???".to_string()), get_version()]
}

fn get_version() -> &'static str {
  const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");
  VERSION.unwrap_or("unknown")
}

fn get_dirs(argv: &argmap::Map) -> Option<String> {
  let outdir = argv.get("outdir").or_else(|| argv.get("o"))
    .and_then(|x| x.first());
  let edb_dir = argv.get("edb").or_else(|| argv.get("e"))
    .and_then(|x| x.first().map(|s| s.clone()))
    .or_else(|| outdir.and_then(|d: &String| {
      let mut p = std::path::PathBuf::from(d);
      p.push("edb");
      p.to_str().map(|s| s.to_string())
    }));
  edb_dir
}

pub struct Monitor {
  stop: Arc<RwLock<bool>>,
}

impl Monitor {
  pub fn open(progress: Arc<RwLock<Progress>>) -> Self {
    let p = progress.clone();
    let stop = Arc::new(RwLock::new(false));
    let s = stop.clone();
    task::spawn(async move {
      let mut interval = stream::interval(std::time::Duration::from_secs(1));
      let mut first = true;
      while let Some(_) = interval.next().await {
        {
          let pr = p.read().await;
          Self::print(&pr, first);
          first = false;
        }
        p.write().await.tick();
        if *s.read().await {
          let pr = p.read().await;
          Self::print(&pr, false);
          break
        }
      }
    });
    Self { stop }
  }
  fn print(p: &Progress, first: bool) {
    let n = p.stages.len();
    if first {
      eprint!["{}", p];
    } else {
      let mut parts = vec!["\x1b[K"];
      for _ in 0..n {
        parts.push("\x1b[1A\x1b[K");
      }
      eprint!["{}{}", parts.join(""), p];
    }
  }
  pub async fn end(&mut self) {
    *self.stop.write().await = true;
  }
}

fn get_ingest_options(argv: &argmap::Map) -> IngestOptions {
  let mut ingest_options = IngestOptions::default();
  let o_channel_size = argv.get("channel_size")
    .or_else(|| argv.get("channel-size"))
    .and_then(|x| x.first())
    .map(|x| x.replace("_","").parse().expect("invalid number for --channel_size"));
  if let Some(channel_size) = o_channel_size {
    ingest_options.channel_size = channel_size;
  }
  let o_way_batch_size = argv.get("way_batch_size")
    .or_else(|| argv.get("way-batch-size"))
    .and_then(|x| x.first())
    .map(|x| x.replace("_","").parse().expect("invalid number for --way_batch_size"));
  if let Some(way_batch_size) = o_way_batch_size {
    ingest_options.way_batch_size = way_batch_size;
  }
  let o_relation_batch_size = argv.get("relation_batch_size")
    .or_else(|| argv.get("relation-batch-size"))
    .and_then(|x| x.first())
    .map(|x| x.replace("_","").parse().expect("invalid number for --relation_batch_size"));
  if let Some(relation_batch_size) = o_relation_batch_size {
    ingest_options.relation_batch_size = relation_batch_size;
  }
  let o_ingest_node = argv.get("no_ingest_node")
    .or_else(|| argv.get("no_ingest_nodes"))
    .or_else(|| argv.get("no-ingest-node"))
    .or_else(|| argv.get("no-ingest-nodes"))
    .map(|x| x.first());
  if o_ingest_node.is_some() {
    ingest_options.ingest_node = false;
  }
  let o_ingest_way = argv.get("no_ingest_way")
    .or_else(|| argv.get("no_ingest_ways"))
    .or_else(|| argv.get("no-ingest-way"))
    .or_else(|| argv.get("no-ingest-ways"))
    .map(|x| x.first());
  if o_ingest_way.is_some() {
    ingest_options.ingest_way = false;
  }
  let o_ingest_relation = argv.get("no_ingest_relation")
    .or_else(|| argv.get("no_ingest_relations"))
    .or_else(|| argv.get("no-ingest-relation"))
    .or_else(|| argv.get("no-ingest-relations"))
    .map(|x| x.first());
  if o_ingest_relation.is_some() {
    ingest_options.ingest_relation = false;
  }
  ingest_options
}
