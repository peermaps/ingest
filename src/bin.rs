#![feature(backtrace)]
use peermaps_ingest::{Ingest,EDB,Progress};
use async_std::{prelude::*,sync::{Arc,RwLock},task,stream};

type Error = Box<dyn std::error::Error+Send+Sync>;

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
    Some("ingest") => {
      let o_pbf_file = argv.get("pbf").or_else(|| argv.get("f"))
        .and_then(|x| x.first());
      if o_pbf_file.is_none() {
        println!["--pbf or -f option required\n"];
        print!["{}", usage(&args)];
        std::process::exit(1);
      }
      let pbf_file = o_pbf_file.unwrap();
      let channel_size: usize = argv.get("channel_size")
        .or_else(|| argv.get("channel-size"))
        .and_then(|x| x.first())
        .map(|x| x.replace("_","").parse().expect("invalid number for --channel_size"))
        .unwrap_or(10_000);
      let way_batch_size: usize = argv.get("way_batch_size")
        .or_else(|| argv.get("way-batch-size"))
        .and_then(|x| x.first())
        .map(|x| x.replace("_","").parse().expect("invalid number for --way_batch_size"))
        .unwrap_or(10_000_000);
      let relation_batch_size = argv.get("relation_batch_size")
        .or_else(|| argv.get("relation-batch-size"))
        .and_then(|x| x.first())
        .map(|x| x.replace("_","").parse().expect("invalid number for --relation_batch_size"))
        .unwrap_or(500_000);
      let edb_dir = get_dirs(&argv);
      if edb_dir.is_none() {
        print!["{}", usage(&args)];
        std::process::exit(1);
      }
      let mut ingest = Ingest::new(
        open_eyros(&std::path::Path::new(&edb_dir.unwrap())).await?,
        &["ingest"]
      );
      if argv.contains_key("no-monitor") {
        ingest.ingest(&pbf_file, channel_size, way_batch_size, relation_batch_size).await;
      } else {
        let mut p = Monitor::open(ingest.progress.clone());
        ingest.ingest(&pbf_file, channel_size, way_batch_size, relation_batch_size).await;
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

    ingest - runs pbf and process phases
      -f, --pbf     osm pbf file to ingest or "-" for stdin (default)
      -e, --edb     eyros db dir to write spatial data
      -o, --outdir  write level and eyros db in this dir in xq/ and edb/

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
