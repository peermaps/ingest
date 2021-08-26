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
      let stdin_file = "-".to_string();
      let pbf_file = argv.get("pbf").or_else(|| argv.get("f"))
        .and_then(|x| x.first())
        .unwrap_or(&stdin_file);
      let edb_dir = get_dirs(&argv);
      if edb_dir.is_none() {
        print!["{}", usage(&args)];
        std::process::exit(1);
      }
      let mut ingest = Ingest::new(
        open_eyros(&std::path::Path::new(&edb_dir.unwrap())).await?,
        std::fs::File::open(pbf_file)?,
        &["pbf","process"]
      );
      if argv.contains_key("no-monitor") {
        ingest.process().await;
      } else {
        let mut p = Monitor::open(ingest.progress.clone());
        ingest.process().await;
        p.end().await;
      }
    },
    Some("changeset") => {
      unimplemented![]
      /*
      let o5c_file = argv.get("o5c").or_else(|| argv.get("f"))
        .and_then(|x| x.first());
      let (xq_dir, edb_dir) = get_dirs(&argv);
      if o5c_file.is_none() || xq_dir.is_none() || edb_dir.is_none() {
        eprint!["{}",usage(&args)];
        std::process::exit(1);
      }
      let mut ingest = Ingest::new(
        XQ::from_fields(
          Box::new(osmxq::FileStorage::open_from_path(&xq_dir.unwrap()).await?),
          get_fields(&argv)
        ).await?,
        open_eyros(&std::path::Path::new(&edb_dir.unwrap())).await?
      );
      let o5c_stream: Box<dyn io::Read+Send+Unpin> = match o5c_file.unwrap().as_str() {
        "-" => Box::new(io::stdin()),
        x => Box::new(File::open(x).await?),
      };
      ingest.changeset(o5c_stream).await?;
      eprintln![""];
      */
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
      -x, --xq      osmxq dir to write normalized quad data
      -e, --edb     eyros db dir to write spatial data
      -o, --outdir  write level and eyros db in this dir in xq/ and edb/

      Consult --defaults for additional arguments.

    pbf - parse pbf and write normalized data to level db
      -f, --pbf     osm pbf file to ingest or "-" for stdin (default)
      -x, --xq      osmxq dir to write normalized quad data
      -e, --edb     eyros db dir to write spatial data
      -o, --outdir  write level and eyros db in this dir in xq/ and edb/

      Consult --defaults for additional arguments.

    process - write georender-pack data to eyros db from populated level db
      -x, --xq      osmxq dir to write normalized quad data
      -e, --edb     eyros db dir to write spatial data
      -o, --outdir  write level and eyros db in this dir in xq/ and edb/

      Consult --defaults for additional arguments.

    changeset - ingest data from an o5c changeset
      -f, --o5c     o5c changeset file or "-" for stdin (default)
      -x, --xq      osmxq dir to write normalized quad data
      -e, --edb     eyros db dir to write spatial data
      -o, --outdir  write level and eyros db in this dir in xq/ and edb/

    -h, --help     Print this help message
    -v, --version  Print the version string ({})
    -d, --defaults Print default osmxq field values.

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
