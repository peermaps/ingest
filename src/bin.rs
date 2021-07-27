#![feature(backtrace)]
use peermaps_ingest::{Ingest,EDB,Progress};
use async_std::{prelude::*,sync::{Arc,RwLock},task,stream};
use osmxq::XQ;

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
    .booleans(&["help","h","defaults","d"])
    .parse(std::env::args());
  if argv.contains_key("help") || argv.contains_key("h") {
    print!["{}", usage(&args)];
    return Ok(());
  }
  if argv.contains_key("version") || argv.contains_key("v") {
    println!["{}", get_version()];
    return Ok(());
  }
  if argv.contains_key("defaults") || argv.contains_key("d") {
    show_defaults();
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
      let (xq_dir, edb_dir) = get_dirs(&argv);
      if xq_dir.is_none() || edb_dir.is_none() {
        print!["{}", usage(&args)];
        std::process::exit(1);
      }
      let mut ingest = Ingest::new(
        XQ::from_fields(
          Box::new(osmxq::FileStorage::open_from_path(&xq_dir.unwrap()).await?),
          get_fields(&argv)
        ).await?,
        open_eyros(&std::path::Path::new(&edb_dir.unwrap())).await?,
        &["pbf","process"]
      );
      let pbf_stream: Box<dyn std::io::Read+Send> = match pbf_file.as_str() {
        "-" => Box::new(std::io::stdin()),
        x => Box::new(std::fs::File::open(x)?),
      };
      let mut p = Monitor::open(ingest.progress.clone());
      ingest.load_pbf(pbf_stream).await?;
      ingest.process().await;
      p.end().await;
    },
    Some("pbf") => {
      let stdin_file = "-".to_string();
      let pbf_file = argv.get("pbf").or_else(|| argv.get("f"))
        .and_then(|x| x.first())
        .unwrap_or(&stdin_file);
      let (xq_dir, edb_dir) = get_dirs(&argv);
      if xq_dir.is_none() || edb_dir.is_none() {
        eprint!["{}", usage(&args)];
        std::process::exit(1);
      }
      let mut ingest = Ingest::new(
        XQ::from_fields(
          Box::new(osmxq::FileStorage::open_from_path(&xq_dir.unwrap()).await?),
          get_fields(&argv)
        ).await?,
        open_eyros(&std::path::Path::new(&edb_dir.unwrap())).await?,
        &["pbf"]
      );
      let pbf_stream: Box<dyn std::io::Read+Send> = match pbf_file.as_str() {
        "-" => Box::new(std::io::stdin()),
        x => Box::new(std::fs::File::open(x)?),
      };
      let mut p = Monitor::open(ingest.progress.clone());
      ingest.load_pbf(pbf_stream).await?;
      p.end().await;
    },
    Some("process") => {
      let (xq_dir, edb_dir) = get_dirs(&argv);
      if xq_dir.is_none() || edb_dir.is_none() {
        eprint!["{}", usage(&args)];
        std::process::exit(1);
      }
      let mut ingest = Ingest::new(
        XQ::from_fields(
          Box::new(osmxq::FileStorage::open_from_path(&xq_dir.unwrap()).await?),
          get_fields(&argv)
        ).await?,
        open_eyros(&std::path::Path::new(&edb_dir.unwrap())).await?,
        &["process"]
      );
      let mut p = Monitor::open(ingest.progress.clone());
      ingest.process().await;
      p.end().await;
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

fn get_dirs(argv: &argmap::Map) -> (Option<String>,Option<String>) {
  let outdir = argv.get("outdir").or_else(|| argv.get("o"))
    .and_then(|x| x.first());
  let xq_dir = argv.get("xq").or_else(|| argv.get("x"))
    .and_then(|x| x.first().map(|s| s.clone()))
    .or_else(|| outdir.and_then(|d: &String| {
      let mut p = std::path::PathBuf::from(d);
      p.push("xq");
      p.to_str().map(|s| s.to_string())
    }));
  let edb_dir = argv.get("edb").or_else(|| argv.get("e"))
    .and_then(|x| x.first().map(|s| s.clone()))
    .or_else(|| outdir.and_then(|d: &String| {
      let mut p = std::path::PathBuf::from(d);
      p.push("edb");
      p.to_str().map(|s| s.to_string())
    }));
  (xq_dir,edb_dir)
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

fn get_fields(argv: &argmap::Map) -> osmxq::Fields {
  fn parse<T: std::str::FromStr>(x: &str) -> T where <T as std::str::FromStr>::Err: std::fmt::Debug {
    x.replace("_","").parse().unwrap()
  }
  fn get<'a>(argv: &'a argmap::Map, x: &str) -> Option<&'a String> {
    argv.get(x)
      .or_else(|| argv.get(&x.replace("-","_")))
      .and_then(|xs| xs.first())
  }
  let mut fields = osmxq::Fields::default();
  if let Some(x) = get(argv, "id_block_size") {
    fields.id_block_size = parse(x);
  }
  if let Some(x) = get(argv, "id_cache_size") {
    fields.id_cache_size = parse(x);
  }
  if let Some(x) = get(argv, "id_flush_size") {
    fields.id_flush_size = parse(x);
  }
  if let Some(x) = get(argv, "id_flush_top") {
    fields.id_flush_top = parse(x);
  }
  if let Some(x) = get(argv, "id_flush_max_age") {
    fields.id_flush_max_age = parse(x);
  }
  if let Some(x) = get(argv, "record_cache_size") {
    fields.record_cache_size = parse(x);
  }
  if let Some(x) = get(argv, "quad_block_size") {
    fields.quad_block_size = parse(x);
  }
  if let Some(x) = get(argv, "quad_flush_size") {
    fields.quad_flush_size = parse(x);
  }
  if let Some(x) = get(argv, "quad_flush_top") {
    fields.quad_flush_top = parse(x);
  }
  if let Some(x) = get(argv, "quad_flush_max_age") {
    fields.quad_flush_max_age = parse(x);
  }
  if let Some(x) = get(argv, "missing_flush_size") {
    fields.missing_flush_size = parse(x);
  }
  fields
}

fn show_defaults() {
  let fields = osmxq::Fields::default();
  println!["--id_block_size={}", fields.id_block_size];
  println!["--id_cache_size={}", fields.id_cache_size];
  println!["--id_flush_size={}", fields.id_flush_size];
  println!["--id_flush_top={}", fields.id_flush_top];
  println!["--id_flush_max_age={}", fields.id_flush_max_age];
  println!["--record_cache_size={}", fields.record_cache_size];
  println!["--quad_block_size={}", fields.quad_block_size];
  println!["--quad_flush_size={}", fields.quad_flush_size];
  println!["--quad_flush_top={}", fields.quad_flush_top];
  println!["--quad_flush_max_age={}", fields.quad_flush_max_age];
  println!["--missing_flush_size={}", fields.missing_flush_size];
}
