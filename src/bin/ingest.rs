#![feature(backtrace)]
use peermaps_ingest::{Ingest,EStore,LStore,Phase,EDB};
use rocksdb::{DB,Options};
use async_std::{io,fs::File};

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
    .booleans(&["help","h"])
    .parse(std::env::args());
  if argv.contains_key("help") || argv.contains_key("h") {
    print!["{}", usage(&args)];
    return Ok(());
  }
  if argv.contains_key("version") || argv.contains_key("v") {
    println!["{}", get_version()];
    return Ok(());
  }

  let mut counter: u64 = 0;
  let start_time = std::time::Instant::now();
  let mut phase_time = std::time::Instant::now();
  let mut last_print = std::time::Instant::now();
  let mut last_phase: Option<Phase> = None;
  let mut last_count: u64 = 0;
  let mut rate = None;
  let reporter = Box::new(move |phase: Phase, res| {
    let mut should_print = false;
    if let Err(e) = res {
      eprintln!["\x1b[1K\r[{}] {} error: {}",
        hms(start_time.elapsed().as_secs_f64() as u32), phase.to_string(), e];
      should_print = true;
    } else {
      counter += 1;
      if last_print.elapsed().as_secs_f64() >= 1.0 {
        should_print = true;
        rate = Some((counter-last_count) as f64 / last_print.elapsed().as_secs_f64());
        last_print = std::time::Instant::now();
        last_count = counter;
      }
      if last_phase.as_ref().and_then(|p| Some(p != &phase)).unwrap_or(false) {
        eprintln!["\x1b[1K\r[{}] {} {} ({:.0}/s)", hms(start_time.elapsed().as_secs_f64() as u32),
          last_phase.as_ref().unwrap().to_string(), counter,
          counter as f64/phase_time.elapsed().as_secs_f64()];
        counter = 1;
        rate = None;
        should_print = true;
        phase_time = std::time::Instant::now();
      }
    }
    if should_print {
      let elapsed = start_time.elapsed().as_secs_f64() as u32;
      eprint!["\x1b[1K\r[{}] {} {} ({})", hms(elapsed), phase.to_string(), counter,
        match rate { None => "---".to_string(), Some(x) => format!["{:.0}/s", x] }];
    }
    last_phase = Some(phase);
  });

  match args.get(1).map(|x| x.as_str()) {
    None => print!["{}", usage(&args)],
    Some("help") => print!["{}", usage(&args)],
    Some("version") => print!["{}", get_version()],
    Some("ingest") => {
      let stdin_file = "-".to_string();
      let pbf_file = argv.get("pbf").or_else(|| argv.get("f"))
        .and_then(|x| x.first())
        .unwrap_or(&stdin_file);
      let (ldb_dir, edb_dir) = get_dirs(&argv);
      if ldb_dir.is_none() || edb_dir.is_none() {
        print!["{}", usage(&args)];
        std::process::exit(1);
      }
      let mut ingest = Ingest::new(
        LStore::new(open(std::path::Path::new(&ldb_dir.unwrap()))?),
        EStore::new(open_eyros(&std::path::Path::new(&edb_dir.unwrap())).await?)
      ).reporter(reporter);
      let pbf_stream: Box<dyn std::io::Read+Send> = match pbf_file.as_str() {
        "-" => Box::new(std::io::stdin()),
        x => Box::new(std::fs::File::open(x)?),
      };
      ingest.load_pbf(pbf_stream).await?;
      ingest.process().await;
      eprintln![""];
    },
    Some("pbf") => {
      let stdin_file = "-".to_string();
      let pbf_file = argv.get("pbf").or_else(|| argv.get("f"))
        .and_then(|x| x.first())
        .unwrap_or(&stdin_file);
      let (ldb_dir, edb_dir) = get_dirs(&argv);
      if ldb_dir.is_none() || edb_dir.is_none() {
        eprint!["{}", usage(&args)];
        std::process::exit(1);
      }
      let mut ingest = Ingest::new(
        LStore::new(open(std::path::Path::new(&ldb_dir.unwrap()))?),
        EStore::new(open_eyros(&std::path::Path::new(&edb_dir.unwrap())).await?)
      ).reporter(reporter);
      let pbf_stream: Box<dyn std::io::Read+Send> = match pbf_file.as_str() {
        "-" => Box::new(std::io::stdin()),
        x => Box::new(std::fs::File::open(x)?),
      };
      ingest.load_pbf(pbf_stream).await?;
      ingest.process().await;
      eprintln![""];
    },
    Some("process") => {
      let (ldb_dir, edb_dir) = get_dirs(&argv);
      if ldb_dir.is_none() || edb_dir.is_none() {
        eprint!["{}", usage(&args)];
        std::process::exit(1);
      }
      let mut ingest = Ingest::new(
        LStore::new(open(std::path::Path::new(&ldb_dir.unwrap()))?),
        EStore::new(open_eyros(&std::path::Path::new(&edb_dir.unwrap())).await?)
      ).reporter(reporter);
      ingest.process().await;
      eprintln![""];
    },
    Some("changeset") => {
      let o5c_file = argv.get("o5c").or_else(|| argv.get("f"))
        .and_then(|x| x.first());
      let (ldb_dir, edb_dir) = get_dirs(&argv);
      if o5c_file.is_none() || ldb_dir.is_none() || edb_dir.is_none() {
        eprint!["{}",usage(&args)];
        std::process::exit(1);
      }
      let mut ingest = Ingest::new(
        LStore::new(open(std::path::Path::new(&ldb_dir.unwrap()))?),
        EStore::new(open_eyros(&std::path::Path::new(&edb_dir.unwrap())).await?)
      ).reporter(reporter);
      let o5c_stream: Box<dyn io::Read+Unpin> = match o5c_file.unwrap().as_str() {
        "-" => Box::new(io::stdin()),
        x => Box::new(File::open(x).await?),
      };
      ingest.changeset(o5c_stream).await?;
      eprintln![""];
    },
    Some(cmd) => {
      eprintln!["unrecognized command {}", cmd];
      std::process::exit(1);
    },
  }
  Ok(())
}

fn open(path: &std::path::Path) -> Result<DB,Error> {
  let mut options = Options::default();
  options.create_if_missing(true);
  options.set_compression_type(rocksdb::DBCompressionType::Snappy);
  DB::open(&options, path).map_err(|e| e.into())
}

async fn open_eyros(file: &std::path::Path) -> Result<EDB,Error> {
  eyros::Setup::from_path(&std::path::Path::new(&file))
    .build().await
}

fn usage(args: &[String]) -> String {
  format![indoc::indoc![r#"usage: {} COMMAND {{OPTIONS}}

    ingest - runs pbf and process phases
      -f, --pbf     osm pbf file to ingest or "-" for stdin (default)
      -l, --ldb     level db dir to write normalized data
      -e, --edb     eyros db dir to write spatial data
      -o, --outdir  write level and eyros db in this dir in ldb/ and edb/

    pbf - parse pbf and write normalized data to level db
      -f, --pbf     osm pbf file to ingest or "-" for stdin (default)
      -l, --ldb     level db dir to write normalized data
      -e, --edb     eyros db dir to write spatial data
      -o, --outdir  write level and eyros db in this dir in ldb/ and edb/

    process - write georender-pack data to eyros db from populated level db
      -l, --ldb     level db dir to write normalized data
      -e, --edb     eyros db dir to write spatial data
      -o, --outdir  write level and eyros db in this dir in ldb/ and edb/

    changeset - ingest data from an o5c changeset
      -f, --o5c     o5c changeset file or "-" for stdin (default)
      -l, --ldb     level db dir to write normalized data
      -e, --edb     eyros db dir to write spatial data
      -o, --outdir  write level and eyros db in this dir in ldb/ and edb/

    -h, --help     Print this help message
    -v, --version  Print the version string ({})

  "#], args.get(0).unwrap_or(&"???".to_string()), get_version()]
}

fn get_version() -> &'static str {
  const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");
  VERSION.unwrap_or("unknown")
}

fn get_dirs(argv: &argmap::Map) -> (Option<String>,Option<String>) {
  let outdir = argv.get("outdir").or_else(|| argv.get("o"))
    .and_then(|x| x.first());
  let ldb_dir = argv.get("ldb").or_else(|| argv.get("l"))
    .and_then(|x| x.first().map(|s| s.clone()))
    .or_else(|| outdir.and_then(|d: &String| {
      let mut p = std::path::PathBuf::from(d);
      p.push("ldb");
      p.to_str().map(|s| s.to_string())
    }));
  let edb_dir = argv.get("edb").or_else(|| argv.get("e"))
    .and_then(|x| x.first().map(|s| s.clone()))
    .or_else(|| outdir.and_then(|d: &String| {
      let mut p = std::path::PathBuf::from(d);
      p.push("edb");
      p.to_str().map(|s| s.to_string())
    }));
  (ldb_dir,edb_dir)
}

fn hms(t: u32) -> String {
  let s = t % 60;
  let m = (t / 60) % 60;
  let h = t / 3600;
  format!["{:02}:{:02}:{:02}", h, m, s]
}
