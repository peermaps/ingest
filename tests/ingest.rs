use peermaps_ingest::{Ingest,Key,EStore,LStore};
use leveldb::{database::Database,options::Options};
use async_std::{prelude::*,fs::File};
use tempfile::Builder as Tmpfile;
use eyros::Coord as C;
use georender_pack::{Feature,Point,Area};

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

  {
    let mut estore = ingest.estore.lock().await;
    let mut stream = estore.db.query(&((3.0,-15.0),(15.0,45.0))).await?;
    let mut results = vec![];
    while let Some(result) = stream.next().await {
      let (pt,v) = result?;
      results.push((pt,georender_pack::decode(&v.data)?));
    }
    results.sort_by(|a,b| {
      let a_id = get_id(&a.1);
      let b_id = get_id(&b.1);
      fn get_id(x: &georender_pack::Feature) -> u64 {
        match x {
          georender_pack::Feature::Point(x) => x.id,
          georender_pack::Feature::Line(x) => x.id,
          georender_pack::Feature::Area(x) => x.id,
        }
      }
      match (a_id < b_id, a_id > b_id) {
        (true,_) => std::cmp::Ordering::Less,
        (_,true) => std::cmp::Ordering::Greater,
        (false,false) => std::cmp::Ordering::Equal,
      }
    });
    assert_eq![
      results,
      vec![
        ((C::Interval(13.00,13.02),C::Interval(37.00,37.01)), Feature::Area(Area {
          id: 555*3+1,
          feature_type: get_type("leisure.park"),
          positions: vec![ 13.00,37.00, 13.01,37.01, 13.02,37.00 ],
          cells: vec![1,0,2],
          labels: "\x0e=triangle park\x00".as_bytes().to_vec(),
        })),
        ((C::Interval(5.000,5.010),C::Interval(-10.010,-10.000)), Feature::Area(Area {
          id: 700*3+2,
          feature_type: get_type("natural.water"),
          positions: vec![
            5.000, -10.000, 5.000, -10.010, 5.010, -10.010, 5.010, -10.000,
            5.005, -10.003, 5.006, -10.004, 5.007, -10.003,
          ],
          cells: vec![0,1,4,5,4,1,3,0,4,6,5,1,3,4,6,6,1,2,2,3,6],
          labels: "\x0a=cool lake\x00".as_bytes().to_vec(),
        })),
        ((C::Scalar(13.02),C::Scalar(37.00)), Feature::Point(Point {
          id: 1312*3+0,
          feature_type: get_type("amenity.cafe"),
          point: (13.02,37.00),
          labels: vec![0],
        })),
        ((C::Scalar(13.03),C::Scalar(37.03)), Feature::Point(Point {
          id: 2000*3+0,
          feature_type: get_type("amenity.bus_station"),
          point: (13.03,37.03),
          labels: vec![0],
        })),
      ]
    ];
  }

  eprintln!["changeset"];
  ingest.changeset(Box::new(File::open(&o5c_file).await?)).await?;

  {
    let mut estore = ingest.estore.lock().await;
    let mut stream = estore.db.query(&((3.0,-15.0),(15.0,45.0))).await?;
    let mut results = vec![];
    while let Some(result) = stream.next().await {
      let (pt,v) = result?;
      results.push((pt,georender_pack::decode(&v.data)?));
    }
    results.sort_by(|a,b| {
      let a_id = get_id(&a.1);
      let b_id = get_id(&b.1);
      fn get_id(x: &georender_pack::Feature) -> u64 {
        match x {
          georender_pack::Feature::Point(x) => x.id,
          georender_pack::Feature::Line(x) => x.id,
          georender_pack::Feature::Area(x) => x.id,
        }
      }
      match (a_id < b_id, a_id > b_id) {
        (true,_) => std::cmp::Ordering::Less,
        (_,true) => std::cmp::Ordering::Greater,
        (false,false) => std::cmp::Ordering::Equal,
      }
    });
    assert_eq![
      results,
      vec![
        ((C::Interval(5.000,5.010),C::Interval(-10.010,-10.000)), Feature::Area(Area {
          id: 700*3+2,
          feature_type: get_type("natural.water"),
          positions: vec![
            4.999,  -9.999, 5.000, -10.010, 5.010, -10.010, 5.001, -10.001,
            5.005, -10.003, 5.006, -10.004, 5.007, -10.003,
          ],
          cells: vec![0,1,4,5,4,1,3,0,4,6,5,1,3,4,6,6,1,2,2,3,6],
          labels: "\x0a=cool lake\x00".as_bytes().to_vec(),
        })),
        ((C::Scalar(13.02),C::Scalar(37.00)), Feature::Point(Point {
          id: 1312*3+0,
          feature_type: get_type("amenity.cafe"),
          point: (13.02,37.00),
          labels: vec![0],
        })),
        ((C::Scalar(13.03),C::Scalar(37.04)), Feature::Point(Point {
          id: 2000*3+0,
          feature_type: get_type("highway.bus_stop"),
          point: (13.03,37.04),
          labels: vec![0],
        })),
      ]
    ];
  }

  Ok(())
}

fn open(path: &std::path::Path) -> Result<Database<Key>,Error> {
  let mut options = Options::new();
  options.create_if_missing = true;
  options.compression = leveldb_sys::Compression::Snappy;
  Database::open(path, options).map_err(|e| e.into())
}

fn get_type(key: &str) -> u64 {
  *georender_pack::osm_types::get_types().get(key).unwrap()
}
