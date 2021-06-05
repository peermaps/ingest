use peermaps_ingest::{Ingest,Key,EStore,LStore};
use leveldb::{database::Database,options::Options};
use async_std::{prelude::*,fs::File};
use tempfile::Builder as Tmpfile;
use eyros::{Coord as C};
use georender_pack::{Feature,Point,Line,Area};
use pretty_assertions::assert_eq;

type Error = Box<dyn std::error::Error+Send+Sync>;

#[async_std::test]
async fn ingest() -> Result<(),Error> {
  let dir = Tmpfile::new().prefix("peermaps-ingest").tempdir()?;
  let mut ldb_dir = std::path::PathBuf::from(&dir.path());
  ldb_dir.push("ldb");
  let mut edb_dir = std::path::PathBuf::from(&dir.path());
  edb_dir.push("edb");

  let mut pbf_file = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
  pbf_file.push("tests/data/0/ingest.pbf");

  let mut ingest = Ingest::new(
    LStore::new(open(std::path::Path::new(&ldb_dir))?),
    EStore::new(eyros::open_from_path2(&std::path::Path::new(&edb_dir)).await?)
  );
  ingest.load_pbf(std::fs::File::open(&pbf_file)?).await?;
  ingest.process().await;
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
    let ex_positions = vec![ 13.00,37.00, 13.01,37.01, 13.02,37.00 ];
    let ex_cells = earcutr::earcut(&ex_positions.iter()
      .map(|p| *p as f64).collect(), &vec![], 2);
    assert_eq![
      results,
      vec![
        ((C::Interval(13.00,13.02),C::Interval(37.00,37.01)), Feature::Area(Area {
          id: 555*3+1,
          feature_type: get_type("leisure.park"),
          positions: ex_positions,
          cells: ex_cells,
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

  {
    let mut o5c_file = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    o5c_file.push("tests/data/0/changeset0.o5c");
    ingest.changeset(Box::new(File::open(&o5c_file).await?)).await?;

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
    let ex_positions = vec![
      4.999,  -9.999, 5.000, -10.010, 5.010, -10.010, 5.001, -10.001,
      5.005, -10.003, 5.006, -10.004, 5.007, -10.003,
    ];
    let ex_cells = earcutr::earcut(&ex_positions.iter()
      .map(|p| *p as f64).collect(), &vec![4], 2);
    assert_eq![
      results,
      vec![
        ((C::Interval(4.999,5.010),C::Interval(-10.010,-9.999)), Feature::Area(Area {
          id: 700*3+2,
          feature_type: get_type("natural.water"),
          positions: ex_positions,
          cells: ex_cells, // vec![0, 4, 6, 1, 2, 3, 1, 3, 0, 5, 4, 0],
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

  {
    let mut o5c_file = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    o5c_file.push("tests/data/0/changeset1.o5c");
    ingest.changeset(Box::new(File::open(&o5c_file).await?)).await?;

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
    let w_ex_positions = vec![
      5.004, -10.006, 5.005, -10.006, 5.005, -10.007, 5.004, -10.007,
    ];
    let w_ex_cells = earcutr::earcut(&w_ex_positions.iter()
      .map(|p| *p as f64).collect(), &vec![], 2);
    let r_ex_positions = vec![
      4.999,  -9.999, 5.000, -10.010, 5.010, -10.010, 5.001, -10.001,
      5.005, -10.003, 5.006, -10.004, 5.007, -10.003,
      5.004, -10.006, 5.005, -10.006, 5.005, -10.007, 5.004, -10.007,
    ];
    let r_ex_cells = earcutr::earcut(&r_ex_positions.iter()
      .map(|p| *p as f64).collect(), &vec![4,7], 2);
    assert_eq![
      results,
      vec![
        ((C::Interval(5.004,5.005),C::Interval(-10.007,-10.006)), Feature::Area(Area {
          id: 602*3+1,
          feature_type: get_type("place.island"),
          positions: w_ex_positions,
          cells: w_ex_cells,
          labels: vec![0],
        })),
        ((C::Interval(4.999,5.010),C::Interval(-10.010,-9.999)), Feature::Area(Area {
          id: 700*3+2,
          feature_type: get_type("natural.water"),
          positions: r_ex_positions,
          cells: r_ex_cells,
          labels: "\x0a=Cool Lake\x00".as_bytes().to_vec(),
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

  {
    let mut o5c_file = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    o5c_file.push("tests/data/0/changeset2.o5c");
    ingest.changeset(Box::new(File::open(&o5c_file).await?)).await?;

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
    let w_ex_positions = vec![
      5.004, -10.006, 5.005, -10.006, 5.005, -10.007, 5.003, -10.008,
    ];
    let w_ex_cells = earcutr::earcut(&w_ex_positions.iter()
      .map(|p| *p as f64).collect(), &vec![], 2);
    let r_ex_positions = vec![
      4.999,  -9.999, 5.000, -10.010, 5.010, -10.010, 5.001, -10.001,
      5.005, -10.003, 5.006, -10.004, 5.007, -10.003,
      5.004, -10.006, 5.005, -10.006, 5.005, -10.007, 5.003, -10.008,
    ];
    let r_ex_cells = earcutr::earcut(&r_ex_positions.iter()
      .map(|p| *p as f64).collect(), &vec![4,7], 2);
    assert_eq![
      results,
      vec![
        ((C::Interval(5.003,5.005),C::Interval(-10.008,-10.006)), Feature::Area(Area {
          id: 602*3+1,
          feature_type: get_type("place.island"),
          positions: w_ex_positions,
          cells: w_ex_cells,
          labels: vec![0],
        })),
        ((C::Interval(4.999,5.010),C::Interval(-10.010,-9.999)), Feature::Area(Area {
          id: 700*3+2,
          feature_type: get_type("natural.water"),
          positions: r_ex_positions,
          cells: r_ex_cells,
          labels: "\x0a=Cool Lake\x00".as_bytes().to_vec(),
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
        ((C::Scalar(5.003),C::Scalar(-10.008)), Feature::Point(Point {
          id: 9104*3+0,
          feature_type: get_type("amenity.boat_rental"),
          point: (5.003,-10.008),
          labels: vec![0],
        })),
      ]
    ];
  }

  {
    let mut o5c_file = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    o5c_file.push("tests/data/0/changeset3.o5c");
    ingest.changeset(Box::new(File::open(&o5c_file).await?)).await?;

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
    let w_ex_positions = vec![
      5.004, -10.006, 5.005, -10.006, 5.005, -10.007, 5.003, -10.008,
    ];
    let w_ex_cells = earcutr::earcut(&w_ex_positions.iter()
      .map(|p| *p as f64).collect(), &vec![], 2);
    let r_ex_positions = vec![
      4.999,  -9.999, 5.000, -10.010, 5.010, -10.010, 5.001, -10.001,
      5.005, -10.003, 5.006, -10.004, 5.007, -10.003,
      5.004, -10.006, 5.005, -10.006, 5.005, -10.007, 5.003, -10.008,
    ];
    let r_ex_cells = earcutr::earcut(&r_ex_positions.iter()
      .map(|p| *p as f64).collect(), &vec![4,7], 2);
    let c_ex_positions = vec![
      7.000, 15.020, 7.000, 15.000, 7.010, 15.010,
      7.002, 15.002, 7.002, 15.018, 7.008, 15.010,
    ];
    let c_ex_cells = earcutr::earcut(&c_ex_positions.iter()
      .map(|p| *p as f64).collect(), &vec![3], 2);
    assert_eq![
      results,
      vec![
        ((C::Interval(5.003,5.005),C::Interval(-10.008,-10.006)), Feature::Area(Area {
          id: 602*3+1,
          feature_type: get_type("place.island"),
          positions: w_ex_positions,
          cells: w_ex_cells,
          labels: vec![0],
        })),
        ((C::Interval(4.999,5.010),C::Interval(-10.010,-9.999)), Feature::Area(Area {
          id: 700*3+2,
          feature_type: get_type("natural.water"),
          positions: r_ex_positions,
          cells: r_ex_cells,
          labels: "\x0a=Cool Lake\x00".as_bytes().to_vec(),
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
        ((C::Interval(6.998,7.012),C::Interval(14.998,15.022)), Feature::Line(Line {
          id: 4003*3+1,
          feature_type: get_type("historic.castle_wall"),
          positions: vec![ 6.998, 14.998, 7.012, 15.010, 6.998, 15.022 ],
          labels: vec![0],
        })),
        ((C::Interval(7.000,7.010),C::Interval(15.000,15.020)), Feature::Area(Area {
          id: 4004*3+2,
          feature_type: get_type("historic.castle"),
          positions: c_ex_positions,
          cells: c_ex_cells,
          labels: vec![0],
        })),
        ((C::Scalar(5.003),C::Scalar(-10.008)), Feature::Point(Point {
          id: 9104*3+0,
          feature_type: get_type("amenity.boat_rental"),
          point: (5.003,-10.008),
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
