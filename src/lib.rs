#![warn(clippy::future_not_send)]
#![feature(async_closure,backtrace)]
pub mod encoder;
pub use encoder::*;
pub mod error;
pub use error::*;
mod record;
use osmxq::{XQ,RW,Record};
mod value;
mod progress;
pub use progress::Progress;

pub const BACKREF_PREFIX: u8 = 1;
pub const REF_PREFIX: u8 = 2;

use std::collections::HashMap;
use async_std::{sync::{Arc,Mutex},task,channel};
use futures::future::join_all;

type NodeDeps = HashMap<u64,(f32,f32)>;
type WayDeps = HashMap<u64,Vec<u64>>;

type R = Decoded;
type T = eyros::Tree2<f32,f32,V>;
type P = (eyros::Coord<f32>,eyros::Coord<f32>);
type V = value::V;
pub type EDB = eyros::DB<random_access_disk::RandomAccessDisk,T,P,V>;

#[derive(Debug,Clone,PartialEq)]
pub enum Phase {
  Pbf(),
  Process(),
  Changeset(),
}

impl ToString for Phase {
  fn to_string(&self) -> String {
    match self {
      Phase::Pbf() => "pbf",
      Phase::Process() => "process",
      Phase::Changeset() => "changeset",
    }.to_string()
  }
}

type Reporter = Arc<Mutex<Option<Box<dyn FnMut(Phase, Result<(),Error>) -> ()+Send+Sync>>>>;
pub struct Ingest<S> where S: osmxq::RW {
  xq: Arc<Mutex<XQ<S,R>>>,
  db: EDB,
  place_other: u64,
  reporter: Reporter,
}

impl<S> Ingest<S> where S: osmxq::RW+'static {
  pub fn new(xq: XQ<S,R>, db: EDB) -> Self {
    Self {
      xq: Arc::new(Mutex::new(xq)),
      db,
      place_other: *georender_pack::osm_types::get_types().get("place.other").unwrap(),
      reporter: Arc::new(Mutex::new(None)),
    }
  }

  pub fn reporter(mut self, f: Box<dyn FnMut(Phase, Result<(),Error>) -> ()+Send+Sync>) -> Self {
    self.reporter = Arc::new(Mutex::new(Some(f)));
    self
  }

  pub async fn load_pbf<R: std::io::Read+Send+'static>(&mut self, pbf: R) -> Result<(),Error> {
    let (sender,receiver) = channel::bounded::<Decoded>(1_000);
    let mut work = vec![];
    work.push(task::spawn(async move {
      let sc = sender.clone();
      osmpbf::ElementReader::new(pbf).for_each(move |element| {
        let r = Decoded::from_pbf_element(&element).unwrap();
        let s = sc.clone();
        task::block_on(async move {
          s.send(r).await.unwrap();
        });
      }).unwrap();
      sender.close();
    }));
    {
      async fn add<S>(xqc: Arc<Mutex<XQ<S,Decoded>>>, records: &[Decoded], reporter: Reporter) where S: RW {
        let mut xq = xqc.lock().await;
        if let Some(f) = reporter.lock().await.as_mut() {
          if let Err(err) = xq.add_records(&records).await {
            f(Phase::Pbf(), Err(err.into()));
          } else {
            f(Phase::Pbf(), Ok(()));
          }
        } else {
          if let Err(_) = xq.add_records(&records).await {}
        }
      }
      let xqc = self.xq.clone();
      let reporter = self.reporter.clone();
      work.push(task::spawn(async move {
        let mut records = Vec::with_capacity(100_000);
        while let Ok(record) = receiver.recv().await {
          records.push(record);
          if records.len() >= 100_000 {
            add(xqc.clone(), &records, reporter.clone()).await;
            records.clear();
          }
        }
        if !records.is_empty() {
          add(xqc.clone(), &records, reporter.clone()).await;
        }
        {
          let mut xq = xqc.lock().await;
          xq.finish().await.unwrap();
          xq.flush().await.unwrap();
        }
      }));
    }
    join_all(work).await;
    Ok(())
  }

  // loop over the db, denormalize the records, georender-pack the data into eyros
  pub async fn process(&mut self) -> () {
    let mut xq = self.xq.lock().await;
    let quad_ids = xq.get_quad_ids();
    for q_id in quad_ids {
      let records = xq.read_quad_denorm(q_id).await.unwrap();
      let mut batch = Vec::with_capacity(records.len());
      for (_r_id,r,deps) in records {
        match &r {
          Decoded::Node(node) => {
            if node.feature_type == self.place_other { continue }
            let encoded = georender_pack::encode::node_from_parsed(
              node.id*3+0, (node.lon,node.lat), node.feature_type, &node.labels
            ).unwrap();
            if encoded.is_empty() { continue }
            batch.push(eyros::Row::Insert(
              (eyros::Coord::Scalar(node.lon),eyros::Coord::Scalar(node.lat)),
              encoded.into()
            ));
          },
          Decoded::Way(way) => {
            if way.feature_type == self.place_other { continue }
            let mut pdeps = HashMap::new();
            for d in deps {
              if let Some(p) = d.get_position() {
                pdeps.insert(d.get_id()/3, p);
              }
            }
            let mut bbox = (f32::INFINITY,f32::INFINITY,f32::NEG_INFINITY,f32::NEG_INFINITY);
            for (lon,lat) in pdeps.values() {
              bbox.0 = bbox.0.min(*lon);
              bbox.1 = bbox.1.min(*lat);
              bbox.2 = bbox.2.max(*lon);
              bbox.3 = bbox.3.max(*lat);
            }
            let encoded = georender_pack::encode::way_from_parsed(
              way.id*3+1, way.feature_type, way.is_area, &way.labels, &way.refs, &pdeps
            ).unwrap();
            if encoded.is_empty() { continue }
            let point = (
              eyros::Coord::Interval(bbox.0,bbox.2),
              eyros::Coord::Interval(bbox.1,bbox.3),
            );
            batch.push(eyros::Row::Insert(point, encoded.into()));
          },
          Decoded::Relation(relation) => {
            if relation.feature_type == self.place_other { continue }
            let mut node_deps: NodeDeps = HashMap::new();
            let mut way_deps: WayDeps = HashMap::new();

            for d in deps {
              if let Some(p) = d.get_position() {
                node_deps.insert(d.get_id(), p);
                continue;
              }
              let drefs = d.get_refs();
              if drefs.is_empty() { continue }
              way_deps.insert(d.get_id(), drefs.to_vec());
            }
            let mut bbox = (f32::INFINITY,f32::INFINITY,f32::NEG_INFINITY,f32::NEG_INFINITY);
            for p in node_deps.values() {
              bbox.0 = bbox.0.min(p.0);
              bbox.1 = bbox.1.min(p.1);
              bbox.2 = bbox.2.max(p.0);
              bbox.3 = bbox.3.max(p.1);
            }
            let members = relation.members.iter().map(|m| {
              georender_pack::Member::new(
                m/2,
                match m%2 {
                  0 => georender_pack::MemberRole::Outer(),
                  _ => georender_pack::MemberRole::Inner(),
                },
                georender_pack::MemberType::Way()
              )
            }).collect::<Vec<_>>();
            let encoded = georender_pack::encode::relation_from_parsed(
              relation.id*3+2, relation.feature_type, relation.is_area,
              &relation.labels, &members, &node_deps, &way_deps
            ).unwrap();
            let point = (
              eyros::Coord::Interval(bbox.0,bbox.2),
              eyros::Coord::Interval(bbox.1,bbox.3),
            );
            batch.push(eyros::Row::Insert(point, encoded.into()));
          },
        }
      }
      self.db.batch(&batch).await.unwrap();
    }
    self.db.sync().await.unwrap();
  }
}
