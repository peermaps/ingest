#![warn(clippy::future_not_send)]
#![feature(async_closure,backtrace,available_concurrency)]
pub mod encoder;
pub use encoder::*;
pub mod error;
pub use error::*;
mod value;
mod progress;
pub mod denorm;
pub use progress::Progress;
use osmpbf_parser::{Parser,Scan};

pub const BACKREF_PREFIX: u8 = 1;
pub const REF_PREFIX: u8 = 2;

use std::collections::HashMap;
use async_std::{sync::{Arc,Mutex,RwLock},task,channel};
use futures::future::join_all;

type T = eyros::Tree2<f32,f32,V>;
type P = (eyros::Coord<f32>,eyros::Coord<f32>);
type V = value::V;
pub type EDB = eyros::DB<random_access_disk::RandomAccessDisk,T,P,V>;

pub struct Ingest {
  db: Arc<Mutex<EDB>>,
  place_other: u64,
  pub progress: Arc<RwLock<Progress>>,
}

impl Ingest {
  pub fn new(db: EDB, stages: &[&str]) -> Self {
    Self {
      db: Arc::new(Mutex::new(db)),
      place_other: *georender_pack::osm_types::get_types().get("place.other").unwrap(),
      progress: Arc::new(RwLock::new(Progress::new(stages))),
    }
  }

  // loop over the pbf, denormalize the records, georender-pack the data into eyros
  pub async fn ingest(&mut self, pbf_file: &str) -> () {
    const BATCH_SEND_SIZE: usize = 10_000;
    const BATCH_SIZE: usize = 100_000;
    self.progress.write().await.start("ingest");
    let mut work = vec![];
    let (batch_sender,batch_receiver) = channel::bounded(100);
    let scan_table = {
      let h = std::fs::File::open(pbf_file).unwrap();
      let pbf_len = h.metadata().unwrap().len();
      let mut scan = Scan::new(Parser::new(Box::new(h)));
      scan.scan(0, pbf_len).unwrap();
      let scan_table = scan.table.clone();
      scan_table
    };


    let mnactive = Arc::new(Mutex::new(3));

    { // node thread
      let place_other = self.place_other.clone();
      let file = pbf_file.to_string();
      let bs = batch_sender.clone();
      let table = scan_table.clone();
      let nactive = mnactive.clone();
      work.push(task::spawn(async move {
        let mut element_counter = 0;
        let mut batch = vec![];
        let nproc = std::thread::available_concurrency().map(|n| n.get()).unwrap_or(1);
        let node_receiver = {
          let scans = (0..nproc).map(|_| {
            let h = std::fs::File::open(&file).unwrap();
            let parser = Parser::new(Box::new(h));
            Scan::from_table(parser, table.clone())
          }).collect::<Vec<_>>();
          denorm::get_nodes_ch(scans, 10_000).await
        };
        while let Ok(nodes) = node_receiver.recv().await {
          for node in nodes.iter() {
            element_counter += 1;
            let tags = node.tags.iter()
              .map(|(k,v)| (k.as_str(),v.as_str()))
              .collect::<Vec<(&str,&str)>>();
            let (ft,labels) = georender_pack::tags::parse(&tags).unwrap();
            if ft == place_other { continue }
            let r_encoded = georender_pack::encode::node_from_parsed(
              (node.id as u64)*3+0, (node.lon as f32, node.lat as f32), ft, &labels
            );
            if let Ok(encoded) = r_encoded {
              if encoded.is_empty() { continue }
              batch.push(eyros::Row::Insert(
                (
                  eyros::Coord::Scalar(node.lon as f32),
                  eyros::Coord::Scalar(node.lat as f32)
                ),
                encoded.into()
              ));
              if batch.len() >= BATCH_SEND_SIZE {
                bs.send((element_counter,batch.clone())).await.unwrap();
                batch.clear();
                element_counter = 0;
              }
            }
          }
        }
        if !batch.is_empty() {
          bs.send((element_counter,batch)).await.unwrap();
        }
        {
          let mut n = nactive.lock().await;
          *n -= 1;
          if *n == 0 { bs.close(); }
        }
      }));
    }

    { // way thread
      let place_other = self.place_other.clone();
      let file = pbf_file.to_string();
      let bs = batch_sender.clone();
      let table = scan_table.clone();
      let nactive = mnactive.clone();
      work.push(task::spawn(async move {
        let mut batch = vec![];
        let mut element_counter = 0;
        let nproc = std::thread::available_concurrency().map(|n| n.get()).unwrap_or(1);
        {
          let mut offset = 0;
          loop {
            let (o_next_offset,ways) = {
              let scans = (0..nproc).map(|_| {
                let h = std::fs::File::open(&file).unwrap();
                let parser = Parser::new(Box::new(h));
                Scan::from_table(parser, table.clone())
              }).collect::<Vec<_>>();
              denorm::get_ways(scans, 10_000, offset, 10_000_000).await
            };
            let way_ref_table = denorm::way_ref_table(&ways);
            let node_receiver = {
              let scans = (0..nproc).map(|_| {
                let h = std::fs::File::open(&file).unwrap();
                let parser = Parser::new(Box::new(h));
                Scan::from_table(parser, table.clone())
              }).collect::<Vec<_>>();
              denorm::get_nodes_bare_ch(scans, 10_000).await
            };
            let all_node_deps = denorm::denormalize_ways(&way_ref_table, node_receiver).await.unwrap();
            for way in ways {
              element_counter += 1;
              let tags = way.tags.iter()
                .map(|(k,v)| (k.as_str(),v.as_str()))
                .collect::<Vec<(&str,&str)>>();
              let (ft,labels) = georender_pack::tags::parse(&tags).unwrap();
              if ft == place_other { continue }
              let mut pdeps = HashMap::new();
              for (node_id,(lon,lat)) in all_node_deps.get(&way.id).unwrap_or(&vec![]) {
                pdeps.insert(*node_id as u64, (*lon as f32, *lat as f32));
              }
              let mut bbox = (f32::INFINITY,f32::INFINITY,f32::NEG_INFINITY,f32::NEG_INFINITY);
              if pdeps.len() <= 1 { continue }
              for (lon,lat) in pdeps.values() {
                bbox.0 = bbox.0.min(*lon);
                bbox.1 = bbox.1.min(*lat);
                bbox.2 = bbox.2.max(*lon);
                bbox.3 = bbox.3.max(*lat);
              }
              let refs = way.refs.iter().map(|r| *r as u64).collect::<Vec<u64>>();
              let is_area = osm_is_area::way(&tags, &refs);
              let r_encoded = georender_pack::encode::way_from_parsed(
                (way.id as u64)*3+1, ft, is_area, &labels, &refs, &pdeps
              );
              if let Ok(encoded) = r_encoded {
                if encoded.is_empty() { continue }
                let point = (
                  eyros::Coord::Interval(bbox.0,bbox.2),
                  eyros::Coord::Interval(bbox.1,bbox.3),
                );
                batch.push(eyros::Row::Insert(point, encoded.into()));
                if batch.len() >= BATCH_SEND_SIZE {
                  bs.send((element_counter,batch.clone())).await.unwrap();
                  batch.clear();
                  element_counter = 0;
                }
              }
            }
            if let Some(next_offset) = o_next_offset {
              offset = next_offset;
            } else {
              break;
            }
          }
        }
        if !batch.is_empty() {
          bs.send((element_counter,batch)).await.unwrap();
        }
        {
          let mut n = nactive.lock().await;
          *n -= 1;
          if *n == 0 { bs.close(); }
        }
      }));
    }

    /*
      work.push(task::spawn(async move {
        let mut batch = vec![];
        let mut element_counter = 0;
        //for receiver in &[n_receiver,w_receiver,r_receiver] {
        for receiver in &[w_receiver] {
          while let Ok((offset,len)) = receiver.recv().await {
            let items = cache.get_items(&mut parser, offset, len).await.unwrap();
            for item in items {
              element_counter += 1;
              match item {
                element::Element::Node(node) => {
                },
                element::Element::Way(way) => {
                },
                element::Element::Relation(relation) => {
                  let tags = relation.tags.iter()
                    .map(|(k,v)| (k.as_str(),v.as_str()))
                    .collect::<Vec<(&str,&str)>>();
                  let (ft,labels) = georender_pack::tags::parse(&tags).unwrap();
                  if ft == place_other { continue }
                  let is_area = osm_is_area::relation(&tags, &vec![1]);
                  if !is_area { continue }
                  let members = relation.members.iter()
                    .filter(|m| &m.role == "inner" || &m.role == "outer")
                    .map(|m| georender_pack::Member::new(
                      m.id as u64,
                      match m.role.as_str() {
                        "outer" => georender_pack::MemberRole::Outer(),
                        "inner" => georender_pack::MemberRole::Inner(),
                        _ => panic!["unexpected role should have been filtered out"],
                      },
                      georender_pack::MemberType::Way()
                    ))
                    .collect::<Vec<_>>();
                  if members.is_empty() { continue }

                  let mut node_deps: NodeDeps = HashMap::new();
                  let mut way_deps: WayDeps = HashMap::new();

                  for m in relation.members.iter() {
                    if m.member_type == element::MemberType::Way {
                      if let Some(way) = cache.get_way(&mut parser, m.id).await.unwrap() {
                        if way.id != m.id { continue }
                        let refs = way.refs.iter().map(|r| *r as u64).collect::<Vec<u64>>();
                        way_deps.insert(way.id as u64, refs);
                        for r in way.refs.iter() {
                          if let Some(node) = cache.get_node(&mut parser, *r).await.unwrap() {
                            if node.id != *r { continue }
                            node_deps.insert(node.id as u64, (node.lon as f32, node.lat as f32));
                          }
                        }
                      }
                    }
                  }
                  if node_deps.len() <= 1 { continue }
                  let mut bbox = (f32::INFINITY,f32::INFINITY,f32::NEG_INFINITY,f32::NEG_INFINITY);
                  for p in node_deps.values() {
                    bbox.0 = bbox.0.min(p.0);
                    bbox.1 = bbox.1.min(p.1);
                    bbox.2 = bbox.2.max(p.0);
                    bbox.3 = bbox.3.max(p.1);
                  }
                  let r_encoded = georender_pack::encode::relation_from_parsed(
                    (relation.id as u64)*3+2, ft, is_area,
                    &labels, &members, &node_deps, &way_deps
                  );
                  if let Ok(encoded) = r_encoded {
                    let point = (
                      eyros::Coord::Interval(bbox.0,bbox.2),
                      eyros::Coord::Interval(bbox.1,bbox.3),
                    );
                    batch.push(eyros::Row::Insert(point, encoded.into()));
                  }
                },
              }
              if batch.len() >= BATCH_SEND_SIZE {
                bs.send((element_counter,batch.clone())).await.unwrap();
                batch.clear();
                element_counter = 0;
              }
            }
          }
        }
        bs.send((element_counter,batch.clone())).await.unwrap();
        batch.clear();

        {
          let mut n = nactive.lock().await;
          *n -= 1;
          if *n == 0 { bs.close(); }
        }
      }));
    }
    */

    {
      let progress = self.progress.clone();
      let db_c = self.db.clone();
      work.push(task::spawn_local(async move {
        let mut db = db_c.lock().await;
        let mut sync_count = 0;
        let mut batch = vec![];
        while let Ok((element_counter,rows)) = batch_receiver.recv().await {
          batch.extend(rows);
          if batch.len() >= BATCH_SIZE {
            db.batch(&batch).await.unwrap();
            sync_count += batch.len();
            batch.clear();
            if sync_count > 5_000_000 {
              db.sync().await.unwrap();
              sync_count = 0;
            }
          }
          progress.write().await.add("ingest", element_counter);
        }
        db.batch(&batch).await.unwrap();
        db.sync().await.unwrap();
      }));
    }

    {
      let mut n = mnactive.lock().await;
      *n -= 1;
      if *n == 0 { batch_sender.close(); }
    }
    join_all(work).await;
    self.progress.write().await.end("ingest");
  }
}
