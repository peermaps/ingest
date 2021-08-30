#![warn(clippy::future_not_send)]
#![feature(async_closure,backtrace,available_concurrency)]
pub mod error;
pub use error::*;
mod value;
mod progress;
pub mod denorm;
pub use progress::Progress;
use osmpbf_parser::{Parser,Scan,ScanTable,element};
mod par_scan;
use par_scan::parallel_scan;

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
  place_other: u64,
  pub progress: Arc<RwLock<Progress>>,
}

pub struct IngestOptions {
  pub channel_size: usize,
  pub way_batch_size: usize,
  pub relation_batch_size: usize,
  pub ingest_node: bool,
  pub ingest_way: bool,
  pub ingest_relation: bool,
}

impl Default for IngestOptions {
  fn default() -> Self {
    Self {
      channel_size: 500,
      way_batch_size: 10_000_000,
      relation_batch_size: 1_000_000,
      ingest_node: true,
      ingest_way: true,
      ingest_relation: true,
    }
  }
}

impl Ingest {
  pub fn new(stages: &[&str]) -> Self {
    Self {
      place_other: *georender_pack::osm_types::get_types().get("place.other").unwrap(),
      progress: Arc::new(RwLock::new(Progress::new(stages))),
    }
  }

  // build the scan table
  pub async fn scan(&mut self, pbf_file: &str) -> ScanTable {
    self.progress.write().await.start("scan");
    let scan_table = {
      let nproc = std::thread::available_concurrency().map(|n| n.get()).unwrap_or(1);
      let parsers = (0..nproc).map(|_| {
        let h = std::fs::File::open(pbf_file).unwrap();
        Parser::new(Box::new(h))
      }).collect::<Vec<_>>();
      let file_size = std::fs::File::open(pbf_file).unwrap().metadata().unwrap().len();
      parallel_scan(parsers, 0, file_size).await.unwrap()
    };
    self.progress.write().await.end("scan");
    scan_table
  }

  // loop over the pbf, denormalize the records, georender-pack the data into eyros
  pub async fn ingest(
    &mut self, mut db: EDB, pbf_file: &str, scan_table: ScanTable,
    ingest_options: &IngestOptions
  ) -> () {
    const BATCH_SEND_SIZE: usize = 10_000;
    const BATCH_SIZE: usize = 100_000;
    self.progress.write().await.start("ingest");
    let mut work = vec![];
    let (batch_sender,batch_receiver) = channel::bounded(100);
    let mnactive = Arc::new(Mutex::new(1));

    if ingest_options.ingest_node { // node thread
      *mnactive.lock().await += 1;
      let place_other = self.place_other.clone();
      let file = pbf_file.to_string();
      let bs = batch_sender.clone();
      let table = scan_table.clone();
      let nactive = mnactive.clone();
      let channel_size = ingest_options.channel_size;
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
          denorm::get_nodes_ch(scans, channel_size).await
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

    if ingest_options.ingest_way { // way thread
      *mnactive.lock().await += 1;
      let place_other = self.place_other.clone();
      let file = pbf_file.to_string();
      let bs = batch_sender.clone();
      let table = scan_table.clone();
      let nactive = mnactive.clone();
      let channel_size = ingest_options.channel_size;
      let way_batch_size = ingest_options.way_batch_size;
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
              denorm::get_ways(scans, channel_size, offset, way_batch_size).await
            };
            let way_ref_table = denorm::way_ref_table(&ways);
            let node_receiver = {
              let node_offsets = denorm::get_node_offsets_from_ways(&table, &ways);
              let scans = (0..nproc).map(|_| {
                let h = std::fs::File::open(&file).unwrap();
                let parser = Parser::new(Box::new(h));
                Scan::from_table(parser, table.clone())
              }).collect::<Vec<_>>();
              denorm::get_nodes_bare_ch_from_offsets(scans, channel_size, &node_offsets).await
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
              for r in way.refs.iter() {
                if let Some((lon,lat)) = all_node_deps.get(r) {
                  pdeps.insert(*r as u64, (*lon as f32, *lat as f32));
                }
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

    if ingest_options.ingest_relation { // relation thread
      *mnactive.lock().await += 1;
      let place_other = self.place_other.clone();
      let file = pbf_file.to_string();
      let bs = batch_sender.clone();
      let table = scan_table.clone();
      let nactive = mnactive.clone();
      let channel_size = ingest_options.channel_size;
      let relation_batch_size = ingest_options.relation_batch_size;
      work.push(task::spawn(async move {
        let mut batch = vec![];
        let mut element_counter = 0;
        let nproc = std::thread::available_concurrency().map(|n| n.get()).unwrap_or(1);
        {
          let mut offset = 0;
          loop {
            let (o_next_offset,relations) = {
              let scans = (0..nproc).map(|_| {
                let h = std::fs::File::open(&file).unwrap();
                let parser = Parser::new(Box::new(h));
                Scan::from_table(parser, table.clone())
              }).collect::<Vec<_>>();
              denorm::get_relations(scans, channel_size, offset, relation_batch_size).await
            };
            let relation_ref_table = denorm::relation_ref_table(&relations);
            let way_receiver = {
              let scans = (0..nproc).map(|_| {
                let h = std::fs::File::open(&file).unwrap();
                let parser = Parser::new(Box::new(h));
                Scan::from_table(parser, table.clone())
              }).collect::<Vec<_>>();
              let way_offsets = denorm::get_way_offsets_from_relations(&table, &relations);
              denorm::get_ways_bare_ch_from_offsets(scans, channel_size, &way_offsets).await
            };
            let (all_node_deps,all_way_deps) = {
              let scans = (0..nproc).map(|_| {
                let h = std::fs::File::open(&file).unwrap();
                let parser = Parser::new(Box::new(h));
                Scan::from_table(parser, table.clone())
              }).collect::<Vec<_>>();
              denorm::denormalize_relations(
                scans, channel_size, &relation_ref_table, way_receiver
              ).await.unwrap()
            };

            for relation in relations {
              element_counter += 1;
              let tags = relation.tags.iter()
                .map(|(k,v)| (k.as_str(),v.as_str()))
                .collect::<Vec<(&str,&str)>>();
              let (ft,labels) = georender_pack::tags::parse(&tags).unwrap();
              if ft == place_other { continue }
              let is_area = osm_is_area::relation(&tags, &vec![1]);
              if !is_area { continue }
              let members = relation.members.iter()
                .filter(|m| m.member_type == element::MemberType::Way)
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

              let mut node_deps = HashMap::new();
              let mut way_deps = HashMap::new();
              for m in members.iter() {
                if let Some(refs) = all_way_deps.get(&(m.id as i64)) {
                  way_deps.insert(m.id as u64, refs.iter()
                    .map(|r| *r as u64).collect::<Vec<u64>>());
                  for r in refs.iter() {
                    if let Some((lon,lat)) = all_node_deps.get(r) {
                      node_deps.insert(*r as u64, (*lon as f32, *lat as f32));
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

    {
      let progress = self.progress.clone();
      work.push(task::spawn_local(async move {
        let mut sync_count = 0;
        let mut batch = vec![];
        while let Ok((element_counter,rows)) = batch_receiver.recv().await {
          batch.extend(rows);
          if batch.len() >= BATCH_SIZE {
            db.batch(&batch).await.unwrap();
            sync_count += batch.len();
            batch.clear();
            if sync_count > 500_000 {
              db.sync().await.unwrap();
              sync_count = 0;
            }
          }
          progress.write().await.add("ingest", element_counter);
        }
        if !batch.is_empty() {
          db.batch(&batch).await.unwrap();
        }
        db.sync().await.unwrap();
        progress.write().await.add("ingest", 0);
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
