#![warn(clippy::future_not_send)]
#![feature(async_closure,backtrace,available_concurrency)]
pub mod encoder;
pub use encoder::*;
pub mod error;
pub use error::*;
//mod record;
mod value;
mod progress;
pub use progress::Progress;
use osmpbf_parser::{Parser,Scan,ScanTable,element};
use lru::LruCache;

pub const BACKREF_PREFIX: u8 = 1;
pub const REF_PREFIX: u8 = 2;

use std::collections::HashMap;
use async_std::{sync::{Arc,Mutex,RwLock},task,channel};
use futures::future::join_all;

type NodeDeps = HashMap<u64,(f32,f32)>;
type WayDeps = HashMap<u64,Vec<u64>>;

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
    const BATCH_SIZE: usize = 100_000;
    self.progress.write().await.start("ingest");
    let mut work = vec![];
    let nproc = std::thread::available_concurrency().map(|n| n.get()).unwrap_or(1);
    let (node_sender,node_receiver) = channel::unbounded();
    let (way_sender,way_receiver) = channel::unbounded();
    let (relation_sender,relation_receiver) = channel::unbounded();
    let (batch_sender,batch_receiver) = channel::bounded(100);
    let scan_table = {
      let h = std::fs::File::open(pbf_file).unwrap();
      let pbf_len = h.metadata().unwrap().len();
      let mut scan = Scan::new(Parser::new(Box::new(h)));
      scan.scan(0, pbf_len).unwrap();
      let scan_table = scan.table.clone();
      work.push(task::spawn(async move {
        for (offset,len) in scan.get_node_blob_offsets() {
          if offset > 0 { // skip blob_header
            node_sender.send((offset,len)).await.unwrap();
          }
        }
        for (offset,len) in scan.get_way_blob_offsets() {
          way_sender.send((offset,len)).await.unwrap();
        }
        for (offset,len) in scan.get_relation_blob_offsets() {
          relation_sender.send((offset,len)).await.unwrap();
        }
      }));
      scan_table
    };

    for receiver in &[node_receiver,way_receiver,relation_receiver] {
      for _ in 0..nproc {
        let cache = Cache::new(scan_table.clone());
        let h = std::fs::File::open(pbf_file).unwrap();
        let mut parser = Parser::new(Box::new(h));
        let place_other = self.place_other.clone();
        let bs = batch_sender.clone();
        let recv = receiver.clone();
        work.push(task::spawn(async move {
          let mut batch = vec![];
          for (offset,len) in recv.recv().await {
            let items = cache.get_items(&mut parser, offset, len).await.unwrap();
            for item in items {
              match item {
                element::Element::Node(node) => {
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
                    if batch.len() >= BATCH_SIZE {
                      bs.send(batch.clone()).await.unwrap();
                      batch.clear();
                    }
                  }
                },
                element::Element::Way(way) => {
                  let tags = way.tags.iter()
                    .map(|(k,v)| (k.as_str(),v.as_str()))
                    .collect::<Vec<(&str,&str)>>();
                  let (ft,labels) = georender_pack::tags::parse(&tags).unwrap();
                  if ft == place_other { continue }
                  let mut pdeps = HashMap::new();
                  for r in way.refs.iter() {
                    if let Some(node) = cache.get_node(&mut parser, *r).await.unwrap() {
                      if node.id != *r { continue }
                      pdeps.insert(node.id as u64, (node.lon as f32, node.lat as f32));
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
                    if batch.len() >= BATCH_SIZE {
                      bs.send(batch.clone()).await.unwrap();
                      batch.clear();
                    }
                  }
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
                    if batch.len() >= BATCH_SIZE {
                      bs.send(batch.clone()).await.unwrap();
                      batch.clear();
                    }
                  }
                },
              }
            }
          }
        }));
      }
    }

    {
      let progress = self.progress.clone();
      let db_c = self.db.clone();
      work.push(task::spawn_local(async move {
        let mut db = db_c.lock().await;
        let mut sync_count = 0;
        while let Ok(batch) = batch_receiver.recv().await {
          db.batch(&batch).await.unwrap();
          progress.write().await.add("ingest", batch.len());
          sync_count += batch.len();
          if sync_count > 5_000_000 {
            db.sync().await.unwrap();
            sync_count = 0;
          }
        }
      }));
    }
    join_all(work).await;

    self.db.lock().await.sync().await.unwrap();
    self.progress.write().await.end("ingest");
  }
}

#[derive(Debug,Clone)]
pub struct Cache {
  scan_table: ScanTable,
  node_cache: Arc<Mutex<LruCache<i64,element::Node>>>,
  way_cache: Arc<Mutex<LruCache<i64,element::Way>>>,
  relation_cache: Arc<Mutex<LruCache<i64,element::Relation>>>,
}

impl Cache {
  pub fn new(scan_table: ScanTable) -> Self {
    Self {
      scan_table,
      node_cache: Arc::new(Mutex::new(LruCache::new(10_000_000))),
      way_cache: Arc::new(Mutex::new(LruCache::new(1_000_000))),
      relation_cache: Arc::new(Mutex::new(LruCache::new(1_000))),
    }
  }
  pub async fn get_items<F: std::io::Read+std::io::Seek>(
    &self, parser: &mut Parser<F>, offset: u64, len: usize
  ) -> Result<Vec<element::Element>,Error> {
    let blob = parser.read_blob(offset,len)?;
    let items = blob.decode_primitive()?.decode();
    let mut ncache = self.node_cache.lock().await;
    let mut wcache = self.way_cache.lock().await;
    let mut rcache = self.relation_cache.lock().await;
    for item in items.iter() {
      match item {
        element::Element::Node(node) => {
          ncache.put(node.id, node.clone());
        },
        element::Element::Way(way) => {
          wcache.put(way.id, way.clone());
        },
        element::Element::Relation(relation) => {
          rcache.put(relation.id, relation.clone());
        },
      }
    }
    Ok(items)
  }
  pub async fn get_node<F: std::io::Read+std::io::Seek>(
    &self, parser: &mut Parser<F>, id: i64
  ) -> Result<Option<element::Node>,Error> {
    if let Some(node) = self.node_cache.lock().await.get(&id) {
      return Ok(Some(node.clone()));
    }
    for (offset,len) in self.scan_table.get_node_blob_offsets_for_id(id) {
      let items = self.get_items(parser, offset, len).await?;
      for item in items {
        match item {
          element::Element::Node(node) => {
            if node.id == id { return Ok(Some(node)) }
          },
          _ => { break }
        }
      }
    }
    Ok(None)
  }
  pub async fn get_way<F: std::io::Read+std::io::Seek>(
    &self, parser: &mut Parser<F>, id: i64
  ) -> Result<Option<element::Way>,Error> {
    if let Some(way) = self.way_cache.lock().await.get(&id) {
      return Ok(Some(way.clone()));
    }
    for (offset,len) in self.scan_table.get_way_blob_offsets_for_id(id) {
      let items = self.get_items(parser, offset, len).await?;
      for item in items {
        match item {
          element::Element::Way(way) => {
            if way.id == id { return Ok(Some(way)) }
          },
          _ => { break }
        }
      }
    }
    Ok(None)
  }
  pub async fn get_relation<F: std::io::Read+std::io::Seek>(
    &self, parser: &mut Parser<F>, id: i64
  ) -> Result<Option<element::Relation>,Error> {
    if let Some(relation) = self.relation_cache.lock().await.get(&id) {
      return Ok(Some(relation.clone()));
    }
    for (offset,len) in self.scan_table.get_relation_blob_offsets_for_id(id) {
      let items = self.get_items(parser, offset, len).await?;
      for item in items {
        match item {
          element::Element::Relation(relation) => {
            if relation.id == id { return Ok(Some(relation)) }
          },
          _ => { break }
        }
      }
    }
    Ok(None)
  }
}
