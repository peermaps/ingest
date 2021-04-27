pub mod encoder;
pub use encoder::*;
pub mod store;
pub use store::*;
pub mod varint;

pub const BACKREF_PREFIX: u8 = 1;
pub const REF_PREFIX: u8 = 2;

use std::collections::{HashMap,HashSet};
use async_std::{prelude::*,sync::{Arc,Mutex},io};
use desert::FromBytesLE;

use leveldb::iterator::{Iterable,LevelDBIterator};
use leveldb::options::ReadOptions;

type Error = Box<dyn std::error::Error+Send+Sync>;
type NodeDeps = HashMap<u64,(f32,f32)>;
type WayDeps = HashMap<u64,Vec<u64>>;

type P = (eyros::Coord<f32>,eyros::Coord<f32>);

pub struct Ingest {
  pub lstore: Arc<Mutex<LStore>>,
  pub estore: Arc<Mutex<EStore>>,
  place_other: u64,
}

impl Ingest {
  pub fn new(lstore: LStore, estore: EStore) -> Self {
    Self {
      lstore: Arc::new(Mutex::new(lstore)),
      estore: Arc::new(Mutex::new(estore)),
      place_other: *georender_pack::osm_types::get_types().get("place.other").unwrap(),
    }
  }

  // write the pbf into leveldb
  pub async fn load_pbf(&self, pbf_file: &str) -> Result<(),Error> {
    let mut lstore = self.lstore.lock().await;
    osmpbf::ElementReader::from_path(pbf_file)?.for_each(|element| {
      let (key,value) = encode_osmpbf(&element).unwrap();
      lstore.put(Key::from(&key), &value).unwrap(); // TODO: should bubble up
    })?;
    lstore.flush()?;
    Ok(())
  }

  // loop over the db, denormalize the records, georender-pack the data,
  // store into eyros, and write backrefs into leveldb
  pub async fn process(&self) -> Result<(),Error> {
    let gt = Key::from(&vec![ID_PREFIX]);
    let lt = Key::from(&vec![ID_PREFIX,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff]);
    let db = {
      let lstore = self.lstore.lock().await;
      lstore.db.clone()
    };
    let mut iter = {
      let i = db.iter(ReadOptions::new());
      i.seek(&gt);
      i.take_while(move |(k,_)| *k < lt)
    };
    while let Some((key,value)) = iter.next() {
      // presumably, if a feature doesn't have distinguishing tags,
      // it could be referred to by other geometry, so skip it
      // to save space in the final output
      match decode(&key.data,&value)? {
        Decoded::Node(node) => {
          self.create_node(&node).await?;
        },
        Decoded::Way(way) => {
          self.create_way(&way).await?;
        },
        Decoded::Relation(relation) => {
          self.create_relation(&relation).await?;
        },
      }
    }
    {
      let mut estore = self.estore.lock().await;
      estore.flush().await?;
      estore.sync().await?;
    }
    {
      let mut lstore = self.lstore.lock().await;
      lstore.flush()?;
    }
    Ok(())
  }

  // import changes from an o5c changeset file
  pub async fn changeset(&mut self, infile: Box<dyn io::Read+Unpin>) -> Result<(),Error> {
    let mut stream = o5m_stream::decode(infile);
    while let Some(result) = stream.next().await {
      let dataset = result?;
      let m = match &dataset {
        o5m_stream::Dataset::Node(node) => {
          match &node.data {
            Some(_) => Some((false,node.id*3+0)), // modify or create
            None => Some((true,node.id*3+0)), // delete
          }
        },
        o5m_stream::Dataset::Way(way) => {
          match &way.data {
            Some(data) => { // modify or create
              let mut deps = HashMap::with_capacity(data.refs.len());
              self.get_way_deps(data.refs.iter(), &mut deps).await?;
              let refs = self.get_refs(way.id*3+1).await?;
              let prev_set = refs.into_iter().collect::<HashSet<u64>>();
              let new_set = deps.keys().map(|r| *r).into_iter().collect::<HashSet<u64>>();
              {
                let mut lstore = self.lstore.lock().await;
                for r in prev_set.difference(&new_set) {
                  lstore.del(Key::from(&backref_key(*r, way.id*3+1)?))?;
                }
                for r in new_set.difference(&prev_set) {
                  lstore.put(Key::from(&backref_key(*r, way.id*3+1)?),&vec![])?;
                }
              }
              Some((false,way.id*3+1))
            },
            None => Some((true,way.id*3+1)), // delete
          }
        },
        o5m_stream::Dataset::Relation(relation) => {
          match &relation.data {
            Some(data) => { // modify or create
              let mut node_deps = HashMap::new();
              let mut way_deps = HashMap::with_capacity(data.members.len());
              let refs = data.members.iter().map(|m| m.id).collect::<Vec<u64>>();
              self.get_relation_deps(refs.iter(), &mut node_deps, &mut way_deps).await?;
              let prev_refs = self.get_refs(relation.id*3+2).await?;
              let prev_set = prev_refs.into_iter().collect::<HashSet<u64>>();
              let new_set = way_deps.keys().map(|r| *r).into_iter().collect::<HashSet<u64>>();
              {
                let mut lstore = self.lstore.lock().await;
                for r in prev_set.difference(&new_set) {
                  lstore.del(Key::from(&backref_key(*r, relation.id*3+2)?))?;
                }
                for r in new_set.difference(&prev_set) {
                  lstore.put(Key::from(&backref_key(*r, relation.id*3+2)?),&vec![])?;
                }
              }
              Some((false,relation.id*3+2))
            },
            None => Some((true,relation.id*3+2)), // delete
          }
        },
        _ => None,
      };
      if let Some((is_rm,ex_id)) = m {
        let backrefs = self.get_backrefs(ex_id).await?;
        if is_rm {
          // delete the record but don't bother dealing with backrefs
          // because the referred-to elements *should* be deleted too.
          // not the job of this ingest script to verify changeset integrity
          if let Some(pt) = self.get_point(ex_id).await? {
            let mut estore = self.estore.lock().await;
            estore.delete(pt, ex_id).await?;
          }
          let mut lstore = self.lstore.lock().await;
          lstore.del(Key::from(&id_key(ex_id)?))?;
          for r in backrefs.iter() {
            lstore.del(Key::from(&backref_key(*r, ex_id)?))?;
          }
        } else {
          let mut prev_points = Vec::with_capacity(backrefs.len());
          for r in backrefs.iter() {
            if let Some(pt) = self.get_point(*r).await? {
              prev_points.push(pt);
            }
          }
          if let Some(encoded) = encode_o5m(&dataset)? {
            let mut lstore = self.lstore.lock().await;
            lstore.put(Key::from(&id_key(ex_id)?), &encoded)?;
          }
          for (r,prev_point) in backrefs.iter().zip(prev_points.iter()) {
            self.recalculate(*r, prev_point).await?;
          }
          self.estore.lock().await.check_flush().await?;
        }
      }
    }
    let mut lstore = self.lstore.lock().await;
    lstore.flush()?;
    let mut estore = self.estore.lock().await;
    estore.flush().await?;
    estore.sync().await?;
    Ok(())
  }

  // call this when one of a record's dependants changes
  #[async_recursion::async_recursion]
  async fn recalculate(&self, ex_id: u64, prev_point: &P) -> Result<(),Error> {
    let key = Key::from(&id_key(ex_id)?);
    let res = self.lstore.lock().await.get(&key)?;
    if let Some(buf) = res {
      match decode(&key.data,&buf)? {
        Decoded::Node(_node) => {}, // nothing to update
        Decoded::Way(way) => {
          if let Some((new_point,_deps,encoded)) = self.encode_way(&way).await? {
            let backrefs = self.get_backrefs(ex_id).await?;
            let mut prev_points = Vec::with_capacity(backrefs.len());
            for r in backrefs.iter() {
              if let Some(p) = self.get_point(*r).await? {
                prev_points.push(p);
              }
            }
            {
              let mut estore = self.estore.lock().await;
              estore.push_update(prev_point, &new_point, &encoded.into());
            }
            for (r,p) in backrefs.iter().zip(prev_points.iter()) {
              self.recalculate(*r, p).await?;
            }
          }
        },
        Decoded::Relation(relation) => {
          if let Some((new_point,_deps,encoded)) = self.encode_relation(&relation).await? {
            let backrefs = self.get_backrefs(ex_id).await?;
            let mut prev_points = Vec::with_capacity(backrefs.len());
            for r in backrefs.iter() {
              if let Some(p) = self.get_point(*r).await? {
                prev_points.push(p);
              }
            }
            {
              let mut estore = self.estore.lock().await;
              estore.push_update(prev_point, &new_point, &encoded.into());
            }
            for (r,p) in backrefs.iter().zip(prev_points.iter()) {
              self.recalculate(*r, p).await?;
            }
          }
        },
      }
    }
    Ok(())
  }

  #[async_recursion::async_recursion]
  async fn get_point(&self, ex_id: u64) -> Result<Option<P>,Error> {
    fn merge(a: &P, b: &P) -> P {
      (merge_pt(&a.0,&b.0),merge_pt(&a.1,&b.1))
    }
    fn merge_pt(a: &eyros::Coord<f32>, b: &eyros::Coord<f32>) -> eyros::Coord<f32> {
      let amin = match a {
        eyros::Coord::Scalar(x) => x,
        eyros::Coord::Interval(x,_) => x,
      };
      let amax = match a {
        eyros::Coord::Scalar(x) => x,
        eyros::Coord::Interval(_,x) => x,
      };
      let bmin = match b {
        eyros::Coord::Scalar(x) => x,
        eyros::Coord::Interval(x,_) => x,
      };
      let bmax = match b {
        eyros::Coord::Scalar(x) => x,
        eyros::Coord::Interval(_,x) => x,
      };
      eyros::Coord::Interval(amin.min(*bmin), amax.max(*bmax))
    }
    Ok(match ex_id%3 {
      0 => {
        let mut lstore = self.lstore.lock().await;
        if let Some(buf) = lstore.get(&Key::from(&id_key(ex_id)?))? {
          let mut offset = 0;
          let (s,lon) = f32::from_bytes_le(&buf[offset..])?;
          offset += s;
          let (_s,lat) = f32::from_bytes_le(&buf[offset..])?;
          Some((eyros::Coord::Scalar(lon),eyros::Coord::Scalar(lat)))
        } else {
          None
        }
      },
      _ => {
        let refs = self.get_refs(ex_id).await?;
        let mut bbox = None;
        for r in refs.iter() {
          if let Some(p) = self.get_point(*r).await? {
            if let Some(b) = bbox {
              bbox = Some(merge(&b, &p));
            } else {
              bbox = Some(p);
            }
          }
        }
        bbox
      },
    })
  }

  async fn create_node(&self, node: &DecodedNode) -> Result<(),Error> {
    if node.feature_type == self.place_other { return Ok(()) }
    let encoded = georender_pack::encode::node_from_parsed(
      node.id*3+0, (node.lon,node.lat), node.feature_type, &node.labels
    )?;
    if !encoded.is_empty() {
      let point = (eyros::Coord::Scalar(node.lon),eyros::Coord::Scalar(node.lat));
      let mut estore = self.estore.lock().await;
      estore.create(point, encoded.into()).await?;
    }
    Ok(())
  }

  async fn encode_way(&self, way: &DecodedWay) -> Result<Option<(P,NodeDeps,Vec<u8>)>,Error> {
    let mut deps = HashMap::with_capacity(way.refs.len());
    self.get_way_deps(way.refs.iter(), &mut deps).await?;
    let mut bbox = (f32::INFINITY,f32::INFINITY,f32::NEG_INFINITY,f32::NEG_INFINITY);
    for (lon,lat) in deps.values() {
      bbox.0 = bbox.0.min(*lon);
      bbox.1 = bbox.1.min(*lat);
      bbox.2 = bbox.2.max(*lon);
      bbox.3 = bbox.3.max(*lat);
    }
    let encoded = georender_pack::encode::way_from_parsed(
      way.id*3+1, way.feature_type, way.is_area, &way.labels, &way.refs, &deps
    )?;
    if encoded.is_empty() {
      Ok(None)
    } else {
      let point = (
        eyros::Coord::Interval(bbox.0,bbox.2),
        eyros::Coord::Interval(bbox.1,bbox.3),
      );
      Ok(Some((point,deps,encoded)))
    }
  }

  async fn create_way(&self, way: &DecodedWay) -> Result<(),Error> {
    if let Some((point,deps,encoded)) = self.encode_way(way).await? {
      if way.feature_type != self.place_other {
        let mut estore = self.estore.lock().await;
        estore.create(point, encoded.into()).await?;
      }
      let mut lstore = self.lstore.lock().await;
      for r in deps.keys() {
        // node -> way backref
        lstore.put(Key::from(&backref_key(*r*3+0, way.id*3+1)?), &vec![])?;
      }
    }
    Ok(())
  }

  async fn create_relation(&self, relation: &DecodedRelation) -> Result<(),Error> {
    if let Some((point,deps,encoded)) = self.encode_relation(relation).await? {
      if relation.feature_type != self.place_other {
        let mut estore = self.estore.lock().await;
        estore.create(point, encoded.into()).await?;
      }
      let mut lstore = self.lstore.lock().await;
      for way_id in deps.keys() {
        // way -> relation backref
        lstore.put(Key::from(&backref_key(way_id*3+1,relation.id*3+2)?), &vec![])?;
      }
    }
    Ok(())
  }

  async fn encode_relation(&self, relation: &DecodedRelation) -> Result<Option<(P,WayDeps,Vec<u8>)>,Error> {
    let mut node_deps = HashMap::new();
    let mut way_deps = HashMap::with_capacity(relation.members.len());
    let refs = relation.members.iter().map(|m| m/2).collect::<Vec<u64>>();
    self.get_relation_deps(refs.iter(), &mut node_deps, &mut way_deps).await?;
    let mut bbox = (f32::INFINITY,f32::INFINITY,f32::NEG_INFINITY,f32::NEG_INFINITY);
    for refs in way_deps.values() {
      for r in refs.iter() {
        if let Some(p) = node_deps.get(r) {
          bbox.0 = bbox.0.min(p.0);
          bbox.1 = bbox.1.min(p.1);
          bbox.2 = bbox.2.max(p.0);
          bbox.3 = bbox.3.max(p.1);
        }
      }
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
    )?;
    if encoded.is_empty() {
      Ok(None)
    } else {
      let point = (
        eyros::Coord::Interval(bbox.0,bbox.2),
        eyros::Coord::Interval(bbox.1,bbox.3),
      );
      Ok(Some((point,way_deps,encoded)))
    }
  }

  async fn get_way_deps(&self, iter: impl Iterator<Item=&u64>, deps: &mut NodeDeps) -> Result<(),Error> {
    let mut lstore = self.lstore.lock().await;
    let mut key_data = [ID_PREFIX,0,0,0,0,0,0,0,0];
    key_data[0] = ID_PREFIX;
    let mut first = None;
    for r in iter {
      if first.is_none() {
        first = Some(*r);
      } else if first == Some(*r) {
        continue;
      }
      let s = varint::encode(r*3+0,&mut key_data[1..])?;
      let key = Key::from(&key_data[0..1+s]);
      if let Some(buf) = lstore.get(&key)? {
        match decode(&key.data,&buf)? {
          Decoded::Node(node) => {
            deps.insert(*r,(node.lon,node.lat));
          },
          _ => {},
        }
      }
    }
    Ok(())
  }

  async fn get_relation_deps(&self, iter: impl Iterator<Item=&u64>,
    node_deps: &mut NodeDeps, way_deps: &mut WayDeps
  ) -> Result<(),Error> {
    let mut key_data = [ID_PREFIX,0,0,0,0,0,0,0,0];
    key_data[0] = ID_PREFIX;
    for m in iter {
      let s = varint::encode(m*3+1,&mut key_data[1..])?;
      let key = Key::from(&key_data[0..1+s]);
      let res = {
        let mut lstore = self.lstore.lock().await;
        lstore.get(&key)?
      };
      if let Some(buf) = res {
        match decode(&key.data,&buf)? {
          Decoded::Way(way) => {
            way_deps.insert(way.id, way.refs.clone());
            self.get_way_deps(way.refs.iter(), node_deps).await?;
          },
          _ => {},
        }
      }
    }
    Ok(())
  }

  async fn get_backrefs(&self, ex_id: u64) -> Result<Vec<u64>,Error> {
    let mut lstore = self.lstore.lock().await;
    let ex_id_len = varint::length(ex_id);
    let gt = Key::from(&{
      let mut key = vec![0u8;1+ex_id_len];
      key[0] = BACKREF_PREFIX;
      varint::encode(ex_id, &mut key[1..])?;
      key
    });
    let lt = Key::from(&{
      let mut key = vec![0u8;1+ex_id_len+8];
      key[0] = BACKREF_PREFIX;
      let s = varint::encode(ex_id, &mut key[1..])?;
      for i in 0..8 { key[1+s+i] = 0xff }
      key
    });
    let mut results = vec![];
    let mut iter = lstore.keys_iter(lt, gt);
    while let Some(key) = iter.next() {
      let (_,r_id) = varint::decode(&key.data[1+ex_id_len..])?;
      results.push(r_id);
    }
    Ok(results)
  }

  async fn get_refs(&self, ex_id: u64) -> Result<Vec<u64>,Error> {
    let mut lstore = self.lstore.lock().await;
    Ok(match ex_id%3 {
      0 => vec![], // node
      1 => { // way
        if let Some(buf) = lstore.get(&Key::from(&id_key(ex_id)?))? {
          let mut offset = 0;
          let (s,_fta) = varint::decode(&buf[offset..])?;
          offset += s;
          let (s,reflen) = varint::decode(&buf[offset..])?;
          offset += s;
          let mut refs = Vec::with_capacity(reflen as usize);
          for _ in 0..reflen {
            let (s,r) = varint::decode(&buf[offset..])?;
            offset += s;
            refs.push(r);
          }
          refs
        } else {
          vec![]
        }
      },
      _ => { // relation
        if let Some(buf) = lstore.get(&Key::from(&id_key(ex_id)?))? {
          let mut offset = 0;
          let (s,_fta) = varint::decode(&buf[offset..])?;
          offset += s;
          let (s,mlen) = varint::decode(&buf[offset..])?;
          offset += s;
          let mut refs = Vec::with_capacity(mlen as usize);
          for _ in 0..mlen {
            let (s,r) = varint::decode(&buf[offset..])?;
            offset += s;
            refs.push(r/2);
          }
          refs
        } else {
          vec![]
        }
      },
    })
  }
}

fn backref_key(a: u64, b: u64) -> Result<Vec<u8>,Error> {
  // both a and b are extended ids
  let mut key = vec![0u8;1+varint::length(a)+varint::length(b)];
  key[0] = BACKREF_PREFIX;
  let s = varint::encode(a, &mut key[1..])?;
  varint::encode(b, &mut key[1+s..])?;
  Ok(key)
}
