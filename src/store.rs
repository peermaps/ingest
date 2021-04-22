use crate::varint;
use leveldb::database::{Database,batch::{Batch,Writebatch}};
use leveldb::{options::{ReadOptions,WriteOptions},kv::KV};
use leveldb::iterator::{Iterable,LevelDBIterator};
use std::collections::HashMap;
use desert::{ToBytes,FromBytes,CountBytes};
use eyros::Value;
use async_std::sync::Arc;

type Error = Box<dyn std::error::Error+Send+Sync>;

type S = random_access_disk::RandomAccessDisk;
type P = (eyros::Coord<f32>,eyros::Coord<f32>);
type T = eyros::Tree2<f32,f32,V>;

#[derive(Debug,Clone,Hash)]
pub struct V {
  pub data: Vec<u8>,
}
impl Into<V> for Vec<u8> {
  fn into(self) -> V {
    V { data: self }
  }
}
impl Into<Vec<u8>> for V {
  fn into(self) -> Vec<u8> {
    self.data
  }
}
impl eyros::Value for V {
  type Id = u64;
  fn get_id(&self) -> Self::Id {
    let offset = 1 + varint::decode(&self.data[1..]).unwrap().0;
    varint::decode(&self.data[offset..]).unwrap().1
  }
}
impl ToBytes for V {
  fn to_bytes(&self) -> Result<Vec<u8>,failure::Error> {
    self.data.to_bytes()
  }
}
impl CountBytes for V {
  fn count_from_bytes(src: &[u8]) -> Result<usize,failure::Error> {
    <Vec<u8>>::count_from_bytes(src)
  }
  fn count_bytes(&self) -> usize {
    self.data.count_bytes()
  }
}
impl FromBytes for V {
  fn from_bytes(src: &[u8]) -> Result<(usize,Self),failure::Error> {
    let (size,data) = <Vec<u8>>::from_bytes(src)?;
    Ok((size, Self { data }))
  }
}

pub struct EStore {
  pub batch_size: usize,
  pub batch: Vec<eyros::Row<P,V>>,
  pub db: eyros::DB<S,T,P,V>,
  pub sync_interval: usize,
  pub flush_count: usize,
}

impl EStore {
  pub fn new(db: eyros::DB<S,T,P,V>) -> Self {
    Self {
      batch_size: 10_000,
      batch: Vec::with_capacity(10_000),
      db,
      sync_interval: 10,
      flush_count: 0,
    }
  }
  pub async fn create(&mut self, point: P, value: V) -> Result<(),Error> {
    self.batch.push(eyros::Row::Insert(point,value));
    if self.batch.len() >= self.batch_size {
      self.flush().await?;
    }
    Ok(())
  }
  pub fn push_update(&mut self, prev_point: &P, new_point: &P, value: &V) -> () {
    self.batch.extend_from_slice(&[
      eyros::Row::Delete(prev_point.clone(), value.get_id()),
      eyros::Row::Insert(new_point.clone(), value.clone()),
    ]);
  }
  pub async fn update(&mut self, prev_point: &P, new_point: &P, value: &V) -> Result<(),Error> {
    self.push_update(prev_point, new_point, value);
    self.check_flush().await?;
    Ok(())
  }
  pub async fn delete(&mut self, point: P, id: <V as Value>::Id) -> Result<(),Error> {
    self.batch.push(eyros::Row::Delete(point,id));
    self.check_flush().await?;
    Ok(())
  }
  pub async fn check_flush(&mut self) -> Result<(),Error> {
    if self.batch.len() >= self.batch_size {
      self.flush().await?;
    }
    Ok(())
  }
  pub async fn flush(&mut self) -> Result<(),Error> {
    if !self.batch.is_empty() {
      self.db.batch(&self.batch).await?;
      self.batch.clear();
      self.flush_count += 1;
      if self.flush_count >= self.sync_interval {
        self.sync().await?;
      }
    }
    Ok(())
  }
  pub async fn sync(&mut self) -> Result<(),Error> {
    if self.flush_count > 0 {
      self.db.sync().await?;
    }
    self.flush_count = 0;
    Ok(())
  }
}

#[derive(Clone,Hash,PartialOrd,PartialEq,Ord,Eq)]
pub struct Key { pub data: Vec<u8> }
impl Key {
  pub fn from(key: &[u8]) -> Self {
    Key { data: key.to_vec() }
  }
}
impl db_key::Key for Key {
  fn from_u8(key: &[u8]) -> Self { Key { data: key.into() } }
  fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T { f(&self.data) }
}

pub enum LWrite {
  Put((Key,Vec<u8>)),
  Del(Key)
}
pub enum LUpdate {
  Put(Vec<u8>),
  Del()
}

pub struct LStore {
  pub batch_size: usize,
  pub batch: Vec<LWrite>,
  pub db: Arc<Database<Key>>,
  pub cache: lru::LruCache<Key,Vec<u8>>,
  pub updates: HashMap<Key,LUpdate>,
  pub count: usize,
}

impl LStore {
  pub fn new(db: Database<Key>) -> Self {
    Self {
      batch_size: 10_000,
      batch: vec![],
      db: Arc::new(db),
      cache: lru::LruCache::new(10_000),
      updates: HashMap::new(),
      count: 0,
    }
  }
  pub fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>,Error> {
    match self.updates.get(key) {
      Some(LUpdate::Put(res)) => Ok(Some(res.to_vec())),
      Some(LUpdate::Del()) => Ok(None),
      None => match self.cache.get(key) {
        Some(buf) => Ok(Some(buf.to_vec())),
        None => {
          let res = self.db.get(ReadOptions::new(), key.clone())?;
          if let Some(r) = &res {
            self.cache.put(key.clone(), r.clone());
          }
          Ok(res)
        },
      },
    }
  }
  pub fn put(&mut self, key: Key, value: &[u8]) -> Result<(),Error> {
    self.batch.push(LWrite::Put((key.clone(), value.to_vec())));
    self.cache.pop(&key);
    if self.batch_size > 0 && self.batch.len() >= self.batch_size {
      self.flush()?;
    } else {
      self.updates.insert(key, LUpdate::Put(value.to_vec()));
    }
    self.count += 1;
    Ok(())
  }
  pub fn del(&mut self, key: Key) -> Result<(),Error> {
    self.batch.push(LWrite::Del(key.clone()));
    self.cache.pop(&key);
    if self.batch_size > 0 && self.batch.len() >= self.batch_size {
      self.flush()?;
    } else {
      self.updates.insert(key, LUpdate::Del());
    }
    self.count += 1;
    Ok(())
  }
  pub fn flush(&mut self) -> Result<(),Error> {
    let mut wbatch = Writebatch::new();
    for b in self.batch.iter() {
      match b {
        LWrite::Put((key,value)) => wbatch.put(key.clone(),value),
        LWrite::Del(key) => wbatch.delete(key.clone()),
      }
    }
    self.db.write(WriteOptions::new(), &wbatch)?;
    self.batch.clear();
    Ok(())
  }
  // does not splice records in from self.updated:
  pub fn iter(&mut self, lt: Key, gt: Key) -> impl Iterator<Item=(Key,Vec<u8>)>+'_ {
    let i = self.db.iter(ReadOptions::new());
    i.seek(&gt);
    i.take_while(move |(k,_)| *k < lt)
  }
  pub fn keys_iter(&mut self, lt: Key, gt: Key) -> impl Iterator<Item=Key>+'_ {
    let ki = self.db.keys_iter(ReadOptions::new());
    ki.seek(&gt);
    // todo: cache the sorted keys
    let mut ukeys = self.updates.iter()
      .filter(|(_,v)| match v { LUpdate::Put(_) => true, _ => false })
      .skip_while(|(k,_)| *k < &gt)
      .take_while(|(k,_)| *k < &lt)
      .map(|(key,_)| key.clone())
      .collect::<Vec<Key>>();
    ukeys.sort();
    interleaved_ordered::interleave_ordered(
      ki.take_while(move |k| *k < lt),
      ukeys
    )
  }
}
