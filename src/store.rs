use leveldb::database::{Database,batch::{Batch,Writebatch}};
use leveldb::{options::{ReadOptions,WriteOptions},kv::KV};
use leveldb::iterator::{Iterable,LevelDBIterator};
use std::collections::HashMap;
use desert::{ToBytes,FromBytes,CountBytes,varint};
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
  fn to_bytes(&self) -> Result<Vec<u8>,Error> {
    self.data.to_bytes()
  }
  fn write_bytes(&self, buf: &mut [u8]) -> Result<usize,Error> {
    self.data.write_bytes(buf)
  }
}
impl CountBytes for V {
  fn count_from_bytes(src: &[u8]) -> Result<usize,Error> {
    <Vec<u8>>::count_from_bytes(src)
  }
  fn count_bytes(&self) -> usize {
    self.data.count_bytes()
  }
}
impl FromBytes for V {
  fn from_bytes(src: &[u8]) -> Result<(usize,Self),Error> {
    let (size,data) = <Vec<u8>>::from_bytes(src)?;
    Ok((size, Self { data }))
  }
}

#[derive(Debug,Clone,Hash)]
pub enum Op {
  Insert(usize),
  Update(usize,usize), // delete, insert
  Delete(usize),
}

pub struct EStore {
  pub batch_size: usize,
  pub batch: Vec<Option<eyros::Row<P,V>>>,
  pub inserts: HashMap<<V as Value>::Id,Op>,
  pub db: eyros::DB<S,T,P,V>,
  pub sync_interval: usize,
  pub flush_count: usize,
}

impl EStore {
  pub fn new(db: eyros::DB<S,T,P,V>) -> Self {
    Self {
      batch_size: 10_000,
      batch: Vec::with_capacity(10_000),
      inserts: HashMap::new(),
      db,
      sync_interval: 10,
      flush_count: 0,
    }
  }
  pub fn push_create(&mut self, point: P, value: V) -> () {
    self.inserts.insert(value.get_id(), Op::Insert(self.batch.len()));
    self.batch.push(Some(eyros::Row::Insert(point,value)));
  }
  pub async fn create(&mut self, point: P, value: V) -> Result<(),Error> {
    self.push_create(point, value);
    self.check_flush().await?;
    Ok(())
  }
  pub fn push_update(&mut self, prev_point: &P, new_point: &P, value: &V) -> () {
    let id = value.get_id();
    let replace = match self.inserts.get_mut(&id) {
      Some(Op::Insert(i)) => {
        // replace existing insert
        self.batch[*i] = Some(eyros::Row::Insert(new_point.clone(), value.clone()));
        None
      },
      Some(Op::Update(_,i)) => {
        // replace insert, leaving delete
        self.batch[*i] = Some(eyros::Row::Insert(new_point.clone(), value.clone()));
        None
      },
      Some(Op::Delete(i)) => {
        // leave existing delete, push new insert
        let j = self.batch.len();
        self.batch[j] = Some(eyros::Row::Insert(new_point.clone(), value.clone()));
        Some(Op::Update(*i,j))
      },
      None => {
        let i = self.batch.len();
        let j = i+1;
        self.batch.extend_from_slice(&[
          Some(eyros::Row::Delete(prev_point.clone(), id)),
          Some(eyros::Row::Insert(new_point.clone(), value.clone())),
        ]);
        Some(Op::Update(i,j))
      },
    };
    if let Some(v) = replace {
      self.inserts.insert(id, v);
    }
  }
  pub async fn update(&mut self, prev_point: &P, new_point: &P, value: &V) -> Result<(),Error> {
    self.push_update(prev_point, new_point, value);
    self.check_flush().await?;
    Ok(())
  }
  pub fn push_delete(&mut self, point: P, id: <V as Value>::Id) -> () {
    let (rm,replace) = match self.inserts.get(&id) {
      Some(Op::Insert(i)) => {
        self.batch[*i] = None;
        (true,None)
      },
      Some(Op::Update(i,j)) => {
        self.batch[*j] = None;
        (false,Some(Op::Delete(*i)))
      },
      Some(Op::Delete(_)) => {
        // no-op to delete again
        (false,None)
      },
      None => {
        let i = self.batch.len();
        self.batch.push(Some(eyros::Row::Delete(point,id)));
        (false,Some(Op::Delete(i)))
      },
    };
    if rm {
      self.inserts.remove(&id);
    } else if let Some(v) = replace {
      self.inserts.insert(id, v);
    }
  }
  pub async fn delete(&mut self, point: P, id: <V as Value>::Id) -> Result<(),Error> {
    self.push_delete(point, id);
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
      self.db.batch(&self.batch
        .iter()
        .filter(|row| row.is_some())
        .map(|row| row.as_ref().unwrap().clone())
        .collect::<Vec<_>>()
      ).await?;
      self.batch.clear();
      self.inserts.clear();
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
#[derive(PartialOrd,PartialEq,Ord,Eq)]
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
    let mut ukeys = self.updates.iter().collect::<Vec<_>>();
    ukeys.sort();
    let lt_c = lt.clone();
    let mut prev = None;
    interleaved_ordered::interleave_ordered(
      ki.take_while(move |k| k < &lt_c),
      ukeys.into_iter()
        .filter(|(_,v)| match v { LUpdate::Put(_) => true, _ => false })
        .skip_while(move |(k,_)| *k < &gt)
        .take_while(move |(k,_)| *k < &lt)
        .map(|(key,_)| (*key).clone())
    ).filter(move |k| {
      let cur = Some(k.clone());
      let res = prev != cur;
      prev = cur;
      res
    })
  }
}
