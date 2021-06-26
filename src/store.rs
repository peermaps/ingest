use rocksdb::{DBWithThreadMode,MultiThreaded,SnapshotWithThreadMode,
  WriteBatch,WriteOptions,IteratorMode,Direction};
use std::collections::HashMap;
use desert::{ToBytes,FromBytes,CountBytes,varint};
use eyros::Value;
use async_std::sync::Arc;

type Error = Box<dyn std::error::Error+Send+Sync>;

type S = random_access_disk::RandomAccessDisk;
type P = (eyros::Coord<f32>,eyros::Coord<f32>);
type T = eyros::Tree2<f32,f32,V>;
pub type EDB = eyros::DB<S,T,P,V>;

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
  pub db: EDB,
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
  pub fn is_full(&mut self) -> bool {
    self.batch.len() >= self.batch_size
  }
  pub async fn check_flush(&mut self) -> Result<(),Error> {
    if self.is_full() {
      self.flush().await?;
    }
    Ok(())
  }
  pub async fn optimize(&mut self, rebuild_depth: usize) -> Result<(),Error> {
    self.db.optimize(rebuild_depth).await
  }
  pub async fn flush(&mut self) -> Result<(),Error> {
    if !self.batch.is_empty() {
      let opts = eyros::BatchOptions::new()
        .error_if_missing(false)
        .rebuild_depth(4);
      self.db.batch_with_options(&self.batch
        .iter()
        .filter(|row| row.is_some())
        .map(|row| row.as_ref().unwrap().clone())
        .collect::<Vec<_>>(),
        &opts
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

pub enum LWrite {
  Put((Vec<u8>,Vec<u8>)),
  Del(Vec<u8>)
}
#[derive(PartialOrd,PartialEq,Ord,Eq)]
pub enum LUpdate {
  Put(Vec<u8>),
  Del()
}

macro_rules! impl_lstore {
  ($LStore:ident, ($($L:tt),*), $DB:ty) => {
    pub struct $LStore<$($L),*> {
      pub batch_size: usize,
      pub batch: Vec<LWrite>,
      pub db: Arc<$DB>,
      pub cache: lru::LruCache<Vec<u8>,Vec<u8>>,
      pub updates: HashMap<Vec<u8>,LUpdate>,
      pub count: usize,
    }

    impl<$($L),*> $LStore<$($L),*> {
      pub fn new(db: $DB) -> Self {
        Self {
          batch_size: 100_000,
          batch: vec![],
          db: Arc::new(db),
          cache: lru::LruCache::new(100_000),
          updates: HashMap::new(),
          count: 0,
        }
      }
      pub fn new_with_same_db(&self) -> Self {
        Self {
          batch_size: 100_000,
          batch: vec![],
          db: self.db.clone(),
          cache: lru::LruCache::new(100_000),
          updates: HashMap::new(),
          count: 0,
        }
      }
      pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>,Error> {
        match self.updates.get(key) {
          Some(LUpdate::Put(res)) => Ok(Some(res.to_vec())),
          Some(LUpdate::Del()) => Ok(None),
          None => match self.cache.get(&key.to_vec()) {
            Some(buf) => Ok(Some(buf.to_vec())),
            None => {
              let res = self.db.get(key)?;
              if let Some(r) = &res {
                self.cache.put(key.to_vec(), r.clone());
              }
              Ok(res)
            },
          },
        }
      }
      // does not splice records in from self.updated:
      pub fn iter(&mut self, lt: &[u8], gt: &[u8]) -> impl Iterator<Item=(Vec<u8>,Vec<u8>)>+'_ {
        let i = self.db.iterator(IteratorMode::From(gt, Direction::Forward));
        let lt_b = lt.to_vec().into_boxed_slice();
        i.take_while(move |(k,_)| *k < lt_b)
          .map(|(k,v)| (k.to_vec(),v.to_vec()))
      }
      pub fn keys_iter(&mut self, lt: Vec<u8>, gt: Vec<u8>) -> impl Iterator<Item=Vec<u8>>+'_ {
        let i = self.db.iterator(IteratorMode::From(&gt, Direction::Forward));
        // todo: cache the sorted keys
        let mut ukeys = self.updates.iter().collect::<Vec<_>>();
        ukeys.sort();
        let lt_c = lt.clone();
        let mut prev = None;
        interleaved_ordered::interleave_ordered(
          i.map(|(k,_)| k.to_vec()).take_while(move |k| k < &lt_c),
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
  }
}
impl_lstore![LStore, (), DBWithThreadMode<MultiThreaded>];
impl_lstore![LStoreSnapshot, ('a), SnapshotWithThreadMode<'a,DBWithThreadMode<MultiThreaded>>];

impl LStore {
  pub fn snapshot(&self) -> LStoreSnapshot {
    LStoreSnapshot {
      batch_size: self.batch_size,
      batch: vec![],
      db: Arc::new(self.db.snapshot()),
      cache: lru::LruCache::new(10_000),
      updates: HashMap::new(),
      count: 0,
    }
  }
  pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(),Error> {
    self.batch.push(LWrite::Put((key.to_vec(), value.to_vec())));
    self.cache.pop(&key.to_vec());
    if self.batch_size > 0 && self.batch.len() >= self.batch_size {
      self.flush()?;
    } else {
      self.updates.insert(key.to_vec(), LUpdate::Put(value.to_vec()));
    }
    self.count += 1;
    Ok(())
  }
  pub fn del(&mut self, key: &[u8]) -> Result<(),Error> {
    self.batch.push(LWrite::Del(key.to_vec()));
    self.cache.pop(&key.to_vec());
    if self.batch_size > 0 && self.batch.len() >= self.batch_size {
      self.flush()?;
    } else {
      self.updates.insert(key.to_vec(), LUpdate::Del());
    }
    self.count += 1;
    Ok(())
  }
  pub fn flush(&mut self) -> Result<(),Error> {
    if !self.batch.is_empty() {
      let mut wbatch = WriteBatch::default();
      for b in self.batch.drain(..) {
        match b {
          LWrite::Put((key,value)) => wbatch.put(key,value),
          LWrite::Del(key) => wbatch.delete(key),
        }
      }
      self.db.write_opt(wbatch, &WriteOptions::new())?;
      self.updates.clear();
    }
    Ok(())
  }
  pub fn sync(&mut self) -> Result<(),Error> {
    self.flush()?;
    self.db.flush()?;
    Ok(())
  }
}
