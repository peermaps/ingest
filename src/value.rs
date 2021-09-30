use desert::{varint,ToBytes,FromBytes,CountBytes};
use crate::Error;

#[derive(Debug,Clone,Hash)]
pub struct V {
  pub data: Vec<u8>,
}
impl V {
  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }
  pub fn len(&self) -> usize {
    self.data.len()
  }
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
    let mut offset = 0;
    let (s,_len) = varint::decode(&self.data[offset..]).unwrap();
    offset += s;
    offset += 1; // feature_type
    let (s,_type) = varint::decode(&self.data[offset..]).unwrap();
    offset += s;
    let (_s,id) = varint::decode(&self.data[offset..]).unwrap();
    id
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
