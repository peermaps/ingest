use crate::encoder::{Decoded,DecodedNode,DecodedWay,DecodedRelation};
use crate::Error;
use std::collections::HashMap;
use desert::{varint,CountBytesBE,ToBytesBE,FromBytesBE};
use osmxq::{Record,RecordId,Position};

impl Record for Decoded {
  fn get_id(&self) -> RecordId {
    match self {
      Decoded::Node(node) => node.id*3+0,
      Decoded::Way(way) => way.id*3+1,
      Decoded::Relation(relation) => relation.id*3+2,
    }
  }
  fn get_refs(&self) -> Vec<RecordId> {
    match self {
      Decoded::Node(_node) => vec![],
      Decoded::Way(way) => way.refs.iter()
        .map(|w| w*3+0).collect::<Vec<RecordId>>(),
      Decoded::Relation(relation) => relation.members.iter()
        .map(|m| m/2*3+1).collect::<Vec<RecordId>>(),
    }
  }
  fn get_position(&self) -> Option<Position> {
    match self {
      Decoded::Node(node) => Some((node.lon,node.lat)),
      Decoded::Way(_way) => None,
      Decoded::Relation(_relation) => None,
    }
  }
  fn pack(records: &HashMap<RecordId,Self>) -> Vec<u8> where Self: Sized {
    let mut size = 0;
    for (r_id,r) in records {
      size += varint::length(*r_id);
      match &r {
        Decoded::Node(node) => {
          size += node.lon.count_bytes_be();
          size += node.lat.count_bytes_be();
          size += varint::length(node.feature_type);
          size += node.labels.len();
        },
        Decoded::Way(way) => {
          let fta = way.feature_type*2 + match way.is_area { false => 0, true => 1};
          size += varint::length(fta);
          size += varint::length(way.refs.len() as u64);
          for wr in way.refs.iter() {
            size += varint::length(*wr as u64);
          }
          size += way.labels.len();
        },
        Decoded::Relation(relation) => {
          let fta = relation.feature_type*2 + match relation.is_area { false => 0, true => 1};
          size += varint::length(fta);
          size += varint::length(relation.members.len() as u64);
          for m in relation.members.iter() {
            size += varint::length(*m as u64);
          }
          size += relation.labels.len();
        },
      }
    }
    let mut buf = vec![0u8;size];
    let mut offset = 0;
    for (r_id,r) in records {
      //assert_eq![*r_id, r.get_id(), "r_id != r.get_id()"];
      offset += varint::encode(*r_id, &mut buf[offset..]).unwrap();
      match &r {
        Decoded::Node(node) => {
          offset += node.lon.write_bytes_be(&mut buf[offset..]).unwrap();
          offset += node.lat.write_bytes_be(&mut buf[offset..]).unwrap();
          offset += varint::encode(node.feature_type, &mut buf[offset..]).unwrap();
          buf[offset..offset+node.labels.len()].copy_from_slice(&node.labels);
          offset += node.labels.len();
        },
        Decoded::Way(way) => {
          let fta = way.feature_type*2 + match way.is_area { false => 0, true => 1};
          offset += varint::encode(fta, &mut buf[offset..]).unwrap();
          offset += varint::encode(way.refs.len() as u64, &mut buf[offset..]).unwrap();
          for wr in way.refs.iter() {
            offset += varint::encode(*wr as u64, &mut buf[offset..]).unwrap();
          }
          buf[offset..offset+way.labels.len()].copy_from_slice(&way.labels);
          offset += way.labels.len();
        },
        Decoded::Relation(relation) => {
          let fta = relation.feature_type*2 + match relation.is_area { false => 0, true => 1};
          offset += varint::encode(fta, &mut buf[offset..]).unwrap();
          offset += varint::encode(relation.members.len() as u64, &mut buf[offset..]).unwrap();
          for m in relation.members.iter() {
            offset += varint::encode(*m as u64, &mut buf[offset..]).unwrap();
          }
          buf[offset..offset+relation.labels.len()].copy_from_slice(&relation.labels);
          offset += relation.labels.len();
        },
      }
    }
    //assert_eq![buf.len(), offset, "buf.len() != offset ({} != {})", buf.len(), offset];
    buf
  }
  fn unpack(buf: &[u8], records: &mut HashMap<RecordId,Self>) -> Result<usize,Error> where Self: Sized {
    if buf.is_empty() { return Ok(0) }
    let mut offset = 0;
    while offset < buf.len() {
      let (s,xid) = varint::decode(&buf[offset..]).unwrap();
      offset += s;
      let id = xid/3;
      records.insert(xid, match xid%3 {
        0 => {
          let (s,lon) = f32::from_bytes_be(&buf[offset..]).unwrap();
          offset += s;
          let (s,lat) = f32::from_bytes_be(&buf[offset..]).unwrap();
          offset += s;
          let (s,feature_type) = varint::decode(&buf[offset..]).unwrap();
          offset += s;
          let label_len = georender_pack::label::scan(&buf[offset..]).unwrap();
          let labels = buf[offset..offset+label_len].into();
          offset += label_len;
          Decoded::Node( DecodedNode { id, lon, lat, feature_type, labels })
        },
        1 => {
          let (s,fta) = varint::decode(&buf[offset..]).unwrap();
          offset += s;
          let is_area = match fta%2 { 0 => false, _ => true };
          let feature_type = fta/2;
          let (s,ref_len) = varint::decode(&buf[offset..]).unwrap();
          offset += s;
          let mut refs = Vec::with_capacity(ref_len as usize);
          for _ in 0..ref_len {
            let (s,r) = varint::decode(&buf[offset..]).unwrap();
            offset += s;
            refs.push(r);
          }
          let label_len = georender_pack::label::scan(&buf[offset..]).unwrap();
          let labels = buf[offset..offset+label_len].into();
          offset += label_len;
          Decoded::Way(DecodedWay { id, feature_type, is_area, refs, labels })
        },
        _ => {
          let (s,fta) = varint::decode(&buf[offset..]).unwrap();
          offset += s;
          let is_area = match fta%2 { 0 => false, _ => true };
          let feature_type = fta/2;
          let (s,m_len) = varint::decode(&buf[offset..]).unwrap();
          offset += s;
          let mut members = Vec::with_capacity(m_len as usize);
          for _ in 0..m_len {
            let (s,r) = varint::decode(&buf[offset..]).unwrap();
            offset += s;
            members.push(r);
          }
          let label_len = georender_pack::label::scan(&buf[offset..]).unwrap();
          let labels = buf[offset..offset+label_len].into();
          offset += label_len;
          Decoded::Relation(DecodedRelation { id, feature_type, is_area, members, labels })
        },
      });
    }
    //assert_eq![buf.len(), offset, "buf.len() != offset ({} != {})", buf.len(), offset];
    Ok(offset)
  }
}
