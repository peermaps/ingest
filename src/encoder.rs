use desert::{ToBytesBE,FromBytesBE};
use crate::varint;

type Error = Box<dyn std::error::Error+Send+Sync>;
pub const ID_PREFIX: u8 = 0;

#[derive(Debug,Clone,PartialEq)]
pub enum Decoded {
  Node(DecodedNode),
  Way(DecodedWay),
  Relation(DecodedRelation)
}
#[derive(Debug,Clone,PartialEq)]
pub struct DecodedNode {
  pub id: u64,
  pub lon: f32,
  pub lat: f32,
  pub feature_type: u64,
  pub labels: Vec<u8>,
}
#[derive(Debug,Clone,PartialEq)]
pub struct DecodedWay {
  pub id: u64,
  pub feature_type: u64,
  pub is_area: bool,
  pub refs: Vec<u64>,
  pub labels: Vec<u8>,
}
#[derive(Debug,Clone,PartialEq)]
pub struct DecodedRelation {
  pub id: u64,
  pub feature_type: u64,
  pub is_area: bool,
  pub members: Vec<u64>, // id*2 + (1 for inner, 0 for outer)
  pub labels: Vec<u8>,
}

pub fn encode_osmpbf<'a>(element: &osmpbf::Element) -> Result<(Vec<u8>,Vec<u8>),Error> {
  let tags = match element {
    osmpbf::Element::Node(node) => node.tags().collect::<Vec<_>>(),
    osmpbf::Element::DenseNode(node) => node.tags().collect::<Vec<_>>(),
    osmpbf::Element::Way(way) => way.tags().collect::<Vec<_>>(),
    osmpbf::Element::Relation(relation) => relation.tags().collect::<Vec<_>>(),
  };
  let (ft,labels) = georender_pack::tags::parse(&tags)?;
  let ex_id = match element {
    osmpbf::Element::Node(node) => (node.id() as u64)*3+0,
    osmpbf::Element::DenseNode(node) => (node.id() as u64)*3+0,
    osmpbf::Element::Way(way) => (way.id() as u64)*3+1,
    osmpbf::Element::Relation(relation) => (relation.id() as u64)*3+2,
  };

  let id_bytes = id_key(ex_id)?;
  match element {
    osmpbf::Element::Node(node) => {
      let mut buf = vec![0u8;4+4+varint::length(ft)+labels.len()];
      let mut offset = 0;
      offset += (node.lon() as f32).write_bytes_be(&mut buf[offset..])?;
      offset += (node.lat() as f32).write_bytes_be(&mut buf[offset..])?;
      offset += varint::encode(ft, &mut buf[offset..])?;
      buf[offset..].copy_from_slice(&labels);
      Ok((id_bytes,buf))
    },
    osmpbf::Element::DenseNode(node) => {
      let mut buf = vec![0u8;4+4+varint::length(ft)+labels.len()];
      let mut offset = 0;
      offset += (node.lon() as f32).write_bytes_be(&mut buf[offset..])?;
      offset += (node.lat() as f32).write_bytes_be(&mut buf[offset..])?;
      offset += varint::encode(ft, &mut buf[offset..])?;
      buf[offset..].copy_from_slice(&labels);
      Ok((id_bytes,buf))
    },
    osmpbf::Element::Way(way) => {
      let refs: Vec<i64> = way.refs().into_iter().collect();
      let rsize = varint::length(refs.len() as u64)
        + refs.iter().fold(0usize,|sum,r| sum + varint::length(*r as u64));
      let is_area = osm_is_area::way(&way.tags().collect(), &way.refs().collect()) as u64;
      let fta = ft*2+is_area;
      let mut buf = vec![0u8;varint::length(fta)+rsize+labels.len()];
      let mut offset = 0;
      offset += varint::encode(fta, &mut buf[offset..])?;
      offset += varint::encode(refs.len() as u64, &mut buf[offset..])?;
      for r in refs.iter() {
        offset += varint::encode(*r as u64, &mut buf[offset..])?;
      }
      buf[offset..].copy_from_slice(&labels);
      Ok((id_bytes,buf))
    },
    osmpbf::Element::Relation(relation) => {
      let members: Vec<u64> = relation.members()
        .filter(|m| {
          let role = m.role().unwrap();
          m.member_type == osmpbf::RelMemberType::Way
            && (role == "inner" || role == "outer")
        })
        .map(|m| {
          (m.member_id as u64)*2 + match m.role().unwrap() { "inner" => 1, _ => 0 }
        })
        .collect();
      let msize = varint::length(members.len() as u64)
        + members.iter().fold(0usize,|sum,m| sum + varint::length(*m));
      let is_area = osm_is_area::relation(&relation.tags().collect(), &vec![1]) as u64;
      let fta = ft*2+is_area;
      let mut buf = vec![0u8;varint::length(fta)+msize+labels.len()];
      let mut offset = 0;
      offset += varint::encode(fta, &mut buf[offset..])?;
      offset += varint::encode(members.len() as u64, &mut buf[offset..])?;
      for m in members.iter() {
        offset += varint::encode(*m as u64, &mut buf[offset..])?;
      }
      buf[offset..].copy_from_slice(&labels);
      Ok((id_bytes,buf))
    },
  }
}

pub fn encode_o5m<'a>(dataset: &o5m_stream::Dataset) -> Result<Option<(Vec<u8>,Vec<u8>)>,Error> {
  let (ex_id,tags) = match dataset {
    o5m_stream::Dataset::Node(node) => {
      (node.id*3+0,&node.tags)
    },
    o5m_stream::Dataset::Way(way) => {
      (way.id*3+1,&way.tags)
    },
    o5m_stream::Dataset::Relation(relation) => {
      (relation.id*3+2,&relation.tags)
    },
    _ => { return Ok(None) },
  };
  eprintln!["ex_id={}, tags={:?}",ex_id,tags];
  // TODO
  Ok(Some((vec![],vec![])))
}

pub fn decode(key: &[u8], value: &[u8]) -> Result<Decoded,Error> {
  if key[0] != ID_PREFIX {
    return Err(Box::new(failure::err_msg("attempted to decode a non-ID key").compat()));
  }
  let (_,ex_id) = varint::decode(&key[1..])?;
  let id = ex_id/3;
  Ok(match ex_id%3 {
    0 => {
      let mut offset = 0;
      let (s,lon) = f32::from_bytes_be(&value[offset..])?;
      offset += s;
      let (s,lat) = f32::from_bytes_be(&value[offset..])?;
      offset += s;
      let (s,feature_type) = varint::decode(&value[offset..])?;
      offset += s;
      let labels = value[offset..].into();
      Decoded::Node(DecodedNode { id, lon, lat, feature_type, labels })
    },
    1 => {
      let mut offset = 0;
      let (s,fta) = varint::decode(&value[offset..])?;
      offset += s;
      let feature_type = fta/2;
      let is_area = match fta%2 { 0 => false, _ => true };
      let (s,rlen) = varint::decode(&value[offset..])?;
      offset += s;
      let mut refs = Vec::with_capacity(rlen as usize);
      for _ in 0..rlen {
        let (s,r) = varint::decode(&value[offset..])?;
        offset += s;
        refs.push(r);
      }
      let labels = value[offset..].into();
      Decoded::Way(DecodedWay { id, feature_type, is_area, refs, labels })
    },
    _ => {
      let mut offset = 0;
      let (s,fta) = varint::decode(&value[offset..])?;
      offset += s;
      let feature_type = fta/2;
      let is_area = match fta%2 { 0 => false, _ => true };
      let (s,mlen) = varint::decode(&value[offset..])?;
      offset += s;
      let mut members = Vec::with_capacity(mlen as usize);
      for _ in 0..mlen {
        let (s,m) = varint::decode(&value[offset..])?;
        offset += s;
        members.push(m);
      }
      let labels = value[offset..].into();
      Decoded::Relation(DecodedRelation { id, feature_type, is_area, members, labels })
    },
  })
}

pub fn id_key(ex_id: u64) -> Result<Vec<u8>,Error> {
  let mut id_bytes = vec![0u8;1+varint::length(ex_id)];
  id_bytes[0] = ID_PREFIX;
  varint::encode(ex_id, &mut id_bytes[1..])?;
  Ok(id_bytes)
}
