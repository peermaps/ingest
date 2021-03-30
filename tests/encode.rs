use peermaps_ingest::encoder::{encode,decode,Decoded,DecodedNode,DecodedWay,DecodedRelation};

type Error = Box<dyn std::error::Error+Send+Sync>;

#[test]
fn encoder() -> Result<(),Error> {
  osmpbf::ElementReader::from_path("tests/data/node.pbf")?.for_each(|element| {
    let (key,value) = encode(&element).unwrap();
    let decoded = decode(&key,&value).unwrap();
    assert_eq![decoded, Decoded::Node(DecodedNode {
      id: 1312,
      feature_type: 712, // place.other
      lon: 13.0,
      lat: 37.0,
      labels: vec![0],
    })];
  })?;
  osmpbf::ElementReader::from_path("tests/data/way.pbf")?.for_each(|element| {
    let (key,value) = encode(&element).unwrap();
    let decoded = decode(&key,&value).unwrap();
    assert_eq![decoded, Decoded::Way(DecodedWay {
      id: 555,
      feature_type: 47, // amenity.cafe
      refs: vec![600,601,602],
      labels: vec![0],
      is_area: false,
    })];
  })?;
  osmpbf::ElementReader::from_path("tests/data/relation.pbf")?.for_each(|element| {
    let (key,value) = encode(&element).unwrap();
    let decoded = decode(&key,&value).unwrap();
    assert_eq![decoded, Decoded::Relation(DecodedRelation {
      id: 700,
      feature_type: 644, // natural.water
      members: vec![701*2+0,702*2+1,703*2+0],
      labels: "\x0e=lake whatever\x05x=...\x00".into(),
      is_area: false,
    })];
  })?;
  Ok(())
}
