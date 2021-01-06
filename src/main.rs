//use georender_pack::encode;
use std::sync::{Arc, Mutex};
use std::vec::Vec;
mod denormalize;
use osmpbf::{Element, ElementReader};
use std::env;
use vadeen_osm::osm_io::error::Error;
use vadeen_osm::{geo::Coordinate, Node, OsmBuilder, Relation, Tag, Way};

fn main() -> std::result::Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    let pbf = &args[1];
    let output = &args[2];

    write_denormalized_data(pbf, output)?;
    return Ok(());
}

fn get_meta(_tags: osmpbf::TagIter, info: osmpbf::Info) -> vadeen_osm::Meta {
    let author = vadeen_osm::AuthorInformation {
        change_set: info.changeset().unwrap() as u64,
        uid: info.uid().unwrap() as u64,
        user: info.user().unwrap().unwrap().to_string(),
        created: info.milli_timestamp().unwrap(),
    };

    let mut tags = Vec::with_capacity(_tags.len());
    _tags.for_each(|t| {
        tags.push(Tag {
            key: t.0.to_string(),
            value: t.1.to_string(),
        })
    });

    return vadeen_osm::Meta {
        version: Some(info.version().unwrap() as u32),
        tags: tags,
        author: Some(author),
    };
}

fn get_dense_meta(_tags: osmpbf::DenseTagIter, info: osmpbf::DenseNodeInfo) -> vadeen_osm::Meta {
    let author = vadeen_osm::AuthorInformation {
        change_set: info.changeset() as u64,
        uid: info.uid() as u64,
        user: info.user().unwrap().to_string(),
        created: info.milli_timestamp(),
    };

    let mut tags = Vec::with_capacity(_tags.len());
    _tags.for_each(|t| {
        tags.push(Tag {
            key: t.0.to_string(),
            value: t.1.to_string(),
        })
    });
    return vadeen_osm::Meta {
        version: Some(info.version() as u32),
        tags: tags,
        author: Some(author),
    };
}

fn write_denormalized_data(pbf: &str, output: &str) -> std::result::Result<bool, Error> {
    let reader = ElementReader::from_path(pbf).unwrap();
    let writer = Arc::new(Mutex::new(denormalize::Writer::new(output)));

    let total = reader
        .par_map_reduce(
            |element| match element {
                Element::Node(node) => {
                    let node = Node {
                        id: node.id(),
                        coordinate: Coordinate::new(node.lat(), node.lon()),
                        meta: get_meta(node.tags(), node.info()),
                    };
                    let count = writer.lock().unwrap().add_node(node);
                    return count;
                }
                Element::DenseNode(node) => {
                    let count = writer.lock().unwrap().add_node(vadeen_osm::Node {
                        id: node.id,
                        coordinate: Coordinate::new(node.lon(), node.lat()),
                        meta: get_dense_meta(node.tags(), node.info().unwrap().clone()),
                    });
                    return count;
                }
                Element::Relation(rel) => {
                    let mut members = Vec::new();
                    for member in rel.members() {
                        let var_name = match member.member_type {
                            osmpbf::RelMemberType::Node => vadeen_osm::RelationMember::Node(
                                member.member_id,
                                "node".to_string(),
                            ),
                            osmpbf::RelMemberType::Way => {
                                vadeen_osm::RelationMember::Way(member.member_id, "way".to_string())
                            }
                            osmpbf::RelMemberType::Relation => {
                                vadeen_osm::RelationMember::Relation(
                                    member.member_id,
                                    "relation".to_string(),
                                )
                            }
                        };
                        members.push(var_name);
                    }
                    let count = writer.lock().unwrap().add_relation(Relation {
                        id: rel.id(),
                        members: members,
                        meta: get_meta(rel.tags(), rel.info()),
                    });
                    return count;
                }
                Element::Way(way) => {
                    let count = writer.lock().unwrap().add_way(Way {
                        id: way.id(),
                        refs: way.refs().collect(),
                        meta: get_meta(way.tags(), way.info()),
                    });
                    return count;
                }
            },
            || 0_u64,
            |a, b| a + b,
        )
        .unwrap();

    println!("Wrote {} elements", total);

    return Ok(true);
}

#[test]
fn read_write_fixture() {
    // Create a builder.
    let mut builder = OsmBuilder::default();

    // Add a polygon to the map.
    builder.add_polygon(
        vec![
            vec![
                // Outer polygon
                (66.29, -3.177),
                (66.29, -0.9422),
                (64.43, -0.9422),
                (64.43, -3.177),
                (66.29, -3.177),
            ],
            vec![
                // One inner polygon
                (66.0, -2.25),
                (65.7, -2.5),
                (65.7, -2.0),
                (66.0, -2.25),
            ],
            // Add more inner polygons here.
        ],
        vec![("natural", "water")],
    );

    // Add polyline to the map.
    builder.add_polyline(vec![(66.29, 1.2), (64.43, 1.2)], vec![("power", "line")]);

    // Add point
    builder.add_point((66.19, 1.3), vec![("power", "tower")]);

    // Build into Osm structure.
    let osm = builder.build();

    let output = "testoutput";
    let mut writer = denormalize::Writer::new(output);
    let node = osm.nodes[0].clone();
    let rel = osm.relations[0].clone();
    let way = osm.ways[0].clone();
    writer.add_node(osm.nodes[0].clone());
    writer.add_way(osm.ways[0].clone());
    writer.add_relation(osm.relations[0].clone());

    let reader = denormalize::Reader::new(output);
    let read_node = reader.read_node(node.id);
    let read_way = reader.read_way(way.id);
    let read_rel = reader.read_relation(rel.id);
    assert_eq!(read_node.id, node.id);
    assert_eq!(read_node.coordinate, node.coordinate);
    assert_eq!(read_node.meta, node.meta);

    assert_eq!(read_way.id, way.id);
    assert_eq!(read_way.refs, way.refs);
    assert_eq!(read_way.meta, way.meta);

    assert_eq!(read_rel.id, rel.id);
    assert_eq!(read_rel.members, rel.members);
    assert_eq!(read_rel.meta, rel.meta);
}
