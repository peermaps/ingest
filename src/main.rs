//use georender_pack::encode;
use std::vec::Vec;
mod denormalize;
use osmpbf::{Element, ElementReader};
use std::env;
use vadeen_osm::{geo::Coordinate, Tag, Way, Node, Relation};
use vadeen_osm::osm_io::error::Error;

fn main() -> std::result::Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    let pbf = &args[1];
    let output = &args[2];

    write_denormalized_data(pbf, output)?;
    return Ok(());
}

fn get_meta (_tags: osmpbf::TagIter, info : osmpbf::Info) -> vadeen_osm::Meta {
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

fn get_dense_meta (_tags: osmpbf::DenseTagIter, info : osmpbf::DenseNodeInfo) -> vadeen_osm::Meta {
    let author = vadeen_osm::AuthorInformation {
        change_set: info.changeset() as u64,
        uid: info.uid() as u64,
        user: info.user().unwrap().to_string(),
        created: info.milli_timestamp(),
    };

    let mut tags= Vec::with_capacity(_tags.len());
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
    let mut writer = denormalize::Writer::new(output);
    let mut total = 0;

    reader
        .for_each(|element| match element {
            Element::Node(node) => {
                let node = Node {
                    id: node.id(),
                    coordinate: Coordinate::new(node.lat(), node.lon()),
                    meta: get_meta(node.tags(), node.info())
                };
                let count = writer.add_node(node);
                total += count;
            }
            Element::DenseNode(node) => {
                let count = writer.add_node(vadeen_osm::Node {
                    id: node.id,
                    coordinate: Coordinate::new(node.lon(), node.lat()),
                    meta: get_dense_meta(node.tags(), node.info().unwrap().clone())
                });
                total += count;
            }
            Element::Relation(rel) => {
                let mut members = Vec::new();
                for member in rel.members() {
                    let var_name = match member.member_type {
                        osmpbf::RelMemberType::Node => {
                            vadeen_osm::RelationMember::Node (
                                member.member_id, "node".to_string()
                            )
                        }
                        osmpbf::RelMemberType::Way => {
                            vadeen_osm::RelationMember::Way(
                                member.member_id, "way".to_string()
                            )
                        }
                        osmpbf::RelMemberType::Relation => {
                            vadeen_osm::RelationMember::Relation(
                                member.member_id, "relation".to_string()
                            )
                        }
                    };
                    members.push(var_name);
                }
                let count = writer.add_relation(Relation {
                    id: rel.id(),
                    members: members,
                    meta: get_meta(rel.tags(), rel.info())
                });
            }
            Element::Way(way) => {
                let count = writer.add_way(Way {
                    id: way.id(),
                    refs: way.refs().collect(),
                    meta: get_meta(way.tags(), way.info())
                });
                total += count;
            }
        })
        .unwrap();

    println!("Wrote {} elements", total);

    return Ok(true);
}
