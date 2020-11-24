//use georender_pack::encode;
mod denormalize;
use osmpbf::{Element, ElementReader};
use std::env;
use vadeen_osm::geo::Coordinate;
use vadeen_osm::osm_io::error::Error;

fn main() -> std::result::Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    let pbf = &args[1];
    let output = &args[2];

    write_denormalized_data(pbf, output)?;
    return Ok(());
}

fn write_denormalized_data(pbf: &str, output: &str) -> std::result::Result<bool, Error> {
    let reader = ElementReader::from_path(pbf).unwrap();
    let writer = denormalize::Writer::new(output);

    let mut total = 0;

    reader
        .for_each(|element| match element {
            Element::Node(node) => {
                let info = node.info();
                let author = vadeen_osm::AuthorInformation {
                    change_set: info.changeset().unwrap() as u64,
                    uid: info.uid().unwrap() as u64,
                    user: info.user().unwrap().unwrap().to_string(),
                    created: info.milli_timestamp().unwrap(),
                };
                let version = info.version().unwrap();
                let tags = node
                    .tags()
                    .into_iter()
                    .map(|t| {
                        vadeen_osm::Tag {
                            key: t.0,
                            value: t.1,
                        };
                    })
                    .collect();
                let meta = vadeen_osm::Meta {
                    version: Some(version as u32),
                    tags: tags,
                    author: Some(author),
                };
                let node = vadeen_osm::Node {
                    id: node.id(),
                    coordinate: Coordinate::new(node.lat(), node.lon()),
                    meta: meta,
                };
                let count = writer.add_node();
                total += count;
            }
            Element::DenseNode(node) => {
                let info = node.info().unwrap();
                let count = writer.add_node(vadeen_osm::Node {
                    id: node.id,
                    coordinate: Coordinate::new(node.lon(), node.lat()),
                    meta: vadeen_osm::Meta {
                        version: Some(info.version() as u32),
                        tags: node.tags().into_iter().collect(),
                        author: Some(vadeen_osm::AuthorInformation {
                            change_set: info.changeset() as u64,
                            uid: info.uid() as u64,
                            user: info.user().unwrap().to_string(),
                            created: info.milli_timestamp(),
                        }),
                    },
                });
                total += count;
            }
            Element::Relation(rel) => {
                let info = rel.info();
                let count = writer.add_relation(vadeen_osm::Relation {
                    id: way.id,
                    members: way.members().into_iter().collect(),
                    meta: vadeen_osm::Meta {
                        version: info.version(),
                        tags: way.tags().into_iter().collect(),
                        author: vadeen_osm::AuthorInformation {
                            change_set: info.changeset(),
                            uid: info.uid(),
                            user: info.user(),
                            created: info.milli_timestamp(),
                        },
                    },
                });
            }
            Element::Way(way) => {
                let info = way.info();
                let count = writer.add_way(vadeen_osm::Way {
                    id: way.id,
                    refs: way.raw_refs(),
                    meta: vadeen_osm::Meta {
                        version: info.version(),
                        tags: way.tags().into_iter().collect(),
                        author: vadeen_osm::AuthorInformation {
                            change_set: info.changeset(),
                            uid: info.uid(),
                            user: info.user(),
                            created: info.milli_timestamp(),
                        },
                    },
                });
                total += count;
            }
        })
        .unwrap();

    println!("Wrote {} elements", total);

    return Ok(true);
}
