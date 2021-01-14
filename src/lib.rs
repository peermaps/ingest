#![feature(nll, async_closure)]

mod writer;
pub use writer::*;

mod reader;
pub use reader::*;

mod tags;

use eyros::{Mix, Mix2, Row, DB};
use georender_pack::encode;
use osmpbf::{Element, ElementReader};
use std::iter::Iterator;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::vec::Vec;
use vadeen_osm::osm_io::error::Error;
use vadeen_osm::{geo::Coordinate, Node, Relation, Way};

type P = Mix2<f64, f64>;
type V = Vec<u8>;
type E = Box<dyn std::error::Error + Sync + Send>;

pub async fn write_to_db(output: &str, db: &str) -> Result<(), E> {
    let reader = Reader::new(output);
    let mut db: DB<_, P, V> = DB::open_from_path(&PathBuf::from(db)).await?;

    for entry in reader.walk_nodes() {
        let buf = entry?.path();
        let filepath = buf.to_str().unwrap();

        let retain = filepath.ends_with("o5m");
        if !retain {
            continue;
        }

        println!("entry {:}", filepath);
        let osm = reader.read_raw(filepath);
        let node = osm.nodes[0].clone();
        let id = node.id as u64;
        let point = (node.coordinate.lon(), node.coordinate.lat());
        let tags = node
            .meta
            .tags
            .iter()
            .map(|t| (t.key.as_ref(), t.value.as_ref()))
            .collect();

        let value = encode::node(id, point, tags)?;
        let row = Row::Insert(Mix2::new(Mix::Scalar(point.0), Mix::Scalar(point.1)), value);

        let mut batch = Vec::new();
        batch.push(row);

        db.batch(&batch.as_slice()).await?;
    }
    return Ok(());
}

pub fn denormalize(pbf: &str, output: &str) -> Result<bool, Error> {
    let reader = ElementReader::from_path(pbf).unwrap();
    let writer = Arc::new(Mutex::new(Writer::new(output)));

    let total = reader
        .par_map_reduce(
            |element| match element {
                Element::Node(node) => {
                    let node = Node {
                        id: node.id(),
                        coordinate: Coordinate::new(node.lat(), node.lon()),
                        meta: tags::get_meta(node.tags(), node.info()),
                    };
                    let count = writer.lock().unwrap().add_node(node);
                    return count;
                }
                Element::DenseNode(node) => {
                    let count = writer.lock().unwrap().add_node(vadeen_osm::Node {
                        id: node.id,
                        coordinate: Coordinate::new(node.lon(), node.lat()),
                        meta: tags::get_dense_meta(node.tags(), node.info().unwrap().clone()),
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
                        meta: tags::get_meta(rel.tags(), rel.info()),
                    });
                    return count;
                }
                Element::Way(way) => {
                    let count = writer.lock().unwrap().add_way(Way {
                        id: way.id(),
                        refs: way.refs().collect(),
                        meta: tags::get_meta(way.tags(), way.info()),
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

#[async_std::test]
async fn read_write_fixture() -> Result<(), E> {
    use crate::Writer;
    use async_std::prelude::*;
    use async_std::stream::*;
    use std::fs;
    use vadeen_osm::OsmBuilder;

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
    let coordinates = (66.19, 1.3);
    builder.add_point(coordinates, vec![("power", "tower")]);

    // Build into Osm structure.
    let osm = builder.build();

    let output = "denormalized_test";
    let mut writer = Writer::new(output);

    let node = osm.nodes[0].clone();
    let rel = osm.relations[0].clone();
    let way = osm.ways[0].clone();

    writer.add_node(osm.nodes[0].clone());
    writer.add_way(osm.ways[0].clone());
    writer.add_relation(osm.relations[0].clone());

    let reader = Reader::new(output);
    let read_node = reader.read_node(node.id as u64);
    let read_way = reader.read_way(way.id as u64);
    let read_rel = reader.read_relation(rel.id as u64);

    assert_eq!(read_node.id, node.id);
    assert_eq!(read_node.coordinate, node.coordinate);
    assert_eq!(read_node.meta, node.meta);

    assert_eq!(read_way.id, way.id);
    assert_eq!(read_way.refs, way.refs);
    assert_eq!(read_way.meta, way.meta);

    assert_eq!(read_rel.id, rel.id);
    assert_eq!(read_rel.members, rel.members);
    assert_eq!(read_rel.meta, rel.meta);

    let db_path = "test_db";
    write_to_db(output, db_path).await;

    let mut db: DB<_, P, V> = DB::open_from_path(&PathBuf::from(db_path)).await?;

    let bbox = ((180.0, -180.0), (90.0, -90.0));
    let mut stream = db.query(&bbox).await?;

    while let Some(result) = stream.next().await {
        println!("{:?}", result?);
    }

    //fs::remove_dir_all(output);
    //fs::remove_dir_all(db_path);

    Ok(())
}
