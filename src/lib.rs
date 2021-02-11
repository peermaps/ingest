#![feature(nll, async_closure)]

mod writer;
pub use writer::*;

mod reader;
pub use reader::*;

mod tags;

use eyros::{Mix, Mix2, Row, DB};
use georender_pack::encode;
use osm_is_area;
use osmpbf::{Element, ElementReader};
use std::collections::HashMap;
use std::iter::Iterator;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::vec::Vec;
use vadeen_osm::osm_io::error::Error;
use vadeen_osm::{geo::Coordinate, Node, Relation, Way};

type P = Mix2<f64, f64>;
type V = Vec<u8>;
type E = Box<dyn std::error::Error + Sync + Send + 'static>;

struct PointDependencies {
    values: HashMap<i64, (f64, f64)>,
    reader: Reader,
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
}

impl PointDependencies {
    pub fn new<'a>(output: &str) -> PointDependencies {
        return PointDependencies {
            values: HashMap::new(),
            reader: Reader::new(output),
            xmin: 0.0,
            ymin: 0.0,
            xmax: 0.0,
            ymax: 0.0,
        };
    }

    pub fn insert(&mut self, id: i64) {
        match self.reader.read_node(id as u64) {
            Some(node) => {
                let point = (node.coordinate.lon(), node.coordinate.lat());
                self.xmin = point.0.min(self.xmin);
                self.ymin = point.1.min(self.ymin);
                self.xmax = point.0.max(self.xmax);
                self.ymax = point.1.max(self.ymax);
                self.values.insert(id, point);
            }
            None => {
                println!("Failed to read node with id {}", id);
            }
        }
    }

    pub fn insert_way(&mut self, id: i64) {
        match self.reader.read_way(id as u64) {
            Some(way) => {
                for id in way.refs {
                    self.insert(id);
                }
            }
            None => {
                println!("Failed to read node with id {}", id);
            }
        }
    }
}

pub async fn write_to_db(output: &str, db: &str) -> Result<(), E> {
    let reader = Reader::new(output);
    let mut db: DB<_, P, V> = DB::open_from_path(&PathBuf::from(db)).await?;

    let mut deps = PointDependencies::new(output);

    for osm in reader.walk() {
        if osm.relations.len() > 0 {
            let rel = osm.relations[0].clone();
            let id = rel.id as u64;
            let members = rel.members.clone();
            let mut member_ids: Vec<i64> = Vec::with_capacity(members.len());

            for member in members {
                let id = member.ref_id();
                member_ids.push(id);

                match member.role() {
                    "node" => deps.insert(id),
                    "way" => deps.insert_way(id),
                    "relation" => {
                        // TODO: relation within a relation???
                    }
                    _ => {}
                }
            }

            let tags = rel
                .meta
                .tags
                .iter()
                .map(|t| (t.key.as_ref(), t.value.as_ref()))
                .collect();

            if osm_is_area::relation(&tags, &member_ids) {
                let value = encode::way(id, tags, member_ids, &deps.values)?;
                let mut batch = Vec::new();
                let row = Row::Insert(
                    Mix2::new(
                        Mix::Interval(deps.xmin, deps.xmax),
                        Mix::Interval(deps.ymin, deps.ymax),
                    ),
                    value,
                );
                batch.push(row);

                db.batch(&batch.as_slice()).await?;
            }
        }

        if osm.ways.len() > 0 {
            // osm.ways should only have 1 item
            let way = osm.ways[0].clone();
            let id = way.id as u64;
            let refs = way.refs.clone();
            for id in way.refs {
                deps.insert(id);
            }

            let tags = way
                .meta
                .tags
                .iter()
                .map(|t| (t.key.as_ref(), t.value.as_ref()))
                .collect();

            let value = encode::way(id, tags, refs, &deps.values)?;
            let mut batch = Vec::new();
            let row = Row::Insert(
                Mix2::new(
                    Mix::Interval(deps.xmin, deps.xmax),
                    Mix::Interval(deps.ymin, deps.ymax),
                ),
                value,
            );
            batch.push(row);

            db.batch(&batch.as_slice()).await?;
        }

        if osm.nodes.len() > 0 {
            // osm.nodes should only have 1 item
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
    }
    return Ok(());
}

pub fn denormalize(pbf: &str, output: &str) -> Result<u64, Error> {
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
                    writer.lock().unwrap().add_node(node);
                    return 1;
                }
                Element::DenseNode(node) => {
                    writer.lock().unwrap().add_node(vadeen_osm::Node {
                        id: node.id,
                        coordinate: Coordinate::new(node.lon(), node.lat()),
                        meta: tags::get_dense_meta(node.tags(), node.info().unwrap().clone()),
                    });
                    return 1;
                }
                Element::Relation(rel) => {
                    let mut members = Vec::new();
                    for member in rel.members() {
                        let coverted_mem = match member.member_type {
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
                        members.push(coverted_mem);
                    }
                    writer.lock().unwrap().add_relation(Relation {
                        id: rel.id(),
                        members: members,
                        meta: tags::get_meta(rel.tags(), rel.info()),
                    });
                    return 1;
                }
                Element::Way(way) => {
                    writer.lock().unwrap().add_way(Way {
                        id: way.id(),
                        refs: way.refs().collect(),
                        meta: tags::get_meta(way.tags(), way.info()),
                    });
                    return 1;
                }
            },
            || 0,
            |a, b| a + b,
        )
        .unwrap();

    println!("Wrote {} elements", total);

    return Ok(total);
}

#[async_std::test]
async fn read_write_dummy() -> Result<(), E> {
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
    match reader.read_node(node.id as u64) {
        Some(read) => {
            assert_eq!(read.id, node.id);
            assert_eq!(read.coordinate, node.coordinate);
            assert_eq!(read.meta, node.meta);
        }
        None => {
            assert!(false, "node {} was none", node.id);
        }
    }

    match reader.read_way(way.id as u64) {
        Some(read) => {
            assert_eq!(read.id, way.id);
            assert_eq!(read.refs, way.refs);
            assert_eq!(read.meta, way.meta);
        }
        None => {
            assert!(false, "way {} was none", way.id);
        }
    }

    match reader.read_relation(rel.id as u64) {
        Some(read) => {
            assert_eq!(read.id, rel.id);
            assert_eq!(read.members, rel.members);
            assert_eq!(read.meta, rel.meta);
        }
        None => {
            assert!(false, "relation {} was none", rel.id);
        }
    }

    fs::remove_dir_all(output);

    Ok(())
}

#[async_std::test]
async fn read_write_fixture() -> Result<(), E> {
    use crate::Reader;
    use async_std::prelude::*;
    use async_std::stream::*;
    use std::fs;
    use vadeen_osm::OsmBuilder;
    let fixture = "fixtures/somewhere.pbf";
    let output = "fixtures/test";
    match denormalize(fixture, output) {
        Ok(total_elements) => {
            let db_path = "test_db";
            let reader = Reader::new(output);

            write_to_db(output, db_path).await;

            let mut db: DB<_, P, V> = DB::open_from_path(&PathBuf::from(db_path)).await.unwrap();
            let bbox = ((-180.0, -90.0), (180.0, 90.0));
            let mut stream = db.query(&bbox).await.unwrap();

            let mut query_elements = 0;

            while let Some(result) = stream.next().await {
                query_elements += 1;
            }

            assert_eq!(query_elements, total_elements);
            return Ok(());
        }
        Err(e) => {
            eprintln!("{}", e);
            return Ok(());
        }
    }
}
