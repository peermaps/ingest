use hex;
use std::fs;
use std::path::PathBuf;
use vadeen_osm::osm_io;
use vadeen_osm::OsmBuilder;

pub struct Reader {
    output: String,
}

impl Reader {
    pub fn new(output: &str) -> Reader {
        return Reader {
            output: output.to_string(),
        };
    }

    pub fn read_node(&self, id: i64) -> vadeen_osm::Node {
        let osm = self.read("nodes", id);
        return osm.nodes[0].clone();
    }

    pub fn read_way(&self, id: i64) -> vadeen_osm::Way {
        let osm = self.read("ways", id);
        return osm.ways[0].clone();
    }

    pub fn read_relation(&self, id: i64) -> vadeen_osm::Relation {
        let osm = self.read("relations", id);
        return osm.relations[0].clone();
    }

    fn read(&self, dir: &str, id: i64) -> vadeen_osm::Osm {
        let bytes = id.to_be_bytes();
        let mut i = 0;

        let mut readable = PathBuf::new();
        readable.push(&self.output);
        readable.push(&dir);
        while i < bytes.len() {
            let pre = hex::encode(&bytes[i..i + 1]);
            i += 1;
            readable.push(&pre);
        }

        let rest = hex::encode(&bytes[i - 1..bytes.len()]);
        match osm_io::read(&format!("{}/{}.o5m", readable.to_str().unwrap(), rest)) {
            Ok(osm) => {
                return osm;
            }
            Err(e) => {
                eprintln!("{}", e);
                return OsmBuilder::default().build();
            }
        }
    }
}

#[test]
fn read_write_fixture() {
    use crate::Writer;
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

    let output = "testoutput_rw";
    let mut writer = Writer::new(output);
    let node = osm.nodes[0].clone();
    let rel = osm.relations[0].clone();
    let way = osm.ways[0].clone();
    writer.add_node(osm.nodes[0].clone());
    writer.add_way(osm.ways[0].clone());
    writer.add_relation(osm.relations[0].clone());

    let reader = Reader::new(output);
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
    fs::remove_dir_all(output);
}
