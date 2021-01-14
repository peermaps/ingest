use hex;
use jwalk::{WalkDir, WalkDirGeneric};
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

    pub fn walk_nodes(&self) -> WalkDirGeneric<((), ())> {
        let mut nodes = PathBuf::new();
        nodes.push(&self.output);
        nodes.push("nodes");
        println!("nodes {:?}", nodes.to_str());

        return WalkDir::new(nodes);
    }

    pub fn read_node(&self, id: u64) -> vadeen_osm::Node {
        let osm = self.read("nodes", id);
        return osm.nodes[0].clone();
    }

    pub fn read_way(&self, id: u64) -> vadeen_osm::Way {
        let osm = self.read("ways", id);
        return osm.ways[0].clone();
    }

    pub fn read_relation(&self, id: u64) -> vadeen_osm::Relation {
        let osm = self.read("relations", id);
        return osm.relations[0].clone();
    }

    pub fn read_raw(&self, filepath: &str) -> vadeen_osm::Osm {
        match osm_io::read(filepath) {
            Ok(osm) => {
                return osm;
            }
            Err(e) => {
                eprintln!("{}", e);
                return OsmBuilder::default().build();
            }
        }
    }

    pub fn read(&self, dir: &str, id: u64) -> vadeen_osm::Osm {
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
        let filepath = format!("{}/{}.o5m", readable.to_str().unwrap(), rest);
        return self.read_raw(&filepath);
    }
}
