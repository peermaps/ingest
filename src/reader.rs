use hex;
use jwalk::{DirEntry, DirEntryIter, Error, WalkDir};
use std::path::PathBuf;
use std::result::Result;
use vadeen_osm::osm_io;
use vadeen_osm::{Osm, OsmBuilder};

pub struct Reader {
    output: String,
}

impl Reader {
    pub fn new(output: &str) -> Reader {
        return Reader {
            output: output.to_string(),
        };
    }

    pub fn walk(
        &self,
    ) -> std::iter::Map<DirEntryIter<((), ())>, fn(Result<DirEntry<((), ())>, Error>) -> Osm> {
        let mut nodes = PathBuf::new();
        nodes.push(&self.output);
        fn convert_to_osm(entry: Result<DirEntry<((), ())>, Error>) -> Osm {
            let buf = entry.unwrap().path();
            let filepath = buf.to_str().unwrap();

            let retain = filepath.ends_with("o5m");
            if retain {
                return read_raw(filepath);
            } else {
                return OsmBuilder::default().build();
            }
        }

        let walker = WalkDir::new(nodes).min_depth(3);
        return walker.into_iter().map(convert);
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
        return read_raw(&filepath);
    }
}

pub fn read_raw(filepath: &str) -> vadeen_osm::Osm {
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
