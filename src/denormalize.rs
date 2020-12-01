use hex;
use std::fs;
use vadeen_osm::osm_io;
use vadeen_osm::Osm;
use lru::LruCache;

pub struct Writer {
    nodes: String,
    ways: String,
    relations: String,
    cache: LruCache<String, bool>
}

fn create_directory(path: &str) {
    match fs::create_dir(path) {
        Ok(_) => {
            //Created directory!("{}", path);
        }
        Err(_e) => {
            //eprintln!("{}", _e);
        }
    }
}

impl Writer {
    pub fn new(output: &str) -> Writer {
        let nodes = format!("{}/{}", output, "nodes");
        let ways = format!("{}/{}", output, "ways");
        let relations = format!("{}/{}", output, "relations");
        create_directory(output);
        
        let cache = LruCache::new(1000);
        create_directory(&nodes);
        create_directory(&ways);
        create_directory(&relations);
        return Writer {
            cache,
            nodes,
            ways,
            relations,
        };
    }

    fn write(&mut self, dir: &str, id: i64, osm: &Osm) -> u64 {
        let bytes = id.to_le_bytes();
        let pre = hex::encode(&bytes[0..2]);
        let writable_dir = &format!("{}/{}", dir, pre);
        match self.cache.get(&pre) {
            Some(_) => {

            } None => {
                create_directory(writable_dir);
                self.cache.put(pre, true);
            }
        }

        let rest = hex::encode(&bytes[2..bytes.len()]);
        match osm_io::write(&format!("{}/{}.o5m", writable_dir, rest), osm) {
            Ok(_) => {
                return 1;
            }
            Err(e) => {
                eprintln!("{}", e);
                return 0;
            }
        }
    }

    pub fn add_relation(&mut self, relation: vadeen_osm::Relation) -> u64 {
        let mut osm = Osm::default();
        let id = relation.id;
        osm.add_relation(relation);
        return self.write(&self.relations.clone(), id, &osm);
    }

    pub fn add_way(&mut self, way: vadeen_osm::Way) -> u64 {
        let mut osm = Osm::default();
        let id = way.id;
        osm.add_way(way);
        return self.write(&self.ways.clone(), id, &osm);
    }

    pub fn add_node(&mut self, node: vadeen_osm::Node) -> u64 {
        let mut osm = Osm::default();
        let id = node.id;
        osm.add_node(node);
        return self.write(&self.nodes.clone(), id, &osm);
    }
}
