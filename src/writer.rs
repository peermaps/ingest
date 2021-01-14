use hex;
use lru::LruCache;
use std::fs;
use std::path::{Path, PathBuf};
use vadeen_osm::osm_io;
use vadeen_osm::Osm;

pub struct Writer {
    output: String,
    cache: LruCache<String, bool>,
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
        let out = Path::new(output);
        let nodes = out.join("nodes");
        let ways = out.join("ways");
        let relations = out.join("relations");
        create_directory(output);

        let cache = LruCache::new(1000);
        let nodes = nodes.to_str().unwrap();
        let ways = ways.to_str().unwrap();
        let relations = relations.to_str().unwrap();
        create_directory(nodes);
        create_directory(ways);
        create_directory(relations);
        return Writer {
            cache,
            output: output.to_string(),
        };
    }

    fn write(&mut self, dir: &str, id: i64, osm: &Osm) -> u64 {
        let bytes = id.to_be_bytes();
        let mut i = 0;

        let mut writable_dir = PathBuf::new();
        writable_dir.push(&self.output);
        writable_dir.push(&dir);
        while i < bytes.len() {
            let pre = hex::encode(&bytes[i..i + 1]);
            i += 1;
            writable_dir.push(&pre);
            match self.cache.get(&pre) {
                Some(_) => {}
                None => {
                    let written = writable_dir.to_str().unwrap();
                    create_directory(written);
                    self.cache.put(written.to_string(), true);
                    //println!("{}", written);
                }
            }
        }

        let rest = hex::encode(&bytes[i - 1..bytes.len()]);
        match osm_io::write(
            &format!("{}/{}.o5m", writable_dir.to_str().unwrap(), rest),
            osm,
        ) {
            Ok(_) => {
                return id as u64;
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
        return self.write("relations", id, &osm);
    }

    pub fn add_way(&mut self, way: vadeen_osm::Way) -> u64 {
        let mut osm = Osm::default();
        let id = way.id;
        osm.add_way(way);
        return self.write("ways", id, &osm);
    }

    pub fn add_node(&mut self, node: vadeen_osm::Node) -> u64 {
        let mut osm = Osm::default();
        let id = node.id;
        osm.add_node(node);
        return self.write("nodes", id, &osm);
    }
}

#[test]
fn write_node() {
    use vadeen_osm::*;
    let output = "testoutput_node";
    let mut writer = Writer::new(output);

    let node = Node {
        id: 1,
        coordinate: (66.29, -3.177).into(),
        meta: Meta {
            tags: vec![("key", "value").into()],
            version: Some(3),
            author: Some(AuthorInformation {
                created: 12345678,
                change_set: 1,
                uid: 1234,
                user: "Username".to_string(),
            }),
        },
    };
    writer.add_node(node);
    fs::remove_dir_all(output);
}

#[test]
fn write_way() {
    use vadeen_osm::*;
    let output = "testoutput_way";
    let mut writer = Writer::new(output);

    let way = Way {
        id: 2,
        refs: vec![1],
        meta: Default::default(),
    };
    writer.add_way(way);
    fs::remove_dir_all(output);
}

#[test]
fn write_rel() {
    use vadeen_osm::*;
    let output = "testoutput_rel";
    let mut writer = Writer::new(output);

    let relation = Relation {
        id: 3,
        members: vec![RelationMember::Way(2, "role".to_owned())],
        meta: Default::default(),
    };

    writer.add_relation(relation);
    fs::remove_dir_all(output);
}
