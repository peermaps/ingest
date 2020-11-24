use vadeen_osm::osm_io::error::Error;
use std::fs;
use vadeen_osm::osm_io;
use vadeen_osm::Osm;

pub struct Writer {
    nodes: String,
    ways: String,
    relations: String,
}

fn create_directory(path: &str) {
    match fs::create_dir(path) {
        Ok(_) => {
            println!("Created directory {}", path);
        }
        Err(_) => {}
    }
}

impl Writer {
    pub fn new(output: &str) -> Writer {
        let nodes = format!("{}/{}", output, "nodes");
        let ways = format!("{}/{}", output, "ways");
        let relations = format!("{}/{}", output, "relations");
        create_directory(output);
        create_directory(&nodes);
        create_directory(&ways);
        create_directory(&relations);
        return Writer {
            nodes,
            ways,
            relations,
        };
    }

    fn write (&self, dir: &str, id: i64, osm: &Osm) -> Result<(),Error> {
        return osm_io::write(&format!("{}/{}.o5m", dir, id), osm);
    }

    pub fn add_relation(&self, relation: vadeen_osm::Relation) -> u64 {
        let mut osm = Osm::default();
        let id = relation.id;
        osm.add_relation(relation);
        match self.write(&self.relations, id, &osm) {
            Ok(_) => {
                return 1;
            }
            Err(_) => {
                return 0;
            }
        }
    }

    pub fn add_way(&self, way: vadeen_osm::Way) -> u64 {
        let mut osm= Osm::default();
        let id = way.id;
        osm.add_way(way);
        match self.write(&self.ways, id, &osm) {
            Ok(_) => {
                return 1;
            }
            Err(_) => {
                return 0;
            }
        }
    }

    pub fn add_node(&self, node: vadeen_osm::Node) -> u64 {
        let mut osm= Osm::default();
        let id = node.id;
        osm.add_node(node);
        match self.write(&self.nodes, id, &osm) {
            Ok(_) => {
                return 1;
            }
            Err(_) => {
                return 0;
            }
        }
    }
}


/*
        for id in refs {
            let node = self.read_node(id).unwrap();
            points.push(node.coordinate);
        }
        */

        /*
        for m in members {
            match m.member_type {
                osmpbf::RelMemberType::Node => {
                    let node = self.read_node(m.member_id).unwrap();
                    points.push(node.coordinate)
                }
                osmpbf::RelMemberType::Way => {
                    let way = self.read_way(m.member_id).unwrap();
                }
                osmpbf::RelMemberType::Relation => {
                    let rel = self.read_relation(m.member_id).unwrap();
                }
            }
        }
        */


    /*
    fn read (&self, dirname: &str, id: i64) -> Result<Osm, Error> {
        return osm_io::read(&format!("{}/{}.o5m", dirname, id));
    }

    fn read_way (&self, id: i64) -> Result<vadeen_osm::Way, Error> {
        match self.read(&self.ways, id) {
            Ok(osm) => {
                let way = &osm.ways[0];
                return Ok(way.clone());
            } 
            Err(err) => {
                println!("Failed to find way {}", id);
                eprintln!("{}", err);
                panic!(err);
            }
        }
    }

    fn read_relation (&self, id: i64) -> Result<vadeen_osm::Relation, Error> {
        match self.read(&self.relations, id) {
            Ok(osm) => {
                return Ok(osm.relations[0]);
            } 
            Err(err) => {
                println!("Failed to find relation {}", id);
                eprintln!("{}", err);
                panic!(err);
            }
        }
    }

    fn read_node (&self, id: i64) -> Result<&vadeen_osm::Node, Error> {
        match self.read(&self.nodes, id) {
            Ok(osm) => {
                return Ok(&osm.nodes[0]);
            } 
            Err(err) => {
                println!("Failed to find node {}", id);
                eprintln!("{}", err);
                panic!(err);
            }
        }
    }

    */