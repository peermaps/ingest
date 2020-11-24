use std::fs;
use vadeen_osm::osm_io::error::Error;
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


    fn read (&self, dirname: &str, id: i64) -> Result<Osm, Error> {
        return osm_io::read(&format!("{}/{}.o5m", dirname, id));
    }

    fn read_way (&self, id: i64) -> Result<vadeen_osm::Way, Error> {
        match self.read(&self.ways, id) {
            Ok(osm) => {
                return Ok(osm.ways[0]);
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

    pub fn add_relation(&self, relation: vadeen_osm::Relation) -> u64 {
        let mut osm = Osm::default();

        osm.add_relation(relation);
        let writing = &format!("{}/{}.o5m", self.ways, relation.id);
        println!("Writing {}", writing);
        match osm_io::write(writing, &osm) {
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

        osm.add_way(way);
        let writing = &format!("{}/{}.o5m", self.ways, way.id);
        println!("Writing {}", writing);
        match osm_io::write(writing, &osm) {
            Ok(_) => {
                return 1;
            }
            Err(_) => {
                return 0;
            }
        }
    }

    pub fn add_node(&self, node: vadeen_osm::Node) -> u64 {
        let mut osm = Osm::default();
        osm.add_node(node);
        let writing = &format!("{}/{}.o5m", self.nodes, node.id);
        println!("Writing {}", writing);
        match osm_io::write(writing, &osm) {
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