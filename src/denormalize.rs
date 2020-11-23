use std::fs;
use vadeen_osm::osm_io::write;
use vadeen_osm::OsmBuilder;

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

    pub fn add_relation(&self, relation: osmpbf::elements::Relation) -> u64 {
        return 0;
    }

    pub fn add_way(&self, relation: osmpbf::elements::Way) -> u64 {
        return 0;
    }

    pub fn add_node(&self, id: i64, point: (f64, f64), tags: Vec<(&str, &str)>) -> u64 {
        let mut builder = OsmBuilder::default();
        builder.add_point(point, tags);
        let osm = builder.build();
        let writing = &format!("{}/{}.o5m", self.nodes, id);
        println!("Writing {}", writing);
        match write(writing, &osm) {
            Ok(_) => {
                return 1;
            }
            Err(_) => {
                return 0;
            }
        }
    }
}
