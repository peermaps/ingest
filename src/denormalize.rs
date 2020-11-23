use std::fs;
use vadeen_osm::osm_io::write;
use vadeen_osm::OsmBuilder;

pub struct Writer<'a> {
    output: &'a str,
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

impl<'a> Writer<'a> {
    pub fn new(output: &'a str) -> Writer<'a> {
        let nodes = format!("{}/{}", output, "nodes");
        let ways = format!("{}/{}", output, "ways");
        let relations = format!("{}/{}", output, "relations");
        create_directory(output);
        create_directory(&nodes);
        create_directory(&ways);
        create_directory(&relations);
        return Writer {
            output,
            nodes,
            ways,
            relations,
        };
    }

    pub fn add_relation(&self, relation: osmpbf::elements::Relation) {}

    pub fn add_way(&self, relation: osmpbf::elements::Way) {}

    pub fn add_node(&self, id: i64, point: (f64, f64), tags: Vec<(&str, &str)>) {
        let mut builder = OsmBuilder::default();
        builder.add_point(point, tags);
        let osm = builder.build();
        let writing = &format!("{}/{}.o5m", self.nodes, id);
        println!("Writing {}", writing);
        match write(writing, &osm) {
            Ok(_) => {
                println!("Success");
            }
            Err(_) => {
                println!("Failed");
            }
        }
    }
}
