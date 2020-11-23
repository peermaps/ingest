//use georender_pack::encode;
mod denormalize;
use osmpbf::{Element, ElementReader};
use std::env;
use std::result::Result;
use vadeen_osm::osm_io::error::Error;

fn main() -> std::result::Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    let pbf = &args[1];
    let output = &args[2];

    read(pbf, output);
    return Ok(());
}

fn read(pbf: &str, output: &str) -> std::result::Result<bool, Error> {
    let reader = ElementReader::from_path(pbf).unwrap();

    let writer = denormalize::Writer::new(output);

    let total = reader
        .par_map_reduce(
            |element| match element {
                Element::Node(node) => {
                    return writer.add_node(
                        node.id(),
                        (node.lon(), node.lat()),
                        node.tags().into_iter().collect(),
                    );
                }
                Element::DenseNode(node) => {
                    return writer.add_node(
                        node.id,
                        (node.lon(), node.lat()),
                        node.tags().into_iter().collect(),
                    );
                }
                Element::Relation(rel) => {
                    return writer.add_relation(rel);
                }
                Element::Way(way) => {
                    return writer.add_way(way);
                }
            },
            || 0_u64,     // Zero is the identity value for addition
            |a, b| a + b, // Sum the partial results
        )
        .unwrap();

    println!("Wrote {} elements", total);

    return Ok(true);
}
