//use georender_pack::encode;
mod denormalize;
use osmpbf::{Element, ElementReader};
use std::env;
use vadeen_osm::osm_io::error::Error;

fn main() -> std::result::Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    let pbf = &args[1];
    let output = &args[2];

    write_denormalized_data(pbf, output)?;
    return Ok(());
}

fn write_denormalized_data(pbf: &str, output: &str) -> std::result::Result<bool, Error> {
    let reader = ElementReader::from_path(pbf).unwrap();
    let writer = denormalize::Writer::new(output);

    let mut total = 0;

    reader
        .for_each(
            |element| match element {
                Element::Node(node) => {
                    let count = writer.add_node(
                        node.id(),
                        (node.lon(), node.lat()),
                        node.tags().into_iter().collect(),
                    );
                    total += count;
                }
                Element::DenseNode(node) => {
                    let count = writer.add_node(
                        node.id,
                        (node.lon(), node.lat()),
                        node.tags().into_iter().collect(),
                    );
                    total += count;
                }
                Element::Relation(rel) => {
                    /*
                    return writer.add_relation(
                        rel.id(), 
                        rel.members().into_iter().collect(),
                        rel.tags().into_iter().collect()
                    );
                    */
                }
                Element::Way(way) => {
                    let count = writer.add_way(
                        way.id(), 
                        way.refs().into_iter().collect(),
                        way.tags().into_iter().collect()
                    );
                    total += count;
                }
            },
        )
        .unwrap();

    println!("Wrote {} elements", total);

    return Ok(true);
}
