use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ingest::denormalize::Writer;
use rand::prelude::*;
use std::fs;
use vadeen_osm::{Osm, OsmBuilder};

fn cleanup(output: &str) -> std::io::Result<()> {
    fs::remove_dir_all(output)?;
    Ok(())
}

fn generate_data(n: u64) -> Osm {
    let mut builder = OsmBuilder::default();

    let nodes = n / 5;
    let ways = n / 7;
    let relations = n / 10;

    let mut i = 0;
    let mut rng = thread_rng();
    while i < nodes {
        // inclusive range
        let coord: (f64, f64) = (rng.gen_range(-90.0..=90.0), rng.gen_range(-180.0..=180.0));
        builder.add_point(coord, vec![("power", "tower")]);
        i += 1;
    }
    return builder.build();
}

fn write_data(output: &str, osm: &Osm) {
    let mut writer = Writer::new(output);
    for node in &osm.nodes {
        writer.add_node(node.clone());
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("write nodes");
    group.sample_size(10);
    let output = "bench_output";

    let data = generate_data(10 * 1000);
    group.bench_function("write 10,000", |b| {
        b.iter(|| write_data(output, black_box(&data)))
    });

    cleanup(output);

    let data = generate_data(10 * 10000);
    group.bench_function("write 100,000", |b| {
        b.iter(|| write_data(output, black_box(&data)))
    });

    cleanup(output);

    let data = generate_data(10 * 10000);
    group.bench_function("write 1,000,000", |b| {
        b.iter(|| write_data(output, black_box(&data)))
    });

    cleanup(output);

    let data = generate_data(10 * 100000);
    group.bench_function("write 10,000,000", |b| {
        b.iter(|| write_data(output, black_box(&data)))
    });

    cleanup(output);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
