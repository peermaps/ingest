# peermaps_ingest

Converts OSM data into the peermaps on-disk format.

## Usage

```rust
use ingest;
use vadeen_osm::*;

let writer = ingest::Writer::new(output);

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


writer.add_node(node)
```

See [src/main.rs](src/main.rs) for a more detailed example with parallelization for large
pbf files.


## Development 

For integration tests

```
cargo run /path/to/my/osm.pbf output/
```

To run the unit tests:

```
cargo test
```


