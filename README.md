# peermaps-ingest

Converts OSM data into the peermaps on-disk format.

This is done in two passes of the data. The first pass writes records from a pbf
file into a leveldb database keyed by id. The second pass writes all the
features that can be rendered into an [eyros][] database with payloads in the
[georender][] format.

[eyros]: https://github.com/peermaps/eyros
[georender]: https://github.com/peermaps/docs/blob/master/georender.md

# command-line usage

```
usage: target/release/peermaps-ingest COMMAND {OPTIONS}

ingest - scans and processes a pbf
  -f, --pbf     osm pbf file to ingest or "-" for stdin (default)
  -e, --edb     eyros db dir to write spatial data
  -o, --outdir  write eyros db in this dir in edb/

  --no-ingest-node      skip over processing nodes
  --no-ingest-way       skip over processing nodes
  --no-ingest-relation  skip over processing nodes

scan - scans a pbf, outputting a scan file
  -f, --pbf     osm pbf file to ingest or "-" for stdin (default)
  -o, --outdir  write a scan file in this dir
  --scan_file   write scan file with explicit path

ingest-from-scan - process a pbf from an existing scan
  -f, --pbf     osm pbf file to ingest or "-" for stdin (default)
  -e, --edb     eyros db dir to write spatial data
  -o, --outdir  write eyros db in this dir in edb/ and read scan file
  --scan_file   read scan file with explicit path

  --no-ingest-node      skip over processing nodes
  --no-ingest-way       skip over processing nodes
  --no-ingest-relation  skip over processing nodes

-h, --help     Print this help message
-v, --version  Print the version string (2.0.0)
```

# install

To get the command-line program:

```
cargo install peermaps-ingest
```

