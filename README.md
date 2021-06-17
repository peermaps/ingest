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
usage: peermaps-ingest COMMAND {OPTIONS}

ingest - runs pbf and process phases
  -f, --pbf     osm pbf file to ingest or "-" for stdin (default)
  -l, --ldb     level db dir to write normalized data
  -e, --edb     eyros db dir to write spatial data
  -o, --outdir  write level and eyros db in this dir in ldb/ and edb/

pbf - parse pbf and write normalized data to level db
  -f, --pbf     osm pbf file to ingest or "-" for stdin (default)
  -l, --ldb     level db dir to write normalized data
  -e, --edb     eyros db dir to write spatial data
  -o, --outdir  write level and eyros db in this dir in ldb/ and edb/

process - write georender-pack data to eyros db from populated level db
  -l, --ldb     level db dir to write normalized data
  -e, --edb     eyros db dir to write spatial data
  -o, --outdir  write level and eyros db in this dir in ldb/ and edb/

changeset - ingest data from an o5c changeset
  -f, --o5c     o5c changeset file or "-" for stdin (default)
  -l, --ldb     level db dir to write normalized data
  -e, --edb     eyros db dir to write spatial data
  -o, --outdir  write level and eyros db in this dir in ldb/ and edb/
```

# install

To get the command-line program:

```
cargo install peermaps-ingest
```

