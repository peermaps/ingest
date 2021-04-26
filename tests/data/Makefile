all: ingest.pbf changeset.o5c node.pbf way.pbf relation.pbf

clean:
	rm ingest.pbf changeset.o5c node.pbf way.pbf relation.pbf

ingest.pbf: ingest.xml
	osmconvert ingest.xml --out-pbf > ingest.pbf

changeset.o5c: changeset.xml
	osmconvert changeset.xml --out-o5c > changeset.o5c

node.pbf: node.xml
	osmconvert node.xml --out-pbf > node.pbf

way.pbf: way.xml
	osmconvert way.xml --out-pbf > way.pbf

relation.pbf: relation.xml
	osmconvert relation.xml --out-pbf > relation.pbf

