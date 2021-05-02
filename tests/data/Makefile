all: ingest.pbf changeset0.o5c changeset1.o5c changeset2.o5c node.pbf way.pbf relation.pbf

clean:
	rm ingest.pbf changeset0.o5c changeset1.o5c changeset2.o5c node.pbf way.pbf relation.pbf

ingest.pbf: ingest.xml
	osmconvert ingest.xml --out-pbf > ingest.pbf

changeset0.o5c: changeset0.xml
	osmconvert changeset0.xml --out-o5c > changeset0.o5c

changeset1.o5c: changeset1.xml
	osmconvert changeset1.xml --out-o5c > changeset1.o5c

changeset2.o5c: changeset2.xml
	osmconvert changeset2.xml --out-o5c > changeset2.o5c

node.pbf: node.xml
	osmconvert node.xml --out-pbf > node.pbf

way.pbf: way.xml
	osmconvert way.xml --out-pbf > way.pbf

relation.pbf: relation.xml
	osmconvert relation.xml --out-pbf > relation.pbf

