all: ingest.pbf node.pbf way.pbf relation.pbf \
	changeset0.o5c changeset1.o5c changeset2.o5c changeset3.o5c

clean:
	rm ingest.pbf node.pbf way.pbf relation.pbf \
		changeset0.o5c changeset1.o5c changeset2.o5c changeset3.o5c

ingest.pbf: ingest.xml
	osmconvert ingest.xml --out-pbf > ingest.pbf

changeset0.o5c: changeset0.xml
	osmconvert changeset0.xml --out-o5c > changeset0.o5c

changeset1.o5c: changeset1.xml
	osmconvert changeset1.xml --out-o5c > changeset1.o5c

changeset2.o5c: changeset2.xml
	osmconvert changeset2.xml --out-o5c > changeset2.o5c

changeset3.o5c: changeset3.xml
	osmconvert changeset3.xml --out-o5c > changeset3.o5c

node.pbf: node.xml
	osmconvert node.xml --out-pbf > node.pbf

way.pbf: way.xml
	osmconvert way.xml --out-pbf > way.pbf

relation.pbf: relation.xml
	osmconvert relation.xml --out-pbf > relation.pbf

