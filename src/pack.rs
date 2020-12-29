/*
        for id in refs {
            let node = self.read_node(id).unwrap();
            points.push(node.coordinate);
        }
        */

        /*
        for m in members {
            match m.member_type {
                osmpbf::RelMemberType::Node => {
                    let node = self.read_node(m.member_id).unwrap();
                    points.push(node.coordinate)
                }
                osmpbf::RelMemberType::Way => {
                    let way = self.read_way(m.member_id).unwrap();
                }
                osmpbf::RelMemberType::Relation => {
                    let rel = self.read_relation(m.member_id).unwrap();
                }
            }
        }
        */


    /*
    fn read (&self, dirname: &str, id: i64) -> Result<Osm, Error> {
        return osm_io::read(&format!("{}/{}.o5m", dirname, id));
    }

    fn read_way (&self, id: i64) -> Result<vadeen_osm::Way, Error> {
        match self.read(&self.ways, id) {
            Ok(osm) => {
                let way = &osm.ways[0];
                return Ok(way.clone());
            } 
            Err(err) => {
                println!("Failed to find way {}", id);
                eprintln!("{}", err);
                panic!(err);
            }
        }
    }

    fn read_relation (&self, id: i64) -> Result<vadeen_osm::Relation, Error> {
        match self.read(&self.relations, id) {
            Ok(osm) => {
                return Ok(osm.relations[0]);
            } 
            Err(err) => {
                println!("Failed to find relation {}", id);
                eprintln!("{}", err);
                panic!(err);
            }
        }
    }

    fn read_node (&self, id: i64) -> Result<&vadeen_osm::Node, Error> {
        match self.read(&self.nodes, id) {
            Ok(osm) => {
                return Ok(&osm.nodes[0]);
            } 
            Err(err) => {
                println!("Failed to find node {}", id);
                eprintln!("{}", err);
                panic!(err);
            }
        }
    }

    */se std::iter::FromIterator;