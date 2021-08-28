use crate::error::Error;
use std::collections::HashMap;
use osmpbf_parser::{Scan,Element,element};
use std::io::{Read,Seek};
use async_std::{channel,sync::{Arc,Mutex},task};

pub async fn get_nodes_bare_ch<F: Read+Seek+Send+'static>(
  scans: Vec<Scan<F>>, n: usize
) -> channel::Receiver<Vec<(i64,(f64,f64))>> {
  let mnactive = Arc::new(Mutex::new(scans.len()));
  let (node_sender,node_receiver) = channel::bounded(n);
  let (offset_sender,offset_receiver) = channel::unbounded();
  for (offset,byte_len,_len) in scans[0].get_node_blob_offsets() {
    offset_sender.send((offset,byte_len)).await.unwrap();
  }
  offset_sender.close();
  for mut scan in scans {
    let node_s = node_sender.clone();
    let offset_r = offset_receiver.clone();
    let nactive = mnactive.clone();
    task::spawn(async move {
      while let Ok((offset,len)) = offset_r.recv().await {
        let blob = scan.parser.read_blob(offset,len).unwrap();
        let items = blob.decode_primitive().unwrap().decode();
        node_s.send(items.iter()
          .filter_map(|element| match element {
            Element::Node(node) => Some((node.id,(node.lon,node.lat))),
            _ => None,
          })
          .collect()
        ).await.unwrap();
      }
      {
        let mut n = nactive.lock().await;
        *n -= 1;
        if *n == 0 { node_s.close(); }
      }
    });
  }
  node_receiver
}

pub async fn get_nodes_ch<F: Read+Seek+Send+'static>(
  scans: Vec<Scan<F>>, n: usize
) -> channel::Receiver<Vec<element::Node>> {
  let mnactive = Arc::new(Mutex::new(scans.len()));
  let (node_sender,node_receiver) = channel::bounded(n);
  let (offset_sender,offset_receiver) = channel::unbounded();
  for (offset,byte_len,_len) in scans[0].get_node_blob_offsets() {
    offset_sender.send((offset,byte_len)).await.unwrap();
  }
  offset_sender.close();
  for mut scan in scans {
    let node_s = node_sender.clone();
    let offset_r = offset_receiver.clone();
    let nactive = mnactive.clone();
    task::spawn(async move {
      while let Ok((offset,len)) = offset_r.recv().await {
        let blob = scan.parser.read_blob(offset,len).unwrap();
        let items = blob.decode_primitive().unwrap().decode();
        node_s.send(items.iter()
          .filter_map(|element| match element {
            Element::Node(node) => Some(node),
            _ => None,
          })
          .cloned()
          .collect()
        ).await.unwrap();
      }
      {
        let mut n = nactive.lock().await;
        *n -= 1;
        if *n == 0 { node_s.close(); }
      }
    });
  }
  node_receiver
}

pub async fn get_ways_bare_ch<F: Read+Seek+Send+'static>(
  mut scans: Vec<Scan<F>>, n: usize
) -> channel::Receiver<Vec<(i64,Vec<i64>)>> {
  let mnactive = Arc::new(Mutex::new(scans.len()));
  let (way_sender,way_receiver) = channel::bounded(n);
  let (offset_sender,offset_receiver) = channel::unbounded();
  for (offset,byte_len,_len) in scans[0].get_way_blob_offsets() {
    offset_sender.send((offset,byte_len)).await.unwrap();
  }
  offset_sender.close();
  for mut scan in scans {
    let way_s = way_sender.clone();
    let offset_r = offset_receiver.clone();
    let nactive = mnactive.clone();
    task::spawn(async move {
      while let Ok((offset,len)) = offset_r.recv().await {
        let blob = scan.parser.read_blob(offset,len).unwrap();
        let items = blob.decode_primitive().unwrap().decode();
        way_s.send(items.iter()
          .filter_map(|element| match element {
            Element::Way(way) => Some((way.id,way.refs.clone())),
            _ => None,
          })
          .collect()
        ).await.unwrap();
      }
      {
        let mut n = nactive.lock().await;
        *n -= 1;
        if *n == 0 { way_s.close(); }
      }
    });
  }
  way_receiver
}


pub async fn get_ways<F: Read+Seek+Send+'static>(
  mut scans: Vec<Scan<F>>, ch_bound: usize, start: u64, n: usize,
) -> (Option<u64>,Vec<element::Way>) {
  let mnactive = Arc::new(Mutex::new(scans.len()));
  let (way_sender,way_receiver): (
    channel::Sender<Vec<element::Way>>,
    channel::Receiver<Vec<element::Way>>,
  ) = channel::bounded(ch_bound);
  let (offset_sender,offset_receiver) = channel::unbounded();
  let mut count = 0;
  let mut next_offset = None;
  for (offset,byte_len,len) in scans[0].get_way_blob_offsets() {
    if offset < start { continue }
    count += len;
    if count > n {
      next_offset = Some(offset);
      break;
    }
    offset_sender.send((offset,byte_len)).await.unwrap();
  }
  offset_sender.close();
  for mut scan in scans {
    let way_s = way_sender.clone();
    let offset_r = offset_receiver.clone();
    let nactive = mnactive.clone();
    task::spawn(async move {
      while let Ok((offset,len)) = offset_r.recv().await {
        let blob = scan.parser.read_blob(offset,len).unwrap();
        let items = blob.decode_primitive().unwrap().decode();
        way_s.send(items.iter()
          .filter_map(|element| match element {
            Element::Way(way) => Some(way),
            _ => None,
          })
          .cloned()
          .collect()
        ).await.unwrap();
      }
      {
        let mut n = nactive.lock().await;
        *n -= 1;
        if *n == 0 { way_s.close(); }
      }
    });
  }
  let mut ways = vec![];
  while let Ok(way_group) = way_receiver.recv().await {
    ways.extend(way_group);
  }
  (next_offset,ways)
}

pub async fn get_relations<F: Read+Seek+Send+'static>(
  mut scans: Vec<Scan<F>>, ch_bound: usize, start: u64, n: usize,
) -> (Option<u64>,Vec<element::Relation>) {
  let mnactive = Arc::new(Mutex::new(scans.len()));
  let (relation_sender,relation_receiver): (
    channel::Sender<Vec<element::Relation>>,
    channel::Receiver<Vec<element::Relation>>,
  ) = channel::bounded(ch_bound);
  let (offset_sender,offset_receiver) = channel::unbounded();
  let mut count = 0;
  let mut next_offset = None;
  for (offset,byte_len,len) in scans[0].get_relation_blob_offsets() {
    if offset < start { continue }
    count += len;
    if count > n {
      next_offset = Some(offset);
      break;
    }
    offset_sender.send((offset,byte_len)).await.unwrap();
  }
  offset_sender.close();
  for mut scan in scans {
    let relation_s = relation_sender.clone();
    let offset_r = offset_receiver.clone();
    let nactive = mnactive.clone();
    task::spawn(async move {
      while let Ok((offset,len)) = offset_r.recv().await {
        let blob = scan.parser.read_blob(offset,len).unwrap();
        let items = blob.decode_primitive().unwrap().decode();
        relation_s.send(items.iter()
          .filter_map(|element| match element {
            Element::Relation(relation) => Some(relation),
            _ => None,
          })
          .cloned()
          .collect()
        ).await.unwrap();
      }
      {
        let mut n = nactive.lock().await;
        *n -= 1;
        if *n == 0 { relation_s.close(); }
      }
    });
  }
  let mut relations = vec![];
  while let Ok(relation_group) = relation_receiver.recv().await {
    relations.extend(relation_group);
  }
  (next_offset,relations)
}

pub fn way_ref_table(ways: &[element::Way]) -> HashMap<i64,Vec<i64>> {
  let mut ref_table: HashMap<i64,Vec<i64>> = HashMap::new();
  for way in ways.iter() {
    for r in way.refs.iter() {
      if let Some(way_ids) = ref_table.get_mut(&r) {
        way_ids.push(way.id);
      } else {
        ref_table.insert(*r, vec![way.id]);
      }
    }
  }
  ref_table
}

pub async fn denormalize_ways(
  ref_table: &HashMap<i64,Vec<i64>>,
  node_receiver: channel::Receiver<Vec<(i64,(f64,f64))>>,
) -> Result<HashMap<i64,(f64,f64)>,Error> {
  let mut result: HashMap<i64,(f64,f64)> = HashMap::new();
  while let Ok(nodes) = node_receiver.recv().await {
    for (node_id,(lon,lat)) in nodes {
      if ref_table.contains_key(&node_id) {
        result.insert(node_id,(lon,lat));
      }
    }
  }
  Ok(result)
}

pub fn relation_ref_table(relations: &[element::Relation]) -> HashMap<i64,Vec<i64>> {
  let mut ref_table: HashMap<i64,Vec<i64>> = HashMap::new();
  for relation in relations.iter() {
    for m in relation.members.iter() {
      if let Some(relation_ids) = ref_table.get_mut(&m.id) {
        relation_ids.push(relation.id);
      } else {
        ref_table.insert(m.id, vec![relation.id]);
      }
    }
  }
  ref_table
}

pub async fn denormalize_relations(
  relation_ref_table: &HashMap<i64,Vec<i64>>,
  node_receiver: channel::Receiver<Vec<(i64,(f64,f64))>>,
  way_receiver: channel::Receiver<Vec<(i64,Vec<i64>)>>,
) -> Result<(HashMap<i64,(f64,f64)>,HashMap<i64,Vec<i64>>),Error> {
  let mut way_deps: HashMap<i64,Vec<i64>> = HashMap::new();
  let mut way_ref_table: HashMap<i64,Vec<i64>> = HashMap::new();

  while let Ok(ways) = way_receiver.recv().await {
    for (way_id,refs) in ways {
      for r in refs.iter() {
        if let Some(way_ids) = way_ref_table.get_mut(&r) {
          way_ids.push(way_id);
        } else {
          way_ref_table.insert(*r, vec![way_id]);
        }
      }
      if relation_ref_table.contains_key(&way_id) {
        way_deps.insert(way_id,refs);
      }
    }
  }
  let node_deps = denormalize_ways(&way_ref_table, node_receiver).await?;
  Ok((node_deps, way_deps))
}