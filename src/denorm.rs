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
) -> Result<HashMap<i64,Vec<(i64,(f64,f64))>>,Error> {
  let mut result: HashMap<i64,Vec<(i64,(f64,f64))>> = HashMap::new();
  while let Ok(nodes) = node_receiver.recv().await {
    for (node_id,(lon,lat)) in nodes {
      for way_id in ref_table.get(&node_id).unwrap_or(&vec![]) {
        if let Some(points) = result.get_mut(way_id) {
          points.push((node_id,(lon,lat)));
        } else {
          result.insert(*way_id, vec![(node_id,(lon,lat))]);
        }
      }
    }
  }
  Ok(result)
}
