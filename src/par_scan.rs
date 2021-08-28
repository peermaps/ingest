use crate::Error;
use async_std::{channel,task};
use osmpbf_parser::{Parser,ScanTable,element,Element};
use futures::future::join_all;
use std::ops::Bound::Included;
use std::io::{Read,Seek};

pub async fn parallel_scan<F: Read+Seek+Send+'static>(
  mut parsers: Vec<Parser<F>>, start: u64, end: u64
) -> Result<ScanTable,Error> {
  let (offset_sender,offset_receiver) = channel::unbounded();
  {
    let mut parser = parsers.pop().unwrap();
    task::spawn(async move {
      let mut offset = start;
      while offset < end {
        let (blob_header_len,blob_header) = parser.read_blob_header(offset).unwrap();
        let blob_offset = offset + blob_header_len;
        let blob_len = blob_header.datasize as usize;
        let len = blob_header_len + blob_len as u64;
        if offset > 0 { // skip header
          offset_sender.send((blob_offset,blob_len)).await.unwrap();
        }
        offset += len;
      }
      offset_sender.close();
    });
  }

  let mut table_work: Vec<task::JoinHandle<Result<ScanTable,Error>>> = vec![];
  for mut parser in parsers {
    let r = offset_receiver.clone();
    table_work.push(task::spawn(async move {
      let mut table = ScanTable::default();
      while let Ok((blob_offset,blob_len)) = r.recv().await {
        let blob = parser.read_blob(blob_offset,blob_len)?;
        let items = blob.decode_primitive()?.decode();
        let mut etype = element::MemberType::Node;
        let mut min_id = i64::MAX;
        let mut max_id = i64::MIN;
        for item in items.iter() {
          match item {
            Element::Node(node) => {
              min_id = node.id.min(min_id);
              max_id = node.id.max(max_id);
            },
            Element::Way(way) => {
              etype = element::MemberType::Way;
              min_id = way.id.min(min_id);
              max_id = way.id.max(max_id);
            },
            Element::Relation(relation) => {
              etype = element::MemberType::Relation;
              min_id = relation.id.min(min_id);
              max_id = relation.id.max(max_id);
            },
          }
        }
        if !items.is_empty() {
          let iv = (Included(min_id),Included(max_id));
          match etype {
            element::MemberType::Node => {
              table.node_interval_offsets.insert(
                iv.clone(),
                (blob_offset,blob_len,items.len())
              );
              table.nodes.insert(iv);
            },
            element::MemberType::Way => {
              table.way_interval_offsets.insert(
                iv.clone(),
                (blob_offset,blob_len,items.len())
              );
              table.ways.insert(iv);
            },
            element::MemberType::Relation => {
              table.relation_interval_offsets.insert(
                iv.clone(),
                (blob_offset,blob_len,items.len())
              );
              table.relations.insert(iv);
            },
          }
        }
      }
      Ok(table)
    }));
  }

  let mut scan_table = ScanTable::default();
  for table in join_all(table_work).await {
    scan_table.extend(&table?)
  }
  Ok(scan_table)
}
