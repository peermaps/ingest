use std::collections::VecDeque;
use crate::value::V;
use eyros::tree::InsertValue;

type P = (eyros::Coord<f32>,eyros::Coord<f32>);
type B = ((f32,f32),(f32,f32));
type I<'a> = (B,Vec<(P,InsertValue<'a,P,V>)>);

pub fn divide<'a>(n: usize, bucket: I<'a>) -> Vec<I<'a>> {
  const DEPTH_LIMIT: usize = 30;

  if bucket.1.len() <= n {
    return vec![bucket];
  }
  let (nx,ny) = (2,2);
  let mut res = vec![];
  let mut queue = VecDeque::new();
  queue.push_back((0,bucket));
  while let Some((depth,(q_bbox,q_inserts))) = queue.pop_front() {
    if q_inserts.is_empty() { continue }
    let mut boxes: Vec<I<'a>> = Vec::with_capacity(nx*ny);
    let q_span = (
      (q_bbox.1).0 - (q_bbox.0).0,
      (q_bbox.1).1 - (q_bbox.0).1,
    );
    for iy in 0..ny {
      for ix in 0..nx {
        let bbox = (
          (
            (ix as f32 / nx as f32) * q_span.0 + (q_bbox.0).0,
            (iy as f32 / ny as f32) * q_span.1 + (q_bbox.0).1,
          ),
          (
            ((ix as f32 + 1.0) / nx as f32) * q_span.0 + (q_bbox.0).0,
            ((iy as f32 + 1.0) / ny as f32) * q_span.1 + (q_bbox.0).1,
          ),
        );
        boxes.push((bbox,vec![]));
      }
    }
    let q_inserts_len = q_inserts.len();
    for (p,insert) in q_inserts {
      // find the bbox with the largest overlap
      let mut best = (0.0, 0);
      for (j,(bbox,_)) in boxes.iter().enumerate() {
        let area = overlap_area(&bbox, &p);
        if area > best.0 {
          best = (area, j);
        }
      }
      boxes[best.1].1.push((p,insert));
    }
    for (bbox,inserts) in boxes {
      if !inserts.is_empty() && inserts.len() <= n {
        res.push((bbox,inserts));
      } else if !inserts.is_empty() && inserts.len() == q_inserts_len {
        let all_big = inserts.iter()
          .all(|(p,_)| coord_span_ge(&p.0, q_span.0) || coord_span_ge(&p.1, q_span.1));
        if all_big || depth+1 >= DEPTH_LIMIT {
          res.push((bbox,inserts));
        } else {
          queue.push_back((depth+1,(bbox,inserts)));
        }
      } else if !inserts.is_empty() && depth+1 >= DEPTH_LIMIT {
        res.push((bbox,inserts));
      } else if !inserts.is_empty() {
        queue.push_back((depth+1,(bbox,inserts)));
      }
    }
  }
  res
}

fn overlap_area(bbox: &B, p: &P) -> f32 {
  match p {
    (eyros::Coord::Scalar(x),eyros::Coord::Scalar(y)) => {
      if &(bbox.0).0 <= x && x <= &(bbox.1).0 && &(bbox.0).1 <= y && y <= &(bbox.1).1 {
        1.0
      } else {
        0.0
      }
    },
    (eyros::Coord::Interval(x0,x1),eyros::Coord::Interval(y0,y1)) => {
      if &(bbox.0).0 > x1 || &(bbox.1).0 < x0 { return 0.0; }
      if &(bbox.0).1 > y1 || &(bbox.1).1 < y0 { return 0.0; }
      let x = x1.min((bbox.1).0) - x0.max((bbox.0).0);
      let y = y1.min((bbox.1).1) - y0.max((bbox.0).1);
      x * y
    },
    _ => 0.0
  }
}

fn coord_span_ge(p: &eyros::Coord<f32>, span: f32) -> bool {
  match p {
    eyros::Coord::Scalar(_x) => false,
    eyros::Coord::Interval(min,max) => (max - min) > span,
  }
}
