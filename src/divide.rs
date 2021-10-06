use crate::value::V;
use eyros::tree::InsertValue;

type P = (eyros::Coord<f32>,eyros::Coord<f32>);
type B = ((f32,f32),(f32,f32));
type I<'a> = (B,&'a [(P,InsertValue<'a,P,V>)]);

pub fn divide<'a>(n: usize, bucket: I<'a>) -> Vec<I<'a>> {
  unimplemented![]
}
