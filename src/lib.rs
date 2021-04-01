pub mod encoder;
pub use encoder::*;
pub mod varint;

pub const BACKREF_PREFIX: u8 = 1;
