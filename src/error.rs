use crate::ID_PREFIX;
use std::backtrace::Backtrace;
pub type Error = Box<dyn std::error::Error+Send+Sync>;

#[derive(Debug)]
pub struct IngestError {
  kind: IngestErrorKind,
  backtrace: Backtrace,
}

#[derive(Debug)]
pub enum IngestErrorKind {
  NonIdKey { prefix: u8 },
}

impl IngestErrorKind {
  pub fn raise<T>(self) -> Result<T,IngestError> {
    Err(IngestError {
      kind: self,
      backtrace: Backtrace::capture(),
    })
  }
}

impl std::error::Error for IngestError {
  fn backtrace<'a>(&'a self) -> Option<&'a Backtrace> {
    Some(&self.backtrace)
  }
}

impl std::fmt::Display for IngestError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match &self.kind {
      IngestErrorKind::NonIdKey { prefix } => {
        write![f, "expected {} (ID_PREFIX), found {}", ID_PREFIX, prefix]
      },
    }
  }
}
