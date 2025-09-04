use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
pub struct QueryId(Ulid);

impl QueryId {
    pub fn new() -> Self { Self(Ulid::new()) }

    /// To be used only for testing
    pub fn test(id: u64) -> Self { Self(Ulid::from_parts(id, 0)) }
}

impl std::fmt::Display for QueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "P-{}", self.0.to_string()) }
}
