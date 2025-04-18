use serde::{Deserialize, Serialize};

use crate::id::ID;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Presence {
    pub node_id: ID,
    pub durable: bool,
}

impl std::fmt::Display for Presence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "Presence({} {})", self.node_id, self.durable) }
}
