use serde::{Deserialize, Serialize};

use crate::{id::EntityId, Attested, EntityState};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Presence {
    pub node_id: EntityId,
    pub durable: bool,
    pub system_root: Option<Attested<EntityState>>,
}

impl std::fmt::Display for Presence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Presence(node_id {} durable {} system_root: {:?})", self.node_id, self.durable, self.system_root)
    }
}
