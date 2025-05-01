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
        match &self.system_root {
            Some(r) => {
                write!(f, "Presence[{}: durable {} system_root: {}]", self.node_id.to_base64_short(), self.durable, r.payload)
            }
            None => {
                write!(f, "Presence[{}: durable {}]", self.node_id.to_base64_short(), self.durable)
            }
        }
    }
}
