use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{auth::Attested, clock::Clock, collection::CollectionId, id::ID};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Event {
    pub id: ID,
    pub collection: CollectionId,
    pub entity_id: ID,
    pub operations: BTreeMap<String, Vec<Operation>>,
    /// The set of concurrent events (usually only one) which is the precursor of this event
    pub parent: Clock,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Operation {
    pub diff: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct State {
    /// The current accumulated state of the entity inclusive of all events up to this point
    pub state_buffers: BTreeMap<String, Vec<u8>>,
    /// The set of concurrent events (usually only one) which have been applied to the entity state above
    pub head: Clock,
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Event({} {}/{} {} {})",
            self.id,
            self.collection,
            self.entity_id,
            self.parent,
            self.operations
                .iter()
                .map(|(backend, ops)| format!("{} => {}b", backend, ops.iter().map(|op| op.diff.len()).sum::<usize>()))
                .collect::<Vec<_>>()
                .join(" ")
        )
    }
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "State(clock {} buffers {})",
            self.head,
            self.state_buffers.iter().map(|(backend, buf)| format!("{} => {}b", backend, buf.len())).collect::<Vec<_>>().join(" ")
        )
    }
}

impl Attested<Event> {
    pub fn collection(&self) -> &CollectionId { &self.payload.collection }
}
