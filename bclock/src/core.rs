use sha2::{Digest, Sha256};
use uuid::Uuid;

pub struct EntityId(Uuid);
impl AsRef<[u8]> for EntityId {
    fn as_ref(&self) -> &[u8] { self.0.as_bytes() }
}

// An Entity is essentially a vertex in a graph database that has a unique id, a state, and lineage of events.
pub struct Entity {
    id: EntityId,
    state: u64,  // just a counter for now
    head: Clock, // the head of the entity's causal history
}

// EventId is a sha256
struct EventId([u8; 32]);

impl AsRef<[u8]> for EventId {
    fn as_ref(&self) -> &[u8] { &self.0 }
}

struct Event {
    entity_id: EntityId,
    payload: i64, // just an increment for now
    precursors: Clock,
}
impl Event {
    fn id(&self) -> EventId {
        let mut hasher = Sha256::new();
        hasher.update(self.entity_id.as_ref());
        hasher.update(&self.payload.to_le_bytes());
        self.precursors.digest(&mut hasher);
        EventId(hasher.finalize().into())
    }
}
struct Clock {
    events: Vec<EventId>,
}

impl Clock {
    fn digest(&self, hasher: &mut Sha256) {
        for event in &self.events {
            hasher.update(event);
        }
    }
}
