struct Entity {
    id: Uuid,
    state: u64, // just a counter for now
}

// EventId is a sha256
struct EventId([u8; 32]);

struct Event {
    entity_id: Uuid,
    payload: i64, // just an increment for now
    parent: Clock,
}
impl Event {
    fn id(&self) -> EventId {
        let mut hasher = Sha256::new();
        hasher.update(&self.entity_id.to_bytes());
        hasher.update(&self.payload.to_bytes());
        hasher.update(&self.parent.hash().to_bytes());
        EventId(hasher.finalize().into())
    }
}
struct Clock {
    events: Vec<EventId>,
}
