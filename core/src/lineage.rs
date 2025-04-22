use ankurah_proto::{Attested, Clock, EntityId, Event, EventId};

use crate::{error::RetrievalError, storage::StorageCollection};

pub trait GetEvents {
    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError>;
}

/// a trait for events and eventlike things that can be descended
pub trait EventLike {
    type Id: Eq + PartialEq; // we do not compare event ids except equality, so we do not need to implement Ord

    fn id(&self) -> Self::Id;
    fn parent(&self) -> &Self;
}
pub trait ClockLike {
    type Id: Eq + PartialEq; // we do not compare event ids except equality, so we do not need to implement Ord
    fn members(&self) -> &[Self::Id];
}

impl EventLike for Event {
    type Id = EventId;
    fn id(&self) -> EventId { self.id() }
    fn parent(&self) -> &ClockLike<Self::Id> { &self.parent }
}

impl<T: StorageCollection> GetEvents for T {
    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> { self.get_events(event_ids).await }
}

pub fn descends<T: GetEvents>(getter: &T, possible_ancestor: &Clock, subject: &Clock) -> Result<bool, RetrievalError> {
    // The goal is to implement this function, and write a test suite that exercises linear,
    // concurrent, and unrelated histories. assume Cycles are not possible because the real
    //implementation is using hashes.
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestEvent {
        id: usize,
        parent: Vec<usize>,
    }

    impl EventLike for TestEvent {
        fn id(&self) -> EventId { self.id }
        fn parent(&self) -> &Clock { &self.parent }
    }
}
