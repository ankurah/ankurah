//! Core traits for event DAG types.

use std::fmt::Display;

/// Abstract trait for event IDs in the DAG.
/// in practice this will be a proto::EventId, but we need to abstract it for testing
pub trait EventId: Eq + PartialEq + Clone + Ord + Display + Send + Sync {}

/// Abstract trait for clock types (sets of event IDs).
/// in practice this will be a proto::Clock, but we need to abstract it for testing
pub trait TClock {
    type Id: EventId;

    /// Get the member event IDs in this clock.
    fn members(&self) -> &[Self::Id];
}

/// Abstract trait for events in the DAG.
/// in practice this will be a proto::Event, but we need to abstract it for testing
pub trait TEvent: Display {
    type Id: EventId;
    type Parent: TClock<Id = Self::Id>;

    /// Get this event's ID.
    fn id(&self) -> Self::Id;

    /// Get this event's parent clock.
    fn parent(&self) -> &Self::Parent;
}

// Blanket implementation for common ID types
impl EventId for u32 {}
impl EventId for u64 {}
impl EventId for String {}

// For ankurah_proto types
impl EventId for ankurah_proto::EventId {}

impl TClock for ankurah_proto::Clock {
    type Id = ankurah_proto::EventId;

    fn members(&self) -> &[Self::Id] { self.as_slice() }
}

impl TEvent for ankurah_proto::Event {
    type Id = ankurah_proto::EventId;
    type Parent = ankurah_proto::Clock;

    fn id(&self) -> Self::Id { self.id() }

    fn parent(&self) -> &Self::Parent { &self.parent }
}
