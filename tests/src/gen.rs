//! Correct generation stamping for hand-forged integration-test events.
//!
//! The forge helpers build `proto::Event` values from a parent *clock*, not from
//! parent events, so they cannot read the parents' generations to stamp
//! `1 + max(parent generations)`. This registry, keyed by `EventId`, lets a
//! helper look those up. It is sound across tests and threads because an
//! `EventId` is a content hash over the generation, so a found entry is always
//! the one correct value.
//!
//! A parent tip absent from the registry is a real committed event (created
//! through the normal commit path, not forged here). Such an event carries
//! generation 1 in this milestone: the commit lane stamps 1 until M2 wires the
//! topological `1 + max(parent)` stamp. Hence the `unwrap_or(1)` fallback, which
//! makes the common shape (a forged edit over a real genesis) stamp 2.

use ankurah::proto::{self, Clock, Event, EventId};
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

static REGISTRY: LazyLock<Mutex<HashMap<EventId, u32>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// The generation a child parented on `parent` must carry: `1 + max` of the
/// parent tips' generations (`1` when `parent` is empty). See the module note
/// on the `unwrap_or(1)` fallback for untracked real events.
pub fn generation_for(parent: &Clock) -> u32 {
    let registry = REGISTRY.lock().unwrap();
    Event::generation_from_parents(parent.iter().map(|id| registry.get(id).copied().unwrap_or(1)))
}

/// Record a forged event's `(id, generation)` so its forged children stamp correctly.
pub fn register(event: &Event) { REGISTRY.lock().unwrap().insert(event.id(), event.generation); }

/// Build an event stamped with the correct generation for its parents and
/// register it. The common path for the forge helpers.
pub fn stamped_event(entity_id: proto::EntityId, collection: proto::CollectionId, operations: proto::OperationSet, parent: Clock) -> Event {
    let generation = generation_for(&parent);
    let event = Event { entity_id, collection, operations, parent, generation };
    register(&event);
    event
}
