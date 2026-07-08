//! Correct generation stamping for hand-built test events.
//!
//! Test helpers construct `proto::Event` values directly from parent *ids* (not
//! parent events), so they cannot see the parents' generations to stamp
//! `1 + max(parent generations)` themselves. This module holds a process-wide
//! registry keyed by `EventId` so a helper can look those generations up.
//!
//! The registry is sound across tests and threads for one reason: an `EventId`
//! is a content hash *over the generation*, so two events sharing an id
//! necessarily share a generation. A lookup can therefore only ever return the
//! one correct value or miss (a forged or dangling parent never built here),
//! and a miss degrades to the genesis rule, which is untested for those
//! adversarial shapes.

use ankurah_proto::{Event, EventId};
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

static REGISTRY: LazyLock<Mutex<HashMap<EventId, u32>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// The generation a child of `parent_ids` must carry: `1 + max(parent
/// generations)`, or `1` when no parent is known (a genesis, or a forged
/// parent never registered here).
pub(crate) fn generation_for(parent_ids: &[EventId]) -> u32 {
    let registry = REGISTRY.lock().unwrap();
    let known = parent_ids.iter().filter_map(|id| registry.get(id).copied());
    Event::generation_from_parents(known)
}

/// Record a built event's `(id, generation)` so its children stamp correctly.
pub(crate) fn register(event: &Event) { REGISTRY.lock().unwrap().insert(event.id(), event.generation); }

/// Build an event stamped with the correct generation for its parents and
/// register it. The common path for the id-parent test helpers.
pub(crate) fn stamped(
    entity_id: ankurah_proto::EntityId,
    collection: &str,
    operations: ankurah_proto::OperationSet,
    parent_ids: &[EventId],
) -> Event {
    let generation = generation_for(parent_ids);
    let event =
        Event { entity_id, collection: collection.into(), operations, parent: ankurah_proto::Clock::from(parent_ids.to_vec()), generation };
    register(&event);
    event
}
