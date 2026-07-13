//! Registry-free event forging for integration tests.
//!
//! An event's generation is read from the payloads of its parent EVENTS,
//! never from a side store: the M1-review follow-up ruling (2026-07-09) bans
//! generation registries of any kind, because a value read from the payload
//! is covered by the event id while any side store reintroduces the mistake
//! and forgery surface the mandatory hashed field exists to kill.
//!
//! Three constructors, by parent provenance:
//! - [`event_with_parents`]: the parent events are in scope; stamp from them.
//! - [`event_onto_stored_head`]: only a head clock is in scope; resolve the
//!   parent events from the node's storage. An unresolvable parent PANICS: a
//!   real parent that cannot be read back is a harness bug, never a value to
//!   guess (no silent fallback).
//! - [`event_claiming`]: the parent id is deliberately fabricated or the
//!   claim is deliberately wrong; the caller states the claimed generation
//!   explicitly, which makes adversarial construction self-documenting.

use ankurah::core::storage::StorageCollectionWrapper;
use ankurah::proto::{self, Clock, Event};

/// Forge an event over its parent EVENTS: the parent clock is their ids and
/// the generation is `1 + max` of their payload generations (an empty list
/// forges a genesis at generation 1).
pub fn event_with_parents(
    entity_id: proto::EntityId,
    model: proto::EntityId,
    operations: proto::OperationSet,
    parents: &[&Event],
) -> Event {
    Event {
        entity_id,
        model,
        operations,
        parent: Clock::from(parents.iter().map(|p| p.id()).collect::<Vec<_>>()),
        generation: Event::generation_from_parents(parents.iter().map(|p| p.generation)),
    }
}

/// Forge an event onto an explicit parent clock with an EXPLICIT claimed
/// generation. For tests that fabricate parent ids (no true generation
/// exists to resolve) or deliberately mis-stamp. Honest constructions use
/// [`event_with_parents`] or [`event_onto_stored_head`].
pub fn event_claiming(
    entity_id: proto::EntityId,
    model: proto::EntityId,
    operations: proto::OperationSet,
    parent: Clock,
    generation: u32,
) -> Event {
    Event { entity_id, model, operations, parent, generation }
}

/// Forge an event onto `parent`, resolving every parent EVENT from the given
/// collection storage to stamp the true generation. Panics on a missing
/// parent: resolution failure is a harness bug (the test forged onto a head
/// whose events it never committed), never a license to guess.
pub async fn event_onto_stored_head(
    storage: &StorageCollectionWrapper,
    entity_id: proto::EntityId,
    model: proto::EntityId,
    operations: proto::OperationSet,
    parent: Clock,
) -> Event {
    let parent_ids: Vec<proto::EventId> = parent.as_slice().to_vec();
    let found = storage.get_events(parent_ids.clone()).await.expect("parent event lookup must succeed");
    let generations: Vec<u32> = parent_ids
        .iter()
        .map(|pid| {
            found
                .iter()
                .find(|e| e.payload.id() == *pid)
                .unwrap_or_else(|| panic!("parent event {pid} is not in storage; forge onto a committed head or use event_claiming"))
                .payload
                .generation
        })
        .collect();
    Event { entity_id, model, operations, parent, generation: Event::generation_from_parents(generations) }
}
