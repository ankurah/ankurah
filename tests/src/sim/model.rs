//! Deterministic seed ids and hand-forged events.
//!
//! The commit path (`trx.create`) draws a random genesis nonce, and `EntityId`
//! is the genesis event's content hash, so that entropy poisons every downstream
//! id and defeats the determinism audit.
//! The harness sidesteps this the way the containment tests already do
//! (`tests/tests/update_batch_containment.rs::forge_title_event`): it
//! constructs `proto::Event` values directly with entity ids derived from the
//! seed, so ids are a pure function of the schedule. Events still flow through
//! the real Node ingest (`handle_peer_message` / `add_event` / `set_state`), so the
//! applier, staging, containment, and head-maintenance logic under test is the
//! production code, not a mock.

use ankurah::core::property::backend::{lww::LWWBackend, PropertyBackend};
use ankurah::core::property::PropertyKey;
use ankurah::core::value::Value;
use ankurah::proto::{self, Attested};
use ankurah::Model;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The LWW-backed model the scenarios drive. Two independently-writable
/// fields let a scenario produce genuinely concurrent, commuting writes
/// (different fields) as well as conflicting writes (same field) so the LWW
/// tiebreak is exercised.
#[derive(Debug, Clone, Serialize, Deserialize, Model)]
pub struct SimRecord {
    #[active_type(LWW)]
    pub title: String,
    #[active_type(LWW)]
    pub body: String,
}

/// A deterministic 32-byte id-shaped seed from a small integer. These bytes are
/// used directly for deliberately unknown ids and as deterministic genesis
/// entropy; created entity ids themselves are always derived from the complete
/// genesis preimage.
pub fn entity_id(counter: u64) -> proto::EntityId {
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&counter.to_be_bytes());
    proto::EntityId::from_bytes(bytes)
}

/// The `SimRecord` collection id.
pub fn sim_collection() -> proto::CollectionId { SimRecord::collection() }

/// The model-definition id the sim stamps on every forged `SimRecord` event,
/// state, and subscription-update item (#330). The harness forges wire
/// envelopes that bypass schema registration/relay, so ingress `resolve_model`
/// would reject an unknown id; [`super::node::build_nodes`] seeds this exact id
/// into every node's catalog so resolution routes it to the `SimRecord`
/// collection. Constant and deterministic, so it is identical across every node
/// in a run and across the two determinism-audit runs. It is deliberately
/// distinct from the domain-hashed well-known model ids and from the
/// seed-shaped unknown ids used by this harness.
pub fn sim_model_id() -> proto::EntityId { proto::EntityId::from_bytes([0x5B; 32]) }

/// Stable catalog identity for each simulated property. The simulation
/// bypasses ordinary transactions and forges events directly, so its payloads
/// must use the same id-keyed address that typed query registration retains.
pub fn sim_property_id(field: Field) -> proto::EntityId {
    let mut bytes = [0x5C; 32];
    bytes[31] = match field {
        Field::Title => 1,
        Field::Body => 2,
    };
    proto::EntityId::from_bytes(bytes)
}

/// Stable membership identity for each simulated property.
pub fn sim_membership_id(field: Field) -> proto::EntityId {
    let mut bytes = [0x5D; 32];
    bytes[31] = match field {
        Field::Title => 1,
        Field::Body => 2,
    };
    proto::EntityId::from_bytes(bytes)
}

/// Decode the `(title, body)` LWW field values from a materialized `proto::State`
/// as a subscriber would read them, for the C5 coherence checks that compare a
/// recorded read against the converged truth. An unset field, or a state with no
/// LWW buffer, reads as `None`, matching how the view getter surfaces an unset
/// field. Decoding failure is treated as absence rather than panicking, so a
/// malformed buffer degrades to a comparison miss, not a harness crash.
pub fn field_values(state: &proto::State) -> (Option<String>, Option<String>) {
    let Some(buffer) = state.state_buffers.0.get("lww") else { return (None, None) };
    let Ok(backend) = LWWBackend::from_state_buffer(buffer) else { return (None, None) };
    let read = |field| match backend.get(&PropertyKey::Id(sim_property_id(field))) {
        Some(Value::String(s)) => Some(s),
        _ => None,
    };
    (read(Field::Title), read(Field::Body))
}

/// Which LWW field a write targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    Title,
    Body,
}

impl Field {
    pub fn name(self) -> &'static str {
        match self {
            Field::Title => "title",
            Field::Body => "body",
        }
    }
}

/// Build the LWW `OperationSet` for setting one field to a value.
fn lww_ops(field: Field, value: &str) -> proto::OperationSet {
    let backend = LWWBackend::new();
    backend.set(PropertyKey::Id(sim_property_id(field)), Some(Value::String(value.to_owned())));
    let ops = backend.to_operations().unwrap().expect("a written LWW backend yields operations");
    proto::OperationSet(BTreeMap::from([("lww".to_owned(), ops)]))
}

/// Forge a deterministic, structurally valid genesis event. `seed` supplies
/// the nonce/timestamp entropy, while the returned entity id is derived from
/// the full genesis preimage exactly as production does.
pub fn genesis_event(seed: proto::EntityId, system: Option<proto::EntityId>, field: Field, value: &str) -> proto::Event {
    let seed_bytes = seed.to_bytes();
    let nonce = seed_bytes;
    let timestamp = u64::from_be_bytes(seed_bytes[24..].try_into().expect("fixed slice length"));
    let operations = lww_ops(field, value);
    let entity_id = proto::EntityId::from(proto::EventId::from_genesis_parts(&system, &nonce, timestamp, &operations));
    proto::Event {
        model: sim_model_id(),
        entity_id,
        parent: proto::Clock::default(),
        body: proto::EventBody::Genesis { system, nonce, timestamp, operations },
    }
}

/// Forge a non-genesis event parented on `parent`, setting `field` to `value`.
pub fn edit_event(entity: proto::EntityId, parent: proto::Clock, field: Field, value: &str) -> proto::Event {
    proto::Event { model: sim_model_id(), entity_id: entity, body: proto::EventBody::Update { operations: lww_ops(field, value) }, parent }
}

/// Wrap a forged event as an unsigned `Attested<Event>`. Under `PermissiveAgent`
/// attestations are empty, so this is byte-deterministic.
pub fn attest(event: proto::Event) -> Attested<proto::Event> { Attested::opt(event, None) }

/// Sort an event lineage into a causal (parents-before-children) order.
///
/// `dump_entity_events` returns events in storage-key (event-id) order, which
/// is not causal. The `CommitTransaction` request path applies a batch in
/// arrival order without re-sorting (its production callers, sequential local
/// commits, already emit parent-first), so a child-before-parent batch would
/// strand the child. The realistic relay emits causal order; the harness
/// matches that here. Ties (concurrent events at the same depth) break by event
/// id for determinism. Cycles are impossible in a content-addressed DAG.
pub fn causal_sort(mut events: Vec<Attested<proto::Event>>) -> Vec<Attested<proto::Event>> {
    use std::collections::{HashMap, HashSet};

    // Deduplicate by event id up front: the algorithm places each id once, so a
    // repeated event would otherwise leave a copy unplaceable and force the
    // defensive fallback. Keep the first occurrence.
    {
        let mut seen = HashSet::new();
        events.retain(|e| seen.insert(e.payload.id()));
    }

    // Index events by id and record which ids are present in this set.
    let present: HashSet<proto::EventId> = events.iter().map(|e| e.payload.id()).collect();

    // Kahn's algorithm over parents restricted to the present set.
    let mut remaining_parents: HashMap<proto::EventId, HashSet<proto::EventId>> = HashMap::new();
    for e in &events {
        let id = e.payload.id();
        let deps: HashSet<proto::EventId> = e.payload.parent.iter().filter(|p| present.contains(p)).cloned().collect();
        remaining_parents.insert(id, deps);
    }

    let mut ordered: Vec<Attested<proto::Event>> = Vec::with_capacity(events.len());
    let mut placed: HashSet<proto::EventId> = HashSet::new();

    while ordered.len() < events.len() {
        // All events whose present-parents are already placed, chosen in event-id
        // order for a deterministic tie-break.
        let mut ready: Vec<proto::EventId> = remaining_parents
            .iter()
            .filter(|(id, deps)| !placed.contains(*id) && deps.iter().all(|d| placed.contains(d)))
            .map(|(id, _)| id.clone())
            .collect();
        ready.sort();
        if ready.is_empty() {
            // Defensive: should not happen in an acyclic DAG. Append the rest in
            // id order to guarantee termination.
            events.sort_by_key(|e| e.payload.id());
            for e in events.into_iter() {
                if placed.insert(e.payload.id()) {
                    ordered.push(e);
                }
            }
            break;
        }
        for id in ready {
            if let Some(pos) = events.iter().position(|e| e.payload.id() == id) {
                let e = events.remove(pos);
                placed.insert(id);
                ordered.push(e);
            }
        }
    }
    ordered
}
