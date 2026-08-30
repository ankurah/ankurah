//! Deterministic entity ids and hand-forged events.
//!
//! Every event id, and therefore every entity id, is a hash over a
//! creator-random nonce, so events minted by the real commit path
//! (`trx.create`) carry fresh entropy on every run and would defeat the
//! determinism audit. The harness sidesteps this the way the containment
//! tests already do (`tests/tests/update_batch_containment.rs`): it
//! constructs `proto::Event` values directly, with a nonce derived from the
//! event's own content instead of drawn at random, so every id is a pure
//! function of the schedule. Events still flow through the real Node ingest
//! (`handle_message` / `add_event` / `set_state`), so the applier, staging,
//! containment, and head-maintenance logic under test is the production
//! code, not a mock.

use ankurah::core::property::backend::{lww::LWWBackend, PropertyBackend};
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

/// A deterministic id from a small integer that names NO genesis event, so
/// no entity can ever legitimately carry it. The uncreated-entity scenarios
/// use it to assert that such an id never materializes anywhere.
///
/// The low 8 bytes hold the counter and the rest are zero, so distinct
/// counters give distinct, stable ids with no entropy.
pub fn uncreated_entity_id(counter: u64) -> proto::EntityId {
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&counter.to_be_bytes());
    proto::EntityId::from_bytes(bytes)
}

/// The system root every forged genesis binds itself to.
///
/// A real system root's id is derived from its own genesis and so differs on
/// every run; the harness needs a constant to keep ids seed-derived. Nothing
/// checks an event's `system` field against the receiving node's root yet,
/// so a constant is sound here. Whichever branch adds that check has to seed
/// the sim nodes with a deterministic root instead.
pub fn sim_system_id() -> proto::EntityId { proto::EntityId::from_bytes([0x5A; 32]) }

/// A nonce derived from the event's own content and its place in the schedule,
/// rather than drawn at random.
///
/// This is the one thing the harness deliberately does differently from the
/// production mint, and it is what restores the property the sim depends on:
/// an event's id is a pure function of what the schedule asked for, so a run
/// can be compared against a previous run.
///
/// `mint_seq` is the caller's own monotonic count of mints, and it is what
/// keeps two mints of the same content distinct — asking twice for the same
/// field set to the same value from the same parent is two events, and without
/// the sequence number the two would share an id and collapse into one. The
/// counter belongs to the caller rather than to a process-wide static because a
/// shared static would be bumped by whatever other scenario happened to be
/// running in the same test binary, which is exactly the entropy the harness
/// exists to keep out.
///
/// Harness ids therefore do NOT carry the distinctness a production nonce does:
/// production draws fresh bytes so that two `create()` calls with identical
/// payloads are different entities even when nothing in the caller
/// distinguishes them, and no determinism audit here exercises that.
fn content_nonce(mint_seq: u64, parts: &[&[u8]]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"ankurah.sim.nonce.v0");
    hasher.update(mint_seq.to_be_bytes());
    for part in parts {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}

/// The `SimRecord` collection id.
pub fn sim_collection() -> proto::CollectionId { SimRecord::collection() }

/// The model-entity id the sim seeds into every node's catalog and asserts
/// in every forged creation event's membership Add. Derived from the forged
/// catalog genesis's own content ([`forged_catalog_rows`]), so it is a pure
/// function of fixed content: identical across every node in a run and
/// across the two determinism-audit runs.
pub fn sim_model_id() -> proto::EntityId { forged_catalog_rows().model }

/// The catalog identity of each simulated property, derived like
/// [`sim_model_id`].
pub fn sim_property_id(field: Field) -> proto::EntityId {
    let rows = forged_catalog_rows();
    match field {
        Field::Title => rows.properties[0],
        Field::Body => rows.properties[1],
    }
}

/// The membership identity binding each simulated property to the model,
/// derived like [`sim_model_id`].
pub fn sim_membership_id(field: Field) -> proto::EntityId {
    let rows = forged_catalog_rows();
    match field {
        Field::Title => rows.memberships[0],
        Field::Body => rows.memberships[1],
    }
}

/// The catalog rows describing `SimRecord`: one model row, a property row
/// per field, and the membership row binding each to the model, forged by
/// [`crate::catalog_forge`] with ids derived from each row's own genesis
/// content -- a pure function of the definition, identical across nodes,
/// runs, and the determinism audit's paired executions.
///
/// The rows are planted in the durable engine BEFORE its node is
/// constructed, so the catalog projection derives resolution for
/// "simrecord" from real stored rows -- the membership gate on the remote
/// commit funnel judges every forged genesis against that resolution -- and
/// serves them to each ephemeral node's projection over the sim transport
/// with real event lineage behind every state. `seed_sim_schema` still
/// binds each node's compiled descriptor cells locally.
pub fn forged_catalog_rows() -> &'static crate::catalog_forge::ForgedCatalog {
    static FORGED: std::sync::OnceLock<crate::catalog_forge::ForgedCatalog> = std::sync::OnceLock::new();
    FORGED.get_or_init(|| {
        crate::catalog_forge::forge_catalog("simrecord", "SimRecord", &[("title", "lww", "string"), ("body", "lww", "string")], b"sim")
    })
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
    let read = |field: Field| match backend.get(&field.property_id()) {
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

    /// The forged durable identity this field writes under: the same seeded
    /// id `seed_registered_schema` admits into every node's catalog, so
    /// harness writes and the compiled binding address one identity.
    pub fn property_id(self) -> proto::PropertyId { proto::PropertyId::EntityId(sim_property_id(self)) }
}

/// Build the LWW `OperationSet` for setting one field to a value.
fn lww_ops(field: Field, value: &str) -> proto::OperationSet {
    let backend = LWWBackend::new();
    backend.set(field.property_id(), Some(Value::String(value.to_owned())));
    let ops = backend.to_operations().unwrap().expect("a written LWW backend yields operations");
    proto::OperationSet::from_backends(BTreeMap::from([("lww".to_owned(), ops)]))
}

/// Forge an entity's genesis event, deriving its entity id from the event's
/// own content exactly as the production mint does. `counter` is the caller's
/// mint sequence number, which distinguishes two entities that would otherwise
/// be created with identical content; `field`/`value` seed the initial state.
pub fn genesis_event(counter: u64, field: Field, value: &str) -> proto::Event {
    let mut operations = lww_ops(field, value);
    operations.push(proto::Operation::Membership(proto::Membership::Add(ankurah::ModelId::EntityId(sim_model_id()))));
    let system = Some(sim_system_id());
    let nonce = content_nonce(counter, &[field.name().as_bytes(), value.as_bytes()]);
    let author = proto::AuthorId::Unknown;
    let entity_id: proto::EntityId = proto::EventId::from_genesis_parts(&system, &nonce, SIM_TIMESTAMP, &author, &operations).into();
    proto::Event {
        collection: SimRecord::collection(),
        entity_id,
        parent: proto::Clock::default(),
        body: proto::EventBody::Genesis { system, nonce, timestamp: SIM_TIMESTAMP, author, operations },
    }
}

/// Forge a non-genesis event parented on `parent`, setting `field` to `value`.
///
/// `mint_seq` is the caller's mint sequence number. Two edits of the same field
/// to the same value from the same parent are two events, and this is what
/// keeps them two; see [`content_nonce`].
pub fn edit_event(entity: proto::EntityId, parent: proto::Clock, field: Field, value: &str, mint_seq: u64) -> proto::Event {
    let nonce =
        content_nonce(mint_seq, &[entity.to_bytes().as_slice(), parent.to_base64().as_bytes(), field.name().as_bytes(), value.as_bytes()]);
    proto::Event {
        collection: SimRecord::collection(),
        entity_id: entity,
        parent,
        body: proto::EventBody::Update {
            nonce,
            timestamp: SIM_TIMESTAMP,
            author: proto::AuthorId::Unknown,
            operations: lww_ops(field, value),
        },
    }
}

/// The advisory timestamp every forged event carries. Fixed, because a
/// wall-clock read would be entropy in the digest.
const SIM_TIMESTAMP: u64 = 0;

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
