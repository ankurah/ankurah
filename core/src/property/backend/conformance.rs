//! Backend conformance kit: executable laws every `PropertyBackend` must satisfy.
//!
//! TEST CODE ONLY. This entire module is `#![cfg(test)]`: it compiles into
//! core's own test builds and contributes nothing to the shipped library. It
//! lives inside core (rather than the integration-test crate) because the
//! laws exercise `apply_layer`, whose `EventLayer` parameter is crate
//! private; when the #267 remainder designs the public layer-view API (D8),
//! the kit can graduate to a standalone test-support crate that external
//! backend authors import directly.
//!
//! The property backend boundary (RFC ankurah#267) is a contract that any
//! implementation, including ones written outside this crate, must honor. This
//! module encodes that contract as reusable property tests parameterized over a
//! backend, so the laws run identically against every reference implementation
//! and a future third backend adopts the whole suite in a few lines (see
//! "Adopting the kit" below).
//!
//! # What the laws certify, and what they do not
//!
//! These laws are convergence and determinism laws. They certify that:
//!
//! - a backend round-trips its own state buffer and its own operations losslessly
//!   (including whatever provenance the backend needs for later resolution);
//! - two backend instances fed the same operations reach the same state
//!   (operation replay across nodes);
//! - `apply_layer` result is invariant under permutation of the mutually
//!   concurrent events within a layer (the events in one layer carry no causal
//!   relationship, so their arrival order must not change the outcome);
//! - the same event DAG delivered under different layer-arrival schedules
//!   converges to byte-identical state (cross-order determinism);
//! - the `_with_event` provenance hook is honored (LWW records the writing event
//!   id and uses it for conflict resolution; a CRDT is free to ignore it).
//!
//! They deliberately do NOT certify *merge usefulness* or *interleaving intent*.
//! The adversarial synthesis of the phase 2 literature review (PR ankurah#275,
//! design-deltas verdict 267-A, drawing on the interleaving paper of Weidner and
//! Kleppmann and on OpSets) established that every one of these laws passes on a
//! semantically wrong merge: a list-move that duplicates its element, a
//! rich-text edit whose spans interleave backwards, an LWW register that silently
//! discards the losing concurrent write all still converge deterministically and
//! satisfy every law here. Convergence is that the replicas agree; it is not that
//! they agree on something a user would call correct.
//!
//! Per verdict 267-A ("silence is the one unacceptable option"), intent quality
//! is addressed rather than ignored:
//!
//! - Where the intended outcome is mechanically checkable, the kit adds a law
//!   for it. For LWW this is the causal-dominance law (the
//!   `causal_dominance_beats_hash_tiebreak` test): a causally-later write must
//!   always beat a causally-earlier one regardless of content-hash tiebreak order
//!   (verdict 267-C). For Yrs this is the interleaving probe (the
//!   `sequence_intent_interleaving_characterization` test), which runs the backend
//!   directly against the adversarial concurrent-insert configuration and pins the
//!   *actual* merge outcome (verdict 267-B); it is a characterization test, not a
//!   guarantee that the DAG discipline preserves intent.
//! - Everything else about merge semantics (whether LWW's silent loss is
//!   acceptable for a given field, whether structured text belongs in a CRDT
//!   rather than an LWW register) is out of scope for the kit and is the
//!   application's and the schema author's responsibility. See verdicts 267-D and
//!   267-B.
//!
//! # Constructible-state scope (`ValueEntry::Pending`)
//!
//! LWW carries a `ValueEntry::Pending` variant for wire-applied-but-uncommitted
//! values. Its disposition is an open question bound to RFC ankurah#272 and the
//! ankurah#267 remainder, so the kit does not decide it and does not exercise it.
//! Every law here drives backends only through their committed, event-tracked
//! paths (`apply_operations_with_event`, `apply_layer`, `to_state_buffer`), which
//! is the set of states a backend actually reaches during normal DAG integration.
//! A `Pending` value is read-visible but neither merges nor persists (it is
//! rejected by `to_state_buffer` and ignored by `apply_layer`), so it is outside
//! the state space these laws range over. When ankurah#272 decides whether a
//! wire-applied uncommitted value is a Bayou-style tentative participant or a
//! non-authoritative staging marker, the corresponding law belongs in that PR.
//!
//! # Adopting the kit
//!
//! A backend adopts every law by implementing [`ConformanceBackend`] (a
//! test-only description of how to produce events and fresh instances for that
//! backend) and calling the law functions from a `#[test]`. The reference
//! implementations at the bottom of this file (LWW and Yrs) are the worked
//! examples; a third backend mirrors them.

#![cfg(test)]

use std::collections::BTreeMap;
use std::sync::Arc;

use ankurah_proto::{Clock, EntityId, Event, EventId, Operation, OperationSet};

use crate::event_dag::EventLayer;
use crate::property::backend::PropertyBackend;

/// The property backend under test, described for the conformance kit.
///
/// An adopter implements this to tell the laws how to construct fresh instances
/// of the backend and how to turn an abstract "write" into the backend-specific
/// operation payload the laws will thread through events. Everything else (event
/// construction, DAG assembly, layer permutation, replay) is generic.
///
/// A `Write` is one atomic edit the test wants a backend to record: for a
/// register backend it is a `(field, value)` pair; for a sequence backend it is
/// a text insertion. The kit never inspects a `Write`; it only round-trips it
/// through [`ConformanceBackend::stage_write`].
pub(crate) trait ConformanceBackend {
    /// A single abstract edit this backend understands.
    type Write: Clone;

    /// The backend name, matching `PropertyBackend::property_backend_name`.
    fn backend_name() -> &'static str;

    /// A fresh, empty backend instance.
    fn new_backend() -> Arc<dyn PropertyBackend>;

    /// Reconstruct a backend from a state buffer.
    fn from_state_buffer(buffer: &[u8]) -> Arc<dyn PropertyBackend>;

    /// Apply the abstract write to a fresh backend and return the resulting
    /// operations, so the kit can package them into an event. The backend passed
    /// in is scratch space owned by the caller; only the returned operations are
    /// used.
    fn stage_write(backend: &Arc<dyn PropertyBackend>, write: &Self::Write) -> Vec<Operation>;

    /// A deterministic list of mutually-concurrent writes for the permutation and
    /// cross-order laws. Each entry becomes one concurrent event branching off a
    /// shared root. Order in the returned vec is the "canonical" order; the laws
    /// permute it.
    fn concurrent_writes() -> Vec<Self::Write>;

    /// A deterministic linear sequence of writes for the round-trip laws, applied
    /// one after another to a single backend.
    fn linear_writes() -> Vec<Self::Write>;

    /// Assert the backend's provenance obligation after a single write was applied
    /// via `apply_operations_with_event` under `event_id`. LWW asserts the event
    /// id was recorded and is exposed for later resolution; a CRDT that ignores
    /// provenance leaves this a no-op. Called by [`law_provenance`].
    fn check_provenance(_backend: &Arc<dyn PropertyBackend>, _event_id: &EventId) {}
}

// ============================================================================
// Event / layer construction helpers (generic over the adopter)
// ============================================================================

/// Build an event carrying `operations` for `backend_name`, with the given
/// parents. Entity id is derived from `seed` so distinct seeds yield distinct
/// content-hashed event ids. Mirrors the event-building idiom in
/// `event_dag::tests`.
fn make_event(seed: u16, backend_name: &str, operations: Vec<Operation>, parents: &[EventId]) -> Event {
    let mut entity_id_bytes = [0u8; 16];
    entity_id_bytes[0..2].copy_from_slice(&seed.to_be_bytes());
    let entity_id = EntityId::from_bytes(entity_id_bytes);
    Event {
        entity_id,
        collection: "conformance".into(),
        parent: Clock::from(parents.to_vec()),
        operations: OperationSet(BTreeMap::from([(backend_name.to_string(), operations)])),
    }
}

/// Assemble an [`EventLayer`] from event references, deriving the DAG skeleton
/// (event id -> parent ids) from the events themselves plus any extra context
/// events that must be reachable for causal comparison but are not in the layer.
fn layer_from_events(already_applied: &[&Event], to_apply: &[&Event], context: &[&Event]) -> EventLayer {
    let mut dag = BTreeMap::new();
    for event in already_applied.iter().chain(to_apply.iter()).chain(context.iter()) {
        dag.insert(event.id(), event.parent.as_slice().to_vec());
    }
    EventLayer::new(already_applied.iter().map(|e| (*e).clone()).collect(), to_apply.iter().map(|e| (*e).clone()).collect(), Arc::new(dag))
}

// ============================================================================
// Laws
// ============================================================================

/// State-buffer round-trip: `to_state_buffer` then `from_state_buffer` yields a
/// backend whose own re-serialized state buffer is byte-identical, and whose
/// read-visible properties match. This certifies the state buffer carries
/// everything the backend needs to reconstruct itself, including the provenance
/// (LWW event ids) required for future conflict resolution.
pub(crate) fn law_state_buffer_round_trip<B: ConformanceBackend>() {
    let backend = B::new_backend();

    // Apply a linear sequence of writes, each committed under a distinct event id
    // so the backend records provenance where it needs to.
    let scratch = B::new_backend();
    for (i, write) in B::linear_writes().iter().enumerate() {
        let ops = B::stage_write(&scratch, write);
        let event = make_event(i as u16, B::backend_name(), ops.clone(), &[]);
        backend.apply_operations_with_event(&ops, event.id()).expect("apply_operations_with_event");
    }

    let buffer = backend.to_state_buffer().expect("to_state_buffer must succeed for a committed backend");
    let restored = B::from_state_buffer(&buffer);

    let buffer2 = restored.to_state_buffer().expect("restored backend must re-serialize");
    assert_eq!(buffer, buffer2, "[{}] state buffer must round-trip byte-for-byte", B::backend_name());

    assert_eq!(
        backend.property_values(),
        restored.property_values(),
        "[{}] restored backend must expose identical property values",
        B::backend_name()
    );
}

/// Operation round-trip across two nodes: node A applies a linear sequence of
/// writes via `apply_operations_with_event`; the operations from each event are
/// carried to node B (a second, independent backend instance) and applied the
/// same way. Both nodes must expose identical property values and produce
/// byte-identical state buffers. This is the wire path (`to_operations` on the
/// writer, `apply_operations` on the reader) between two nodes that never share
/// in-memory state.
pub(crate) fn law_operation_round_trip_across_nodes<B: ConformanceBackend>() {
    let node_a = B::new_backend();
    let node_b = B::new_backend();

    let scratch = B::new_backend();
    for (i, write) in B::linear_writes().iter().enumerate() {
        let ops = B::stage_write(&scratch, write);
        let event = make_event(i as u16, B::backend_name(), ops.clone(), &[]);
        // Node A records the write under its event id.
        node_a.apply_operations_with_event(&ops, event.id()).expect("node A apply_operations_with_event");
        // The same operations cross to node B under the same event id.
        node_b.apply_operations_with_event(&ops, event.id()).expect("node B apply_operations_with_event");
    }

    assert_eq!(
        node_a.property_values(),
        node_b.property_values(),
        "[{}] two nodes fed the same operations must expose identical property values",
        B::backend_name()
    );
    assert_eq!(
        node_a.to_state_buffer().expect("node A to_state_buffer"),
        node_b.to_state_buffer().expect("node B to_state_buffer"),
        "[{}] two nodes fed the same operations must produce byte-identical state buffers",
        B::backend_name()
    );
}

/// Provenance handling for the `_with_event` variant. This law is asserted at two
/// levels:
///
/// - Universal (all backends): applying an event's operations via
///   `apply_operations_with_event` and via `apply_operations` produces the same
///   read-visible values. Provenance may change internal bookkeeping, but it must
///   not change what a reader sees for a single non-conflicting write.
/// - Backend-specific (via `check_provenance`): the adopter asserts whatever the
///   backend promises about the writing event id. LWW asserts the event id was
///   recorded and is exposed for later conflict resolution; a CRDT asserts
///   nothing (it is free to ignore the id, so its check is a no-op).
///
/// The split exists because provenance is where backends legitimately differ: the
/// contract requires LWW to track the writing event (so `apply_layer` can resolve
/// concurrent writes by causal dominance) but explicitly permits a CRDT to ignore
/// it (RFC #267, `apply_operations_with_event` doc comment).
pub(crate) fn law_provenance<B: ConformanceBackend>() {
    let write = B::linear_writes().into_iter().next().expect("linear_writes must be non-empty");
    let scratch = B::new_backend();
    let ops = B::stage_write(&scratch, &write);
    let event = make_event(0, B::backend_name(), ops.clone(), &[]);

    let tracked = B::new_backend();
    tracked.apply_operations_with_event(&ops, event.id()).expect("apply_operations_with_event");

    let untracked = B::new_backend();
    untracked.apply_operations(&ops).expect("apply_operations");

    assert_eq!(
        tracked.property_values(),
        untracked.property_values(),
        "[{}] a single non-conflicting write must read the same with or without event tracking",
        B::backend_name()
    );

    // Backend-specific provenance obligations.
    B::check_provenance(&tracked, &event.id());
}

/// `apply_layer` invariance under permutation of the events WITHIN a layer.
///
/// The events in a single layer are mutually concurrent (no causal relationship),
/// so the contract requires the merge result to be independent of the order they
/// appear in `to_apply`. This builds a root event plus N concurrent children off
/// that root, then applies the single concurrent layer under every rotation of
/// the children and asserts the final state buffer is byte-identical across all
/// of them.
///
/// A backend that fails this law (a Yjs-family sequence CRDT can be
/// integration-order sensitive; see the module docs and verdict 267-B) is a
/// finding to pin, not a law to relax for other backends.
pub(crate) fn law_within_layer_permutation_invariance<B: ConformanceBackend>() {
    let writes = B::concurrent_writes();
    assert!(writes.len() >= 2, "[{}] concurrent_writes must supply at least two events", B::backend_name());

    // Shared root the concurrent children branch off of.
    let scratch = B::new_backend();
    let root_ops = B::stage_write(&scratch, &writes[0]);
    let root = make_event(1000, B::backend_name(), root_ops.clone(), &[]);

    // Build the concurrent children, all parented on the root.
    let children: Vec<Event> = writes
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let ops = B::stage_write(&scratch, w);
            make_event(2000 + i as u16, B::backend_name(), ops, &[root.id()])
        })
        .collect();

    let permutations = rotations(&children);
    let mut reference: Option<Vec<u8>> = None;

    for perm in &permutations {
        let backend = B::new_backend();
        // Seed the root as committed state (the meet the layer branches from).
        if let Some(ops) = root.operations.get(B::backend_name()) {
            backend.apply_operations_with_event(ops, root.id()).expect("apply root");
        }

        let to_apply: Vec<&Event> = perm.iter().collect();
        let context = [&root];
        let layer = layer_from_events(&[], &to_apply, &context);
        backend.apply_layer(&layer).expect("apply_layer");

        let buffer = backend.to_state_buffer().expect("to_state_buffer after layer");
        match &reference {
            None => reference = Some(buffer),
            Some(expected) => assert_eq!(
                *expected,
                buffer,
                "[{}] apply_layer must be invariant under within-layer permutation of concurrent events",
                B::backend_name()
            ),
        }
    }
}

/// Determinism across delivery orders of the same DAG.
///
/// The same event set, delivered under different layer-arrival schedules, must
/// converge to byte-identical state. This models a diamond DAG (root, two
/// concurrent branches, a merge event joining them) and applies it two ways:
///
/// - schedule 1: the concurrent branches arrive together in one layer, then the
///   merge event arrives in the next layer;
/// - schedule 2: one branch arrives in its own layer, then the other branch and
///   the merge arrive, exercising a different layering of the identical DAG.
///
/// Both schedules deliver the same events with the same parent structure, so a
/// backend whose resolution depends only on graph facts (never arrival order)
/// must reach the same final state buffer.
pub(crate) fn law_cross_order_determinism<B: ConformanceBackend>() {
    let writes = B::concurrent_writes();
    assert!(writes.len() >= 3, "[{}] cross-order law needs at least three writes", B::backend_name());

    let scratch = B::new_backend();

    // root <- {branch_l, branch_r} <- merge
    let root = make_event(3000, B::backend_name(), B::stage_write(&scratch, &writes[0]), &[]);
    let branch_l = make_event(3001, B::backend_name(), B::stage_write(&scratch, &writes[1]), &[root.id()]);
    let branch_r = make_event(3002, B::backend_name(), B::stage_write(&scratch, &writes[2]), &[root.id()]);
    let merge = make_event(3003, B::backend_name(), B::stage_write(&scratch, &writes[0]), &[branch_l.id(), branch_r.id()]);

    let context = [&root, &branch_l, &branch_r, &merge];

    // Schedule 1: [branch_l, branch_r] then [merge].
    let final_1 = {
        let backend = B::new_backend();
        apply_committed_root::<B>(&backend, &root);
        let layer_a = layer_from_events(&[], &[&branch_l, &branch_r], &context);
        backend.apply_layer(&layer_a).expect("schedule 1 layer a");
        let layer_b = layer_from_events(&[&branch_l, &branch_r], &[&merge], &context);
        backend.apply_layer(&layer_b).expect("schedule 1 layer b");
        backend.to_state_buffer().expect("schedule 1 buffer")
    };

    // Schedule 2: [branch_r] then [branch_l] then [merge]. Same DAG, different
    // layering (the two concurrent branches arrive in separate layers, opposite
    // order), so already_applied context differs at each step.
    let final_2 = {
        let backend = B::new_backend();
        apply_committed_root::<B>(&backend, &root);
        let layer_a = layer_from_events(&[], &[&branch_r], &context);
        backend.apply_layer(&layer_a).expect("schedule 2 layer a");
        let layer_b = layer_from_events(&[&branch_r], &[&branch_l], &context);
        backend.apply_layer(&layer_b).expect("schedule 2 layer b");
        let layer_c = layer_from_events(&[&branch_l, &branch_r], &[&merge], &context);
        backend.apply_layer(&layer_c).expect("schedule 2 layer c");
        backend.to_state_buffer().expect("schedule 2 buffer")
    };

    assert_eq!(
        final_1,
        final_2,
        "[{}] the same DAG delivered under different layer-arrival schedules must converge to identical state",
        B::backend_name()
    );
}

/// Apply the backend-relevant operations of `root` as committed state, so the
/// root is the meet the subsequent layers branch from.
fn apply_committed_root<B: ConformanceBackend>(backend: &Arc<dyn PropertyBackend>, root: &Event) {
    if let Some(ops) = root.operations.get(B::backend_name()) {
        backend.apply_operations_with_event(ops, root.id()).expect("apply committed root");
    }
}

/// Every rotation of a slice (n rotations for n elements). Enough distinct
/// within-layer orderings to catch order sensitivity without factorial blowup;
/// the reference backends use small concurrent sets so rotations cover the
/// meaningful cases (first-applied, last-applied, and every position between).
fn rotations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    (0..items.len())
        .map(|shift| {
            let mut v = Vec::with_capacity(items.len());
            v.extend_from_slice(&items[shift..]);
            v.extend_from_slice(&items[..shift]);
            v
        })
        .collect()
}

// ============================================================================
// Reference implementation: LWW
// ============================================================================

mod lww_conformance {
    use super::*;
    use crate::event_dag::CausalRelation;
    use crate::property::backend::lww::LWWBackend;
    use crate::value::Value;

    /// LWW adopter. A `Write` is a `(field, value)` register assignment.
    struct LwwAdopter;

    impl ConformanceBackend for LwwAdopter {
        type Write = (&'static str, &'static str);

        fn backend_name() -> &'static str { LWWBackend::property_backend_name() }

        fn new_backend() -> Arc<dyn PropertyBackend> { Arc::new(LWWBackend::new()) }

        fn from_state_buffer(buffer: &[u8]) -> Arc<dyn PropertyBackend> {
            Arc::new(LWWBackend::from_state_buffer(&buffer.to_vec()).expect("LWW from_state_buffer"))
        }

        fn stage_write(_backend: &Arc<dyn PropertyBackend>, write: &Self::Write) -> Vec<Operation> {
            // Stage on a fresh backend so to_operations returns exactly this write.
            let scratch = LWWBackend::new();
            scratch.set(write.0.to_string(), Some(Value::String(write.1.to_string())));
            scratch.to_operations().expect("to_operations").expect("LWW write must produce operations")
        }

        fn concurrent_writes() -> Vec<Self::Write> {
            // All target the same field so they genuinely conflict.
            vec![("field", "alpha"), ("field", "bravo"), ("field", "charlie")]
        }

        fn linear_writes() -> Vec<Self::Write> { vec![("title", "first"), ("body", "second"), ("title", "third"), ("tag", "fourth")] }

        fn check_provenance(backend: &Arc<dyn PropertyBackend>, event_id: &EventId) {
            // LWW must record the writing event id so apply_layer can later resolve
            // concurrent writes by causal dominance (RFC #267).
            let lww = backend.clone().as_arc_dyn_any().downcast::<LWWBackend>().expect("backend is LWW");
            let recorded = lww.get_event_id(&"title".to_string());
            assert_eq!(recorded.as_ref(), Some(event_id), "LWW must record the writing event id for provenance");
        }
    }

    #[test]
    fn state_buffer_round_trip() { law_state_buffer_round_trip::<LwwAdopter>(); }

    #[test]
    fn operation_round_trip_across_nodes() { law_operation_round_trip_across_nodes::<LwwAdopter>(); }

    #[test]
    fn provenance() { law_provenance::<LwwAdopter>(); }

    #[test]
    fn within_layer_permutation_invariance() { law_within_layer_permutation_invariance::<LwwAdopter>(); }

    #[test]
    fn cross_order_determinism() { law_cross_order_determinism::<LwwAdopter>(); }

    /// Build an LWW event writing a single field, with the given parents.
    fn lww_write_event(seed: u16, field: &str, value: &str, parents: &[EventId]) -> Event {
        let scratch = LWWBackend::new();
        scratch.set(field.to_string(), Some(Value::String(value.to_string())));
        let ops = scratch.to_operations().unwrap().unwrap();
        make_event(seed, LwwAdopter::backend_name(), ops, parents)
    }

    /// Intent law for LWW (verdict 267-C): a causally-LATER write must always beat
    /// a causally-earlier one, regardless of how the content-hash tiebreak would
    /// order their event ids. This is the one intent guarantee LWW actually makes
    /// beyond convergence: it does not merely converge, it converges on the write
    /// that causally happened last. The content-hash tiebreak in `apply_layer` is
    /// load-bearing only for truly concurrent writes; it must never override the
    /// causal order (verdict 267-C promotes this stage ordering from an
    /// implementation detail to an asserted law).
    ///
    /// The test searches seeds for a linear pair (earlier <- later) whose event
    /// ids sort so that the earlier id is LARGER. If the resolver mistakenly fell
    /// to the id tiebreak, the causally-earlier write would win; the law requires
    /// the causally-later write to win anyway.
    #[test]
    fn causal_dominance_beats_hash_tiebreak() {
        let field = "status";

        // Find a seed pair where earlier.id() > later.id() (adversarial for a
        // naive id-max tiebreak). The pair is a linear chain: earlier <- later.
        let mut found = None;
        for early_seed in 0u16..200 {
            let earlier = lww_write_event(early_seed, field, "earlier-write", &[]);
            let later = lww_write_event(early_seed.wrapping_add(5000), field, "later-write", &[earlier.id()]);
            // later descends from earlier by construction; adversarial when the
            // earlier id would win a raw lexicographic max.
            if earlier.id() > later.id() {
                found = Some((earlier, later));
                break;
            }
        }
        let (earlier, later) = found.expect("expected to find an adversarial id-ordered linear pair within seed budget");

        // Sanity: the pair really is linear (later descends from earlier), so a
        // correct resolver decides by causal dominance, not by the id tiebreak.
        let layer = layer_from_events(&[&earlier], &[&later], &[]);
        assert_eq!(layer.compare(&later.id(), &earlier.id()), CausalRelation::Descends);

        // Apply earlier as committed state, then the later event via a layer.
        let backend: Arc<dyn PropertyBackend> = Arc::new(LWWBackend::new());
        if let Some(ops) = earlier.operations.get(LwwAdopter::backend_name()) {
            backend.apply_operations_with_event(ops, earlier.id()).unwrap();
        }
        backend.apply_layer(&layer).unwrap();

        assert_eq!(
            backend.property_value(&field.to_string()),
            Some(Value::String("later-write".to_string())),
            "the causally-later write must win even though the earlier event id sorts larger"
        );
    }
}

// ============================================================================
// Reference implementation: Yrs
// ============================================================================

mod yrs_conformance {
    use super::*;
    use crate::property::backend::yrs::YrsBackend;

    /// Yrs adopter. A `Write` is a text insertion `(field, index, text)`.
    struct YrsAdopter;

    impl ConformanceBackend for YrsAdopter {
        type Write = (&'static str, u32, &'static str);

        fn backend_name() -> &'static str { YrsBackend::property_backend_name() }

        fn new_backend() -> Arc<dyn PropertyBackend> { Arc::new(YrsBackend::new()) }

        fn from_state_buffer(buffer: &[u8]) -> Arc<dyn PropertyBackend> {
            Arc::new(YrsBackend::from_state_buffer(&buffer.to_vec()).expect("Yrs from_state_buffer"))
        }

        fn stage_write(_backend: &Arc<dyn PropertyBackend>, write: &Self::Write) -> Vec<Operation> {
            // A fresh doc's diff against its empty starting state is a self-contained
            // update inserting the text; this is the wire form of one edit.
            let scratch = YrsBackend::new();
            scratch.insert(write.0, write.1, write.2).expect("yrs insert");
            scratch.to_operations().expect("to_operations").expect("yrs write must produce operations")
        }

        fn concurrent_writes() -> Vec<Self::Write> {
            // Concurrent inserts at the same position on the same field: the classic
            // interleaving-prone configuration.
            vec![("body", 0, "aaa"), ("body", 0, "bbb"), ("body", 0, "ccc")]
        }

        fn linear_writes() -> Vec<Self::Write> {
            // Applied to distinct fields so the linear round-trip is unambiguous.
            vec![("title", 0, "Hello"), ("body", 0, "World"), ("tag", 0, "x")]
        }

        // check_provenance: default no-op. Yrs is a CRDT and ignores the writing
        // event id (RFC #267 permits this); it has no per-property provenance to
        // assert.
    }

    #[test]
    fn state_buffer_round_trip() { law_state_buffer_round_trip::<YrsAdopter>(); }

    #[test]
    fn operation_round_trip_across_nodes() { law_operation_round_trip_across_nodes::<YrsAdopter>(); }

    #[test]
    fn provenance() { law_provenance::<YrsAdopter>(); }

    #[test]
    fn cross_order_determinism() { law_cross_order_determinism::<YrsAdopter>(); }

    #[test]
    fn within_layer_permutation_invariance() { law_within_layer_permutation_invariance::<YrsAdopter>(); }

    /// Sequence-intent characterization for Yrs (verdict 267-B).
    ///
    /// The lit review flagged the Yjs family as interleaving-prone: the generic
    /// laws certify that replicas converge, not that they converge on a merge a
    /// user would call correct. This probe runs Yrs directly against the canonical
    /// configuration the review named (two replicas each inserting a run at the
    /// SAME position, concurrently) and pins the ACTUAL merged string, so the
    /// intent question is answered with evidence rather than left silent.
    ///
    /// This is a characterization test, not a guarantee. It asserts three things,
    /// all empirically observed against the current yrs:
    ///
    /// 1. Convergence (the law): both op-application orders of the concurrent
    ///    inserts yield the byte-identical merged string. This holds.
    /// 2. No loss: both concurrent runs survive in full (merged length is the sum).
    /// 3. The observed order: for two disjoint concurrent runs at the same
    ///    position, yrs keeps each run CONTIGUOUS (it does not splice them
    ///    character-by-character) and orders the two blocks deterministically by
    ///    CRDT site precedence. The concrete result is pinned below. If a future
    ///    yrs upgrade changes the merge (e.g. adopts Fugue-style non-interleaving,
    ///    or begins interleaving these runs), the pinned string changes and forces
    ///    a deliberate re-read rather than a silent semantic shift.
    ///
    /// FINDING (surfaced in the PR body): which run leads is NOT reproducible
    /// across process runs. `yrs::Doc::new()` assigns a RANDOM client id, and the
    /// CRDT breaks the tie between two concurrent same-position inserts by site
    /// precedence, so the merged string is "xxxyyy" or "yyyxxx" depending on the
    /// draw. Convergence is unaffected (every replica applies the same event bytes
    /// and agrees), but the concrete merge is a coin flip. This is the executable
    /// proof of verdict 267-B: the laws pin convergence; they cannot pin intent,
    /// and here they cannot even pin the byte outcome because the backend seeds it
    /// from randomness. The test therefore asserts the reproducible invariants
    /// (convergence, no loss, each run contiguous, result is one of the two block
    /// orders) rather than a single hard-coded string.
    ///
    /// The point is to make the merge semantics executable and load-bearing:
    /// structured text must live in a CRDT backend chosen with these semantics
    /// understood, and the generic laws certify convergence but never intent
    /// (module docs; verdicts 267-A, 267-B, 267-D).
    #[test]
    fn sequence_intent_interleaving_characterization() {
        // Two concurrent replicas, each inserting a 3-char run at position 0 of an
        // empty field. No shared root, so the only content is the two concurrent
        // runs; the merged order is entirely the CRDT's merge decision.
        let left = ("body", 0u32, "xxx");
        let right = ("body", 0u32, "yyy");

        let scratch = YrsAdopter::new_backend();
        let event_l = make_event(4000, YrsAdopter::backend_name(), YrsAdopter::stage_write(&scratch, &left), &[]);
        let event_r = make_event(4001, YrsAdopter::backend_name(), YrsAdopter::stage_write(&scratch, &right), &[]);

        // Apply in both orders; both are a single layer of two concurrent events.
        let merged = |order: [&Event; 2]| -> String {
            let backend = YrsAdopter::new_backend();
            let refs: Vec<&Event> = order.to_vec();
            let layer = layer_from_events(&[], &refs, &[]);
            backend.apply_layer(&layer).unwrap();
            match backend.property_value(&"body".to_string()) {
                Some(crate::value::Value::String(s)) => s,
                other => panic!("expected a string body, got {other:?}"),
            }
        };

        let lr = merged([&event_l, &event_r]);
        let rl = merged([&event_r, &event_l]);

        // (1) Convergence: op-application order does not change the merged string.
        // This is deterministic within a run (same event bytes both ways).
        assert_eq!(lr, rl, "Yrs must converge on the same merged string regardless of op-application order");

        // (2) No loss: both concurrent runs survive in full.
        assert_eq!(lr.len(), left.2.len() + right.2.len(), "both concurrent runs must survive the merge");

        // (3) Contiguity: each run appears as an unbroken block (yrs does not splice
        // these disjoint concurrent runs character-by-character), and the overall
        // order is one of the two block orderings. WHICH ordering is the random
        // client-id tie-break (see the FINDING above), so we accept either.
        assert!(lr == "xxxyyy" || lr == "yyyxxx", "expected two contiguous blocks in one of the two site-precedence orders, got {lr:?}");
        assert!(lr.contains("xxx") && lr.contains("yyy"), "each run must appear contiguously");
    }
}
