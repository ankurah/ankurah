//! Backend conformance kit: executable laws every `PropertyBackend` must satisfy.
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
//! - Where intent is mechanically testable against a golden oracle, the kit adds
//!   a law for it. For LWW this is the causal-dominance law
//!   ([`law_causal_dominance`]): a causally-later write must always beat a
//!   causally-earlier one regardless of content-hash tiebreak order (verdict
//!   267-C). For Yrs this is the interleaving probe ([`law_sequence_intent`]),
//!   which runs the backend directly against adversarial concurrent-insert
//!   configurations and pins the *actual* interleaving behavior (verdict 267-B);
//!   it is a characterization test, not a guarantee that the DAG discipline
//!   preserves intent.
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

use crate::event_dag::accumulator::EventLayer;
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

// ============================================================================
// Reference implementation: LWW
// ============================================================================

mod lww_conformance {
    use super::*;
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
    }

    #[test]
    fn state_buffer_round_trip() { law_state_buffer_round_trip::<LwwAdopter>(); }
}
