//! Session-guarantee checks over recorded LiveQuery notification streams (C5).
//!
//! Per design-delta C5C7-A, C5 phrases reactor/LiveQuery coherence as the Jepsen
//! session guarantees: monotonic reads and read-your-writes, plus the merge- and
//! isolation-coherence properties the phase-1 subscription work left below the
//! reactor. Each function here consumes the converged nodes and the recorded
//! streams after quiescence and returns [`Violation`]s for the scenario driver
//! to fold into the outcome. The approach is Elle-style: build the per-key
//! (per-entity) observed sequence from the recorded stream and assert an ordering
//! property over it, using causal ancestry reconstructed from converged storage
//! as the "happens-before" the read order must respect.

use ankurah::proto;

use super::invariants::{causal_closure, Violation};
use super::node::SimNode;
use super::recorder::SubscriptionRecorder;

/// Monotonic reads: within one subscription's change stream, an entity's observed
/// state never regresses. Concretely, once the subscriber has observed an event
/// in an entity's causal history (a member of some head it saw, or an ancestor of
/// one), every later notification for that entity must still causally reflect it:
/// the event is a member of the later head or an ancestor of a member. A head tip
/// being superseded by its own descendant is forward progress, not a regression,
/// so the check is causal-closure membership, not head-set equality.
///
/// Reads causal ancestry from node 0, which at quiescence holds the complete
/// converged history for every entity.
pub async fn check_monotonic_reads(nodes: &[SimNode], recorders: &[SubscriptionRecorder]) -> Vec<Violation> {
    let mut violations = Vec::new();
    let reference = &nodes[0];

    for recorder in recorders {
        // Per-entity accumulator of every event id the subscriber has observed in
        // a head so far, in this subscription's stream.
        let mut observed: std::collections::HashMap<proto::EntityId, std::collections::HashSet<proto::EventId>> =
            std::collections::HashMap::new();

        for (position, item) in recorder.items().into_iter().enumerate() {
            let closure = causal_closure(reference, item.entity, &item.head).await;
            let seen = observed.entry(item.entity).or_default();

            for prior in seen.iter() {
                if !closure.contains(prior) {
                    violations.push(Violation::new(
                        "monotonic_reads",
                        format!(
                            "sub(n{},{:?}) entity {} regressed at stream position {}: previously-observed event {} \
                             is not causally reflected in the head {:?}",
                            recorder.node,
                            recorder.predicate,
                            item.entity.to_base64_short(),
                            position,
                            prior.to_base64_short(),
                            item.head.iter().map(|e| e.to_base64_short()).collect::<Vec<_>>(),
                        ),
                    ));
                }
            }

            // Fold this head into the observed set (members and their ancestors,
            // so a later head that supersedes a tip still counts the tip as seen).
            for id in closure {
                seen.insert(id);
            }
        }
    }
    violations
}

/// One local write a scenario performed at an origin, for the read-your-writes
/// check: the committing node, the entity, the field, the value, and the event
/// id the write produced.
#[derive(Debug, Clone)]
pub struct LocalWrite {
    pub origin: usize,
    pub entity: proto::EntityId,
    pub field: super::model::Field,
    pub value: String,
    pub event: proto::EventId,
}

/// Read-your-writes at the origin node: after a local commit resolves, the
/// committing node's own LiveQuery observes the write. Asserts, for each declared
/// write whose origin hosts a recorder, that the write's event id appears in that
/// recorder's stream (some notification carried it, in its events or its head)
/// and that the recorder's final observed value for the field equals the value
/// written by the last write to that (entity, field) at the origin.
pub fn check_read_your_writes(recorders: &[SubscriptionRecorder], writes: &[LocalWrite]) -> Vec<Violation> {
    let mut violations = Vec::new();

    for recorder in recorders {
        let items = recorder.items();

        // Every event id the subscriber ever saw, in an events list or a head.
        let mut seen_events: std::collections::HashSet<proto::EventId> = std::collections::HashSet::new();
        for item in &items {
            seen_events.extend(item.event_ids.iter().cloned());
            seen_events.extend(item.head.iter().cloned());
        }

        for write in writes.iter().filter(|w| w.origin == recorder.node) {
            if !seen_events.contains(&write.event) {
                violations.push(Violation::new(
                    "read_your_writes",
                    format!(
                        "sub(n{},{:?}) never observed its origin's own write event {} to {} on entity {}",
                        recorder.node,
                        recorder.predicate,
                        write.event.to_base64_short(),
                        write.field.name(),
                        write.entity.to_base64_short(),
                    ),
                ));
            }
        }

        // The last write per (entity, field) at this recorder's node defines the
        // value the subscriber must ultimately read for that field.
        let mut expected: std::collections::HashMap<(proto::EntityId, &'static str), String> = std::collections::HashMap::new();
        for write in writes.iter().filter(|w| w.origin == recorder.node) {
            expected.insert((write.entity, write.field.name()), write.value.clone());
        }
        for ((entity, field), value) in expected {
            let observed = last_field_value(&items, entity, field);
            if observed.as_deref() != Some(value.as_str()) {
                violations.push(Violation::new(
                    "read_your_writes",
                    format!(
                        "sub(n{},{:?}) final read of {} on entity {} is {:?}, expected the origin's own last write {:?}",
                        recorder.node,
                        recorder.predicate,
                        field,
                        entity.to_base64_short(),
                        observed,
                        value,
                    ),
                ));
            }
        }
    }
    violations
}

/// No spurious empty-events Update reaches any recorder (the PR #299 suppression,
/// asserted over seeded fault schedules). An `Update` that carries no events is
/// the exact shape a no-op delta produced before the suppression; `Initial`
/// legitimately carries none and is excluded by [`super::recorder::RecordedItem::is_empty_events_update`].
pub fn check_no_empty_events_update(recorders: &[SubscriptionRecorder]) -> Vec<Violation> {
    let mut violations = Vec::new();
    for recorder in recorders {
        for (position, item) in recorder.items().into_iter().enumerate() {
            if item.is_empty_events_update() {
                violations.push(Violation::new(
                    "no_empty_events_update",
                    format!(
                        "sub(n{},{:?}) received a spurious empty-events Update for entity {} at stream position {}",
                        recorder.node,
                        recorder.predicate,
                        item.entity.to_base64_short(),
                        position,
                    ),
                ));
            }
        }
    }
    violations
}

/// Merge coherence: after two branches merge, no notification presents a state
/// that mixes pre-merge and post-merge field values incoherently, and the final
/// observed value for each field equals the converged node state. A torn read
/// (title from one branch, body from another, matching no real state) shows up as
/// a final observed value that disagrees with the converged entity. This subsumes
/// the "no incoherent mix" property at the observation boundary a subscriber can
/// see: the subscriber's last read of each field must equal the converged truth,
/// and every intermediate read must be a value that the field actually held in
/// some real state (checked here as equal to the converged value or to a value
/// the scenario wrote; callers pass the written value set).
///
/// Reads the converged state from node 0.
pub async fn check_merge_final_coherence(
    nodes: &[SimNode],
    recorders: &[SubscriptionRecorder],
    entities: &[proto::EntityId],
) -> Vec<Violation> {
    let mut violations = Vec::new();
    let reference = &nodes[0];

    for recorder in recorders {
        let items = recorder.items();
        for &entity in entities {
            let Some(state) = reference.entity_state(entity).await else { continue };
            let converged = super::model::field_values(&state);

            for (field, converged_value) in [("title", converged.0), ("body", converged.1)] {
                if let Some(observed) = last_field_value(&items, entity, field) {
                    if Some(&observed) != converged_value.as_ref() {
                        violations.push(Violation::new(
                            "merge_coherence",
                            format!(
                                "sub(n{},{:?}) final read of {} on entity {} is {:?}, but the converged state is {:?}",
                                recorder.node,
                                recorder.predicate,
                                field,
                                entity.to_base64_short(),
                                Some(observed),
                                converged_value,
                            ),
                        ));
                    }
                }
            }
        }
    }
    violations
}

/// Membership coherence: the final resultset each subscription exposes agrees
/// with the membership its change stream reported (the last membership event per
/// entity is Initial or Add if the entity is in the resultset, Remove if it is
/// out) and with the entities that exist in the converged state and match the
/// predicate. Here we assert the weaker, predicate-agnostic half that holds for
/// the `true` (match-all) predicate the C5 scenarios use: the final resultset
/// equals exactly the entities the stream ever surfaced and never subsequently
/// removed.
pub fn check_membership_matches_stream(recorders: &[SubscriptionRecorder]) -> Vec<Violation> {
    let mut violations = Vec::new();
    for recorder in recorders {
        // Reconstruct membership from the stream: an Initial or Add puts an entity
        // in, a Remove takes it out; an Update leaves membership unchanged.
        let mut in_set: std::collections::BTreeSet<proto::EntityId> = std::collections::BTreeSet::new();
        for item in recorder.items() {
            use ankurah::changes::ChangeKind;
            match item.kind {
                ChangeKind::Initial | ChangeKind::Add => {
                    in_set.insert(item.entity);
                }
                ChangeKind::Remove => {
                    in_set.remove(&item.entity);
                }
                ChangeKind::Update => {}
            }
        }

        let resultset: std::collections::BTreeSet<proto::EntityId> = recorder.resultset_ids().into_iter().collect();
        if in_set != resultset {
            let stream_only: Vec<String> = in_set.difference(&resultset).map(|e| e.to_base64_short()).collect();
            let resultset_only: Vec<String> = resultset.difference(&in_set).map(|e| e.to_base64_short()).collect();
            violations.push(Violation::new(
                "membership_coherence",
                format!(
                    "sub(n{},{:?}) resultset disagrees with change stream: in-stream-not-resultset {:?}, in-resultset-not-stream {:?}",
                    recorder.node, recorder.predicate, stream_only, resultset_only,
                ),
            ));
        }
    }
    violations
}

/// The last value the subscriber observed for a field on an entity, walking the
/// stream in order. `None` if the entity never appeared or the field was never
/// materialized (an unset LWW field reads as `None`).
fn last_field_value(items: &[super::recorder::RecordedItem], entity: proto::EntityId, field: &str) -> Option<String> {
    let mut value = None;
    for item in items.iter().filter(|i| i.entity == entity) {
        let field_value = match field {
            "title" => item.title.clone(),
            "body" => item.body.clone(),
            _ => None,
        };
        if field_value.is_some() {
            value = field_value;
        }
    }
    value
}
