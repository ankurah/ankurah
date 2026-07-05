//! Post-quiescence invariants.
//!
//! Convergence and no-lost-write are liveness-adjacent: the testing lit review
//! (aphyr, Kleppmann SEC) is emphatic that convergence is a liveness property no
//! finite run can prove, only violate, and only if checked at an enforced
//! quiescence point (faults healed, in-flight drained). The scenario driver
//! establishes that barrier; these functions run only after it. The
//! convergence assertion follows Jepsen/Elle's causal-suite phrasing: every
//! node's final read of an entity exists, contains every acknowledged write,
//! and is byte-equal across nodes.

use ankurah::proto;
use ankurah::Model;

use super::node::SimNode;

/// A single invariant violation, with enough context to understand the failure
/// once the seeded-failure artifact has been used to reproduce it.
#[derive(Debug, Clone)]
pub struct Violation {
    pub invariant: &'static str,
    pub detail: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "[{}] {}", self.invariant, self.detail) }
}

/// The set of entity ids the harness knows were legitimately created (the
/// "acknowledged writes" universe). Used by no-lost-write and no-phantom.
pub struct ExpectedUniverse {
    /// Entity ids that were committed at some origin during the run.
    pub created: Vec<proto::EntityId>,
    /// Entity ids that were never created and must never be materialized on any
    /// node (reserved unknowns in phantom-eviction scenarios).
    pub forbidden: Vec<proto::EntityId>,
}

/// Byte-canonical form of a materialized state for cross-node equality. The
/// `proto::State` fields are all order-independent (state buffers are a
/// `BTreeMap`, the head clock is sorted), so bincode is a canonical digest.
fn canonical(state: &proto::State) -> Vec<u8> { bincode::serialize(state).expect("State serializes") }

/// Convergence: for every entity any node materialized, all nodes that hold it
/// agree byte-for-byte, and no node that should hold it is missing it. Because
/// the harness propagates every origin change to every node before quiescence,
/// "should hold it" is "all nodes" for every created entity.
pub async fn check_convergence(nodes: &[SimNode], universe: &ExpectedUniverse) -> Vec<Violation> {
    let mut violations = Vec::new();

    for &entity in &universe.created {
        // Gather each node's canonical state (or None if absent).
        let mut states: Vec<(usize, Option<Vec<u8>>)> = Vec::new();
        for node in nodes {
            states.push((node.index, node.entity_state(entity).await.map(|s| canonical(&s))));
        }

        // Every node must have materialized the entity.
        let missing: Vec<usize> = states.iter().filter(|(_, s)| s.is_none()).map(|(i, _)| *i).collect();
        if !missing.is_empty() {
            violations.push(Violation {
                invariant: "convergence",
                detail: format!("entity {} absent on nodes {:?} at quiescence", entity.to_base64_short(), missing),
            });
            continue;
        }

        // All present states must be byte-equal.
        let reference = states[0].1.clone().unwrap();
        let divergent: Vec<usize> = states.iter().filter(|(_, s)| s.as_ref() != Some(&reference)).map(|(i, _)| *i).collect();
        if !divergent.is_empty() {
            let heads: Vec<String> = {
                let mut hs = Vec::new();
                for node in nodes {
                    let h = node.entity_state(entity).await.map(|s| s.head.to_base64_short()).unwrap_or_else(|| "<none>".into());
                    hs.push(format!("n{}={}", node.index, h));
                }
                hs
            };
            violations.push(Violation {
                invariant: "convergence",
                detail: format!(
                    "entity {} diverged; nodes {:?} differ from n{}; heads: {}",
                    entity.to_base64_short(),
                    divergent,
                    states[0].0,
                    heads.join(" ")
                ),
            });
        }
    }

    violations
}

/// No lost write: every entity created at an origin is materialized on every
/// node at quiescence. (A finer per-event check is layered by scenarios that
/// track expected head event ids; this is the entity-presence floor.)
pub async fn check_no_lost_write(nodes: &[SimNode], universe: &ExpectedUniverse) -> Vec<Violation> {
    let mut violations = Vec::new();
    for &entity in &universe.created {
        for node in nodes {
            if node.entity_state(entity).await.is_none() {
                violations.push(Violation {
                    invariant: "no_lost_write",
                    detail: format!("created entity {} never reached n{}", entity.to_base64_short(), node.index),
                });
            }
        }
    }
    violations
}

/// No phantom entity: no node materializes an entity that was never created at
/// an origin. Catches the V6 failure mode where a speculative empty-head
/// resident for an unknown entity is left behind instead of evicted.
pub async fn check_no_phantom(nodes: &[SimNode], universe: &ExpectedUniverse) -> Vec<Violation> {
    let mut violations = Vec::new();
    let expected: std::collections::HashSet<_> = universe.created.iter().copied().collect();
    for node in nodes {
        // Storage-materialized entities: none may be uncreated.
        for entity in node.known_entities().await {
            if !expected.contains(&entity) {
                violations.push(Violation {
                    invariant: "no_phantom",
                    detail: format!("n{} materialized uncreated entity {}", node.index, entity.to_base64_short()),
                });
            }
        }
        // Explicitly forbidden entities must not be materialized even as a
        // resident-only empty-head phantom (the V6 eviction case): the resident-
        // first read catches a phantom that never reached storage.
        for &entity in &universe.forbidden {
            if node.entity_state(entity).await.is_some() {
                violations.push(Violation {
                    invariant: "no_phantom",
                    detail: format!("n{} holds forbidden (never-created) entity {} as a phantom", node.index, entity.to_base64_short()),
                });
            }
        }
    }
    violations
}

/// Head antichain validity: an entity's head clock must be an antichain (no
/// event in the head is an ancestor of another event in the head). A head that
/// still lists an event that a sibling supersedes is a non-antichain tip, the
/// head-maintenance bug class. We check the local closure: for every entity on
/// every node, no head event appears in the parent-closure of another head
/// event (as reconstructed from that node's stored events).
pub async fn check_head_antichain(nodes: &[SimNode], universe: &ExpectedUniverse) -> Vec<Violation> {
    let mut violations = Vec::new();
    for node in nodes {
        for &entity in &universe.created {
            let Some(state) = node.entity_state(entity).await else { continue };
            let head: Vec<proto::EventId> = state.head.to_vec();
            if head.len() < 2 {
                continue; // A single-tip (or empty) head is trivially an antichain.
            }
            // Build the parent map from this node's stored events.
            let events = {
                let collection = match node.node.collections.get(&super::model::SimRecord::collection()).await {
                    Ok(c) => c,
                    Err(_) => continue,
                };
                match collection.dump_entity_events(entity).await {
                    Ok(evs) => evs,
                    Err(_) => continue,
                }
            };
            let parent_of: std::collections::HashMap<proto::EventId, Vec<proto::EventId>> =
                events.iter().map(|e| (e.payload.id(), e.payload.parent.to_vec())).collect();

            // For each head event, compute its strict ancestor set and ensure
            // no other head event is inside it.
            for (i, a) in head.iter().enumerate() {
                let ancestors = strict_ancestors(a, &parent_of);
                for (j, b) in head.iter().enumerate() {
                    if i != j && ancestors.contains(b) {
                        violations.push(Violation {
                            invariant: "head_antichain",
                            detail: format!(
                                "n{} entity {} head not an antichain: {} is an ancestor of {}",
                                node.index,
                                entity.to_base64_short(),
                                b.to_base64_short(),
                                a.to_base64_short()
                            ),
                        });
                    }
                }
            }
        }
    }
    violations
}

/// Strict ancestor set of `start` via the parent map (bounded walk; the map is
/// this node's finite stored history so it terminates).
fn strict_ancestors(
    start: &proto::EventId,
    parent_of: &std::collections::HashMap<proto::EventId, Vec<proto::EventId>>,
) -> std::collections::HashSet<proto::EventId> {
    let mut seen = std::collections::HashSet::new();
    let mut stack: Vec<proto::EventId> = parent_of.get(start).cloned().unwrap_or_default();
    while let Some(id) = stack.pop() {
        if seen.insert(id.clone()) {
            if let Some(parents) = parent_of.get(&id) {
                stack.extend(parents.iter().cloned());
            }
        }
    }
    seen
}

/// Run every invariant and collect all violations. The result is sorted so the
/// SIMFAIL artifact line is itself reproducible: without this, a violation set
/// gathered partly from a storage scan could render in a different textual order
/// across runs of one seed, making the log line C2 consumes non-deterministic
/// even when the trace hash matched.
pub async fn check_all(nodes: &[SimNode], universe: &ExpectedUniverse) -> Vec<Violation> {
    let mut violations = Vec::new();
    violations.extend(check_convergence(nodes, universe).await);
    violations.extend(check_no_lost_write(nodes, universe).await);
    violations.extend(check_no_phantom(nodes, universe).await);
    violations.extend(check_head_antichain(nodes, universe).await);
    violations.sort_by(|a, b| (a.invariant, &a.detail).cmp(&(b.invariant, &b.detail)));
    violations
}

/// The largest head-clock length observed across all entities on all nodes at
/// quiescence. A value >= 2 proves the run actually produced a multi-head, so
/// the antichain invariant was exercised on genuine concurrent state rather
/// than trivially satisfied by single-tip heads. Reported on the outcome for
/// scenarios that mean to assert a multi-head formed.
pub async fn max_head_len(nodes: &[SimNode], universe: &ExpectedUniverse) -> usize {
    let mut max = 0;
    for node in nodes {
        for &entity in &universe.created {
            if let Some(state) = node.entity_state(entity).await {
                max = max.max(state.head.len());
            }
        }
    }
    max
}
