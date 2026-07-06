//! Reactor and LiveQuery coherence under merge schedules (C5), smoke tier.
//!
//! Phase-1 subscription testing stopped below the reactor. These scenarios close
//! that seam using the C1 simulation harness, phrasing coherence as the Jepsen
//! session guarantees per design-delta C5C7-A: monotonic reads and
//! read-your-writes, plus notification coherence under the DivergedSince merge
//! path and cross-subscription isolation (the PR #299 class). Each records the
//! ordered `ChangeSet` stream a subscriber observes and asserts a session
//! guarantee over it after quiescence, alongside the standing convergence
//! invariants.
//!
//! The smoke tier runs a few seeds each in the normal test job. `sim_swarm.rs`
//! carries the SIM_SEEDS-driven swarm variants. Every no-fault scenario also runs
//! through a determinism audit (same seed twice, identical trace): PR #285
//! (stable durable-peer fan-out and subscription-id-ordered notification
//! emission) is the prerequisite that makes a multi-subscription scenario
//! reproducible, so the audit both guards that and is the first thing to fail if
//! it regresses.
//!
//! Faulted scenarios use `FaultConfig::swarm_subscription_safe`, which is the
//! swarm subset minus `reorder`. Reordering the subscription-setup handshake
//! makes the relay retry on a real 5-second timer the deterministic scheduler
//! cannot advance, stalling the barrier and breaking reproducibility (issue
//! #321). The remaining fault kinds still perturb propagation order, so the
//! coherence properties are exercised under an adverse schedule.

use ankurah::proto;
use ankurah_tests::sim::{body, coherence, run_recording, FaultConfig, Field, LocalWrite, SimOutcome, Workload};

// ---------------------------------------------------------------------------
// 1. Monotonic reads.
// ---------------------------------------------------------------------------

/// A LiveQuery's observed resultset never regresses: an entity never reverts to
/// an older state within one subscription's change stream, across any seeded
/// fault schedule. An ephemeral subscriber watches an entity that the durable
/// origin creates and then edits several times; under delay/dup/drop/partition
/// each wave arrives perturbed, but the subscriber's observed causal frontier
/// must only grow.
///
/// Each edit wave is drained with `settle()` before the next. A live ephemeral
/// subscriber receives each committed change as a relayed `SubscriptionUpdate`
/// whose outbound batching uses a real timer (`client_relay.rs` `ready_chunks`);
/// piling several undrained edits into one faulted barrier lets that relay
/// traffic accumulate against the timer and stalls the drain in wall-clock time
/// (the same real-timer class as issue #321). Draining each wave keeps the run
/// deterministic and timely while still delivering every edit under faults, so
/// monotonicity is exercised on a genuinely perturbed stream.
#[test]
fn monotonic_reads_under_faults() {
    for seed in 0..8u64 {
        let faults = FaultConfig::swarm_subscription_safe(seed);
        let outcome = run_recording(
            "monotonic_reads",
            seed,
            faults,
            3,
            body(|w: &mut Workload| {
                Box::pin(async move {
                    w.subscribe_recording(1, "true").await;
                    let e = w.create_at(0, Field::Title, "v0").await;
                    w.settle().await;
                    w.edit_at(0, e, Field::Body, "v1").await;
                    w.settle().await;
                    w.edit_at(0, e, Field::Title, "v2").await;
                    w.settle().await;
                    w.edit_at(0, e, Field::Body, "v3").await;
                })
            }),
            |nodes, recorders| {
                Box::pin(async move {
                    let mut v = coherence::check_monotonic_reads(nodes, recorders).await;
                    v.extend(coherence::check_no_empty_events_update(recorders));
                    v
                })
            },
        );
        outcome.assert_converged();
    }
}

/// The monotonic-reads scenario under a fixed schedule, run twice: the coherence
/// verdict must be reproducible.
///
/// This asserts reproducibility of the session-guarantee outcome, not byte-equal
/// traces. A scenario holding a live subscription cannot promise an identical
/// trace on replay: the client relay drives outbound `SubscriptionUpdate`
/// batching on a real timer (`ready_chunks`) and sends on `task::spawn` tasks
/// whose interleaving the seeded RNG does not control, so under machine load the
/// relay Updates can land in different drain rounds and the trace digest differs
/// (the same real-timer / uncontrolled-spawn class as issue #321). The coherence
/// verdict is stable regardless, because it is computed at quiescence over the
/// converged state and the observed values, not over the delivery interleaving.
/// The trace-hash determinism audit remains in force for the non-subscription C1
/// scenarios (`sim_swarm::swarm_determinism_audit`).
#[test]
fn monotonic_reads_reproducible_verdict() {
    for seed in 0..3u64 {
        let run = || {
            run_recording(
                "monotonic_reads_det",
                seed,
                FaultConfig::none(),
                3,
                body(|w: &mut Workload| {
                    Box::pin(async move {
                        w.subscribe_recording(1, "true").await;
                        let e = w.create_at(0, Field::Title, "v0").await;
                        w.edit_at(0, e, Field::Body, "v1").await;
                        w.edit_at(0, e, Field::Title, "v2").await;
                    })
                }),
                |nodes, recorders| Box::pin(async move { coherence::check_monotonic_reads(nodes, recorders).await }),
            )
        };
        assert_same_verdict(run(), run(), seed);
    }
}

// ---------------------------------------------------------------------------
// 2. Read-your-writes at the origin node.
// ---------------------------------------------------------------------------

/// After a local commit resolves, the committing node's own LiveQuery observes
/// the write. The durable node hosts a subscription and commits locally; its own
/// stream must carry each write's event and end at the written values.
#[test]
fn read_your_writes_at_origin() {
    for seed in 0..6u64 {
        let faults = FaultConfig::swarm_subscription_safe(seed);
        // The body records the writes it makes (with the event id each produced,
        // captured from the post-write head) so the post-quiescence check knows
        // what the origin must have read.
        let writes: std::sync::Arc<std::sync::Mutex<Vec<LocalWrite>>> = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let writes_body = writes.clone();
        let outcome = run_recording(
            "read_your_writes",
            seed,
            faults,
            2,
            body(move |w: &mut Workload| {
                let writes = writes_body.clone();
                Box::pin(async move {
                    // The durable origin (node 0) subscribes to its own writes.
                    w.subscribe_recording(0, "true").await;

                    let e = w.create_at(0, Field::Title, "created").await;
                    record_write(&writes, w, 0, e, Field::Title, "created");
                    w.edit_at(0, e, Field::Body, "edited").await;
                    record_write(&writes, w, 0, e, Field::Body, "edited");
                    w.edit_at(0, e, Field::Title, "re-edited").await;
                    record_write(&writes, w, 0, e, Field::Title, "re-edited");
                })
            }),
            {
                let writes = writes.clone();
                move |_nodes, recorders| {
                    let writes = writes.lock().unwrap().clone();
                    Box::pin(async move { coherence::check_read_your_writes(recorders, &writes) })
                }
            },
        );
        outcome.assert_converged();
    }
}

// ---------------------------------------------------------------------------
// 3. Notification coherence under concurrent merges (DivergedSince).
// ---------------------------------------------------------------------------

/// When two branches merge (the DivergedSince layer-replay path), subscribers
/// observe a consistent progression: no notification mixes pre-merge and
/// post-merge values incoherently (the subscriber's final read of each field
/// equals the converged truth), no empty-events Update spuriously fires, and the
/// resultset membership agrees with the change stream. Two origins make genuinely
/// concurrent commuting edits to different fields; the receiver merges them.
#[test]
fn merge_coherence_under_faults() {
    for seed in 0..8u64 {
        let faults = FaultConfig::swarm_subscription_safe(seed);
        let entity_cell: std::sync::Arc<std::sync::Mutex<Option<proto::EntityId>>> = std::sync::Arc::new(std::sync::Mutex::new(None));
        let entity_body = entity_cell.clone();
        let outcome = run_recording(
            "merge_coherence",
            seed,
            faults,
            3,
            body(move |w: &mut Workload| {
                let entity_cell = entity_body.clone();
                Box::pin(async move {
                    // Node 2 watches everything.
                    w.subscribe_recording(2, "true").await;

                    // Create a base and make sure every node holds it.
                    let e = w.create_at(0, Field::Title, "base").await;
                    *entity_cell.lock().unwrap() = Some(e);
                    w.settle().await;

                    // Two concurrent commuting edits from two origins on the shared
                    // fork: different fields, so they merge via DivergedSince into a
                    // two-tip head carrying both writes.
                    let fork = w.head_of(e).unwrap();
                    w.edit_from(0, e, fork.clone(), Field::Title, "title-from-n0").await;
                    w.edit_from(1, e, fork, Field::Body, "body-from-n1").await;
                })
            }),
            {
                let entity_cell = entity_cell.clone();
                move |nodes, recorders| {
                    let entities: Vec<proto::EntityId> = entity_cell.lock().unwrap().iter().copied().collect();
                    Box::pin(async move {
                        let mut v = coherence::check_monotonic_reads(nodes, recorders).await;
                        v.extend(coherence::check_merge_final_coherence(nodes, recorders, &entities).await);
                        v.extend(coherence::check_no_empty_events_update(recorders));
                        v.extend(coherence::check_membership_matches_stream(recorders));
                        v
                    })
                }
            },
        );
        outcome.assert_converged();
        assert_eq!(outcome.max_head_len, 2, "merge scenario must produce a genuine two-tip head (seed {seed})");
    }
}

/// The merge scenario run twice: the coherence verdict must be reproducible.
/// Asserts the outcome, not a byte-equal trace, for the reason documented on
/// `monotonic_reads_reproducible_verdict` (subscription relay real-timer / spawn
/// ordering, issue #321).
#[test]
fn merge_coherence_reproducible_verdict() {
    for seed in 0..3u64 {
        let run = || {
            run_recording(
                "merge_coherence_det",
                seed,
                FaultConfig::none(),
                3,
                body(|w: &mut Workload| {
                    Box::pin(async move {
                        w.subscribe_recording(2, "true").await;
                        let e = w.create_at(0, Field::Title, "base").await;
                        w.settle().await;
                        let fork = w.head_of(e).unwrap();
                        w.edit_from(0, e, fork.clone(), Field::Title, "t0").await;
                        w.edit_from(1, e, fork, Field::Body, "b1").await;
                    })
                }),
                |_nodes, recorders| Box::pin(async move { coherence::check_no_empty_events_update(recorders) }),
            )
        };
        assert_same_verdict(run(), run(), seed);
    }
}

// ---------------------------------------------------------------------------
// 4. Cross-subscription isolation (PR #299 class).
// ---------------------------------------------------------------------------

/// One query's no-op initialization must not surface on another established
/// query. Subscription A (established first and settled, so it holds the
/// entities) must observe no spurious empty-events Update when subscription B is
/// established afterward on the same node and its initialization re-delivers
/// states A already holds. Run under seeded fault schedules rather than a single
/// gated interleaving, so the regression is pinned across schedules.
#[test]
fn cross_subscription_isolation_under_faults() {
    for seed in 0..8u64 {
        let faults = FaultConfig::swarm_subscription_safe(seed);
        let outcome = run_recording(
            "cross_subscription_isolation",
            seed,
            faults,
            2,
            body(|w: &mut Workload| {
                Box::pin(async move {
                    // Subscription A on node 1, established first.
                    let a = w.subscribe_recording(1, "true").await;
                    assert_eq!(a, 0, "recorder A is index 0");

                    // Entities created on the durable node and converged onto node
                    // 1, so A holds them before B is established.
                    let e0 = w.create_at(0, Field::Title, "a").await;
                    let e1 = w.create_at(0, Field::Title, "b").await;
                    w.settle().await;

                    // Subscription B on the same node, established now. Its
                    // initialization fetches states A already holds; a no-op
                    // re-delivery must not surface on A as an empty-events Update.
                    let b = w.subscribe_recording(1, "true").await;
                    assert_eq!(b, 1, "recorder B is index 1");

                    // A further edit after both subscriptions exist, so real
                    // Updates are present in the stream to distinguish from the
                    // forbidden empty-events ones.
                    w.edit_at(0, e0, Field::Body, "a2").await;
                    w.edit_at(0, e1, Field::Body, "b2").await;
                })
            }),
            |_nodes, recorders| {
                // The load-bearing assertion: NO subscription (A or B) ever sees a
                // spurious empty-events Update. Also assert both streams agree with
                // their resultsets.
                Box::pin(async move {
                    let mut v = coherence::check_no_empty_events_update(recorders);
                    v.extend(coherence::check_membership_matches_stream(recorders));
                    v
                })
            },
        );
        outcome.assert_converged();
    }
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

/// Assert two runs of one seed produced the same coherence verdict: both
/// converged with no invariant or session-guarantee violation, and their sorted
/// violation sets are equal. Reproducibility is asserted on the verdict rather
/// than the trace digest, because a subscription scenario's trace is not
/// byte-stable on replay (see `monotonic_reads_reproducible_verdict`).
fn assert_same_verdict(a: SimOutcome, b: SimOutcome, seed: u64) {
    a.assert_converged();
    b.assert_converged();
    let va: Vec<String> = a.violations.iter().map(|v| v.to_string()).collect();
    let vb: Vec<String> = b.violations.iter().map(|v| v.to_string()).collect();
    assert_eq!(va, vb, "coherence verdict must be reproducible on replay (seed {seed})");
}

/// Capture the event id the just-issued write produced (the single tip of the
/// entity's head after the write) and record it as a `LocalWrite`.
fn record_write(
    writes: &std::sync::Arc<std::sync::Mutex<Vec<LocalWrite>>>,
    w: &Workload,
    origin: usize,
    entity: proto::EntityId,
    field: Field,
    value: &str,
) {
    let head = w.head_of(entity).expect("entity has a head after a write");
    let event = head.to_vec().into_iter().next().expect("a single-tip head after a linear write");
    writes.lock().unwrap().push(LocalWrite { origin, entity, field, value: value.to_owned(), event });
}
