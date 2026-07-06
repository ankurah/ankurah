//! Larger local/nightly swarm tier for the simulation harness (C1).
//!
//! These are `#[ignore]` so the normal test job stays fast (the smoke tier
//! covers CI); run them locally or in the nightly oracle scale-out (C2) with:
//!
//!     SIM_SEEDS=2000 cargo test -p ankurah-tests --test sim_swarm -- --ignored --nocapture
//!
//! Each scenario runs across `SIM_SEEDS` seeds (default 500), every seed under
//! its own swarm fault subset. Any invariant violation prints its self-contained
//! `SIMFAIL` artifact line, which is exactly what C2 consumes: seed, scenario,
//! fault config, and failing invariants are enough to reproduce from the log.
//! The test fails if any seed violated an invariant.

use ankurah::proto;
use ankurah_tests::sim::{
    body, coherence, run_once, run_recording, sweep, CheckFut, FaultConfig, Field, SimNode, SimOutcome, SubscriptionRecorder, Workload,
};

fn seed_count() -> u64 { std::env::var("SIM_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(500) }

/// Report a sweep: print every failure's artifact line, then assert none.
fn report_and_assert(label: &str, total: usize, failures: Vec<SimOutcome>) {
    eprintln!("swarm[{label}]: ran {total} seeds, {} failed", failures.len());
    for f in &failures {
        eprintln!("{}", f.artifact_line());
    }
    assert!(failures.is_empty(), "swarm[{label}]: {} of {} seeds violated an invariant (see SIMFAIL lines above)", failures.len(), total);
}

#[test]
#[ignore = "large tier; run with --ignored locally or in the C2 nightly scale-out"]
fn swarm_multi_entity_churn() {
    let (total, failures) = sweep("swarm_multi_entity_churn", 0..seed_count(), 4, || {
        body(|w: &mut Workload| {
            Box::pin(async move {
                let mut ids = Vec::new();
                for i in 0..6u32 {
                    let origin = (i as usize) % w.node_count();
                    ids.push(w.create_at(origin, Field::Title, &format!("e{i}")).await);
                }
                w.settle().await;
                for (i, id) in ids.iter().enumerate() {
                    let origin = (i + 1) % w.node_count();
                    w.edit_at(origin, *id, Field::Body, &format!("b{i}")).await;
                }
            })
        })
    });
    report_and_assert("multi_entity_churn", total, failures);
}

#[test]
#[ignore = "large tier; run with --ignored locally or in the C2 nightly scale-out"]
fn swarm_concurrent_conflicting() {
    let (total, failures) = sweep("swarm_concurrent_conflicting", 0..seed_count(), 3, || {
        body(|w: &mut Workload| {
            Box::pin(async move {
                let e = w.create_at(0, Field::Title, "base").await;
                w.settle().await;
                let fork = w.head_of(e).unwrap();
                // Three concurrent same-field edits from three origins; LWW must
                // pick one winner all nodes agree on regardless of schedule.
                w.edit_from(0, e, fork.clone(), Field::Title, "A").await;
                w.edit_from(1, e, fork.clone(), Field::Title, "B").await;
                w.edit_from(2, e, fork, Field::Title, "C").await;
            })
        })
    });
    report_and_assert("concurrent_conflicting", total, failures);
}

/// Determinism audit at scale: for many seeds under swarm faults, running the
/// same seed twice must produce a byte-identical trace. A single mismatch is a
/// nondeterminism leak that voids the whole harness, so this is the load-bearing
/// self-check the lit review demands (design-delta C1-A) run beyond the smoke
/// seeds.
#[test]
#[ignore = "large tier; run with --ignored locally or in the C2 nightly scale-out"]
fn swarm_determinism_audit() {
    let mut mismatches = Vec::new();
    let n = seed_count();
    for seed in 0..n {
        let faults = FaultConfig::swarm_from_seed(seed);
        let scenario = || {
            body(|w: &mut Workload| {
                Box::pin(async move {
                    let mut ids = Vec::new();
                    for i in 0..4u32 {
                        let origin = (i as usize) % w.node_count();
                        ids.push(w.create_at(origin, Field::Title, &format!("e{i}")).await);
                    }
                    w.settle().await;
                    for (i, id) in ids.iter().enumerate() {
                        w.edit_at((i + 1) % w.node_count(), *id, Field::Body, &format!("b{i}")).await;
                    }
                })
            })
        };
        let a = run_once("swarm_determinism_audit", seed, faults, 4, scenario());
        let b = run_once("swarm_determinism_audit", seed, faults, 4, scenario());
        if a.trace_hash != b.trace_hash {
            mismatches.push(format!("seed={seed} {} != {}", a.trace_hash, b.trace_hash));
        }
    }
    eprintln!("swarm[determinism_audit]: ran {n} seeds, {} mismatches", mismatches.len());
    for m in &mismatches {
        eprintln!("DETERMINISM MISMATCH {m}");
    }
    assert!(mismatches.is_empty(), "{} of {} seeds produced non-identical traces on replay", mismatches.len(), n);
}

#[test]
#[ignore = "large tier; run with --ignored locally or in the C2 nightly scale-out"]
fn swarm_concurrent_commuting_multihead() {
    let (total, failures) = sweep("swarm_concurrent_commuting", 0..seed_count(), 3, || {
        body(|w: &mut Workload| {
            Box::pin(async move {
                let e = w.create_at(0, Field::Title, "base").await;
                w.settle().await;
                let fork = w.head_of(e).unwrap();
                // Two commuting tips then a stale extension of one tip: a
                // persistent multi-head that must stay a valid antichain and
                // converge under any schedule.
                w.edit_from(1, e, fork.clone(), Field::Title, "tip-A").await;
                let tip_a = w.head_of(e).unwrap();
                w.edit_from(2, e, fork, Field::Body, "tip-B").await;
                w.settle().await;
                w.edit_from(1, e, tip_a, Field::Title, "extend-A").await;
            })
        })
    });
    report_and_assert("concurrent_commuting_multihead", total, failures);
}

// ---------------------------------------------------------------------------
// C5: reactor / LiveQuery coherence swarm variants.
//
// The session-guarantee scenarios from `sim_coherence.rs` swept across
// SIM_SEEDS seeds. Coherence violations are folded into each run's outcome by
// `run_recording`, so a hit carries the same `SIMFAIL` artifact line as a
// convergence violation. These use `FaultConfig::swarm_subscription_safe`
// (swarm minus `reorder`): reordering the subscription-setup handshake makes the
// client relay wait on a real 5-second timer the deterministic scheduler cannot
// advance (issue #321), so `reorder` is excluded for any scenario holding a live
// subscription.
// ---------------------------------------------------------------------------

/// Sweep a recording coherence scenario across `seeds` under
/// `swarm_subscription_safe`, collecting the outcomes that violated any
/// invariant (convergence or coherence). Mirrors `sweep` for the `run_recording`
/// path, which `sweep` cannot drive because its check is async and borrows the
/// nodes and recorders.
fn sweep_coherence<F, C>(
    scenario_name: &'static str,
    seeds: impl Iterator<Item = u64>,
    node_count: usize,
    body_fn: F,
    check_fn: C,
) -> (usize, Vec<SimOutcome>)
where
    F: Fn() -> Box<dyn for<'w, 'b> FnOnce(&'b mut Workload<'w>) -> ankurah_tests::sim::ScenarioFut<'b>>,
    C: Fn() -> Box<dyn for<'a> FnOnce(&'a [SimNode], &'a [SubscriptionRecorder]) -> CheckFut<'a>>,
{
    let mut total = 0;
    let mut failures = Vec::new();
    for seed in seeds {
        total += 1;
        let faults = FaultConfig::swarm_subscription_safe(seed);
        let outcome = run_recording(scenario_name, seed, faults, node_count, body_fn(), check_fn());
        if !outcome.ok() {
            failures.push(outcome);
        }
    }
    (total, failures)
}

/// Monotonic reads at swarm scale: an ephemeral subscriber's observed causal
/// frontier for an entity never regresses across a create-and-edit chain under
/// any seeded fault subset. Each wave is drained (see the smoke scenario for
/// why).
#[test]
#[ignore = "large tier; run with --ignored locally or in the C2 nightly scale-out"]
fn swarm_monotonic_reads() {
    let (total, failures) = sweep_coherence(
        "swarm_monotonic_reads",
        0..seed_count(),
        3,
        || {
            Box::new(body(|w: &mut Workload| {
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
            }))
        },
        || {
            Box::new(|nodes: &[SimNode], recorders: &[SubscriptionRecorder]| -> CheckFut<'_> {
                Box::pin(async move {
                    let mut v = coherence::check_monotonic_reads(nodes, recorders).await;
                    v.extend(coherence::check_no_empty_events_update(recorders));
                    v
                })
            })
        },
    );
    report_and_assert("monotonic_reads", total, failures);
}

/// Notification coherence under concurrent merges at swarm scale: two origins
/// make concurrent commuting edits that a subscriber's node merges via
/// DivergedSince; the observed stream stays coherent (no torn read against the
/// converged state, no empty-events Update, membership agrees with the stream).
#[test]
#[ignore = "large tier; run with --ignored locally or in the C2 nightly scale-out"]
fn swarm_merge_coherence() {
    let entity_cell: std::sync::Arc<std::sync::Mutex<Option<proto::EntityId>>> = std::sync::Arc::new(std::sync::Mutex::new(None));
    let body_cell = entity_cell.clone();
    let check_cell = entity_cell.clone();
    let (total, failures) = sweep_coherence(
        "swarm_merge_coherence",
        0..seed_count(),
        3,
        move || {
            let entity_cell = body_cell.clone();
            Box::new(body(move |w: &mut Workload| {
                let entity_cell = entity_cell.clone();
                Box::pin(async move {
                    w.subscribe_recording(2, "true").await;
                    let e = w.create_at(0, Field::Title, "base").await;
                    *entity_cell.lock().unwrap() = Some(e);
                    w.settle().await;
                    let fork = w.head_of(e).unwrap();
                    w.edit_from(0, e, fork.clone(), Field::Title, "title-from-n0").await;
                    w.edit_from(1, e, fork, Field::Body, "body-from-n1").await;
                })
            }))
        },
        move || {
            let entity_cell = check_cell.clone();
            Box::new(move |nodes: &[SimNode], recorders: &[SubscriptionRecorder]| -> CheckFut<'_> {
                let entities: Vec<proto::EntityId> = entity_cell.lock().unwrap().iter().copied().collect();
                Box::pin(async move {
                    let mut v = coherence::check_monotonic_reads(nodes, recorders).await;
                    v.extend(coherence::check_merge_final_coherence(nodes, recorders, &entities).await);
                    v.extend(coherence::check_no_empty_events_update(recorders));
                    v.extend(coherence::check_membership_matches_stream(recorders));
                    v
                })
            })
        },
    );
    report_and_assert("merge_coherence", total, failures);
}

/// Cross-subscription isolation at swarm scale: subscription B established after
/// subscription A (which already holds the entities) must not surface a spurious
/// empty-events Update on A across any seeded fault subset (the PR #299 class).
#[test]
#[ignore = "large tier; run with --ignored locally or in the C2 nightly scale-out"]
fn swarm_cross_subscription_isolation() {
    let (total, failures) = sweep_coherence(
        "swarm_cross_subscription_isolation",
        0..seed_count(),
        2,
        || {
            Box::new(body(|w: &mut Workload| {
                Box::pin(async move {
                    w.subscribe_recording(1, "true").await;
                    let e0 = w.create_at(0, Field::Title, "a").await;
                    let e1 = w.create_at(0, Field::Title, "b").await;
                    w.settle().await;
                    w.subscribe_recording(1, "true").await;
                    w.edit_at(0, e0, Field::Body, "a2").await;
                    w.settle().await;
                    w.edit_at(0, e1, Field::Body, "b2").await;
                })
            }))
        },
        || {
            Box::new(|_nodes: &[SimNode], recorders: &[SubscriptionRecorder]| -> CheckFut<'_> {
                Box::pin(async move {
                    let mut v = coherence::check_no_empty_events_update(recorders);
                    v.extend(coherence::check_membership_matches_stream(recorders));
                    v
                })
            })
        },
    );
    report_and_assert("cross_subscription_isolation", total, failures);
}

/// Reproducibility audit for a recording coherence scenario at scale: for many
/// seeds under `swarm_subscription_safe`, running the same seed twice must
/// produce the same coherence verdict (both converge with the same violation
/// set). This asserts the verdict, not a byte-equal trace: a scenario holding a
/// live subscription is not trace-stable on replay because the client relay
/// batches on a real timer and sends on uncontrolled `task::spawn` tasks (issue
/// #321), so the delivery interleaving can differ under load even though the
/// coherence outcome does not. The byte-equal trace determinism audit remains in
/// force for the non-subscription C1 scenarios (`swarm_determinism_audit`).
#[test]
#[ignore = "large tier; run with --ignored locally or in the C2 nightly scale-out"]
fn swarm_coherence_reproducible_verdict() {
    let mut mismatches = Vec::new();
    let n = seed_count();
    for seed in 0..n {
        let faults = FaultConfig::swarm_subscription_safe(seed);
        let scenario = || {
            body(|w: &mut Workload| {
                Box::pin(async move {
                    w.subscribe_recording(1, "true").await;
                    let e = w.create_at(0, Field::Title, "v0").await;
                    w.settle().await;
                    w.edit_at(0, e, Field::Body, "v1").await;
                    w.settle().await;
                    w.edit_at(0, e, Field::Title, "v2").await;
                })
            })
        };
        let a = run_recording("swarm_coherence_verdict", seed, faults, 3, scenario(), |nodes, recorders| {
            Box::pin(async move { coherence::check_monotonic_reads(nodes, recorders).await })
        });
        let b = run_recording("swarm_coherence_verdict", seed, faults, 3, scenario(), |nodes, recorders| {
            Box::pin(async move { coherence::check_monotonic_reads(nodes, recorders).await })
        });
        let va: Vec<String> = a.violations.iter().map(|v| v.to_string()).collect();
        let vb: Vec<String> = b.violations.iter().map(|v| v.to_string()).collect();
        if !a.ok() || !b.ok() || va != vb {
            mismatches.push(format!("seed={seed} a_ok={} b_ok={} a_viol={:?} b_viol={:?}", a.ok(), b.ok(), va, vb));
        }
    }
    eprintln!("swarm[coherence_verdict]: ran {n} seeds, {} mismatches", mismatches.len());
    for m in &mismatches {
        eprintln!("VERDICT MISMATCH {m}");
    }
    assert!(mismatches.is_empty(), "{} of {} seeds produced a non-reproducible coherence verdict", mismatches.len(), n);
}
