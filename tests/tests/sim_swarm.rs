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

use ankurah_tests::sim::{body, run_once, sweep, FaultConfig, Field, SimOutcome, Workload};

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
