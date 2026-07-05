//! Smoke tier for the deterministic simulation harness (C1).
//!
//! These run in the normal test job: a few seeds per scenario, fast. The larger
//! local/nightly tier lives in `sim_swarm.rs` behind an ignore attribute; C2
//! owns scheduling it at scale. Every smoke scenario runs through the
//! determinism audit (same seed twice, identical trace) so a nondeterminism
//! leak fails CI immediately, per design-delta C1-A.

use ankurah_tests::sim::{body, run_once, run_with_determinism_audit, FaultConfig, Field, Workload};

/// The kickoff skeleton: two real nodes over the virtual transport, one
/// committed entity change propagating deterministically under an explicit
/// seed, trace recorded, and same-seed-twice trace identity asserted.
#[test]
fn smoke_two_node_single_change_is_deterministic() {
    let outcome = run_with_determinism_audit("two_node_single_change", 1, FaultConfig::none(), 2, || {
        body(|w: &mut Workload| {
            Box::pin(async move {
                let id = w.create_at(0, Field::Title, "hello").await;
                w.edit_at(0, id, Field::Body, "world").await;
            })
        })
    });
    outcome.assert_converged();
    assert!(outcome.trace_len >= 2, "trace should record origin + delivery events, got {}", outcome.trace_len);
}

/// Convergence under a reliable network with three nodes: every node ends
/// byte-equal. Establishes the no-fault baseline the fault scenarios perturb.
#[test]
fn smoke_three_node_reliable_convergence() {
    for seed in 0..3u64 {
        let outcome = run_with_determinism_audit("three_node_reliable", seed, FaultConfig::none(), 3, || {
            body(|w: &mut Workload| {
                Box::pin(async move {
                    let a = w.create_at(0, Field::Title, "a").await;
                    let b = w.create_at(0, Field::Title, "b").await;
                    w.edit_at(0, a, Field::Body, "a-body").await;
                    w.edit_at(0, b, Field::Body, "b-body").await;
                })
            })
        });
        outcome.assert_converged();
    }
}

/// Convergence under swarm faults: each seed draws a random fault subset and
/// still converges at quiescence. This is the core C1 claim, exercised across a
/// handful of seeds in the smoke tier.
#[test]
fn smoke_swarm_faults_converge() {
    for seed in 0..8u64 {
        let faults = FaultConfig::swarm_from_seed(seed);
        let outcome = run_once(
            "swarm_faults",
            seed,
            faults,
            3,
            body(|w: &mut Workload| {
                Box::pin(async move {
                    let a = w.create_at(0, Field::Title, "a").await;
                    w.edit_at(0, a, Field::Body, "b1").await;
                    let b = w.create_at(1, Field::Title, "b").await;
                    w.edit_at(1, b, Field::Body, "b2").await;
                })
            }),
        );
        outcome.assert_converged();
    }
}
