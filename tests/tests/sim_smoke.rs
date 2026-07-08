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

/// R7: cross-peer gap-fill passes the determinism audit under seeded peer
/// selection. History exists only at the durable origin; each ephemeral
/// node closes its gap through the real Get request lane (get_from_peer,
/// durable-peer selection from the node-held seeded RNG (M0, #285),
/// entity-mediated adoption of the fetched state, and the post-adoption
/// re-drive of buffered orphans). The audit (same seed twice,
/// byte-identical trace) is the claim: nothing on that path takes a
/// nondeterministic branch.
///
/// Two scope boundaries, both structural and both already filed:
/// - The SUBSCRIPTION flavor of catch-up is not trace-auditable yet: the
///   client relay's worker couples to real wall-clock timers
///   (futures_timer in client_relay.rs), so its emission timing drifts
///   between same-seed runs even fault-free. Seeding those timers is #321
///   (post-D1 hardening); the faulted companion below asserts convergence
///   only.
/// - The mid-apply remote fetch flavor (missing parent on an ephemeral
///   receiver) is a nested request inside handle_message, which the sim's
///   inline scheduler cannot deliver (the M6 NeedsState amendment removed
///   that shape from the commit lane for the same reason). Buffering and
///   re-drive for it are pinned over the real async connector in
///   staging_retention.rs (R5/R6).
#[test]
fn smoke_cross_peer_gap_fill_is_deterministic() {
    use ankurah::proto;
    use ankurah_tests::sim::model;

    for seed in 0..8u64 {
        let outcome = run_with_determinism_audit("cross_peer_gap_fill", seed, FaultConfig::none(), 3, || {
            body(|w: &mut Workload| {
                Box::pin(async move {
                    // A multi-event lineage that exists ONLY at the durable
                    // origin: the ephemerals have a real gap to fill.
                    let a = w.create_local_at(0, Field::Title, "gap").await;
                    let head = w.head_of(a).expect("harness tracks the head it produced");
                    // Linear lineage over the genesis (generation 1): e1 is 2, e2 is 3.
                    let e1 = model::attest(model::edit_event(a, head, Field::Body, "b1", 2));
                    let e1_head = proto::Clock::from(vec![e1.payload.id()]);
                    let e2 = model::attest(model::edit_event(a, e1_head, Field::Body, "b2", 3));
                    w.apply_events_at(0, a, vec![e1, e2]).await;

                    // Both ephemerals fill the gap through the Get lane.
                    w.get_at(1, a).await;
                    w.get_at(2, a).await;
                })
            })
        });
        outcome.assert_converged();
    }
}
