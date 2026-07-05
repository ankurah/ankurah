//! Scenario coverage for the simulation harness (C1).
//!
//! These exercise the concurrency shapes the harness must be able to reach by
//! search with the convergence invariants armed: genuinely concurrent commuting
//! and conflicting edits, the stale-client multi-head shape (issue #205), and
//! multi-entity churn. Each runs under the determinism audit where the schedule
//! is fixed, and under swarm faults where convergence must still hold at
//! quiescence. The V4/V6 wire shapes have their own file (`sim_wire_shapes`).

use ankurah_tests::sim::{body, run_once, run_with_determinism_audit, FaultConfig, Field, Workload};

/// Two concurrent edits to *different* fields from two origins. They commute, so
/// after convergence every node holds both writes and a two-tip head; the head
/// must be a valid antichain and byte-equal across nodes.
#[test]
fn concurrent_commuting_edits_converge() {
    for seed in 0..4u64 {
        let outcome = run_with_determinism_audit("concurrent_commuting", seed, FaultConfig::none(), 3, || {
            body(|w: &mut Workload| {
                Box::pin(async move {
                    let e = w.create_at(0, Field::Title, "base").await;
                    // Everyone must hold the base before editing it concurrently.
                    w.settle().await;
                    let fork = w.head_of(e).unwrap();
                    // Two concurrent edits from the shared fork, different fields.
                    w.edit_from(1, e, fork.clone(), Field::Title, "from-n1").await;
                    w.edit_from(2, e, fork, Field::Body, "from-n2").await;
                })
            })
        });
        outcome.assert_converged();
        assert_eq!(outcome.max_head_len, 2, "commuting concurrent edits must yield a genuine two-tip head (seed {seed})");
    }
}

/// Two concurrent edits to the *same* field: LWW picks a single winner by
/// causal descent then content-hash tiebreak. Convergence requires all nodes
/// pick the same winner regardless of delivery order.
#[test]
fn concurrent_conflicting_edits_converge() {
    for seed in 0..6u64 {
        let faults = FaultConfig::swarm_from_seed(seed);
        let outcome = run_once(
            "concurrent_conflicting",
            seed,
            faults,
            3,
            body(|w: &mut Workload| {
                Box::pin(async move {
                    let e = w.create_at(0, Field::Title, "base").await;
                    w.settle().await;
                    let fork = w.head_of(e).unwrap();
                    w.edit_from(1, e, fork.clone(), Field::Title, "winner-or-loser-A").await;
                    w.edit_from(2, e, fork, Field::Title, "winner-or-loser-B").await;
                })
            }),
        );
        outcome.assert_converged();
    }
}

/// Stale-client multi-head (issue #205): build a two-tip head, then extend only
/// ONE tip from a node that ignores the other tip. The resulting head must
/// remain a valid antichain (the extended tip's new event supersedes its
/// parent, the untouched tip survives) and converge across nodes.
#[test]
fn stale_client_extends_one_tip_of_multihead() {
    for seed in 0..4u64 {
        let outcome = run_with_determinism_audit("stale_client_multihead", seed, FaultConfig::none(), 3, || {
            body(|w: &mut Workload| {
                Box::pin(async move {
                    let e = w.create_at(0, Field::Title, "base").await;
                    w.settle().await;
                    let fork = w.head_of(e).unwrap();
                    // Concurrent commuting tips -> two-element head everywhere.
                    w.edit_from(1, e, fork.clone(), Field::Title, "tip-A").await;
                    let tip_a = w.head_of(e).unwrap();
                    w.edit_from(2, e, fork, Field::Body, "tip-B").await;
                    // Both tips reach every node.
                    w.settle().await;

                    // A stale client (node 1) extends ONLY tip A, unaware of tip
                    // B. Its new event parents on tip A alone.
                    w.edit_from(1, e, tip_a, Field::Title, "extend-tip-A-only").await;
                })
            })
        });
        outcome.assert_converged();
        // The extended tip supersedes tip A but tip B survives, so the head
        // stays a two-element antichain: proof the multi-head shape persisted.
        assert_eq!(outcome.max_head_len, 2, "stale-client extension must leave a two-tip head (seed {seed})");
    }
}

/// Multi-entity churn under swarm faults: several entities created and edited
/// across all origins. Stresses the harness at breadth while asserting no lost
/// write, no phantom, convergence, and antichain validity for every entity.
#[test]
fn multi_entity_churn_under_faults() {
    for seed in 0..6u64 {
        let faults = FaultConfig::swarm_from_seed(seed);
        let outcome = run_once(
            "multi_entity_churn",
            seed,
            faults,
            4,
            body(|w: &mut Workload| {
                Box::pin(async move {
                    let mut ids = Vec::new();
                    for i in 0..5u32 {
                        let origin = (i as usize) % w.node_count();
                        let id = w.create_at(origin, Field::Title, &format!("e{i}")).await;
                        ids.push(id);
                    }
                    // Every node holds every entity before the edit wave, so an
                    // edit can originate from a node other than the creator.
                    w.settle().await;
                    for (i, id) in ids.iter().enumerate() {
                        let origin = (i + 1) % w.node_count();
                        w.edit_at(origin, *id, Field::Body, &format!("body{i}")).await;
                    }
                })
            }),
        );
        outcome.assert_converged();
    }
}
