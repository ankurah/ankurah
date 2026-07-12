//! V4/V6 wire-shape coverage for the simulation harness (C1).
//!
//! The V4 (bridge events arriving in adversarial order across a gap) and V6
//! (EventOnly updates for entities the receiver has never seen, mixed into a
//! batch with valid items) bug classes live in the SubscriptionUpdate applier
//! (`apply_updates`), not the CommitTransaction path. Those bugs are fixed; the
//! mandate is to demonstrate the harness reaches those shapes with the
//! convergence invariants armed. Each scenario establishes a real subscription
//! (so the applier's relay-context precondition holds), then injects a
//! hand-forged batch through the seeded scheduler and asserts the outcome.

use ankurah::proto;
use ankurah_tests::sim::{body, model, run_once, FaultConfig, Field, Workload};

/// Sanity: a subscription established over the virtual transport lets a plain
/// SubscriptionUpdate (StateAndEvent) from the durable peer apply on the
/// ephemeral node. This proves the relay-context precondition for the V4/V6
/// scenarios is actually satisfied by the harness.
#[test]
fn subscription_update_applies_over_virtual_transport() {
    let outcome = run_once(
        "wire_subscription_smoke",
        1,
        FaultConfig::none(),
        2,
        body(|w: &mut Workload| {
            Box::pin(async move {
                // Ephemeral node 1 subscribes to everything; establish it.
                w.subscribe(1, "true").await;

                // Durable node 0 creates an entity and we deliver its full
                // state to node 1 as a StateAndEvent subscription update.
                let e = w.create_at(0, Field::Title, "hello").await;
                w.deliver_state_update(0, 1, e).await;
            })
        }),
    );
    outcome.assert_converged();
}

/// V6: an EventOnly update for an entity the receiver has never seen, mixed
/// into a batch with valid items, must not poison the batch and must leave no
/// phantom resident for the unknown entity. The harness reaches this shape with
/// the convergence and no-phantom invariants armed.
#[test]
fn v6_unknown_entity_item_does_not_poison_batch_or_leave_phantom() {
    for seed in 0..4u64 {
        let outcome = run_once(
            "v6_unknown_entity_batch",
            seed,
            FaultConfig::none(),
            2,
            body(|w: &mut Workload| {
                Box::pin(async move {
                    w.subscribe(1, "true").await;

                    // Two entities created on node 0 and converged onto node 1,
                    // so node 1 holds them at their creation heads.
                    let a = w.create_at(0, Field::Title, "a0").await;
                    let b = w.create_at(0, Field::Title, "b0").await;
                    w.settle().await;

                    // Forge a valid edit for each (parented on their current
                    // heads) and one EventOnly event for an entity node 1 has
                    // never seen. The forged edits are applied at node 0 too so
                    // the final states converge.
                    let head_a = w.head_of(a).unwrap();
                    let head_b = w.head_of(b).unwrap();
                    let ev_a = model::attest(model::edit_event(a, head_a, Field::Body, "a1"));
                    let ev_b = model::attest(model::edit_event(b, head_b, Field::Body, "b1"));

                    let unknown = w.reserve_unknown_entity();
                    let ghost_parent = proto::Clock::from(vec![proto::EventId::from_bytes([9u8; 32])]);
                    let ev_unknown = model::attest(model::edit_event(unknown, ghost_parent, Field::Title, "ghost"));

                    // Apply the valid edits at node 0 so it and node 1 converge.
                    w.apply_events_at(0, a, vec![ev_a.clone()]).await;
                    w.apply_events_at(0, b, vec![ev_b.clone()]).await;

                    // Deliver the poison batch to node 1: [valid A, unknown, valid B].
                    let item_a = w.event_only_item(a, vec![ev_a]);
                    let item_unknown = w.event_only_item(unknown, vec![ev_unknown]);
                    let item_b = w.event_only_item(b, vec![ev_b]);
                    w.deliver_items(0, 1, vec![item_a, item_unknown, item_b]);
                })
            }),
        );
        // The unknown entity was never created, so if node 1 materialized it the
        // no-phantom invariant fires. A and B must converge with their edits.
        outcome.assert_converged();
    }
}

/// V4: an EventOnly bridge whose events arrive child-first across a gap must
/// still apply parent-first; the child must not gap-jump the head and strand
/// the parent's operations. The harness reaches this shape with convergence
/// armed.
#[test]
fn v4_bridge_events_arrive_child_first() {
    for seed in 0..4u64 {
        let outcome = run_once(
            "v4_bridge_child_first",
            seed,
            FaultConfig::none(),
            2,
            body(|w: &mut Workload| {
                Box::pin(async move {
                    w.subscribe(1, "true").await;

                    let e = w.create_at(0, Field::Title, "base").await;
                    w.settle().await;

                    // Build a two-event chain on node 0: base <- mid <- tip,
                    // writing different fields so both writes are observable.
                    let head0 = w.head_of(e).unwrap();
                    let ev_mid = model::attest(model::edit_event(e, head0, Field::Body, "mid"));
                    let mid_clock = proto::Clock::from(vec![ev_mid.payload.id()]);
                    let ev_tip = model::attest(model::edit_event(e, mid_clock, Field::Title, "tip"));

                    // Apply both at node 0 (parent-first) so it is the source of truth.
                    w.apply_events_at(0, e, vec![ev_mid.clone(), ev_tip.clone()]).await;

                    // Deliver to node 1 as an EventOnly bridge in CHILD-FIRST
                    // order: [tip, mid]. The applier must sort and apply mid then
                    // tip, so the mid write (body) is not dropped.
                    let bridge = w.event_only_item(e, vec![ev_tip, ev_mid]);
                    w.deliver_items(0, 1, vec![bridge]);
                })
            }),
        );
        outcome.assert_converged();
    }
}
