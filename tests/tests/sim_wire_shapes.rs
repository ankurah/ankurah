//! V4/V6 wire-shape coverage for the simulation harness (C1).
//!
//! The V4 (bridge events arriving in adversarial order across a gap) and V6
//! (EventOnly updates for entities the receiver has never seen, mixed into a
//! publication after valid progress) exercise the sequenced state-stream receiver.
//! The
//! mandate is to demonstrate the harness reaches those shapes with the
//! convergence invariants armed. Each scenario establishes a real subscription
//! with a captured origin/sequence, then injects malicious publications through
//! the seeded scheduler and asserts the outcome.

use ankurah::proto;
use ankurah_tests::sim::{body, model, run_once, FaultConfig, Field, Workload};

/// Sanity: a subscription established over the virtual transport lets a plain
/// StateAndEvents live publication from the durable peer apply on the
/// ephemeral node. This proves standing-route admission for the V4/V6
/// scenarios is actually satisfied by the harness.
#[test]
fn scripted_live_snapshot_applies_over_virtual_transport() {
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
                // state to node 1 as a sequenced StateAndEvents live publication.
                w.begin_scripted_stream(0, 1);
                let e = w.create_source_only(0, Field::Title, "hello").await;
                w.deliver_state_update(0, 1, e).await;
            })
        }),
    );
    outcome.assert_converged();
}

/// V6: an event-only publication for an unseen entity fails the stream after
/// preceding valid progress, leaves no phantom and admits no later mutation.
/// Convergence and no-phantom invariants remain armed.
#[test]
fn v6_unknown_entity_fails_stream_without_phantom_or_later_progress() {
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
                    let ev_a = model::attest(model::edit_event(a, head_a, Field::Body, "a1", w.next_mint_seq()));
                    let ev_b = model::attest(model::edit_event(b, head_b, Field::Body, "b1", w.next_mint_seq()));

                    let unknown = w.reserve_unknown_entity();
                    let ghost_parent = proto::Clock::genesis(proto::EventId::from_bytes([9u8; 32]));
                    let ev_unknown = model::attest(model::edit_event(unknown, ghost_parent, Field::Title, "ghost", w.next_mint_seq()));

                    w.begin_scripted_stream(0, 1);
                    // Apply the valid edits only at the source; scripted frames carry them.
                    w.apply_events_at(0, a, vec![ev_a.clone()]).await;
                    w.apply_events_at(0, b, vec![ev_b.clone()]).await;

                    // Preserve valid progress, then fail the origin and test a late frame.
                    let item_a = w.event_only_item(a, vec![ev_a]);
                    let item_unknown = w.event_only_item(unknown, vec![ev_unknown]);
                    let item_b = w.event_only_item(b, vec![ev_b]);
                    // Both valid publications precede the failure. A later
                    // forged edit on the same failed origin must not apply.
                    let late = model::attest(model::edit_event(a, w.head_of(a).unwrap(), Field::Body, "must-not-apply", w.next_mint_seq()));
                    let late = w.event_only_item(a, vec![late]);
                    assert!(w.deliver_items(0, 1, vec![item_a, item_b, item_unknown, late]).await);
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
                    let ev_mid = model::attest(model::edit_event(e, head0, Field::Body, "mid", w.next_mint_seq()));
                    let mid_clock = proto::Clock::singleton(&ev_mid.payload);
                    let ev_tip = model::attest(model::edit_event(e, mid_clock, Field::Title, "tip", w.next_mint_seq()));

                    w.begin_scripted_stream(0, 1);
                    // Apply only at the source; the scripted child-first bridge must carry both.
                    w.apply_events_at(0, e, vec![ev_mid.clone(), ev_tip.clone()]).await;

                    // Deliver to node 1 as an EventOnly bridge in CHILD-FIRST
                    // order: [tip, mid]. The applier must sort and apply mid then
                    // tip, so the mid write (body) is not dropped.
                    let bridge = w.event_only_item(e, vec![ev_tip, ev_mid]);
                    assert!(!w.deliver_items(0, 1, vec![bridge]).await);
                })
            }),
        );
        outcome.assert_converged();
    }
}
