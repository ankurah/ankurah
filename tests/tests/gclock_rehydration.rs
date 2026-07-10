//! D2 M5: the end-to-end eviction/rehydration annotation pin (routed from
//! the M4 test-adequacy panel). A resident evicted MID-FLOW must rehydrate
//! its materialized head generations from the engine EXACTLY, with ZERO
//! event reads (plan REV 5 section K: rehydration reconstitutes the GClock
//! without reading events), and the flow must continue correctly: the next
//! commit stamps from the rehydrated materialization.
//!
//! The observable is the storage boundary (InstrumentedEngine's get_events
//! counter, the M5 fixture pre-task) plus the resident's own State. The
//! per-engine DECODE paths are pinned separately in each engine's
//! gclock_state_roundtrip suite (sled, postgres, sqlite) and the browser
//! suite's entity_gclock_roundtrip (indexeddb); this covers the node-level
//! FLOW on the node-harness engine.

mod common;

use ankurah::core::property::backend::{lww::LWWBackend, PropertyBackend};
use ankurah::policy::DEFAULT_CONTEXT as c;
use ankurah::{proto, Mutable, Node, PermissiveAgent};
use ankurah_tests::common::brute_force_depths;
use anyhow::Result;
use common::*;
use std::collections::BTreeMap;
use std::sync::Arc;

fn title_ops(title: &str) -> proto::OperationSet {
    use ankurah::core::value::Value;
    let backend = LWWBackend::new();
    backend.set("title".into(), Some(Value::String(title.to_owned())));
    let ops = backend.to_operations().unwrap().expect("LWW backend with a write produces operations");
    proto::OperationSet(BTreeMap::from([("lww".to_owned(), ops)]))
}

/// Single-tip flow: create plus two edits, evict, rehydrate, keep editing.
/// The rehydrated GClock is byte-identical, costs zero event reads, and the
/// next commit stamps 1 + max over the REHYDRATED entries (the whole event
/// log then matches its brute-force depths).
#[tokio::test]
async fn gclock_rehydrates_exactly_and_the_flow_continues() -> Result<()> {
    let engine = InstrumentedEngine::new(SledStorageEngine::new_test()?);
    let instruments = engine.instruments();
    let node = Node::new_durable(Arc::new(engine), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    let rec_id = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        trx.commit().await?;
        id
    };
    let view = ctx.get::<RecordView>(rec_id).await?;
    for i in 1..=2 {
        let trx = ctx.begin();
        view.edit(&trx)?.title().set(&format!("t{i}"))?;
        trx.commit().await?;
    }

    let before = node.get_resident_entity(rec_id).expect("resident mid-flow").to_state()?;
    assert!(before.head_generations.matches_head(&before.head), "precondition: the annotation pairs the head");
    assert_eq!(before.head_generations.max_generation(), Some(3), "precondition: genesis plus two edits sits at generation 3");

    // EVICT mid-flow: the view holds the only strong reference.
    drop(view);
    assert!(node.get_resident_entity(rec_id).is_none(), "precondition: dropping the last strong reference evicts the resident");

    // REHYDRATE from the engine.
    let event_reads_before = instruments.get_events_calls();
    let view = ctx.get::<RecordView>(rec_id).await?;
    let after = node.get_resident_entity(rec_id).expect("rehydrated").to_state()?;
    assert_eq!(after.head, before.head, "the head survives");
    assert_eq!(after.head_generations, before.head_generations, "the GClock survives eviction/rehydration EXACTLY (REV 5 K home 3)");
    assert_eq!(
        instruments.get_events_calls() - event_reads_before,
        0,
        "rehydration reconstitutes the materialization WITHOUT reading events (REV 5 K)"
    );

    // The flow continues: the next commit stamps from the rehydrated
    // entries (no event retrieval), and the whole log equals its
    // brute-force depths.
    let trx = ctx.begin();
    view.edit(&trx)?.title().set(&"t3".to_owned())?;
    trx.commit().await?;

    let collection = ctx.collection(&Record::collection()).await?;
    let events = collection.dump_entity_events(rec_id).await?;
    let depths = brute_force_depths(&events);
    for e in &events {
        assert_eq!(
            e.payload.generation,
            depths[&e.payload.id()],
            "every stamp equals its brute-force depth; the post-rehydration commit continued the sequence"
        );
    }
    assert_eq!(depths.values().copied().max(), Some(4), "the post-rehydration edit stamped generation 4");
    Ok(())
}

/// Multi-tip, unequal generations: concurrent siblings delivered through
/// the real ingest leave a two-tip head at generations {2, 3}; the exact
/// per-tip annotation survives eviction and rehydration.
#[tokio::test]
async fn multi_tip_unequal_gclock_survives_rehydration() -> Result<()> {
    let engine = InstrumentedEngine::new(SledStorageEngine::new_test()?);
    let instruments = engine.instruments();
    let node = Node::new_durable(Arc::new(engine), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    let (rec_id, genesis) = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let mut events = trx.commit_and_return_events().await?;
        (id, events.remove(0))
    };

    // Two concurrent branches off the genesis: e1 (generation 2) and
    // e2a <- e2b (generations 2, 3). After ingest the head is the antichain
    // {e1, e2b} with per-tip generations {2, 3}.
    let e1 = ankurah_tests::forge::event_with_parents(rec_id, Record::collection(), title_ops("branch-a"), &[&genesis]);
    let e2a = ankurah_tests::forge::event_with_parents(rec_id, Record::collection(), title_ops("branch-b"), &[&genesis]);
    let e2b = ankurah_tests::forge::event_with_parents(rec_id, Record::collection(), title_ops("branch-b2"), &[&e2a]);
    let expected_tips = proto::GClock::new(vec![(2, e1.id()), (3, e2b.id())]);

    node.commit_remote_transaction(
        &c,
        proto::TransactionId::new(),
        vec![proto::Attested::opt(e1, None), proto::Attested::opt(e2a, None), proto::Attested::opt(e2b, None)],
    )
    .await
    .expect("concurrent branches ingest cleanly");

    let view = ctx.get::<RecordView>(rec_id).await?;
    let before = node.get_resident_entity(rec_id).expect("resident").to_state()?;
    assert_eq!(before.head_generations, expected_tips, "precondition: the two-tip head carries per-tip generations {{2, 3}}");

    drop(view);
    assert!(node.get_resident_entity(rec_id).is_none(), "evicted");

    let event_reads_before = instruments.get_events_calls();
    let _view = ctx.get::<RecordView>(rec_id).await?;
    let after = node.get_resident_entity(rec_id).expect("rehydrated").to_state()?;
    assert_eq!(after.head_generations, expected_tips, "the multi-tip unequal annotation survives EXACTLY");
    assert_eq!(instruments.get_events_calls() - event_reads_before, 0, "zero event reads on rehydration (REV 5 K)");
    Ok(())
}
