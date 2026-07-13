mod common;

use ankurah::proto;
use ankurah_tests::oracles::brute_force_depths;
use anyhow::Result;
use common::*;

/// Assert every stored event's carried generation equals its brute-force
/// topological depth, and return (event count, max depth) for shape checks.
async fn assert_stamps_equal_depth(ctx: &ankurah::Context, entity_id: proto::EntityId, shape: &str) -> Result<(usize, u32)> {
    let collection = ctx.collection(&Record::collection()).await?;
    let events = collection.dump_entity_events(entity_id).await?;
    let depths = brute_force_depths(&events);
    for e in &events {
        let id = e.payload.id();
        let expected = depths[&id];
        assert_eq!(
            e.payload.generation, expected,
            "R-D2-2a [{shape}]: event {id} carries generation {} but its brute-force topological depth is {expected}",
            e.payload.generation
        );
    }
    Ok((events.len(), depths.values().copied().max().unwrap_or(0)))
}

/// R-D2-2a, linear shape: a chain of sequential commits stamps 1, 2, 3.
/// The committer stamps 1 + max(parent generations) at event creation
/// (plan REV 4, D2-2); genesis is exactly 1.
#[tokio::test]
async fn r_d2_2a_linear_stamps_equal_brute_force_depth() -> Result<()> {
    let ctx = durable_sled_setup().await?.context_async(DEFAULT_CONTEXT).await;

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

    let (count, max_depth) = assert_stamps_equal_depth(&ctx, rec_id, "linear").await?;
    assert_eq!(count, 3, "genesis plus two edits");
    assert_eq!(max_depth, 3, "the chain is three levels deep");
    Ok(())
}

/// R-D2-2a, diamond shape: two concurrent edits of the same head (both
/// generation 2) and a merge edit over the two-tip antichain (generation 3).
#[tokio::test]
async fn r_d2_2a_diamond_stamps_equal_brute_force_depth() -> Result<()> {
    let ctx = durable_sled_setup().await?.context_async(DEFAULT_CONTEXT).await;

    let rec_id = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        trx.commit().await?;
        id
    };
    let view = ctx.get::<RecordView>(rec_id).await?;

    // Two transactions fork the same genesis head: concurrent siblings.
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();
    view.edit(&trx1)?.title().set(&"left".to_owned())?;
    view.edit(&trx2)?.artist().set(&"right".to_owned())?;
    trx1.commit().await?;
    trx2.commit().await?;

    // The resident head is now the two-sibling antichain; one more edit
    // parents on both tips: the merge point of the diamond.
    assert_eq!(view.entity().head().len(), 2, "precondition: two-tip antichain");
    let trx = ctx.begin();
    view.edit(&trx)?.title().set(&"merged".to_owned())?;
    trx.commit().await?;

    let (count, max_depth) = assert_stamps_equal_depth(&ctx, rec_id, "diamond").await?;
    assert_eq!(count, 4, "genesis, two siblings, one merge");
    assert_eq!(max_depth, 3, "the merge sits one level above the siblings");
    Ok(())
}

/// R-D2-2a, crossed shape: two CONCURRENT merge edits over the same two-tip
/// antichain (both generation 3, crossing the diamond), then a final merge
/// over those two (generation 4). Exercises max() over parents whose own
/// generations already differ from their count-based positions.
#[tokio::test]
async fn r_d2_2a_crossed_stamps_equal_brute_force_depth() -> Result<()> {
    let ctx = durable_sled_setup().await?.context_async(DEFAULT_CONTEXT).await;

    let rec_id = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        trx.commit().await?;
        id
    };
    let view = ctx.get::<RecordView>(rec_id).await?;

    // Level 2: concurrent siblings of genesis.
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();
    view.edit(&trx1)?.title().set(&"b".to_owned())?;
    view.edit(&trx2)?.artist().set(&"c".to_owned())?;
    trx1.commit().await?;
    trx2.commit().await?;
    assert_eq!(view.entity().head().len(), 2, "precondition: {{B, C}} antichain");

    // Level 3, crossed: two concurrent merges both parented on {B, C}.
    let trx3 = ctx.begin();
    let trx4 = ctx.begin();
    view.edit(&trx3)?.title().set(&"d".to_owned())?;
    view.edit(&trx4)?.artist().set(&"e".to_owned())?;
    trx3.commit().await?;
    trx4.commit().await?;
    assert_eq!(view.entity().head().len(), 2, "precondition: {{D, E}} antichain");

    // Level 4: the final merge over the crossed pair.
    let trx = ctx.begin();
    view.edit(&trx)?.title().set(&"f".to_owned())?;
    trx.commit().await?;

    let (count, max_depth) = assert_stamps_equal_depth(&ctx, rec_id, "crossed").await?;
    assert_eq!(count, 6, "genesis, two siblings, two crossed merges, one final merge");
    assert_eq!(max_depth, 4, "the final merge is four levels deep");
    Ok(())
}

/// R-D2-2a, unequal-depth merge (M4 remediation item 8, test-adequacy
/// panel MAJOR 2's integration half): every other multi-tip head in the
/// suite carries EQUAL tip generations, so nothing bound commit-lane
/// stamping over a head whose per-tip values DIFFER. Shape: local edit A
/// (generation 2) over genesis; a remotely delivered honest chain B, C
/// (generations 2 and 3) widens the resident to the unequal antichain
/// {A, C}; a local merge edit then parents on both tips and must stamp
/// 1 + max(2, 3) = 4, which the brute-force depth oracle verifies over
/// the full dumped log (a per-tip misattribution at the widening site,
/// or a min/first-tip confusion in the stamp, lands on 3 instead).
#[tokio::test]
async fn r_d2_2a_unequal_depth_merge_stamps_equal_brute_force_depth() -> Result<()> {
    use ankurah::core::property::backend::{lww::LWWBackend, PropertyBackend};
    use ankurah::core::property::PropertyKey;
    use ankurah::core::value::Value;
    use ankurah::Mutable;
    use std::collections::BTreeMap;

    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;

    let (rec_id, view, genesis) = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let view = rec.read();
        let mut events = trx.commit_and_return_events().await?;
        (id, view, events.remove(0))
    };
    let record_title = node.catalog.resolve(Record::collection().as_str(), "title").expect("Record.title registered by create");

    // Local edit A: generation 2, head {A}.
    {
        let trx = ctx.begin();
        view.edit(&trx)?.title().set(&"a".to_owned())?;
        trx.commit().await?;
    }

    // The honest remote chain B (child of genesis, generation 2, concurrent
    // with A) and C (child of B, generation 3): the delivery widens the
    // resident head to the UNEQUAL antichain {A, C}.
    let title_ops = |title: &str| {
        let backend = LWWBackend::new();
        backend.set(PropertyKey::Id(record_title), Some(Value::String(title.to_owned())));
        proto::OperationSet(BTreeMap::from([("lww".to_owned(), backend.to_operations().unwrap().expect("ops"))]))
    };
    let b = ankurah_tests::forge::event_with_parents(rec_id, genesis.model, title_ops("b"), &[&genesis]);
    let c = ankurah_tests::forge::event_with_parents(rec_id, b.model, title_ops("c"), &[&b]);
    let c_id = c.id();
    node.commit_remote_transaction(
        &DEFAULT_CONTEXT,
        proto::TransactionId::new(),
        vec![proto::Attested::opt(b, None), proto::Attested::opt(c, None)],
    )
    .await?;
    let head = view.entity().head();
    assert_eq!(head.as_slice().len(), 2, "precondition: two-tip antichain, got {head:?}");
    assert!(head.contains(&c_id), "precondition: the deeper branch's tip C is a head tip");

    // The merge edit parents on {A, C} and must stamp 1 + max(2, 3) = 4.
    let merge_id = {
        let trx = ctx.begin();
        view.edit(&trx)?.title().set(&"merged".to_owned())?;
        let mut events = trx.commit_and_return_events().await?;
        events.remove(0).id()
    };

    let (count, max_depth) = assert_stamps_equal_depth(&ctx, rec_id, "unequal-depth merge").await?;
    assert_eq!(count, 5, "genesis, local A, remote B and C, one merge");
    assert_eq!(max_depth, 4, "the merge sits one level above the DEEPER branch");

    // Sharp value check on the merge itself: max over UNEQUAL parents, not
    // min, not the first tip's value.
    let collection = ctx.collection(&Record::collection()).await?;
    let merge = collection
        .dump_entity_events(rec_id)
        .await?
        .into_iter()
        .find(|e| e.payload.id() == merge_id)
        .expect("the merge event is in the log");
    assert_eq!(merge.payload.generation, 4, "the merge stamps 1 + max over unequal per-tip generations");
    assert_eq!(merge.payload.parent.as_slice().len(), 2, "the merge parents on both tips");
    Ok(())
}

/// GClock pin (iv), THE ORIGINAL MOTIVATING CASE (plan REV 5 section K,
/// "what it buys"): an ephemeral node that adopted a BODILESS state (the
/// get() path ships a StateSnapshot with no event bodies) commits over the
/// adopted head with a correct stamp and ZERO peer event retrievals. The
/// counter sits on the wire: every NodeRequestBody::GetEvents request
/// arriving at the durable peer is counted, and the whole scenario must
/// produce none. Stamp correctness is oracle-checked two ways: the relay
/// itself (the durable peer's admission verification rejects a wrong stamp,
/// pinned by R-D2-2b, so a completed commit means the server accepted the
/// equation against its local parents) and the brute-force depth recompute
/// over the server's full event log.
#[tokio::test]
async fn gclock_bodiless_adoption_commit_stamps_without_peer_event_fetches() -> Result<()> {
    use ankurah::{policy::DEFAULT_CONTEXT, Node, PermissiveAgent};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());

    // Counting connection: the gate filter observes every message arriving
    // at the SERVER and counts event-body retrievals without parking
    // anything (it always returns false).
    let peer_event_fetches = Arc::new(AtomicUsize::new(0));
    let (_conn, _gate) = {
        let counter = peer_event_fetches.clone();
        GatedConnection::new(&client, &server, move |msg: &proto::NodeMessage| {
            if let proto::NodeMessage::Request { request, .. } = msg {
                if matches!(request.body, proto::NodeRequestBody::GetEvents { .. }) {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            }
            false
        })
    };
    client.system.wait_system_ready().await;

    let ctx_s = server.context(DEFAULT_CONTEXT)?;
    let ctx_c = client.context(DEFAULT_CONTEXT)?;

    // Server-side history: genesis plus one edit, so the adopted head sits
    // at depth 2 and a naive stamp of 1 would be provably wrong.
    let rec_id = {
        let trx = ctx_s.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        trx.commit().await?;
        id
    };
    let view_s = ctx_s.get::<RecordView>(rec_id).await?;
    {
        let trx = ctx_s.begin();
        view_s.edit(&trx)?.title().set(&"t1".to_owned())?;
        trx.commit().await?;
    }

    // Bodiless adoption: get() ships a state snapshot, no event bodies.
    let view = ctx_c.get::<RecordView>(rec_id).await?;
    assert_eq!(view.title().unwrap(), "t1", "precondition: the adopted snapshot is current");

    // The motivating commit: parents are exactly the adopted head tips.
    let trx = ctx_c.begin();
    view.edit(&trx)?.title().set(&"from-the-ephemeral".to_owned())?;
    trx.commit().await?;

    assert_eq!(
        peer_event_fetches.load(Ordering::SeqCst),
        0,
        "GClock pin (iv): a bodiless-adoption commit must stamp from the materialized head generations; \
         any peer event retrieval means the stamp read payloads instead"
    );

    // The durable peer accepted the relayed commit (admission-verified
    // against its LOCAL parents); recompute the depth oracle over its log.
    let (count, max_depth) = assert_stamps_equal_depth(&ctx_s, rec_id, "bodiless-adoption commit").await?;
    assert_eq!(count, 3, "genesis, server edit, ephemeral commit");
    assert_eq!(max_depth, 3, "the ephemeral commit stamps 1 + max over the adopted head");
    Ok(())
}
