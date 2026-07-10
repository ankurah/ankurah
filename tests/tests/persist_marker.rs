//! D2 M4 persist-currency marker and hard_reset pins (plan D2-6 as amended
//! by REV 5 section D; obligations (b) and (c)).
//!
//! The observable is the STORAGE boundary: `InstrumentedEngine` counts
//! every set_state call and can hold them open behind a gate, so elision
//! (a set_state that never happens) and two-lane interleavings are
//! measured honestly, not inferred.

mod common;

use ankurah::core::property::backend::{lww::LWWBackend, PropertyBackend};
use ankurah::policy::DEFAULT_CONTEXT as c;
use ankurah::{proto, Mutable, Node, PermissiveAgent};
use anyhow::Result;
use common::*;
use std::collections::BTreeMap;
use std::sync::Arc;

/// The LWW OperationSet for a title write (forged events flow through the
/// real ingest, so the operations must be honest LWW payloads).
fn title_ops(title: &str) -> proto::OperationSet {
    use ankurah::core::value::Value;
    let backend = LWWBackend::new();
    backend.set("title".into(), Some(Value::String(title.to_owned())));
    let ops = backend.to_operations().unwrap().expect("LWW backend with a write produces operations");
    proto::OperationSet(BTreeMap::from([("lww".to_owned(), ops)]))
}

/// R-D2-4a (plan REV 4 section 3 M4): a CURRENT no-op redelivery produces
/// ZERO set_state calls. The resident's persist-currency marker proves a
/// completed persist already covers exactly this head in this reset epoch,
/// so the pipeline's uniform persist is elided at the funnel. The
/// redelivery goes through the remote commit lane (fresh staging, the
/// planner preresolves the already-committed member, the executor still
/// reaches its persist step); before the marker exists, that step writes
/// unconditionally.
#[tokio::test]
async fn r_d2_4a_current_noop_redelivery_produces_zero_set_state_calls() -> Result<()> {
    let engine = InstrumentedEngine::new(SledStorageEngine::new_test()?);
    let instruments = engine.instruments();
    let node = Node::new_durable(Arc::new(engine), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Create the record; the commit's own persist completes and (with M4)
    // stamps the marker for the current head. The fork's view is held
    // across the commit so the canonical resident (whose marker the commit
    // stamped) STAYS resident: markers live on the resident instance and
    // are stamped only by completed set_states, so a dropped-and-rehydrated
    // resident legitimately starts unmarked and pays one redundant write
    // before its own marker takes over.
    let (rec_id, view, genesis) = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let view = rec.read();
        let mut events = trx.commit_and_return_events().await?;
        (id, view, events.remove(0))
    };
    assert_eq!(view.title().unwrap(), "t0", "precondition: the record is resident and current");
    assert!(node.get_resident_entity(rec_id).is_some(), "precondition: the canonical stayed resident across the commit");

    // Redeliver the same event: a current no-op.
    let baseline = instruments.set_state_attempts();
    node.commit_remote_transaction(&c, proto::TransactionId::new(), vec![proto::Attested::opt(genesis, None)])
        .await
        .expect("an idempotent redelivery is clean");

    assert_eq!(
        instruments.set_state_attempts() - baseline,
        0,
        "R-D2-4a: a current no-op redelivery must be served by the persist-currency marker with ZERO set_state calls"
    );
    Ok(())
}

/// The purge pin (REV 5 section D.1, the one-id-one-system invariant):
/// hard_reset clears the resident entity map. After the reset the map is
/// EMPTY (no tombstones: dead weak entries leave too), the old id is
/// unreachable from ingest (get_resident_entity answers None even though a
/// strong reference exists), and a held view keeps reading its stale
/// values unchanged (the successor system simply never hands that entity
/// out again).
#[tokio::test]
async fn hard_reset_purges_the_entity_map_and_held_views_stay_frozen() -> Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    let rec_id = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "stale-title".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        trx.commit().await?;
        id
    };
    // Hold a STRONG reference across the reset.
    let view = ctx.get::<RecordView>(rec_id).await?;
    assert!(node.get_resident_entity(rec_id).is_some(), "precondition: the record is resident");
    assert!(node.resident_count_for_test() >= 1, "precondition: the map holds entries");

    node.system.hard_reset().await?;

    assert_eq!(
        node.resident_count_for_test(),
        0,
        "the purge pin: after hard_reset the entity map is EMPTY, live and dead entries alike (no tombstones)"
    );
    assert!(node.get_resident_entity(rec_id).is_none(), "the purge pin: the dead system's id is unreachable from ingest");
    assert_eq!(view.title().unwrap(), "stale-title", "the held view still reads its stale values unchanged");
    Ok(())
}

/// R-D2-4c (plan REV 4 section 3 M4; REV 5 H erratum: ef68e081 shipped no
/// deterministic regression test, so this constructs the two-lane
/// interleaving fresh). Two arms over one gated store:
///
/// ELISION CONTROL (the arm that is red before the marker lands): with the
/// marker CURRENT, a redelivery elides its persist entirely.
///
/// THE INTERLEAVING (the ef68e081 class the elision must not resurrect):
/// lane A applies a fresh event and its persist is HELD OPEN at the
/// storage gate, so A has not stamped a marker; lane B redelivers the same
/// event, applies as a no-op, and reaches the persist step while A is
/// still parked. The marker (stamped for the pre-A head) does not match
/// the advanced head, so B MUST write: head mismatch defeats the marker.
/// If B elided here, a caller re-reading local storage after B returns
/// could miss state lane A has not yet persisted, which is exactly the
/// race ef68e081 fixed by removing the old no-op elision.
#[tokio::test]
async fn r_d2_4c_two_lane_interleaving_defeats_the_marker_where_current_markers_elide() -> Result<()> {
    let engine = InstrumentedEngine::new(SledStorageEngine::new_test()?);
    let instruments = engine.instruments();
    let node = Node::new_durable(Arc::new(engine), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Held view keeps the canonical (and its stamped marker) resident, as
    // in R-D2-4a above.
    let (rec_id, view, genesis) = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let view = rec.read();
        let mut events = trx.commit_and_return_events().await?;
        (id, view, events.remove(0))
    };

    // ELISION CONTROL: marker current, redelivery writes nothing.
    let baseline = instruments.set_state_attempts();
    node.commit_remote_transaction(&c, proto::TransactionId::new(), vec![proto::Attested::opt(genesis.clone(), None)])
        .await
        .expect("idempotent redelivery is clean");
    assert_eq!(
        instruments.set_state_attempts() - baseline,
        0,
        "elision control: a current-marker redelivery must not call set_state (R-D2-4a mechanism)"
    );

    // THE INTERLEAVING. A fresh event over the current head; lane A's
    // persist parks at the closed gate AFTER the resident advanced.
    let e1 = ankurah_tests::forge::event_with_parents(rec_id, Record::collection(), title_ops("t1"), &[&genesis]);
    let baseline = instruments.set_state_attempts();
    instruments.close_gate();

    let lane_a = {
        let node = node.clone();
        let e1 = e1.clone();
        tokio::spawn(
            async move { node.commit_remote_transaction(&c, proto::TransactionId::new(), vec![proto::Attested::opt(e1, None)]).await },
        )
    };
    instruments.wait_until_parked(1).await;
    let _ = &view; // the held view keeps the canonical resident throughout
    assert!(
        node.get_resident_entity(rec_id).expect("still resident").head().contains(&e1.id()),
        "precondition: lane A advanced the resident in memory; its persist is in flight"
    );

    // Lane B: the same event again, while A is parked. B's apply is a
    // no-op, but the marker still names the PRE-A head: B must persist.
    let lane_b = {
        let node = node.clone();
        let e1 = e1.clone();
        tokio::spawn(
            async move { node.commit_remote_transaction(&c, proto::TransactionId::new(), vec![proto::Attested::opt(e1, None)]).await },
        )
    };
    instruments.wait_until_parked(2).await;

    instruments.open_gate();
    lane_a.await?.expect("lane A commits cleanly");
    lane_b.await?.expect("lane B commits cleanly");

    assert_eq!(
        instruments.set_state_attempts() - baseline,
        2,
        "R-D2-4c: lane B's persist must happen while lane A's is in flight (head mismatch defeats the marker); \
         one write means the elision resurrected the ef68e081 race, three means double-writing"
    );
    Ok(())
}

/// THE MARKER-RACE PIN, end-to-end variant (maintainer ruling 2026-07-09;
/// the mechanism-seam pin lives in core's entity persist_marker_tests): a
/// persist holds open at the storage gate ACROSS a real hard_reset and
/// completes after it. Its marker carries the pre-reset epoch and is never
/// trusted, and the purge already made the stale resident unreachable, so
/// the next delivery of the same event materializes fresh from whatever
/// the straddling persist left behind and PERSISTS FOR REAL (observed at
/// the storage boundary), rather than being elided on dead-system
/// testimony.
#[tokio::test]
async fn straddling_persist_is_never_trusted_end_to_end() -> Result<()> {
    let engine = InstrumentedEngine::new(SledStorageEngine::new_test()?);
    let instruments = engine.instruments();
    let node = Node::new_durable(Arc::new(engine), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // The held view keeps the canonical resident so lane A's straddling
    // persist targets the SAME instance the marker semantics protect.
    let (rec_id, _view, genesis) = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let view = rec.read();
        let mut events = trx.commit_and_return_events().await?;
        (id, view, events.remove(0))
    };

    // Lane A: a fresh edit whose persist parks at the closed gate AFTER the
    // apply and the event commit (the epoch was captured before set_state).
    let e1 = ankurah_tests::forge::event_with_parents(rec_id, Record::collection(), title_ops("t1"), &[&genesis]);
    instruments.close_gate();
    let lane_a = {
        let node = node.clone();
        let e1 = e1.clone();
        tokio::spawn(
            async move { node.commit_remote_transaction(&c, proto::TransactionId::new(), vec![proto::Attested::opt(e1, None)]).await },
        )
    };
    instruments.wait_until_parked(1).await;

    // The reset lands mid-flight: epoch bump, purge, storage wipe. The
    // parked set_state is untouched (only set_state is gated; the wipe
    // goes through delete_all_collections).
    node.system.hard_reset().await?;
    assert_eq!(node.resident_count_for_test(), 0, "precondition: the purge emptied the map while the persist is parked");

    // Release: the straddling persist resumes into post-reset storage.
    // EITHER outcome is legitimate for a dev-only reset racing a persist:
    // it may complete (stamping the pre-reset epoch, which the epoch
    // conjunct then distrusts forever, the mechanism pin) or fail loudly
    // against the wiped engine (sled drops its trees; the open handle
    // errors). What may NEVER happen is the successor system trusting
    // anything the straddler left behind.
    instruments.open_gate();
    let straddle_outcome = lane_a.await?;

    // The successor system starts over: the record's GENESIS delivers into
    // the wiped system (the one-id-one-system invariant makes redelivering
    // descendants of the dead system illegitimate; this pin only needs A
    // delivery on this id to persist for real). The purge made the stale
    // marker unreachable, and even a reachable one would fail the epoch
    // conjunct.
    let baseline = instruments.set_state_attempts();
    node.commit_remote_transaction(&c, proto::TransactionId::new(), vec![proto::Attested::opt(genesis.clone(), None)])
        .await
        .expect("the post-reset delivery is clean");
    assert!(
        instruments.set_state_attempts() - baseline >= 1,
        "the marker-race pin: after a straddling persist (outcome: {straddle_outcome:?}), the next apply must persist for real \
         (no elision on dead-system testimony)"
    );
    Ok(())
}
