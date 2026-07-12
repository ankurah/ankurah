//! D2 M4 persist-currency marker and hard_reset pins (plan D2-6 as amended
//! by REV 5 section D; obligations (b) and (c)).
//!
//! The observable is the STORAGE boundary: `InstrumentedEngine` counts
//! every set_state call and can hold them open behind a gate, so elision
//! (a set_state that never happens) and two-lane interleavings are
//! measured honestly, not inferred.

mod common;

use ankurah::core::property::backend::{lww::LWWBackend, PropertyBackend};
use ankurah::core::property::PropertyKey;
use ankurah::core::storage::StorageEngine as _;
use ankurah::core::value::Value;
use ankurah::policy::DEFAULT_CONTEXT as c;
use ankurah::{proto, Mutable, Node, PermissiveAgent};
use anyhow::Result;
use common::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// The LWW OperationSet for a title write (forged events flow through the
/// real ingest, so the operations must be honest LWW payloads).
fn title_ops(title_property: proto::EntityId, title: &str) -> proto::OperationSet {
    let backend = LWWBackend::new();
    backend.set(PropertyKey::Id(title_property), Some(Value::String(title.to_owned())));
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
/// strong reference exists), and a held entity keeps its stale id-keyed
/// state unchanged. Typed getters intentionally lose their catalog binding
/// when hard_reset flushes the old system's schema, so the frozen-state check
/// reads by the pre-reset property id.
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
    let held = node.get_resident_entity(rec_id).expect("precondition: the record is resident");
    let title_property = node.catalog.resolve(Record::collection().as_str(), "title").expect("Record.title registered by create");
    assert!(node.get_resident_entity(rec_id).is_some(), "precondition: the record is resident");
    assert!(node.resident_count_for_test() >= 1, "precondition: the map holds entries");

    node.system.hard_reset().await?;

    assert_eq!(
        node.resident_count_for_test(),
        0,
        "the purge pin: after hard_reset the entity map is EMPTY, live and dead entries alike (no tombstones)"
    );
    assert!(node.get_resident_entity(rec_id).is_none(), "the purge pin: the dead system's id is unreachable from ingest");
    let lww = held.get_backend::<LWWBackend>().expect("the held entity keeps its LWW backend");
    assert_eq!(
        lww.get(&PropertyKey::Id(title_property)),
        Some(Value::String("stale-title".to_owned())),
        "the held entity keeps its stale id-keyed value unchanged"
    );
    let _ = view;
    Ok(())
}

/// R-D2-4c AS AMENDED by the M4 post-milestone remediation (the adversarial
/// review's finding 2): the ef68e081 two-lane hazard is now closed by
/// ORDERING, not by redundant writing. Two arms over one gated store:
///
/// ELISION CONTROL (the arm that was red before the marker landed): with
/// the marker CURRENT, a redelivery elides its persist entirely.
///
/// THE INTERLEAVING: lane A applies a fresh event and its persist is HELD
/// OPEN at the storage gate mid-span; lane B redelivers the same event,
/// applies as a no-op, and reaches the persist funnel while A is parked.
/// The funnel serializes persists per entity id, so B cannot RESOLVE
/// (write or elide) while A's span is in flight; once A's completed write
/// covers their shared head and stamps the marker, B's elision check reads
/// truthful testimony and elides. When B returns Ok, storage already holds
/// the head B vouches for (read-your-application preserved with ONE write).
///
/// The pre-remediation form of this pin forced B to WRITE while A was in
/// flight (the marker named the pre-A head), which defended the elision
/// side but left the engine-side reorder open: A's and B's writes raced at
/// the blind last-writer engine upsert, and the loser could regress the
/// stored buffer (see
/// concurrent_same_entity_persists_leave_storage_at_the_resident_head).
/// Serialization discharges the same hazard by construction, so the pin
/// now asserts the serialized world's postconditions.
#[tokio::test]
async fn r_d2_4c_two_lane_interleaving_serializes_where_current_markers_elide() -> Result<()> {
    let engine = Arc::new(InstrumentedEngine::new(SledStorageEngine::new_test()?));
    let instruments = engine.instruments();
    let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
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
    let title_property = node.catalog.resolve(Record::collection().as_str(), "title").expect("Record.title registered by create");

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
    // persist parks at the selective hold AFTER the resident advanced (the
    // hold takes exactly one call, so lane B's funnel is free to reach the
    // engine if the serialization ever regressed).
    let e1 = ankurah_tests::forge::event_with_parents(rec_id, genesis.model, title_ops(title_property, "t1"), &[&genesis]);
    let baseline = instruments.set_state_attempts();
    instruments.hold_next(1);

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
    // no-op; its funnel persist must WAIT behind A's in-flight span rather
    // than resolve early (writing OR eliding).
    let mut lane_b = {
        let node = node.clone();
        let e1 = e1.clone();
        tokio::spawn(
            async move { node.commit_remote_transaction(&c, proto::TransactionId::new(), vec![proto::Attested::opt(e1, None)]).await },
        )
    };
    let b_early = tokio::time::timeout(Duration::from_millis(750), &mut lane_b).await;
    assert!(
        b_early.is_err(),
        "R-D2-4c (amended): a sibling persist of the same entity must not resolve while another lane's \
         snapshot-write-stamp span is in flight; an early return here means the funnel no longer serializes"
    );
    assert_eq!(
        instruments.set_state_attempts() - baseline,
        1,
        "R-D2-4c (amended): only lane A's write may have reached the engine while its span is open"
    );

    instruments.release_held();
    lane_a.await?.expect("lane A commits cleanly");
    lane_b.await?.expect("lane B commits cleanly");

    assert_eq!(
        instruments.set_state_attempts() - baseline,
        1,
        "R-D2-4c (amended): exactly ONE write serves both lanes; A's completed persist covers the shared \
         head and stamps the marker, and B elides on that completed-persist testimony"
    );

    // B returned Ok on elision testimony: storage must already hold the
    // head B vouched for.
    let stored = engine.collection(&Record::collection()).await?.get_state(rec_id).await?;
    assert_eq!(
        stored.payload.state.head,
        node.get_resident_entity(rec_id).expect("still resident").head(),
        "when the eliding lane returns, the stored buffer already covers the head its Ok testified to"
    );
    Ok(())
}
/// THE M4 REMEDIATION RED (adversarial review finding 2, MAJOR): once every
/// concurrent funnel persist of ONE entity resolves, a fresh read from
/// storage must yield the RESIDENT head. Every engine's set_state is a
/// blind last-writer upsert, so without per-entity ordering across the
/// snapshot-write-stamp span, a lane that snapshotted an OLDER head can
/// land its write AFTER a sibling's newer one: the stored buffer regresses,
/// fresh fetchers read the old state, and the marker elision makes the
/// regression STICKY (a redelivery of the newer head elides on truthful-
/// looking testimony instead of healing the store) until the head next
/// advances or the process restarts.
///
/// Staging, deterministic in both worlds: lane A applies E1 (a child of
/// genesis) and its persist (snapshot [E1]) is held open AT THE ENGINE via
/// the selective hold; lane B applies E2, a CONCURRENT SIBLING of E1 (also
/// a child of genesis), widening the head to [E1, E2], and drives its own
/// funnel persist. The sibling shape keeps E1 a head tip throughout, so
/// both lanes' change notifications stay clean and the pin isolates the
/// storage boundary. In the unserialized world B's write [E1, E2] commits
/// and B completes while A is parked (the timed join below observes that),
/// so releasing A lands the stale [E1] write LAST and storage regresses.
/// In the serialized world B waits behind A's span (the timed join
/// elapses, pure pacing), the release lets A finish, and B then snapshots
/// the newest head and writes [E1, E2] last. Either way every lane
/// resolves and the pin asserts what production requires of the settled
/// state: storage head == resident head, marker current for exactly that
/// head, never leading storage.
#[tokio::test]
async fn concurrent_same_entity_persists_leave_storage_at_the_resident_head() -> Result<()> {
    let engine = Arc::new(InstrumentedEngine::new(SledStorageEngine::new_test()?));
    let instruments = engine.instruments();
    let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Resident record with a stamped marker; the held view keeps the
    // canonical resident, as in the sibling pins.
    let (rec_id, view, genesis) = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let view = rec.read();
        let mut events = trx.commit_and_return_events().await?;
        (id, view, events.remove(0))
    };
    let title_property = node.catalog.resolve(Record::collection().as_str(), "title").expect("Record.title registered by create");

    let e1 = ankurah_tests::forge::event_with_parents(rec_id, genesis.model, title_ops(title_property, "t1"), &[&genesis]);
    let e2 = ankurah_tests::forge::event_with_parents(rec_id, genesis.model, title_ops(title_property, "t2"), &[&genesis]);

    // Lane A: applies E1, snapshots head [E1], parks at the engine. Only
    // this one call is held; the gate stays open for lane B's write.
    let baseline = instruments.set_state_attempts();
    instruments.hold_next(1);
    let lane_a = {
        let node = node.clone();
        let e1 = e1.clone();
        tokio::spawn(
            async move { node.commit_remote_transaction(&c, proto::TransactionId::new(), vec![proto::Attested::opt(e1, None)]).await },
        )
    };
    instruments.wait_until_parked(1).await;
    assert!(
        node.get_resident_entity(rec_id).expect("still resident").head().contains(&e1.id()),
        "precondition: lane A advanced the resident to [E1]; its persist of that snapshot is parked at the engine"
    );

    // Lane B: applies E2 (resident head [E2]) and drives its own persist.
    let mut lane_b = {
        let node = node.clone();
        let e2 = e2.clone();
        tokio::spawn(
            async move { node.commit_remote_transaction(&c, proto::TransactionId::new(), vec![proto::Attested::opt(e2, None)]).await },
        )
    };

    // Unserialized world: B's write passes the open gate and B completes
    // while A is parked; join it so B's write AND stamp are fully done
    // before the release lands A's stale write last. Serialized world: B is
    // parked behind A's span and cannot resolve until the release below, so
    // this join must elapse; the timeout is pacing, not an assertion.
    let b_early = tokio::time::timeout(Duration::from_millis(1500), &mut lane_b).await;

    instruments.release_held();
    lane_a.await?.expect("lane A commits cleanly");
    match b_early {
        Ok(join) => join?.expect("lane B commits cleanly"),
        Err(_elapsed) => lane_b.await?.expect("lane B commits cleanly"),
    }

    assert_eq!(instruments.set_state_attempts() - baseline, 2, "sanity: each lane persisted exactly once");

    let resident_head = node.get_resident_entity(rec_id).expect("still resident").head();
    assert!(resident_head.contains(&e2.id()), "precondition: the resident settled at [E2]");

    // THE PIN: a fresh read from storage yields the resident head.
    let stored = engine.collection(&Record::collection()).await?.get_state(rec_id).await?;
    assert_eq!(
        stored.payload.state.head, resident_head,
        "M4 remediation (adversarial finding 2): once all persists of an entity resolve, the stored buffer \
         must hold the resident head; a stale snapshot landing last regresses storage, and the marker \
         elision then serves every redelivery from testimony instead of healing the store"
    );

    // And the settled marker never LEADS storage: it is current for exactly
    // the stored head, observed as a current no-op redelivery eliding with
    // zero writes while a fresh storage read still answers the same head.
    let baseline = instruments.set_state_attempts();
    node.commit_remote_transaction(&c, proto::TransactionId::new(), vec![proto::Attested::opt(e2.clone(), None)])
        .await
        .expect("idempotent redelivery is clean");
    assert_eq!(
        instruments.set_state_attempts() - baseline,
        0,
        "the settled marker elides the current no-op redelivery (it names the resident head)"
    );
    let stored = engine.collection(&Record::collection()).await?.get_state(rec_id).await?;
    assert_eq!(
        stored.payload.state.head, resident_head,
        "the elision testified truthfully: storage holds the head the marker names (the marker does not lead storage)"
    );
    let _ = &view;
    Ok(())
}

/// THE MARKER-RACE PIN, end-to-end variant (maintainer ruling 2026-07-09;
/// the mechanism-seam pin lives in core's entity persist_marker_tests),
/// RESTAGED for the reset/persist fence (M4 remediation): a persist parked
/// at the storage gate when a hard_reset arrives is DRAINED first (the
/// fence write guard waits for it), so the old foreground-await staging
/// would deadlock; the reset now runs as a task and completes after the
/// release. The pin's property is unchanged: whatever a persist
/// overlapping a reset leaves behind, the successor system trusts NONE of
/// it, so the next delivery on that id materializes fresh and PERSISTS
/// FOR REAL (observed at the storage boundary), never eliding on
/// dead-system testimony.
#[tokio::test]
async fn straddling_persist_is_never_trusted_end_to_end() -> Result<()> {
    let engine = InstrumentedEngine::new(SledStorageEngine::new_test()?);
    let instruments = engine.instruments();
    let node = Node::new_durable(Arc::new(engine), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // The held view keeps the canonical resident so lane A's overlapping
    // persist targets the SAME instance the marker semantics protect.
    let (rec_id, _view, genesis) = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let view = rec.read();
        let mut events = trx.commit_and_return_events().await?;
        (id, view, events.remove(0))
    };
    let title_property = node.catalog.resolve(Record::collection().as_str(), "title").expect("Record.title registered by create");

    // Lane A: a fresh edit whose persist parks at the closed gate AFTER the
    // apply and the event commit (the epoch was captured before set_state).
    let e1 = ankurah_tests::forge::event_with_parents(rec_id, genesis.model, title_ops(title_property, "t1"), &[&genesis]);
    instruments.close_gate();
    let lane_a = {
        let node = node.clone();
        let e1 = e1.clone();
        tokio::spawn(
            async move { node.commit_remote_transaction(&c, proto::TransactionId::new(), vec![proto::Attested::opt(e1, None)]).await },
        )
    };
    instruments.wait_until_parked(1).await;

    // The reset runs as a task: with the fence it drains lane A before the
    // bump, purge, and wipe; without it (the pre-remediation world) it
    // completed while A was parked. The pin tolerates either interleaving;
    // hard_reset_drains_inflight_persists_before_wiping pins the ordering.
    let reset = {
        let node = node.clone();
        tokio::spawn(async move { node.system.hard_reset().await })
    };

    // Release lane A, then let the reset finish. Lane A's own outcome is
    // deliberately tolerated either way: under the fence it completes
    // cleanly before the wipe; in the unfenced world it raced the wipe and
    // could fail loudly. What may NEVER happen is the successor system
    // trusting anything it left behind.
    instruments.open_gate();
    let straddle_outcome = lane_a.await?;
    reset.await??;
    assert_eq!(node.resident_count_for_test(), 0, "precondition: the reset purged the map");

    // This historical mechanism probe deliberately drives an illegitimate
    // old-system delivery after the wipe. Seed only its old model route so
    // model-metadata ingress can reach the marker path; this is not schema
    // registration and does not model a valid successor-system flow. The
    // purge made the stale marker unreachable, and even a reachable one
    // would fail the epoch conjunct.
    node.catalog.upsert_registered(
        &[proto::RegisteredModel { id: genesis.model, collection: Record::collection().as_str().to_owned(), name: "Record".to_owned() }],
        &[],
        &[],
    );
    let baseline = instruments.set_state_attempts();
    node.commit_remote_transaction(&c, proto::TransactionId::new(), vec![proto::Attested::opt(genesis.clone(), None)])
        .await
        .expect("the post-reset delivery is clean");
    assert!(
        instruments.set_state_attempts() - baseline >= 1,
        "the marker-race pin: after a persist overlapped a reset (outcome: {straddle_outcome:?}), the next apply must persist \
         for real (no elision on dead-system testimony)"
    );
    Ok(())
}

/// THE RESET/PERSIST FENCE PIN (M4 remediation, cross-model review item 5):
/// hard_reset must DRAIN in-flight funnel persists before its epoch bump,
/// purge, and wipe, and must hold new ones out until the wipe completes.
/// Without the fence, a persist parked at the engine when the reset runs
/// lands its write AFTER the wipe: on postgres, set_state's UndefinedTable
/// recovery recreates the state table and the dead system's row survives
/// DURABLY in the successor system's storage (storage/postgres/src/lib.rs),
/// violating the one-id-one-system reset invariant (plan REV 5 section D);
/// sled merely swallows the write into dropped tree handles, which is why
/// the sled-staged discriminator here is the DRAIN ORDERING itself, with
/// the storage-cleanliness invariant asserted alongside.
#[tokio::test]
async fn hard_reset_drains_inflight_persists_before_wiping() -> Result<()> {
    let engine = Arc::new(InstrumentedEngine::new(SledStorageEngine::new_test()?));
    let instruments = engine.instruments();
    let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    let (rec_id, _view, genesis) = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let view = rec.read();
        let mut events = trx.commit_and_return_events().await?;
        (id, view, events.remove(0))
    };
    let title_property = node.catalog.resolve(Record::collection().as_str(), "title").expect("Record.title registered by create");

    // Lane A: a persist in flight, parked at the engine.
    let e1 = ankurah_tests::forge::event_with_parents(rec_id, genesis.model, title_ops(title_property, "t1"), &[&genesis]);
    instruments.close_gate();
    let lane_a = {
        let node = node.clone();
        let e1 = e1.clone();
        tokio::spawn(
            async move { node.commit_remote_transaction(&c, proto::TransactionId::new(), vec![proto::Attested::opt(e1, None)]).await },
        )
    };
    instruments.wait_until_parked(1).await;

    // THE PIN: the reset must NOT resolve while that persist is in flight.
    // (In the fenced world this join elapses deterministically: the write
    // guard cannot be granted while lane A holds the fence in read mode.)
    let mut reset = {
        let node = node.clone();
        tokio::spawn(async move { node.system.hard_reset().await })
    };
    let reset_early = tokio::time::timeout(Duration::from_millis(750), &mut reset).await;
    assert!(
        reset_early.is_err(),
        "the fence pin: hard_reset resolved while a funnel persist was parked at the engine; it must drain \
         in-flight persists before bumping the epoch, purging the map, and wiping storage"
    );
    assert!(node.get_resident_entity(rec_id).is_some(), "while the reset is fenced out, the purge has not run");

    // Release: the drained persist completes FIRST (write and stamp, into
    // intact pre-wipe storage), then the reset proceeds through bump,
    // purge, and wipe.
    instruments.open_gate();
    lane_a.await?.expect("the drained persist completes cleanly, before the wipe");
    reset.await??;

    // The one-id-one-system invariant: the successor system's storage does
    // not contain the dead entity (the drained write preceded the wipe).
    assert!(
        engine.collection(&Record::collection()).await?.get_state(rec_id).await.is_err(),
        "post-reset storage must not contain the dead system's entity"
    );
    assert_eq!(node.resident_count_for_test(), 0, "and the purge ran");
    Ok(())
}

/// THE MATERIALIZATION-POROSITY PIN (M4 remediation item 6, concurrency
/// panel finding 1, MAJOR): the purge's one-id-one-system guarantee (plan
/// REV 5 section D.1) must hold against IN-FLIGHT two-phase
/// materialization, not only against entities already in the map. Every
/// materialization path awaits a storage read and then inserts into the
/// map with no re-check: a read that completed BEFORE a hard_reset can
/// insert a dead-system resident AFTER the purge, leaving the successor
/// system with a dead resident every ingest lane and context read can
/// reach (and a state-feed lane can then persist into successor storage).
///
/// Staging: the record exists in storage but is NOT resident; a context
/// get's baseline read (the with_state not-resident arm) is held at the
/// get_state EGRESS, its pre-reset result in hand; a hard_reset runs; the
/// release lets the materialization continue into its map insert. The pin
/// asserts the dead id does NOT end up resident and successor storage does
/// not contain it. Under the reset fence the interleaving is ordered
/// instead: the reset drains the in-flight materialization (whose insert
/// then lands pre-purge and is purged), so the assertions hold on either
/// side of the race.
#[tokio::test]
async fn hard_reset_is_not_porous_to_inflight_materialization() -> Result<()> {
    let engine = Arc::new(InstrumentedEngine::new(SledStorageEngine::new_test()?));
    let instruments = engine.instruments();
    let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Create the record and drop every strong reference: it lives in
    // storage only.
    let rec_id = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "stale".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        trx.commit().await?;
        id
    };
    assert!(node.get_resident_entity(rec_id).is_none(), "precondition: the record is not resident, storage only");

    // A context get materializes in two phases: the direct storage read
    // (passes, skip slot) and the with_state baseline read (held at the
    // egress with the PRE-RESET state in hand).
    instruments.hold_gets(1, 1);
    let getter = {
        let ctx = ctx.clone();
        tokio::spawn(async move { ctx.get::<RecordView>(rec_id).await })
    };
    instruments.wait_until_get_parked(1).await;

    // The reset: with the fence it drains the in-flight materialization
    // first (this task blocks until the release below); without it (the
    // pre-remediation world) it completes here and the release then
    // inserts a dead-system resident into the purged map.
    let reset = {
        let node = node.clone();
        tokio::spawn(async move { node.system.hard_reset().await })
    };

    instruments.release_held_gets();
    let got = getter.await?;
    reset.await??;

    // THE PIN: the dead system's id must not be resident in the successor
    // system (however the get itself resolved: fenced, its insert landed
    // pre-purge and was purged; a held result is a frozen strong reference,
    // not map residency).
    assert!(
        node.get_resident_entity(rec_id).is_none(),
        "M4 remediation item 6: a storage read that began before a hard_reset must not insert a dead-system \
         resident after the purge (the get resolved as {:?})",
        got.as_ref().map(|view| view.id())
    );
    assert!(
        engine.collection(&Record::collection()).await?.get_state(rec_id).await.is_err(),
        "and successor storage does not contain the dead system's entity"
    );
    assert_eq!(node.resident_count_for_test(), 0, "the map holds nothing after the reset");
    Ok(())
}

/// THE EPOCH-CONJUNCT PIN (M4 remediation item 2, adversarial finding 4,
/// reshaped by the reset fence): a marker stamped BEFORE a hard_reset, on
/// a resident that survives the purge only through held strong
/// references, must never be trusted afterward, even though its HEAD
/// still matches (the one corner where the epoch conjunct is the sole
/// standing defense: the purge covers every map-resolved lane, and any
/// head advance would defeat the marker anyway).
///
/// GREEN FROM BIRTH, per the M4 red-first convention's exception clause:
/// the epoch conjunct has existed since the marker landed (8a718607), so
/// no red is constructible without first breaking production code. Teeth
/// were verified by hostile edit instead (dropping the epoch comparison
/// from persist_marker_current turns exactly this pin red; evidence in
/// the commit body). Note on the finding's original shape: it asked to
/// pin the funnel's capture-epoch-BEFORE-the-await ordering, but the
/// reset fence (item 5) made that ordering unobservable, in the good
/// way: no reset step can interleave a fenced persist span, so capture
/// placement within the span cannot matter. What remains load-bearing,
/// and is pinned here through the REAL funnel at an unchanged head, is
/// the conjunct itself.
#[tokio::test]
async fn a_pre_reset_marker_at_an_unchanged_head_is_never_trusted() -> Result<()> {
    let engine = Arc::new(InstrumentedEngine::new(SledStorageEngine::new_test()?));
    let instruments = engine.instruments();
    let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    let (rec_id, _view, genesis) = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let view = rec.read();
        let mut events = trx.commit_and_return_events().await?;
        (id, view, events.remove(0))
    };
    let title_property = node.catalog.resolve(Record::collection().as_str(), "title").expect("Record.title registered by create");
    // The held STRONG entity is the post-purge survivor this pin drives.
    let held = node.get_resident_entity(rec_id).expect("resident");

    // Lane A: E1 applies (head [E1]); its persist parks at the engine with
    // the pre-reset epoch captured inside its fenced span.
    let e1 = ankurah_tests::forge::event_with_parents(rec_id, genesis.model, title_ops(title_property, "t1"), &[&genesis]);
    instruments.close_gate();
    let lane_a = {
        let node = node.clone();
        let e1 = e1.clone();
        tokio::spawn(
            async move { node.commit_remote_transaction(&c, proto::TransactionId::new(), vec![proto::Attested::opt(e1, None)]).await },
        )
    };
    instruments.wait_until_parked(1).await;

    // The reset queues behind the fence; the release drains lane A first
    // (write into pre-wipe storage, stamp of the PRE-reset epoch), then
    // the reset bumps, purges, and wipes.
    let reset = {
        let node = node.clone();
        tokio::spawn(async move { node.system.hard_reset().await })
    };
    instruments.open_gate();
    lane_a.await?.expect("the drained persist completes cleanly before the wipe");
    reset.await??;

    // The held resident still heads [E1], exactly what the marker names;
    // only the epoch distinguishes the dead testimony from a live one.
    assert!(held.head().contains(&e1.id()), "precondition: the held resident's head is unchanged since the stamp");
    assert!(
        !held.persist_marker_current_for_test(node.reset_epoch_for_test()),
        "the epoch conjunct: a marker stamped before the reset must not be current under the successor epoch, \
         even at an unchanged head"
    );

    // And the funnel itself refuses the dead testimony. Seed only the old
    // model route needed to serialize this deliberately illegitimate held
    // entity; doing so keeps the mechanism seam focused on the epoch
    // conjunct rather than the independent catalog-reset rejection.
    node.catalog.upsert_registered(
        &[proto::RegisteredModel { id: genesis.model, collection: Record::collection().as_str().to_owned(), name: "Record".to_owned() }],
        &[],
        &[],
    );
    let baseline = instruments.set_state_attempts();
    node.funnel_persist_for_test(&held).await?;
    assert_eq!(
        instruments.set_state_attempts() - baseline,
        1,
        "a funnel persist of the held resident after the reset must WRITE, never elide on the dead epoch's marker"
    );
    Ok(())
}

/// THE DUPLICATE-INSTANCE REFUSAL PIN (the codex follow-up review's
/// blocker, coordinator-verified at 92317f65): the per-id span lock orders
/// persists, but the marker and the snapshot are per Entity INSTANCE, so
/// with TWO live instances for one id the ordering alone cannot keep
/// storage coherent. A stale instance persisting LAST blindly upserts its
/// older head (storage REGRESSES behind the canonical), and the
/// canonical's next persist then elides on its own instance-local marker,
/// so the regression is STICKY until the canonical head advances or the
/// process restarts. The remediation makes the funnel refuse any instance
/// that is not the node's canonical resident, under the span lock and
/// BEFORE the elision check; the refusal is an innocent-race item failure,
/// and the sender's redelivery landing on the canonical is the heal path.
///
/// The duplication lever here is conjure_evil_phantom replacing the map
/// entry while the old instance is strongly held: it stands in for the
/// remove_if_phantom eviction race (concurrency panel NOTE 4: a sibling
/// lane's failure evicts the shared entry by id at the executor's
/// failure-eviction site or commit_remote_transaction's phase-one
/// eviction, while another lane still holds the instance, and a later
/// delivery rematerializes a fresh canonical), because no public seam
/// parks a lane between its apply and its funnel entry. The fix closes
/// EVERY ordering of that window, which is what this pin binds:
/// 1. the stale instance's funnel persist returns the typed refusal and
///    never reaches the engine,
/// 2. once every persist resolves, storage covers the CANONICAL head,
/// 3. the canonical's own elision stays truthful (its marker matches both
///    its head and what storage holds).
#[tokio::test]
async fn funnel_refuses_a_stale_duplicate_instance_and_storage_follows_the_canonical() -> Result<()> {
    let engine = Arc::new(InstrumentedEngine::new(SledStorageEngine::new_test()?));
    let instruments = engine.instruments();
    let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Create the record, dropping every strong handle so the creating
    // resident dies with the transaction.
    let (rec_id, genesis) = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let mut events = trx.commit_and_return_events().await?;
        (id, events.remove(0))
    };
    let title_property = node.catalog.resolve(Record::collection().as_str(), "title").expect("Record.title registered by create");
    assert!(node.get_resident_entity(rec_id).is_none(), "precondition: no strong holder, the creating resident died");

    // Rehydrate: the soon-to-be-stale instance S at H1 = {genesis}. Its
    // marker is EMPTY (markers are stamped only by a completed persist of
    // that same instance), so nothing elides S's persist below.
    let stale_view = ctx.get::<RecordView>(rec_id).await?;
    let stale = node.get_resident_entity(rec_id).expect("rehydrated resident");
    let h1 = stale.head();
    assert!(h1.contains(&genesis.id()), "precondition: S sits at the genesis head");

    // The duplicate window: the map entry is replaced under S's feet (the
    // test-only lever standing in for the eviction race described above).
    let _replacement = node.conjure_evil_phantom(rec_id, Record::collection());

    // A delivery rebuilds the canonical at H2 = {e2} through the real
    // remote-commit lane, persisting H2 and stamping the NEW instance's
    // marker.
    let e2 = ankurah_tests::forge::event_with_parents(rec_id, genesis.model, title_ops(title_property, "t1"), &[&genesis]);
    node.commit_remote_transaction(
        &c,
        proto::TransactionId::new(),
        vec![proto::Attested::opt(genesis.clone(), None), proto::Attested::opt(e2.clone(), None)],
    )
    .await
    .expect("the delivery rebuilds the canonical");
    let canonical = node.get_resident_entity(rec_id).expect("canonical rebuilt");
    let h2 = canonical.head();
    assert!(h2.contains(&e2.id()), "precondition: the canonical advanced to H2");
    assert_ne!(stale.head(), canonical.head(), "precondition: two live instances at different heads");
    let stored = engine.collection(&Record::collection()).await?.get_state(rec_id).await?;
    assert_eq!(stored.payload.state.head, h2, "precondition: storage covers the canonical head");

    // THE REFUSAL: the stale instance drives the real funnel LAST, fully
    // serialized on the id lock. The funnel must refuse it with the typed
    // error BEFORE the elision check, and nothing may reach the engine.
    let baseline = instruments.set_state_attempts();
    let refusal = node.funnel_persist_for_test(&stale).await;
    assert!(
        matches!(&refusal, Err(MutationError::StaleInstancePersistRefused(id)) if *id == rec_id),
        "the funnel must refuse a non-canonical instance's persist with the typed refusal, got {refusal:?}"
    );
    assert_eq!(instruments.set_state_attempts() - baseline, 0, "a refused persist never reaches the engine (no write, no stamp)");

    // THE PROPERTY the refusal protects: after every persist resolved,
    // storage covers the canonical head (no blind last-writer regression
    // to H1).
    let stored = engine.collection(&Record::collection()).await?.get_state(rec_id).await?;
    assert_eq!(
        stored.payload.state.head, h2,
        "marker coherence across duplicate instances: storage must cover the canonical head once all persists resolve"
    );

    // And the canonical's own elision stays TRUTHFUL: its instance-local
    // marker names its head, and storage really holds that head.
    let baseline = instruments.set_state_attempts();
    node.funnel_persist_for_test(&canonical).await?;
    assert_eq!(instruments.set_state_attempts() - baseline, 0, "the canonical elides on its own completed-persist testimony");
    let stored = engine.collection(&Record::collection()).await?.get_state(rec_id).await?;
    assert_eq!(stored.payload.state.head, h2, "the elision testified truthfully: storage holds the head the canonical's marker names");

    drop(stale_view);
    Ok(())
}
