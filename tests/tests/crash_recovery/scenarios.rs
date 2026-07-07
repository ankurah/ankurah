//! Crash-point scenarios. Each scenario is a pair of tests:
//!
//! - a `child_*` test that runs the workload and is expected to abort mid-write
//!   (it is a no-op unless the crash environment is set, so it is inert when the
//!   parent binary runs it normally), and
//! - a parent test that spawns the child, waits for the crash, reopens the sled
//!   directory, and asserts the recovery invariant.
//!
//! The parent/child split is what gives true OS-level durability testing: the
//! child is a real process that really dies, so what the parent reopens is
//! exactly what sled persisted up to the crash instant.

use std::sync::Arc;

use ankurah::core::storage::StorageEngine;
use ankurah::proto::{self, Attested};
use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent, View};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;

use crate::harness::{
    assert_state_heads_resolvable, child_crash_point, child_sled_dir, event_present, fresh_sled_dir, handoff_write, handoff_write_event,
    has_persisted_state, persisted_head, reopen_sled, spawn_crash_child, CrashPoint, CrashStorageEngine,
};
use crate::models::{Album, AlbumView};

/// Type alias for the crash-wrapped durable node used across scenarios.
type CrashNode = Node<CrashStorageEngine<SledStorageEngine>, PermissiveAgent>;

/// Build a durable node on a real on-disk sled directory wrapped in the
/// crash-injecting engine, create the system, then arm the crash hook so
/// subsequent operation counts start from zero (bootstrap writes are excluded).
async fn armed_child_node(crash: CrashPoint) -> Result<(CrashNode, Arc<CrashStorageEngine<SledStorageEngine>>)> {
    let dir = child_sled_dir().expect("child must have a sled dir");
    let sled = Arc::new(SledStorageEngine::with_path(dir)?);
    let engine = Arc::new(CrashStorageEngine::new(sled, Some(crash)));
    let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
    node.system.create().await?;
    // Rev 4 (RFC 5.2): a local create auto-registers its model, and the
    // registration executor persists the catalog entities' state first --
    // which would consume `BeforeSetState(0)` before the workload's own
    // write. Register the scenario model as setup so the crash point counts
    // only the workload under test.
    node.context(c)?.register::<Album>().await?;
    // Bootstrap complete: from here on, operations are the workload under test.
    engine.arm();
    Ok((node, engine))
}

/// Generate `n` independent album creation events using a throwaway in-memory
/// durable node. Each album is a distinct entity, so each event is a creation
/// event (empty parent). Returned in commit order as attested events, ready to
/// feed to `commit_remote_transaction` on the node under test.
async fn generate_creation_batch(n: usize) -> Result<Vec<Attested<proto::Event>>> {
    let helper = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    helper.system.create().await?;
    let ctx = helper.context(c)?;
    let trx = ctx.begin();
    for i in 0..n {
        trx.create(&Album { name: format!("Batch {i}"), year: format!("20{i:02}") }).await?;
    }
    let events = trx.commit_and_return_events().await?;
    Ok(events.into_iter().map(Attested::from).collect())
}

/// Re-deliver a set of attested events to a node through the real ingest path
/// (`commit_remote_transaction`, the same entry the relay uses). Models "the
/// peer that sent the batch re-sends it" for the reconvergence invariant.
/// Generic over the storage engine so it works on both the crash-wrapped child
/// node and the plain reopened node.
async fn redeliver<SE>(node: &Node<SE, PermissiveAgent>, events: Vec<Attested<proto::Event>>) -> Result<()>
where SE: StorageEngine + Send + Sync + 'static {
    node.commit_remote_transaction(&c, proto::TransactionId::new(), events).await?;
    Ok(())
}

// ============================================================================
// SCENARIO 1: crash after commit_event, before set_state
// ============================================================================

/// Child for scenario 1. Creates one album and commits. The crash hook aborts
/// just before the first `set_state`, i.e. after the creation event has been
/// committed to storage but before any entity state is written. This is the
/// legal orphaned-event window.
#[tokio::test]
async fn child_commit_event_before_set_state() -> Result<()> {
    let Some(crash) = child_crash_point() else {
        return Ok(()); // inert when run normally in the parent binary
    };
    let (node, _engine) = armed_child_node(crash).await?;
    let ctx = node.context(c)?;

    let trx = ctx.begin();
    let album = trx.create(&Album { name: "Crash One".to_owned(), year: "2001".to_owned() }).await?;
    // Durably record the id so the parent can address the entity after reopen.
    handoff_write("entity", &album.id().to_base64())?;

    // commit() commits the event then writes state; the crash fires inside the
    // state write phase. commit() therefore never returns.
    trx.commit().await?;

    // Unreachable: the crash hook must have aborted during commit.
    panic!("scenario 1 child did not crash: set_state was not intercepted");
}

/// SCENARIO 1 INVARIANT: a crash in the window after `commit_event` and before
/// `set_state` leaves at most an orphaned event and NEVER a state that
/// references a missing event. After reopen, the album either has no persisted
/// state at all (the legal outcome for this window), and whatever events are
/// present are harmless orphans.
#[tokio::test]
async fn scenario_1_commit_event_before_set_state() -> Result<()> {
    let dir = fresh_sled_dir("s1");
    let outcome = spawn_crash_child("scenarios::child_commit_event_before_set_state", &dir, CrashPoint::BeforeSetState(0))?;
    assert!(outcome.crashed(), "child was expected to abort; stdout=\n{}\nstderr=\n{}", outcome.stdout, outcome.stderr);

    let entity_id = outcome.entity_id("entity").expect("child must record the entity id before crashing");

    // Reopen the surviving sled directory through the production opener.
    let engine = reopen_sled(&dir)?;
    let collection = engine.collection(&Album::collection()).await?;

    // The core invariant: no persisted state references a missing event.
    assert_state_heads_resolvable(&collection, &[entity_id]).await?;

    // For this specific window, the state write never started, so there must be
    // no persisted state for the album at all.
    assert!(
        !has_persisted_state(&collection, entity_id).await?,
        "scenario 1: album state must NOT be persisted when the crash preceded set_state"
    );

    cleanup(&dir);
    Ok(())
}

// ============================================================================
// SCENARIO 2: mid-batch crash (some events committed, crash before the rest)
// ============================================================================

/// Number of entities in the mid-batch. The crash targets add_event #2, so
/// entities 0 and 1 complete fully (event + state) and entity 2 onward never
/// begins.
const S2_BATCH: usize = 3;
const S2_CRASH_AT: usize = 2;

/// Child for scenario 2. Receives a multi-entity transaction through the real
/// ingest path (`commit_remote_transaction`), which commits each event then
/// writes its state, one entity at a time. The crash aborts just before the
/// third `add_event`, so the first two entities are fully durable and the rest
/// are absent. The full batch is handed off so the parent can re-deliver it.
#[tokio::test]
async fn child_mid_batch() -> Result<()> {
    let Some(crash) = child_crash_point() else {
        return Ok(());
    };
    let (node, _engine) = armed_child_node(crash).await?;

    let events = generate_creation_batch(S2_BATCH).await?;
    // Record every entity id (in batch order) and the full events for re-delivery.
    for e in &events {
        handoff_write("entity", &e.payload.entity_id.to_base64())?;
        handoff_write_event("event", e)?;
    }

    node.commit_remote_transaction(&c, proto::TransactionId::new(), events).await?;

    panic!("scenario 2 child did not crash: add_event #{S2_CRASH_AT} was not intercepted");
}

/// SCENARIO 2 INVARIANT: a crash partway through a received batch leaves the
/// already-committed entities intact (event present, state resolvable) and the
/// not-yet-reached entities entirely absent, with no persisted state ever
/// referencing an uncommitted event. After reopen, re-delivering the full batch
/// (the sender resending) converges the node to the complete set.
#[tokio::test]
async fn scenario_2_mid_batch() -> Result<()> {
    let dir = fresh_sled_dir("s2");
    let outcome = spawn_crash_child("scenarios::child_mid_batch", &dir, CrashPoint::BeforeAddEvent(S2_CRASH_AT))?;
    assert!(outcome.crashed(), "child was expected to abort; stdout=\n{}\nstderr=\n{}", outcome.stdout, outcome.stderr);

    let entity_ids =
        outcome.handoff.get("entity").map(|v| v.iter().filter_map(|s| ankurah::EntityId::from_base64(s).ok()).collect::<Vec<_>>());
    let entity_ids = entity_ids.expect("child must record entity ids");
    assert_eq!(entity_ids.len(), S2_BATCH, "expected all batch entity ids recorded");
    let events = outcome.events("event");
    assert_eq!(events.len(), S2_BATCH, "expected all batch events recorded");

    // Reopen and verify partial-batch durability against the invariant.
    let engine = reopen_sled(&dir)?;
    let collection = engine.collection(&Album::collection()).await?;

    // Global invariant: nothing persisted references a missing event.
    assert_state_heads_resolvable(&collection, &entity_ids).await?;

    // Entities before the crash point are fully durable; from the crash point on
    // they are entirely absent (state AND event). The crash fired before
    // add_event #S2_CRASH_AT, so entity index i < S2_CRASH_AT is complete.
    for (i, id) in entity_ids.iter().enumerate() {
        let event_id = events[i].payload.id();
        if i < S2_CRASH_AT {
            assert!(has_persisted_state(&collection, *id).await?, "entity {i} committed before crash must have persisted state");
            assert!(event_present(&collection, event_id).await?, "entity {i} committed before crash must have its event");
        } else {
            assert!(!has_persisted_state(&collection, *id).await?, "entity {i} at/after crash must have NO persisted state");
            assert!(!event_present(&collection, event_id).await?, "entity {i} at/after crash must have NO event");
        }
    }

    // Reconvergence: reopen the node under test and re-deliver the whole batch,
    // exactly as the sending peer would on retry. Everything must converge.
    let node = Node::new_durable(Arc::new(engine), PermissiveAgent::new());
    // The system root persisted before the workload, so the reopened durable
    // node loads it and becomes ready on its own (no create/join). Awaiting a
    // collection drives the async catalog load first.
    let collection2 = node.system.collection(&Album::collection()).await?;
    node.system.wait_system_ready().await;
    assert!(node.system.is_system_ready(), "reopened durable node must load its persisted system root");
    redeliver(&node, events.clone()).await?;

    assert_state_heads_resolvable(&collection2, &entity_ids).await?;
    for (i, id) in entity_ids.iter().enumerate() {
        assert!(has_persisted_state(&collection2, *id).await?, "entity {i} must be present after re-delivery");
        assert!(event_present(&collection2, events[i].payload.id()).await?, "entity {i} event must be present after re-delivery");
    }

    cleanup(&dir);
    Ok(())
}

// ============================================================================
// SCENARIO 3: mid-merge crash (crash during a DivergedSince layered merge)
// ============================================================================

/// Child for scenario 3. Builds a genuine concurrent-branch merge on one node:
/// create A, commit B (parent A) so the head is {B}, then commit a concurrent C
/// (also parent A). Committing C locally triggers a DivergedSince merge of B and
/// C. The crash aborts just before the merged state is written (after C's event
/// is committed), so the persistence boundary sees the pre-merge state, never a
/// half-merged one.
///
/// PROBE: the archived hardening list (item 2) flags partial-layer application
/// atomicity. The layered merge applies all layers under a single in-memory lock
/// and then does exactly one `set_state`, so there is no storage operation
/// between layers to interrupt. This scenario asserts that property holds at the
/// persistence boundary: after a mid-merge crash the persisted head is exactly
/// the pre-merge head (resolvable), and C is at most an orphan event. If a
/// persisted state ever referenced a half-merged result, this assertion would
/// fail and the finding would be pinned red and filed.
#[tokio::test]
async fn child_mid_merge() -> Result<()> {
    let Some(crash) = child_crash_point() else {
        return Ok(());
    };
    // Build the pre-merge state with the crash hook NOT yet armed.
    let dir = child_sled_dir().expect("child must have a sled dir");
    let sled = Arc::new(SledStorageEngine::with_path(dir)?);
    let engine = Arc::new(CrashStorageEngine::new(sled, Some(crash)));
    let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(c)?;

    // A: create.
    let album = ctx.begin();
    let album = {
        let a = album.create(&Album { name: "Merge".to_owned(), year: "2000".to_owned() }).await?;
        let id = a.id();
        album.commit().await?;
        id
    };
    handoff_write("entity", &album.to_base64())?;

    // B: first edit (parent A). Head becomes {B}.
    let a_view = ctx.get::<AlbumView>(album).await?;
    let t1 = ctx.begin();
    a_view.edit(&t1)?.name().overwrite(0, 5, "Merge-B")?;
    t1.commit().await?;

    // Record the pre-merge head (should be {B}) so the parent can compare.
    let pre_head = ctx.get::<AlbumView>(album).await?.entity().head().to_vec();
    for id in &pre_head {
        handoff_write("pre_head", &id.to_base64())?;
    }

    // C: concurrent edit, also parented on A (started from the same base as B by
    // taking a fresh transaction on the pre-B view snapshot). Committing C merges
    // B and C. Arm the crash so the merged set_state aborts.
    let t2 = ctx.begin();
    a_view.edit(&t2)?.year().overwrite(0, 4, "2099")?;
    engine.arm();
    t2.commit().await?;

    panic!("scenario 3 child did not crash: merged set_state was not intercepted");
}

/// SCENARIO 3 INVARIANT (partial-merge atomicity at the persistence boundary):
/// after a crash during a DivergedSince merge, the persisted state is never a
/// half-merged result. The persisted head must be resolvable and equal to the
/// pre-merge head; the concurrent event may be present as an orphan but must not
/// be referenced by any persisted state.
#[tokio::test]
async fn scenario_3_mid_merge() -> Result<()> {
    let dir = fresh_sled_dir("s3");
    // The merge's set_state is the first armed set_state.
    let outcome = spawn_crash_child("scenarios::child_mid_merge", &dir, CrashPoint::BeforeSetState(0))?;
    assert!(outcome.crashed(), "child was expected to abort; stdout=\n{}\nstderr=\n{}", outcome.stdout, outcome.stderr);

    let entity_id = outcome.entity_id("entity").expect("child must record the entity id");
    let pre_head: std::collections::HashSet<_> = outcome.event_ids("pre_head").into_iter().collect();
    assert!(!pre_head.is_empty(), "child must record the pre-merge head");

    let engine = reopen_sled(&dir)?;
    let collection = engine.collection(&Album::collection()).await?;

    // Invariant 1: no persisted state references a missing event.
    assert_state_heads_resolvable(&collection, &[entity_id]).await?;

    // Invariant 2: the persisted head is exactly the pre-merge head. A partial or
    // full merge would change the head; the crash preceded persisting the merged
    // state, so the durable head must still be the pre-merge one. This is the
    // probe result: partial-layer application never reaches storage.
    let head = persisted_head(&collection, entity_id).await?.expect("entity state must be persisted (pre-merge state survived)");
    let head_set: std::collections::HashSet<_> = head.as_slice().iter().cloned().collect();
    assert_eq!(
        head_set, pre_head,
        "scenario 3: persisted head after a mid-merge crash must equal the pre-merge head, never a (partial) merge result"
    );

    cleanup(&dir);
    Ok(())
}

// ============================================================================
// SCENARIO 4: crash during entity creation (creation event committed, state not)
// ============================================================================

/// Child for scenario 4. Receives a single entity-creation event through the
/// ingest path and aborts just before its state is written, so the creation
/// event is committed but no state exists. The event is handed off for
/// re-delivery.
#[tokio::test]
async fn child_entity_creation() -> Result<()> {
    let Some(crash) = child_crash_point() else {
        return Ok(());
    };
    let (node, _engine) = armed_child_node(crash).await?;

    let events = generate_creation_batch(1).await?;
    handoff_write("entity", &events[0].payload.entity_id.to_base64())?;
    handoff_write_event("event", &events[0])?;

    node.commit_remote_transaction(&c, proto::TransactionId::new(), events).await?;

    panic!("scenario 4 child did not crash: creation set_state was not intercepted");
}

/// SCENARIO 4 INVARIANT: a crash after a creation event is committed but before
/// its state is written must never leave a state referencing a missing event.
/// After reopen the creation event may be an orphan and the entity may have no
/// state; re-delivering the same creation event (idempotently) then converges
/// the entity to a resolvable, queryable state. Reconvergence is also exercised
/// live over a real inter-node connection.
#[tokio::test]
async fn scenario_4_entity_creation() -> Result<()> {
    let dir = fresh_sled_dir("s4");
    let outcome = spawn_crash_child("scenarios::child_entity_creation", &dir, CrashPoint::BeforeSetState(0))?;
    assert!(outcome.crashed(), "child was expected to abort; stdout=\n{}\nstderr=\n{}", outcome.stdout, outcome.stderr);

    let entity_id = outcome.entity_id("entity").expect("child must record the entity id");
    let events = outcome.events("event");
    assert_eq!(events.len(), 1, "expected the creation event recorded");

    let engine = reopen_sled(&dir)?;
    let collection = engine.collection(&Album::collection()).await?;

    // Invariant: no persisted state references a missing event. The state write
    // never started, so there must be no persisted state for the entity.
    assert_state_heads_resolvable(&collection, &[entity_id]).await?;
    assert!(
        !has_persisted_state(&collection, entity_id).await?,
        "scenario 4: entity state must NOT be persisted when the crash preceded set_state"
    );

    // Reconvergence via re-delivery of the identical creation event.
    let node = Node::new_durable(Arc::new(engine), PermissiveAgent::new());
    let collection2 = node.system.collection(&Album::collection()).await?;
    node.system.wait_system_ready().await;
    assert!(node.system.is_system_ready(), "reopened durable node must be system-ready");
    redeliver(&node, events.clone()).await?;

    assert_state_heads_resolvable(&collection2, &[entity_id]).await?;
    assert!(has_persisted_state(&collection2, entity_id).await?, "entity must be present after re-delivery of the creation event");

    // Reconvergence over a live inter-node connection: a fresh ephemeral peer
    // connected to the recovered node must be able to fetch the entity, proving
    // the recovered node serves it correctly to peers.
    let peer = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let _conn = LocalProcessConnection::new(&peer, &node).await?;
    peer.system.wait_system_ready().await;
    let peer_ctx = peer.context(c)?;
    let query = format!("id = '{}'", entity_id.to_base64());
    let fetched = peer_ctx.fetch::<AlbumView>(query.as_str()).await?;
    assert_eq!(fetched.len(), 1, "fresh peer must fetch the recovered entity from the reopened node");

    cleanup(&dir);
    Ok(())
}

// ============================================================================
// HELPERS
// ============================================================================

fn cleanup(dir: &std::path::Path) { let _ = std::fs::remove_dir_all(dir); }
