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

use ankurah::core::test_helpers::commit_transaction;
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

/// The deterministic Album catalog shared by every crash-test node.
pub(super) fn album_forge() -> &'static ankurah_tests::catalog_forge::ForgedCatalog {
    static FORGED: std::sync::OnceLock<ankurah_tests::catalog_forge::ForgedCatalog> = std::sync::OnceLock::new();
    FORGED.get_or_init(|| {
        ankurah_tests::catalog_forge::forge_catalog(
            Album::descriptor().label,
            "Album",
            &[("name", "yrs", "string"), ("year", "yrs", "string")],
            b"crash-album",
        )
    })
}

/// Bind `node`'s compiled Album declaration to the forged identities.
pub(super) fn seed_album_catalog<SE: StorageEngine + Send + Sync + 'static>(node: &Node<SE, PermissiveAgent>) -> Result<()> {
    let forged = album_forge();
    let model = proto::RegisteredModel {
        id: forged.model,
        label: Album::descriptor().label.to_owned(),
        name: "Album".to_owned(),
        properties: ["name", "year"]
            .iter()
            .enumerate()
            .map(|(i, field)| proto::RegisteredProperty {
                build_id: Album::descriptor().field_by_name(field).expect("Album declares this field").build_id,
                id: forged.properties[i],
                membership_id: forged.memberships[i],
                name: (*field).to_owned(),
                backend: "yrs".to_owned(),
                value_type: "string".to_owned(),
                target_model: None,
                minted_for: Some(forged.model),
                optional: false,
            })
            .collect(),
    };
    ankurah::core::test_helpers::seed_registered_schema(node, Album::descriptor(), &model)?;
    Ok(())
}

async fn armed_child_node(crash: CrashPoint) -> Result<(CrashNode, Arc<CrashStorageEngine<SledStorageEngine>>)> {
    let dir = child_sled_dir().expect("child must have a sled dir");
    let sled = Arc::new(SledStorageEngine::with_path(dir)?);
    // The forged catalog rows go in BEFORE the node exists (its projection
    // reads them at startup) and before the crash hook is armed: bootstrap,
    // not workload.
    ankurah_tests::catalog_forge::plant(&*sled, album_forge()).await?;
    let engine = Arc::new(CrashStorageEngine::new(sled, Some(crash)));
    let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
    node.system.create().await?;
    seed_album_catalog(&node)?;
    node.wait_ready().await?;
    // Bootstrap complete: from here on, operations are the workload under test.
    engine.arm();
    Ok((node, engine))
}

/// Generate `n` independent album creation events using a throwaway in-memory
/// durable node. Each album is a distinct entity, so each event is a creation
/// event (empty parent). Returned in commit order as attested events, ready to
/// feed to `commit_transaction` on the node under test.
pub(super) async fn generate_creation_batch(n: usize) -> Result<Vec<Attested<proto::Event>>> {
    let helper_engine = Arc::new(SledStorageEngine::new_test()?);
    ankurah_tests::catalog_forge::plant(&*helper_engine, album_forge()).await?;
    let helper = Node::new_durable(helper_engine, PermissiveAgent::new());
    helper.system.create().await?;
    seed_album_catalog(&helper)?;
    helper.wait_ready().await?;
    let ctx = helper.context(c)?;
    let trx = ctx.begin();
    for i in 0..n {
        trx.create(&Album { name: format!("Batch {i}"), year: format!("20{i:02}") }).await?;
    }
    let events = trx.commit_and_return_events().await?;
    Ok(events.into_iter().map(Attested::from).collect())
}

/// Re-deliver a set of attested events to a node through the real ingest path
/// (`commit_transaction`, the same entry the relay uses). Models "the
/// peer that sent the batch re-sends it" for the reconvergence invariant.
/// Generic over the storage engine so it works on both the crash-wrapped child
/// node and the plain reopened node.
async fn redeliver<SE>(node: &Node<SE, PermissiveAgent>, events: Vec<Attested<proto::Event>>) -> Result<()>
where SE: StorageEngine + Send + Sync + 'static {
    commit_transaction(&node, &c, proto::TransactionId::new(), events).await?;
    Ok(())
}

// ============================================================================
// SCENARIO 1: crash before the atomic commit
// ============================================================================

/// Child for scenario 1. Creates one album and commits. The crash hook aborts
/// just before the first `commit`, so neither state nor events are written.
#[tokio::test]
async fn child_atomic_commit() -> Result<()> {
    let Some(crash) = child_crash_point() else {
        return Ok(()); // inert when run normally in the parent binary
    };
    let (node, _engine) = armed_child_node(crash).await?;
    let ctx = node.context(c)?;

    let trx = ctx.begin();
    let album = trx.create(&Album { name: "Crash One".to_owned(), year: "2001".to_owned() }).await?;
    // Durably record the id so the parent can address the entity after reopen.
    handoff_write("entity", &album.id().to_base64())?;

    // The crash fires before the atomic event/state batch; commit() never returns.
    trx.commit().await?;

    // Unreachable: the crash hook must have aborted during commit.
    panic!("scenario 1 child did not crash: commit was not intercepted");
}

/// A crash before the atomic batch leaves neither entity state nor its events.
#[tokio::test]
async fn scenario_1_atomic_commit() -> Result<()> {
    let dir = fresh_sled_dir("s1");
    let outcome = spawn_crash_child("scenarios::child_atomic_commit", &dir, CrashPoint::BeforeCommit(0))?;
    assert!(outcome.crashed(), "child was expected to abort; stdout=\n{}\nstderr=\n{}", outcome.stdout, outcome.stderr);

    let entity_id = outcome.entity_id("entity").expect("child must record the entity id before crashing");

    // Reopen the surviving sled directory through the production opener.
    let engine = reopen_sled(&dir)?;

    // The core invariant: no persisted state references a missing event.
    assert_state_heads_resolvable(&engine, &[entity_id]).await?;

    // For this specific window, the state write never started, so there must be
    // no persisted state for the album at all.
    assert!(
        !has_persisted_state(&engine, entity_id).await?,
        "scenario 1: album state must NOT be persisted when the crash preceded commit"
    );

    assert!(engine.dump_entity_events(entity_id).await?.is_empty(), "a failed commit must not leave its events");
    cleanup(&dir);
    Ok(())
}

// ============================================================================
// SCENARIO 2: atomic batch committed, acknowledgement lost
// ============================================================================

/// Several entities must survive together when the commit acknowledgement is lost.
const S2_BATCH: usize = 3;

/// Child for scenario 2. Receives a multi-entity transaction through the real
/// ingest path. The crash fires after storage commits, before the caller gets
/// success. The parent retries the full batch after reopening.
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

    commit_transaction(&node, &c, proto::TransactionId::new(), events).await?;

    panic!("scenario 2 child did not crash after committing the batch");
}

/// The committed batch survives in full even when its acknowledgement is lost;
/// redelivery must be idempotent.
///
/// That re-delivery is also the receiving half of event identity's retry
/// property: the identical minted events arriving a second time converge on the
/// same entities rather than duplicating them. The sending half -- a retry
/// carries the genesis `create()` minted, not a fresh mint under a fresh nonce
/// -- is pinned by `a_relayed_genesis_is_the_one_create_minted` in
/// tests/tests/event_identity.rs.
#[tokio::test]
async fn scenario_2_mid_batch() -> Result<()> {
    let dir = fresh_sled_dir("s2");
    let outcome = spawn_crash_child("scenarios::child_mid_batch", &dir, CrashPoint::AfterCommit(0))?;
    assert!(outcome.crashed(), "child was expected to abort; stdout=\n{}\nstderr=\n{}", outcome.stdout, outcome.stderr);

    let entity_ids =
        outcome.handoff.get("entity").map(|v| v.iter().filter_map(|s| ankurah::EntityId::from_base64(s).ok()).collect::<Vec<_>>());
    let entity_ids = entity_ids.expect("child must record entity ids");
    assert_eq!(entity_ids.len(), S2_BATCH, "expected all batch entity ids recorded");
    let events = outcome.events("event");
    assert_eq!(events.len(), S2_BATCH, "expected all batch events recorded");

    // Reopen and verify that the complete batch survived.
    let engine = reopen_sled(&dir)?;

    // Global invariant: nothing persisted references a missing event.
    assert_state_heads_resolvable(&engine, &entity_ids).await?;

    // Every entity and its event committed before the lost acknowledgement.
    for (i, id) in entity_ids.iter().enumerate() {
        let event_id = events[i].payload.id();
        assert!(has_persisted_state(&engine, *id).await?, "entity {i} committed before crash must have persisted state");
        assert!(event_present(&engine, event_id).await?, "entity {i} committed before crash must have its event");
    }

    // Reconvergence: reopen the node under test and re-deliver the whole batch,
    // exactly as the sending peer would on retry. Everything must converge.
    let node = Node::new_durable(Arc::new(engine), PermissiveAgent::new());
    // The system root persisted before the workload, so the reopened durable
    // node loads it and becomes ready on its own (no create/join). Seeding
    // must wait for that readiness: the seeded binding resolves under the
    // node's system epoch, which exists only once the system is ready.
    node.system.wait_system_ready().await.unwrap();
    assert!(node.system.is_system_ready(), "reopened durable node must load its persisted system root");
    seed_album_catalog(&node)?;
    // The gate on redelivered events consults the catalog, which the
    // reopened projection refills from the planted rows: wait for its
    // first read.
    node.wait_ready().await?;
    redeliver(&node, events.clone()).await?;

    assert_state_heads_resolvable(node.storage.as_ref(), &entity_ids).await?;
    for (i, id) in entity_ids.iter().enumerate() {
        assert!(has_persisted_state(node.storage.as_ref(), *id).await?, "entity {i} must be present after re-delivery");
        assert!(event_present(node.storage.as_ref(), events[i].payload.id()).await?, "entity {i} event must be present after re-delivery");
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
/// C. The crash aborts before C's event and merged state commit, so
/// the persistence boundary sees the pre-merge state, never a
/// half-merged one.
///
/// PROBE: the archived hardening list (item 2) flags partial-layer application
/// atomicity. The layered merge applies all layers under a single in-memory lock
/// and then does exactly one `commit`, so there is no storage operation
/// between layers to interrupt. This scenario asserts that property holds at the
/// persistence boundary: after a mid-merge crash the persisted head is exactly
/// the pre-merge head (resolvable), with C's event also absent. If a
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
    ankurah_tests::catalog_forge::plant(&*sled, album_forge()).await?;
    let engine = Arc::new(CrashStorageEngine::new(sled, Some(crash)));
    let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
    node.system.create().await?;
    seed_album_catalog(&node)?;
    node.wait_ready().await?;
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
    a_view.edit(&t1)?.name()?.overwrite(0, 5, "Merge-B")?;
    t1.commit().await?;

    // Record the pre-merge head (should be {B}) so the parent can compare.
    let pre_head = ctx.get::<AlbumView>(album).await?.entity().head().to_vec();
    for id in &pre_head {
        handoff_write("pre_head", &id.to_base64())?;
    }

    // C: concurrent edit, also parented on A (started from the same base as B by
    // taking a fresh transaction on the pre-B view snapshot). Committing C merges
    // B and C. Arm the crash so the merged commit aborts.
    let t2 = ctx.begin();
    a_view.edit(&t2)?.year()?.overwrite(0, 4, "2099")?;
    engine.arm();
    t2.commit().await?;

    panic!("scenario 3 child did not crash: merged commit was not intercepted");
}

/// SCENARIO 3 INVARIANT (partial-merge atomicity at the persistence boundary):
/// after a crash during a DivergedSince merge, the persisted state is never a
/// half-merged result. The persisted head must be resolvable and equal to the
/// pre-merge head; the uncommitted concurrent event must not
/// be referenced by any persisted state.
#[tokio::test]
async fn scenario_3_mid_merge() -> Result<()> {
    let dir = fresh_sled_dir("s3");
    // The merge's commit is the first armed commit.
    let outcome = spawn_crash_child("scenarios::child_mid_merge", &dir, CrashPoint::BeforeCommit(0))?;
    assert!(outcome.crashed(), "child was expected to abort; stdout=\n{}\nstderr=\n{}", outcome.stdout, outcome.stderr);

    let entity_id = outcome.entity_id("entity").expect("child must record the entity id");
    let pre_head: std::collections::HashSet<_> = outcome.event_ids("pre_head").into_iter().collect();
    assert!(!pre_head.is_empty(), "child must record the pre-merge head");

    let engine = reopen_sled(&dir)?;

    // Invariant 1: no persisted state references a missing event.
    assert_state_heads_resolvable(&engine, &[entity_id]).await?;

    // Invariant 2: the persisted head is exactly the pre-merge head. A partial or
    // full merge would change the head; the crash preceded persisting the merged
    // state, so the durable head must still be the pre-merge one. This is the
    // probe result: partial-layer application never reaches storage.
    let head = persisted_head(&engine, entity_id).await?.expect("entity state must be persisted (pre-merge state survived)");
    let head_set: std::collections::HashSet<_> = head.as_slice().iter().cloned().collect();
    assert_eq!(
        head_set, pre_head,
        "scenario 3: persisted head after a mid-merge crash must equal the pre-merge head, never a (partial) merge result"
    );

    cleanup(&dir);
    Ok(())
}

// ============================================================================
// SCENARIO 4: creation rolls back atomically and succeeds on re-delivery
// ============================================================================

/// Abort before the creation batch commits; hand the event to the parent for re-delivery.
#[tokio::test]
async fn child_entity_creation() -> Result<()> {
    let Some(crash) = child_crash_point() else {
        return Ok(());
    };
    let (node, _engine) = armed_child_node(crash).await?;

    let events = generate_creation_batch(1).await?;
    handoff_write("entity", &events[0].payload.entity_id.to_base64())?;
    handoff_write_event("event", &events[0])?;

    commit_transaction(&node, &c, proto::TransactionId::new(), events).await?;

    panic!("scenario 4 child did not crash: creation commit was not intercepted");
}

/// Neither the event nor state survives a pre-commit crash. Re-delivery creates
/// a queryable entity, also verified through a real inter-node connection.
#[tokio::test]
async fn scenario_4_entity_creation() -> Result<()> {
    let dir = fresh_sled_dir("s4");
    let outcome = spawn_crash_child("scenarios::child_entity_creation", &dir, CrashPoint::BeforeCommit(0))?;
    assert!(outcome.crashed(), "child was expected to abort; stdout=\n{}\nstderr=\n{}", outcome.stdout, outcome.stderr);

    let entity_id = outcome.entity_id("entity").expect("child must record the entity id");
    let events = outcome.events("event");
    assert_eq!(events.len(), 1, "expected the creation event recorded");

    let engine = reopen_sled(&dir)?;

    // Invariant: no persisted state references a missing event. The state write
    // never started, so there must be no persisted state for the entity.
    assert_state_heads_resolvable(&engine, &[entity_id]).await?;
    assert!(
        !has_persisted_state(&engine, entity_id).await?,
        "scenario 4: entity state must NOT be persisted when the crash preceded commit"
    );

    // Reconvergence via re-delivery of the identical creation event.
    let node = Node::new_durable(Arc::new(engine), PermissiveAgent::new());
    // Seeding resolves the compiled binding under the node's system epoch,
    // which exists only once the reopened system is ready.
    node.system.wait_system_ready().await.unwrap();
    assert!(node.system.is_system_ready(), "reopened durable node must be system-ready");
    seed_album_catalog(&node)?;
    // The gate on redelivered events consults the catalog, which the
    // reopened projection refills from the planted rows: wait for its
    // first read.
    node.wait_ready().await?;
    redeliver(&node, events.clone()).await?;

    assert_state_heads_resolvable(node.storage.as_ref(), &[entity_id]).await?;
    assert!(has_persisted_state(node.storage.as_ref(), entity_id).await?, "entity must be present after re-delivery of the creation event");

    // Reconvergence over a live inter-node connection: a fresh ephemeral peer
    // connected to the recovered node must be able to fetch the entity, proving
    // the recovered node serves it correctly to peers.
    let peer = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let _conn = LocalProcessConnection::new(&peer, &node).await?;
    peer.system.wait_system_ready().await.unwrap();
    // Bind the peer to the same forged identities used by the child.
    seed_album_catalog(&peer)?;
    let peer_ctx = peer.context_async(c).await?;
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
