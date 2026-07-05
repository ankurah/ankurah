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
use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;

use crate::harness::{
    assert_state_heads_resolvable, child_crash_point, child_sled_dir, fresh_sled_dir, handoff_write, has_persisted_state, reopen_sled,
    spawn_crash_child, CrashPoint, CrashStorageEngine,
};
use crate::models::Album;

/// Build a durable node on a real on-disk sled directory wrapped in the
/// crash-injecting engine, create the system, then arm the crash hook so
/// subsequent operation counts start from zero (bootstrap writes are excluded).
async fn armed_child_node(
    crash: CrashPoint,
) -> Result<(Node<CrashStorageEngine<SledStorageEngine>, PermissiveAgent>, Arc<CrashStorageEngine<SledStorageEngine>>)> {
    let dir = child_sled_dir().expect("child must have a sled dir");
    let sled = Arc::new(SledStorageEngine::with_path(dir)?);
    let engine = Arc::new(CrashStorageEngine::new(sled, Some(crash)));
    let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
    node.system.create().await?;
    // Bootstrap complete: from here on, operations are the workload under test.
    engine.arm();
    Ok((node, engine))
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
// HELPERS
// ============================================================================

fn cleanup(dir: &std::path::Path) { let _ = std::fs::remove_dir_all(dir); }
