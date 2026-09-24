//! Postgres crash-recovery arm (feature `postgres-crash`, requires Docker).
//!
//! Same deterministic kill-and-reopen model as the sled scenarios, but the
//! durability boundary is a postgres server rather than a local sled directory.
//! The distinction the mandate calls out: on a crash the NODE process dies while
//! the SERVER survives, so recovery is "reconnect a fresh node to the same
//! database and check the invariants".
//!
//! The parent test starts a postgres container, hands the connection URI to the
//! crash child through an env var, and keeps the container alive across the
//! child's death and the parent's reopen. The crash wrapper is engine-generic,
//! so it aborts the child at the batch boundary; completed transactions are durable on
//! the server (the flush hook is a no-op).
//!
//! These tests are gated out of the default `cargo test` because they need
//! Docker. Run with: `cargo test -p ankurah-tests --features postgres-crash`.
use ankurah::core::test_helpers::commit_transaction;

use std::sync::Arc;

use super::scenarios::{album_forge, generate_creation_batch, seed_album_catalog};
use ankurah::proto;
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
use ankurah_storage_postgres::Postgres;
use anyhow::Result;
use testcontainers::ContainerAsync;
use testcontainers_modules::{postgres as pg_module, testcontainers::runners::AsyncRunner};

use crate::harness::{
    assert_state_heads_resolvable, child_crash_point, event_present, handoff_write, handoff_write_event, has_persisted_state,
    spawn_crash_child_with, CrashPoint, CrashStorageEngine,
};

/// Env var carrying the postgres connection URI from parent to crash child.
const ENV_PG_URI: &str = "ANKURAH_C6_PG_URI";

/// Extensions the ankurah postgres engine expects. Mirrors the postgres crate's
/// test init SQL (kept inline so this crate does not reach into that crate's
/// test tree).
const PG_INIT_SQL: &str =
    "CREATE EXTENSION IF NOT EXISTS hstore; CREATE EXTENSION IF NOT EXISTS citext; CREATE EXTENSION IF NOT EXISTS ltree;";

/// A running postgres container plus the URI to reach it. Held by the parent for
/// the whole scenario so the server outlives the crash child.
struct PgFixture {
    _container: ContainerAsync<pg_module::Postgres>,
    uri: String,
}

/// Start a fresh postgres container with the ankurah extensions installed.
async fn start_postgres() -> Result<PgFixture> {
    let container = pg_module::Postgres::default()
        .with_db_name("ankurah")
        .with_user("postgres")
        .with_password("postgres")
        .with_init_sql(PG_INIT_SQL.to_string().into_bytes())
        .start()
        .await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let uri = format!("host={host} port={port} user=postgres password=postgres dbname=ankurah");
    Ok(PgFixture { _container: container, uri })
}

type PgCrashEngine = CrashStorageEngine<Postgres>;

/// Build a durable node on the postgres database named by `ENV_PG_URI`, wrapped
/// in the crash engine, create the system, then arm the crash hook.
async fn armed_child_pg_node(crash: CrashPoint) -> Result<(Node<PgCrashEngine, PermissiveAgent>, Arc<PgCrashEngine>)> {
    let uri = std::env::var(ENV_PG_URI).map_err(|_| anyhow::anyhow!("child missing postgres uri"))?;
    let pg = Arc::new(Postgres::open(&uri).await?);
    let engine = Arc::new(CrashStorageEngine::new(pg, Some(crash)));
    let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
    ankurah_tests::catalog_forge::plant(engine.as_ref(), album_forge()).await?;
    node.system.create().await?;
    seed_album_catalog(&node)?;
    node.wait_ready().await?;
    engine.arm();
    Ok((node, engine))
}

/// Reopen a fresh durable node on the same postgres database (parent side).
async fn reopen_pg_node(uri: &str) -> Result<Node<Postgres, PermissiveAgent>> {
    let pg = Arc::new(Postgres::open(uri).await?);
    let node = Node::new_durable(pg, PermissiveAgent::new());
    node.wait_ready().await?;
    seed_album_catalog(&node)?;
    Ok(node)
}

// ============================================================================
// SCENARIO 1 (postgres): crash before the atomic commit
// ============================================================================

#[tokio::test]
async fn child_pg_atomic_commit() -> Result<()> {
    let Some(crash) = child_crash_point() else {
        return Ok(());
    };
    let (node, _engine) = armed_child_pg_node(crash).await?;
    let events = generate_creation_batch(1).await?;
    handoff_write("entity", &events[0].payload.entity_id.to_base64())?;
    commit_transaction(&node, &c, proto::TransactionId::new(), events).await?;
    panic!("pg scenario 1 child did not crash");
}

/// A crash before commit leaves neither state nor events on the surviving server.
#[tokio::test]
async fn scenario_pg_1_atomic_commit() -> Result<()> {
    let fixture = start_postgres().await?;
    let outcome =
        spawn_crash_child_with("postgres::child_pg_atomic_commit", CrashPoint::BeforeCommit(0), &[(ENV_PG_URI, &fixture.uri)])?;
    assert!(outcome.crashed(), "child was expected to abort; stdout=\n{}\nstderr=\n{}", outcome.stdout, outcome.stderr);

    let entity_id = outcome.entity_id("entity").expect("child must record the entity id");

    let node = reopen_pg_node(&fixture.uri).await?;
    assert_state_heads_resolvable(node.storage.as_ref(), &[entity_id]).await?;
    assert!(
        !has_persisted_state(node.storage.as_ref(), entity_id).await?,
        "pg scenario 1: state must not be persisted when the crash preceded commit"
    );
    Ok(())
}

// ============================================================================
// SCENARIO 2 (postgres): atomic batch committed, acknowledgement lost
// ============================================================================

const PG_S2_BATCH: usize = 3;

#[tokio::test]
async fn child_pg_mid_batch() -> Result<()> {
    let Some(crash) = child_crash_point() else {
        return Ok(());
    };
    let (node, _engine) = armed_child_pg_node(crash).await?;
    let events = generate_creation_batch(PG_S2_BATCH).await?;
    for e in &events {
        handoff_write("entity", &e.payload.entity_id.to_base64())?;
        handoff_write_event("event", e)?;
    }
    commit_transaction(&node, &c, proto::TransactionId::new(), events).await?;
    panic!("pg scenario 2 child did not crash");
}

/// The complete batch survives a lost acknowledgement; redelivery is idempotent.
#[tokio::test]
async fn scenario_pg_2_mid_batch() -> Result<()> {
    let fixture = start_postgres().await?;
    let outcome = spawn_crash_child_with("postgres::child_pg_mid_batch", CrashPoint::AfterCommit(0), &[(ENV_PG_URI, &fixture.uri)])?;
    assert!(outcome.crashed(), "child was expected to abort; stdout=\n{}\nstderr=\n{}", outcome.stdout, outcome.stderr);

    let entity_ids: Vec<_> = outcome
        .handoff
        .get("entity")
        .map(|v| v.iter().filter_map(|s| ankurah::EntityId::from_base64(s).ok()).collect())
        .unwrap_or_default();
    assert_eq!(entity_ids.len(), PG_S2_BATCH);
    let events = outcome.events("event");
    assert_eq!(events.len(), PG_S2_BATCH);

    let node = reopen_pg_node(&fixture.uri).await?;
    assert_state_heads_resolvable(node.storage.as_ref(), &entity_ids).await?;
    for (i, id) in entity_ids.iter().enumerate() {
        let event_id = events[i].payload.id();
        assert!(has_persisted_state(node.storage.as_ref(), *id).await?, "pg entity {i} before crash must have state");
        assert!(event_present(node.storage.as_ref(), event_id).await?, "pg entity {i} before crash must have its event");
    }

    // Reconvergence: re-deliver the full batch on the reopened node.
    commit_transaction(&node, &c, proto::TransactionId::new(), events.clone()).await?;
    assert_state_heads_resolvable(node.storage.as_ref(), &entity_ids).await?;
    for id in &entity_ids {
        assert!(has_persisted_state(node.storage.as_ref(), *id).await?, "pg entity must be present after re-delivery");
    }
    Ok(())
}

// ============================================================================
// SCENARIO 4 (postgres): crash during entity creation
// ============================================================================

#[tokio::test]
async fn child_pg_entity_creation() -> Result<()> {
    let Some(crash) = child_crash_point() else {
        return Ok(());
    };
    let (node, _engine) = armed_child_pg_node(crash).await?;
    let events = generate_creation_batch(1).await?;
    handoff_write("entity", &events[0].payload.entity_id.to_base64())?;
    handoff_write_event("event", &events[0])?;
    commit_transaction(&node, &c, proto::TransactionId::new(), events).await?;
    panic!("pg scenario 4 child did not crash");
}

/// Retrying a creation whose transaction never committed persists it in full.
#[tokio::test]
async fn scenario_pg_4_entity_creation() -> Result<()> {
    let fixture = start_postgres().await?;
    let outcome =
        spawn_crash_child_with("postgres::child_pg_entity_creation", CrashPoint::BeforeCommit(0), &[(ENV_PG_URI, &fixture.uri)])?;
    assert!(outcome.crashed(), "child was expected to abort; stdout=\n{}\nstderr=\n{}", outcome.stdout, outcome.stderr);

    let entity_id = outcome.entity_id("entity").expect("child must record the entity id");
    let events = outcome.events("event");
    assert_eq!(events.len(), 1);

    let node = reopen_pg_node(&fixture.uri).await?;
    assert_state_heads_resolvable(node.storage.as_ref(), &[entity_id]).await?;
    assert!(
        !has_persisted_state(node.storage.as_ref(), entity_id).await?,
        "pg scenario 4: state must not be persisted when the crash preceded commit"
    );

    // Reconvergence via re-delivery of the identical creation event.
    commit_transaction(&node, &c, proto::TransactionId::new(), events.clone()).await?;
    assert_state_heads_resolvable(node.storage.as_ref(), &[entity_id]).await?;
    assert!(has_persisted_state(node.storage.as_ref(), entity_id).await?, "pg entity must be present after re-delivery");
    Ok(())
}
