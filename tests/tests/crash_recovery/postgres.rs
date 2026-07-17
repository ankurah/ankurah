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
//! so it aborts the child mid-write exactly as on sled; postgres autocommits
//! each statement, so writes completed before the crash are already durable on
//! the server (the flush hook is a no-op).
//!
//! These tests are gated out of the default `cargo test` because they need
//! Docker. Run with: `cargo test -p ankurah-tests --features postgres-crash`.

use std::sync::Arc;

use ankurah::proto::{self, Attested};
use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent};
use ankurah_storage_postgres::Postgres;
use anyhow::Result;
use testcontainers::ContainerAsync;
use testcontainers_modules::{postgres as pg_module, testcontainers::runners::AsyncRunner};

use crate::harness::{
    assert_state_heads_resolvable, child_crash_point, event_present, handoff_write, handoff_write_event, has_persisted_state,
    spawn_crash_child_with, CrashPoint, CrashStorageEngine,
};
use crate::models::Album;

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
    node.system.create().await?;
    // Register the scenario model as setup (mirrors scenarios.rs): the
    // registration executor's writes stay out of the armed crash-point
    // counts, and the child's catalog can resolve the batch's model id
    // (#330).
    node.context(c)?.register::<Album>().await?;
    engine.arm();
    Ok((node, engine))
}

/// Reopen a fresh durable node on the same postgres database (parent side).
async fn reopen_pg_node(uri: &str) -> Result<Node<Postgres, PermissiveAgent>> {
    let pg = Arc::new(Postgres::open(uri).await?);
    let node = Node::new_durable(pg, PermissiveAgent::new());
    // Drive the async catalog load, then wait for the persisted root.
    let _ = node.system.collection(&Album::collection()).await?;
    node.system.wait_system_ready().await;
    Ok(node)
}

/// Generate `n` independent album creation events on a throwaway in-memory sled
/// node (event generation is engine-independent).
async fn generate_creation_batch(n: usize, model: proto::EntityId) -> Result<Vec<Attested<proto::Event>>> {
    use ankurah_storage_sled::SledStorageEngine;
    let helper = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    helper.system.create().await?;
    let ctx = helper.context(c)?;
    let trx = ctx.begin();
    for i in 0..n {
        trx.create(&Album { name: format!("PgBatch {i}"), year: format!("20{i:02}") }).await?;
    }
    let events = trx.commit_and_return_events().await?;
    // Rewrite the model id to the target node's (#330); see scenarios.rs.
    Ok(events
        .into_iter()
        .map(|mut event| {
            event.model = model;
            Attested::from(event)
        })
        .collect())
}

// ============================================================================
// SCENARIO 1 (postgres): crash after commit_event, before set_state
// ============================================================================

#[tokio::test]
async fn child_pg_commit_event_before_set_state() -> Result<()> {
    let Some(crash) = child_crash_point() else {
        return Ok(());
    };
    let (node, _engine) = armed_child_pg_node(crash).await?;
    let model = node.catalog.model_id_for(Album::collection().as_str()).expect("Album registered in armed_child_pg_node");
    let events = generate_creation_batch(1, model).await?;
    handoff_write("entity", &events[0].payload.entity_id.to_base64())?;
    node.commit_remote_transaction(&c, proto::TransactionId::new(), events).await?;
    panic!("pg scenario 1 child did not crash");
}

/// SCENARIO 1 (postgres) INVARIANT: identical to the sled arm. A crash after the
/// creation event is committed but before its state is written must leave no
/// persisted state referencing a missing event; here the durable state lives on
/// the surviving postgres server, which the parent reopens.
#[tokio::test]
async fn scenario_pg_1_commit_event_before_set_state() -> Result<()> {
    let fixture = start_postgres().await?;
    let outcome = spawn_crash_child_with(
        "postgres::child_pg_commit_event_before_set_state",
        CrashPoint::BeforeSetState(0),
        &[(ENV_PG_URI, &fixture.uri)],
    )?;
    assert!(outcome.crashed(), "child was expected to abort; stdout=\n{}\nstderr=\n{}", outcome.stdout, outcome.stderr);

    let entity_id = outcome.entity_id("entity").expect("child must record the entity id");

    let node = reopen_pg_node(&fixture.uri).await?;
    let collection = node.system.collection(&Album::collection()).await?;
    assert_state_heads_resolvable(&collection, &[entity_id]).await?;
    assert!(
        !has_persisted_state(&collection, entity_id).await?,
        "pg scenario 1: state must not be persisted when the crash preceded set_state"
    );
    Ok(())
}

// ============================================================================
// SCENARIO 2 (postgres): mid-batch crash
// ============================================================================

const PG_S2_BATCH: usize = 3;
const PG_S2_CRASH_AT: usize = 2;

#[tokio::test]
async fn child_pg_mid_batch() -> Result<()> {
    let Some(crash) = child_crash_point() else {
        return Ok(());
    };
    let (node, _engine) = armed_child_pg_node(crash).await?;
    let model = node.catalog.model_id_for(Album::collection().as_str()).expect("Album registered in armed_child_pg_node");
    let events = generate_creation_batch(PG_S2_BATCH, model).await?;
    for e in &events {
        handoff_write("entity", &e.payload.entity_id.to_base64())?;
        handoff_write_event("event", e)?;
    }
    node.commit_remote_transaction(&c, proto::TransactionId::new(), events).await?;
    panic!("pg scenario 2 child did not crash");
}

/// SCENARIO 2 (postgres) INVARIANT: a crash partway through a received batch
/// leaves the already-committed entities durable on the server and the rest
/// absent, with no state referencing an uncommitted event. Re-delivering the
/// full batch to a reopened node converges it.
#[tokio::test]
async fn scenario_pg_2_mid_batch() -> Result<()> {
    let fixture = start_postgres().await?;
    let outcome =
        spawn_crash_child_with("postgres::child_pg_mid_batch", CrashPoint::BeforeAddEvent(PG_S2_CRASH_AT), &[(ENV_PG_URI, &fixture.uri)])?;
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
    let collection = node.system.collection(&Album::collection()).await?;
    assert_state_heads_resolvable(&collection, &entity_ids).await?;
    for (i, id) in entity_ids.iter().enumerate() {
        let event_id = events[i].payload.id();
        if i < PG_S2_CRASH_AT {
            assert!(has_persisted_state(&collection, *id).await?, "pg entity {i} before crash must have state");
            assert!(event_present(&collection, event_id).await?, "pg entity {i} before crash must have its event");
        } else {
            assert!(!has_persisted_state(&collection, *id).await?, "pg entity {i} at/after crash must have no state");
            assert!(!event_present(&collection, event_id).await?, "pg entity {i} at/after crash must have no event");
        }
    }

    // Reconvergence: re-deliver the full batch on the reopened node.
    node.commit_remote_transaction(&c, proto::TransactionId::new(), events.clone()).await?;
    let collection = node.system.collection(&Album::collection()).await?;
    assert_state_heads_resolvable(&collection, &entity_ids).await?;
    for id in &entity_ids {
        assert!(has_persisted_state(&collection, *id).await?, "pg entity must be present after re-delivery");
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
    let model = node.catalog.model_id_for(Album::collection().as_str()).expect("Album registered in armed_child_pg_node");
    let events = generate_creation_batch(1, model).await?;
    handoff_write("entity", &events[0].payload.entity_id.to_base64())?;
    handoff_write_event("event", &events[0])?;
    node.commit_remote_transaction(&c, proto::TransactionId::new(), events).await?;
    panic!("pg scenario 4 child did not crash");
}

/// SCENARIO 4 (postgres) INVARIANT: a crash after a creation event is committed
/// but before its state is written leaves no state referencing the missing
/// event; re-delivering the identical creation event converges the entity to a
/// resolvable state on the reopened node.
#[tokio::test]
async fn scenario_pg_4_entity_creation() -> Result<()> {
    let fixture = start_postgres().await?;
    let outcome =
        spawn_crash_child_with("postgres::child_pg_entity_creation", CrashPoint::BeforeSetState(0), &[(ENV_PG_URI, &fixture.uri)])?;
    assert!(outcome.crashed(), "child was expected to abort; stdout=\n{}\nstderr=\n{}", outcome.stdout, outcome.stderr);

    let entity_id = outcome.entity_id("entity").expect("child must record the entity id");
    let events = outcome.events("event");
    assert_eq!(events.len(), 1);

    let node = reopen_pg_node(&fixture.uri).await?;
    let collection = node.system.collection(&Album::collection()).await?;
    assert_state_heads_resolvable(&collection, &[entity_id]).await?;
    assert!(
        !has_persisted_state(&collection, entity_id).await?,
        "pg scenario 4: state must not be persisted when the crash preceded set_state"
    );

    // Reconvergence via re-delivery of the identical creation event.
    node.commit_remote_transaction(&c, proto::TransactionId::new(), events.clone()).await?;
    let collection = node.system.collection(&Album::collection()).await?;
    assert_state_heads_resolvable(&collection, &[entity_id]).await?;
    assert!(has_persisted_state(&collection, entity_id).await?, "pg entity must be present after re-delivery");
    Ok(())
}
