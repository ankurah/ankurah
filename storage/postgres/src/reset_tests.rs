//! Cancellation after PostgreSQL accepts work must not let that work cross reset.

use ankurah_core::property::backend::{LWWBackend, PropertyBackend};
use ankurah_core::value::Value;
use ankurah_core::{
    storage::MapId,
    test_helpers::map_conformance::fixtures::{entity, HANG},
};
use ankurah_proto::{PropertyId, SystemProperty};
use tokio::time::timeout;
use tokio_postgres::Client;

use super::*;
use crate::test_server::Server;

async fn waiters(client: &Client, count: i64) -> anyhow::Result<()> {
    timeout(HANG, async {
        loop {
            let found: i64 = client
                .query_one("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'", &[])
                .await?
                .get(0);
            if found == count {
                return anyhow::Ok(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await?
}

#[tokio::test]
async fn cancelled_native_commit_finishes_before_reset() -> anyhow::Result<()> {
    for with_maps in [false, true] {
        let server = Server::start().await?;
        let pool = server.pool().await?;
        let engine = Postgres::new(pool.clone()).await?;
        let mut inspector = pool.dedicated_connection().await?;
        engine.ensure_shared_tables(&mut inspector).await?;
        inspector
            .batch_execute(
                r#"CREATE FUNCTION gate_commit() RETURNS trigger LANGUAGE plpgsql AS $$
                 BEGIN PERFORM pg_advisory_xact_lock(987654321::bigint); RETURN NEW; END $$;
               CREATE CONSTRAINT TRIGGER gate_commit AFTER INSERT ON "_ankurah_entity"
                 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION gate_commit()"#,
            )
            .await?;
        let gate = pool.dedicated_connection().await?;
        gate.batch_execute("BEGIN; SELECT pg_advisory_xact_lock(987654321::bigint)").await?;

        let (state, event) = entity!(1);
        let mut transaction = engine.transaction();
        transaction.set_state(&Clock::default(), &state).await?;
        transaction.add_events(&[event]).await?;
        if with_maps {
            transaction.map_write(vec![MapWrite::Put { map: MapId::new("reset"), key: vec![1], value: vec![1] }]).await?;
        }
        let mut committing = Box::pin(transaction.commit());
        tokio::select! {
            result = &mut committing => panic!("commit passed the native gate: {result:?}"),
            result = waiters(&inspector, 1) => result?,
        }
        let mut resetting = Box::pin(engine.delete_all());
        tokio::select! {
            result = &mut resetting => panic!("reset passed the unfinished native commit: {result:?}"),
            result = waiters(&inspector, 2) => result?,
        }
        drop(committing);
        // A map lease may cancel the server request; an ordinary COMMIT may
        // finish. Reset must cover either outcome after its caller disappears.
        gate.batch_execute("COMMIT").await?;
        timeout(HANG, resetting).await??;
        let rows: i64 = inspector.query_one(r#"SELECT count(*) FROM "_ankurah_entity""#, &[]).await?.get(0);
        let maps: i64 = inspector.query_one(r#"SELECT count(*) FROM "_ankurah_map""#, &[]).await?.get(0);
        assert_eq!((rows, maps), (0, 0));
        let events: Option<String> = inspector.query_one("SELECT to_regclass('_ankurah_event')::text", &[]).await?.get(0);
        assert!(events.is_none(), "reset removed the event table after the native commit ended");
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_preparation_releases_its_native_locks_before_reset() -> anyhow::Result<()> {
    let server = Server::start().await?;
    let pool = server.pool().await?;
    let engine = Postgres::new(pool.clone()).await?;
    let inspector = pool.dedicated_connection().await?;
    let gate = pool.dedicated_connection().await?;
    inspector
        .batch_execute(
            r#"CREATE FUNCTION gate_shared_table() RETURNS event_trigger LANGUAGE plpgsql AS $$
             BEGIN
               IF EXISTS (SELECT FROM pg_event_trigger_ddl_commands() WHERE object_identity = 'public._ankurah_entity') THEN
                 PERFORM pg_advisory_xact_lock(987654322::bigint);
               END IF;
             END $$;
           CREATE EVENT TRIGGER gate_shared_table ON ddl_command_end WHEN TAG IN ('CREATE TABLE')
             EXECUTE FUNCTION gate_shared_table()"#,
        )
        .await?;
    gate.batch_execute("BEGIN; SELECT pg_advisory_xact_lock(987654322::bigint)").await?;
    let mut client = pool.get().await?;
    let mut preparing = Box::pin(engine.ensure_shared_tables(&mut client));
    tokio::select! {
        result = &mut preparing => panic!("preparation passed the native gate: {result:?}"),
        result = waiters(&inspector, 1) => result?,
    }
    drop(preparing);
    let mut resetting = Box::pin(engine.delete_all());
    tokio::select! {
        result = &mut resetting => panic!("reset passed the cancelled native preparation: {result:?}"),
        result = waiters(&inspector, 2) => result?,
    }
    gate.batch_execute("ROLLBACK").await?;
    timeout(HANG, resetting).await??;
    let tables: i64 = inspector.query_one(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('_ankurah_entity', '_ankurah_event', '_ankurah_postgres_model_map')", &[],
    ).await?.get(0);
    assert_eq!(tables, 0, "cancelled preparation made no shared tables");
    // A different session must be able to acquire the preparation lock.
    let mut other = pool.dedicated_connection().await?;
    timeout(HANG, engine.ensure_shared_tables(&mut other)).await??;
    Ok(())
}

#[tokio::test]
async fn cancelled_name_column_and_schema_writes_do_not_cross_reset() -> anyhow::Result<()> {
    for phase in ["model name", "property name", "column DDL"] {
        let server = Server::start().await?;
        let pool = server.pool().await?;
        let engine = Postgres::new(pool.clone()).await?;
        let inspector = pool.dedicated_connection().await?;
        let gate = pool.dedicated_connection().await?;
        let model = ModelId::EntityId(EntityId::from_bytes([2; 32]));
        let projection = engine.materialization(&model).await?;
        let table = match phase {
            "model name" => MODEL_REGISTRATION_TABLE,
            "property name" => COLUMN_MAP_TABLE,
            _ => projection.table(),
        };
        gate.batch_execute(&format!(r#"BEGIN; LOCK TABLE "{table}" IN SHARE MODE"#)).await?;
        let mut client = pool.get().await?;
        let new_model = ModelId::EntityId(EntityId::from_bytes([3; 32]));
        let backend = LWWBackend::new();
        backend.set(PropertyId::System(SystemProperty::Label), Some(Value::String("prepared".into())));
        ankurah_core::test_helpers::commit_backend(&backend, 4)?;
        let mut state = entity!(4).0;
        state.payload.state.state_buffers = StateBuffers(BTreeMap::from([("lww".into(), backend.to_state_buffer()?)]));
        let mut preparing = Box::pin(async {
            match phase {
                "model name" => {
                    engine.table_for_model(&new_model).await?;
                }
                "property name" => {
                    projection.prepare_state(&state).await?;
                }
                _ => {
                    projection.add_missing_columns(&mut client, vec![("pending".into(), "TEXT")]).await?;
                }
            }
            anyhow::Ok(())
        });
        tokio::select! {
            result = &mut preparing => panic!("{phase} passed its native gate: {result:?}"),
            result = waiters(&inspector, 1) => result?,
        }
        drop(preparing);
        let mut resetting = Box::pin(engine.delete_all());
        tokio::select! {
            result = &mut resetting => panic!("reset passed unfinished {phase}: {result:?}"),
            result = waiters(&inspector, 2) => result?,
        }
        gate.batch_execute("ROLLBACK").await?;
        timeout(HANG, resetting).await??;
        let left: i64 = inspector
            .query_one(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ANY($1)",
                &[&vec![MODEL_REGISTRATION_TABLE, COLUMN_MAP_TABLE, projection.table()]],
            )
            .await?
            .get(0);
        assert_eq!(left, 0, "{phase} left no materialization schema or assignments after reset");
    }
    Ok(())
}
