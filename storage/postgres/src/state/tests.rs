use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ankql::ast::{OrderByItem, OrderDirection, Predicate, PropertyPath};
use ankurah_core::storage::{LockName, MapId, MapRead, MapRows, MapWrite, StorageEngine, StorageTransaction};
use ankurah_proto::{AuthorId, Clock, Event, EventId, OperationSet, State, StateBuffers};
use bb8_postgres::PostgresConnectionManager;
use testcontainers::ImageExt;
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

use super::*;

fn state(id: u8, revision: u8) -> Attested<EntityState> {
    Attested::opt(
        EntityState {
            entity_id: EntityId::from_bytes([id; 32]),
            state: State {
                state_buffers: StateBuffers::default(),
                memberships: BTreeSet::new(),
                head: Clock::genesis(EventId::from_bytes([revision; 32])),
            },
        },
        None,
    )
}

async fn write(engine: &Postgres, state: &Attested<EntityState>) -> anyhow::Result<()> {
    let head = match engine.get_state(state.payload.entity_id).await {
        Ok(current) => current.payload.state.head,
        Err(RetrievalError::EntityNotFound(_)) => Clock::default(),
        Err(error) => return Err(error.into()),
    };
    let mut transaction = engine.transaction();
    transaction.set_state(&head, state).await?;
    transaction.commit().await?.committed()?;
    Ok(())
}

async fn collect(engine: &Postgres, since: Option<&StateCursor>) -> anyhow::Result<(BTreeMap<EntityId, Clock>, StateCursor)> {
    let mut snapshot = engine.state_snapshot(since).await?;
    let mut states = BTreeMap::new();
    loop {
        let page = snapshot.next_page(NonZeroUsize::new(3).unwrap()).await?;
        if page.is_empty() {
            break;
        }
        for state in page {
            assert!(states.insert(state.payload.entity_id, state.payload.state.head).is_none(), "one state per entity");
        }
    }
    Ok((states, snapshot.finish().await?))
}

#[tokio::test]
async fn state_snapshots_postgres_11() -> anyhow::Result<()> { contract("11-alpine").await }

#[tokio::test]
async fn state_snapshots_postgres_17() -> anyhow::Result<()> { contract("17").await }

async fn contract(tag: &str) -> anyhow::Result<()> {
    let container = postgres::Postgres::default().with_tag(tag).start().await?;
    let manager = PostgresConnectionManager::new_from_stringlike(
        format!(
            "host={} port={} user=postgres password=postgres dbname=postgres",
            container.get_host().await?,
            container.get_host_port_ipv4(5432).await?,
        ),
        NoTls,
    )?;
    // A state snapshot must not occupy the only normal pooled connection.
    let pool = bb8::Pool::builder().max_size(1).build(manager).await?;
    let engine = Arc::new(Postgres::new(pool.clone()).await?);
    late_commit_and_rollback(&engine).await?;
    engine.delete_all().await?;
    paging_and_selection(&engine).await?;
    cancelled_snapshot_releases_its_session(&engine).await?;
    reset_does_not_block_a_scan_that_needs_map_access(&engine).await?;
    engine.delete_all().await?;
    cursor_reopen_reset_and_migration(&engine, &pool).await?;
    index_queries(&engine).await?;
    engine.delete_all().await?;
    managed_tree_builds_flush_pages_without_occupying_the_source_connection(engine).await?;
    Ok(())
}

async fn managed_tree_builds_flush_pages_without_occupying_the_source_connection(engine: Arc<Postgres>) -> anyhow::Result<()> {
    use ankurah_core::sync::{CacheSettings, TreeManager};
    use ankurah_digest_tree::Ristretto255;

    let mut transaction = engine.transaction();
    for n in 0..600u64 {
        let mut entity = state(0, 1);
        let mut id = [0; 32];
        id[..8].copy_from_slice(&n.to_be_bytes());
        entity.payload.entity_id = EntityId::from_bytes(id);
        transaction.set_state(&Clock::default(), &entity).await?;
    }
    transaction.commit().await?.committed()?;

    let selection = Selection { predicate: Predicate::True, order_by: None, limit: None };
    let manager =
        TreeManager::<_, Ristretto255>::new(engine.clone(), Arc::new(tokio::sync::Mutex::new(())), CacheSettings { bytes: 1 << 20 }, 1);
    // More than two scan pages forces durable map commits while the source
    // snapshot still owns its connection. The normal pool has only one slot.
    let tree = tokio::time::timeout(Duration::from_secs(10), manager.open(selection.clone())).await??;
    assert_eq!(tree.audit().await?.members, 600);
    let view = tree.view();
    let other =
        TreeManager::<_, Ristretto255>::new(engine.clone(), Arc::new(tokio::sync::Mutex::new(())), CacheSettings { bytes: 1 << 20 }, 1);
    assert_eq!(other.open(selection.clone()).await?.audit().await?.members, 600, "the final cursor makes the completed build reusable");

    engine.delete_all().await?;
    write(&engine, &state(2, 3)).await?;
    let reopened = tokio::time::timeout(Duration::from_secs(10), manager.open(selection)).await??;
    assert_eq!(reopened.audit().await?.members, 1);
    assert_eq!(view.count(..).await?, 1, "a live view follows the replacement after reset");
    Ok(())
}

async fn late_commit_and_rollback(engine: &Postgres) -> anyhow::Result<()> {
    let original = state(1, 1);
    write(engine, &original).await?;
    let delayed = engine.pool.dedicated_connection().await?;
    delayed.batch_execute("BEGIN").await?;
    let delayed_id: i64 = delayed.query_one("SELECT txid_current()", &[]).await?.get(0);
    let aborted = engine.pool.dedicated_connection().await?;
    aborted.batch_execute("BEGIN").await?;
    let aborted_id: i64 = aborted.query_one("SELECT txid_current()", &[]).await?.get(0);

    // The later writer commits first. Both unfinished writers belong in the
    // saved cursor, regardless of whether either eventually writes a state.
    let newer = state(1, 2);
    write(engine, &newer).await?;
    let (_, cursor) = collect(engine, None).await?;
    let (_, saved): (String, String) = bincode::deserialize(cursor.as_bytes().strip_prefix(CURSOR_FORMAT).unwrap())?;
    let saved = Boundary::parse(&saved)?;
    assert!(saved.running.contains(&delayed_id));
    assert!(saved.running.contains(&aborted_id));

    let last = state(1, 3);
    delayed
        .execute(
            &format!(r#"UPDATE "{ENTITY_TABLE}" SET "head" = $1, "state_writer" = txid_current() WHERE "id" = $2"#),
            &[&last.payload.state.head, &last.payload.entity_id],
        )
        .await?;
    delayed.batch_execute("COMMIT").await?;
    aborted.batch_execute("ROLLBACK").await?;
    let (changed, next) = collect(engine, Some(&cursor)).await?;
    assert_eq!(changed, BTreeMap::from([(last.payload.entity_id, last.payload.state.head.clone())]));
    assert!(collect(engine, Some(&next)).await?.0.is_empty());

    // An unchanged head may still accompany a canonical state/attestation
    // write. Conservative discovery is intentional; no event insertion is needed.
    write(engine, &last).await?;
    assert_eq!(collect(engine, Some(&next)).await?.0.len(), 1);

    let (_, before_event) = collect(engine, None).await?;
    let mut event_only = engine.transaction();
    event_only.add_events(&[Attested::opt(Event::genesis(None, AuthorId::Unknown, OperationSet::default()), None)]).await?;
    event_only.commit().await?.committed()?;
    assert!(collect(engine, Some(&before_event)).await?.0.is_empty());
    Ok(())
}

async fn paging_and_selection(engine: &Postgres) -> anyhow::Result<()> {
    let mut expected = BTreeMap::new();
    for id in 1..=13 {
        let state = state(id, 1);
        write(engine, &state).await?;
        expected.insert(state.payload.entity_id, state.payload.state.head);
    }
    let mut snapshot = engine.state_snapshot(None).await?;
    let first = snapshot.next_page(NonZeroUsize::new(3).unwrap()).await?;
    let mut found = BTreeMap::new();
    for state in first {
        found.insert(state.payload.entity_id, state.payload.state.head);
    }

    let changed = state(13, 2);
    let inserted = state(14, 1);
    write(engine, &changed).await?;
    write(engine, &inserted).await?;
    loop {
        let page = snapshot.next_page(NonZeroUsize::new(3).unwrap()).await?;
        if page.is_empty() {
            break;
        }
        for state in page {
            assert!(found.insert(state.payload.entity_id, state.payload.state.head).is_none());
        }
    }
    assert_eq!(found, expected);
    let points = snapshot.get_states(&[changed.payload.entity_id, inserted.payload.entity_id, changed.payload.entity_id]).await?;
    assert_eq!(points[0].as_ref().unwrap().payload.state.head, expected[&changed.payload.entity_id]);
    assert!(points[1].is_none());
    assert_eq!(points[0].as_ref().unwrap().payload, points[2].as_ref().unwrap().payload);

    for direction in [None, Some(OrderDirection::Asc), Some(OrderDirection::Desc)] {
        let selection = Selection {
            predicate: Predicate::True,
            order_by: direction.clone().map(|direction| vec![OrderByItem { path: PropertyPath::id(), direction }]),
            limit: Some(2),
        };
        let selected = snapshot.limited_states(&selection).await?;
        let ids: Vec<_> = selected.iter().map(|state| state.payload.entity_id).collect();
        let expected_ids = match direction {
            Some(OrderDirection::Desc) => vec![state(13, 1).payload.entity_id, state(12, 1).payload.entity_id],
            _ => vec![state(1, 1).payload.entity_id, state(2, 1).payload.entity_id],
        };
        assert_eq!(ids, expected_ids);
    }
    assert!(snapshot.limited_states(&Selection { predicate: Predicate::True, order_by: None, limit: None }).await.is_err());
    assert!(snapshot.limited_states(&Selection { predicate: Predicate::True, order_by: None, limit: Some(0) }).await?.is_empty());
    let cursor = snapshot.finish().await?;
    let (changed_states, _) = collect(engine, Some(&cursor)).await?;
    assert_eq!(changed_states.len(), 2);
    assert_eq!(changed_states[&changed.payload.entity_id], changed.payload.state.head);
    Ok(())
}

async fn cursor_reopen_reset_and_migration(engine: &Postgres, pool: &bb8::Pool<PostgresConnectionManager<NoTls>>) -> anyhow::Result<()> {
    write(engine, &state(1, 1)).await?;
    let (_, cursor) = collect(engine, None).await?;
    let reopened = Postgres::new(pool.clone()).await?;
    let held = engine.state_snapshot(None).await?;
    tokio::time::timeout(Duration::from_secs(5), collect(&reopened, Some(&cursor))).await??;
    held.finish().await?;
    assert!(collect(&reopened, Some(&cursor)).await?.0.is_empty());

    let mut oversized_string = CURSOR_FORMAT.to_vec();
    oversized_string.extend(u64::MAX.to_le_bytes());
    let mut trailing_bytes = cursor.as_bytes().to_vec();
    trailing_bytes.push(0);
    for bytes in [Vec::new(), b"another engine".to_vec(), CURSOR_FORMAT.to_vec(), oversized_string, trailing_bytes] {
        assert!(matches!(engine.state_snapshot(Some(&StateCursor::new(bytes))).await, Err(RetrievalError::InvalidStateCursor)));
    }
    let (incarnation, _): (String, String) = bincode::deserialize(cursor.as_bytes().strip_prefix(CURSOR_FORMAT).unwrap())?;
    let mut future = CURSOR_FORMAT.to_vec();
    future.extend(bincode::serialize(&(incarnation, "999999999:999999999:".to_owned()))?);
    assert!(matches!(engine.state_snapshot(Some(&StateCursor::new(future))).await, Err(RetrievalError::InvalidStateCursor)));

    engine.delete_all().await?;
    assert!(matches!(reopened.state_snapshot(Some(&cursor)).await, Err(RetrievalError::InvalidStateCursor)));
    assert!(matches!(engine.state_snapshot(Some(&cursor)).await, Err(RetrievalError::InvalidStateCursor)));
    assert!(collect(engine, None).await?.0.is_empty());

    // An existing store gains markers for its pre-migration states. The
    // synthesized write is discoverable from a cursor preceding the migration.
    write(engine, &state(2, 1)).await?;
    let (_, before_migration) = collect(engine, None).await?;
    pool.get().await?.batch_execute(&format!(r#"ALTER TABLE "{ENTITY_TABLE}" DROP COLUMN "state_writer""#)).await?;
    let migrated = Postgres::new(pool.clone()).await?;
    assert_eq!(collect(&migrated, Some(&before_migration)).await?.0.len(), 1);
    Ok(())
}

async fn cancelled_snapshot_releases_its_session(engine: &Postgres) -> anyhow::Result<()> {
    let blocker = engine.pool.dedicated_connection().await?;
    let inspector = engine.pool.dedicated_connection().await?;
    blocker.batch_execute(&format!(r#"BEGIN; LOCK TABLE "{ENTITY_TABLE}" IN ACCESS EXCLUSIVE MODE"#)).await?;
    let mut opening = Box::pin(engine.state_snapshot(None));
    tokio::select! {
        result = &mut opening => panic!("snapshot did not wait for the table lock: {:?}", result.err()),
        result = snapshot_waiters(&inspector, 1) => result?,
    }
    drop(opening);
    snapshot_waiters(&inspector, 0).await?;

    // A wait for a relation lock must not freeze the read boundary before
    // that lock is granted. The write that unblocks it belongs in the view.
    let mut opening = Box::pin(engine.state_snapshot(None));
    tokio::select! {
        result = &mut opening => panic!("snapshot did not wait for the table lock: {:?}", result.err()),
        result = snapshot_waiters(&inspector, 1) => result?,
    }
    let changed = state(1, 9);
    blocker
        .execute(
            &format!(r#"UPDATE "{ENTITY_TABLE}" SET "head" = $1, "state_writer" = txid_current() WHERE "id" = $2"#),
            &[&changed.payload.state.head, &changed.payload.entity_id],
        )
        .await?;
    blocker.batch_execute("COMMIT").await?;
    let mut snapshot = opening.await?;
    assert_eq!(
        snapshot.get_states(&[changed.payload.entity_id]).await?[0].as_ref().unwrap().payload.state.head,
        changed.payload.state.head
    );
    snapshot.finish().await?;
    Ok(())
}

async fn snapshot_waiters(inspector: &Client, expected: i64) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let count: i64 = inspector
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%LOCK TABLE%_ankurah_entity%IN ACCESS SHARE MODE%'",
                    &[],
                )
                .await?
                .get(0);
            if count == expected {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok(())
}

async fn reset_does_not_block_a_scan_that_needs_map_access(engine: &Postgres) -> anyhow::Result<()> {
    let map = MapId::new("state-snapshot-reset");
    let lock = LockName::new("state-snapshot-reset");
    let key = b"tree generation".to_vec();
    let mut transaction = engine.transaction();
    transaction.map_lock_and_read(&[lock.clone()], vec![]).await?;
    transaction.map_write(vec![MapWrite::Put { map: map.clone(), key: key.clone(), value: vec![1] }]).await?;
    transaction.commit().await?.committed()?;
    let mut snapshot = engine.state_snapshot(None).await?;
    let expected = snapshot.get_states(&[state(1, 1).payload.entity_id]).await?[0].clone().unwrap();

    tokio::time::timeout(Duration::from_secs(5), engine.delete_all()).await??;
    let mut transaction = engine.transaction();
    let answer =
        tokio::time::timeout(Duration::from_secs(5), transaction.map_lock_and_read(&[lock], vec![MapRead::Get { map, keys: vec![key] }]))
            .await??;
    assert_eq!(answer, vec![MapRows::Got(vec![None])], "the tree's old generation is gone; a flush must reject that base");
    drop(transaction);
    assert_eq!(snapshot.get_states(&[expected.payload.entity_id]).await?[0].as_ref().unwrap().payload, expected.payload);
    let cursor = snapshot.finish().await?;
    assert!(matches!(engine.state_snapshot(Some(&cursor)).await, Err(RetrievalError::InvalidStateCursor)));
    Ok(())
}

async fn index_queries(engine: &Postgres) -> anyhow::Result<()> {
    let client = engine.pool.get().await?;
    client
        .execute(
            &format!(
                r#"INSERT INTO "{ENTITY_TABLE}" ("id", "state_buffer", "head", "attestations")
                   SELECT translate(rtrim(encode(decode(lpad(to_hex(n), 64, '0'), 'hex'), 'base64'), '='), '+/', '-_'), $1, $2, $3
                   FROM generate_series(1, 10000) n"#
            ),
            &[&bincode::serialize(&StateBuffers::default())?, &Clock::default(), &Vec::<Vec<u8>>::new()],
        )
        .await?;
    let boundary: i64 = client.query_one("SELECT txid_snapshot_xmax(txid_current_snapshot())", &[]).await?.get(0);
    client
        .execute(
            &format!(
                r#"UPDATE "{ENTITY_TABLE}" SET "state_writer" = txid_current() WHERE "id" IN (SELECT "id" FROM "{ENTITY_TABLE}" LIMIT 10)"#
            ),
            &[],
        )
        .await?;
    client.batch_execute(&format!(r#"ANALYZE "{ENTITY_TABLE}""#)).await?;
    let statement = client
        .prepare(&format!(
            r#"EXPLAIN (FORMAT JSON) {} WHERE e."state_writer" >= $1 AND (e."state_writer", e."id") > ($2, $3)
               ORDER BY e."state_writer", e."id" LIMIT $4"#,
            select_states()
        ))
        .await?;
    for _ in 0..8 {
        let plan: serde_json::Value = client.query_one(&statement, &[&boundary, &0i64, &"", &3i64]).await?.get(0);
        assert!(plan.to_string().contains("_ankurah_entity_state_writer_idx"), "{plan:#}");
    }
    Ok(())
}

#[test]
fn snapshot_boundary_rejects_malformed_coverage() {
    for invalid in ["", "1", "1:2", "1:2::", "-1:2:", "2:1:", "1:4:0", "1:4:4", "1:4:3,2", "1:4:2,2"] {
        assert!(Boundary::parse(invalid).is_err(), "{invalid}");
    }
    assert!(Boundary::parse("1:4:1,3").is_ok());
    assert!(Boundary::parse("4:4:").is_ok());
}
