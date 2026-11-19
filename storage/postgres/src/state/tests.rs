use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ankql::ast::{OrderByItem, OrderDirection, Predicate, PropertyPath, Resolved, Selection};
use ankurah_core::storage::{LockName, MapId, MapRead, MapRows, MapWrite, StorageEngine, StorageTransaction};
use ankurah_proto::{AuthorId, Clock, Event, EventId, OperationSet, State, StateBuffers};
use bb8_postgres::PostgresConnectionManager;
use testcontainers::ImageExt;
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

use super::*;

mod concurrent_builds;

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
    let mut snapshot = engine.state_snapshot(StateScan::Changes(since)).await?;
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
    selection_snapshots_match_fetch_and_release_materialization_locks(&engine).await?;
    selection_queries_share_the_boundary_and_release_materialization_locks(&engine).await?;
    selection_planning_retries_only_for_newly_visible_states(&engine).await?;
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
    let mut snapshot = engine.state_snapshot(StateScan::Changes(None)).await?;
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

    let cursor = snapshot.finish().await?;
    let (changed_states, _) = collect(engine, Some(&cursor)).await?;
    assert_eq!(changed_states.len(), 2);
    assert_eq!(changed_states[&changed.payload.entity_id], changed.payload.state.head);
    Ok(())
}

async fn selected_pages(snapshot: &mut dyn StateSnapshot) -> anyhow::Result<Vec<Attested<EntityState>>> {
    let mut states = Vec::new();
    loop {
        let page = snapshot.next_page(NonZeroUsize::new(1).unwrap()).await?;
        if page.is_empty() {
            return Ok(states);
        }
        assert_eq!(page.len(), 1);
        states.extend(page);
    }
}

async fn selection_queries_share_the_boundary_and_release_materialization_locks(engine: &Postgres) -> anyhow::Result<()> {
    use ankurah_core::entity::TemporaryEntity;
    use ankurah_proto::ModelId;

    engine.delete_all().await?;
    let model = ModelId::EntityId(EntityId::from_bytes([112; 32]));
    let mut seeded = Vec::new();
    for id in [1, 2, 200] {
        let mut entity = state(id, 1);
        entity.payload.state.memberships.insert(model);
        write(engine, &entity).await?;
        seeded.push(entity);
    }
    let (_, previous) = collect(engine, None).await?;
    let selection = Selection { predicate: Predicate::MemberOf(model), order_by: None, limit: Some(0) };
    let mut snapshot = engine.state_snapshot(StateScan::SelectionChanges { selection: &selection, since: Some(&previous) }).await?;
    let mut later = state(3, 2);
    later.payload.state.memberships.insert(model);
    write(engine, &later).await?;

    let ids = [seeded[2].payload.entity_id, seeded[0].payload.entity_id, later.payload.entity_id];
    assert_eq!(snapshot.matching_ids(&ids).await?, ids[..2], "membership ignores LIMIT and excludes post-boundary commits");
    assert_eq!(snapshot.select_after(None, NonZeroUsize::new(2).unwrap()).await?, seeded[..2]);
    let tail = TemporaryEntity::new(seeded[1].payload.entity_id, &seeded[1].payload.state)?;
    let boundary = OrderBoundary::from_item(&selection, &tail);
    assert_eq!(snapshot.select_after(Some(&boundary), NonZeroUsize::MIN).await?, seeded[2..]);
    assert!(snapshot.next_page(NonZeroUsize::MIN).await?.is_empty(), "query operations do not consume or replace change pages");

    // Both helper queries touched a materialization, but neither keeps its
    // relation lock while the caller updates tree maps or another node resets.
    tokio::time::timeout(Duration::from_secs(5), engine.delete_all()).await??;
    assert_eq!(snapshot.get_states(&[seeded[0].payload.entity_id]).await?[0].as_ref(), Some(&seeded[0]));
    assert!(matches!(snapshot.matching_ids(&ids).await,
        Err(RetrievalError::MutationError(error)) if matches!(*error, MutationError::WriteConflict)));
    assert!(snapshot.get_states(&ids).await.is_err(), "a rolled-back snapshot cannot resume outside its read boundary");
    assert!(snapshot.finish().await.is_err(), "a failed query cannot publish its abandoned cursor");
    Ok(())
}

async fn selection_snapshots_match_fetch_and_release_materialization_locks(engine: &Postgres) -> anyhow::Result<()> {
    use ankurah_core::property::backend::{LWWBackend, PropertyBackend};
    use ankurah_core_types::{PropertyId, Value};
    use ankurah_proto::ModelId;

    engine.delete_all().await?;
    let model = ModelId::EntityId(EntityId::from_bytes([101; 32]));
    let score = PropertyId::EntityId(EntityId::from_bytes([201; 32]));
    let json = PropertyId::EntityId(EntityId::from_bytes([202; 32]));
    let mut seeded = Vec::new();
    for (id, value, nested) in [
        (1, Some(5), serde_json::json!(10)),
        (2, Some(5), serde_json::json!(2)),
        (200, None, serde_json::json!(null)),
        (240, Some(9), serde_json::json!("3")),
    ] {
        let backend = LWWBackend::new();
        if let Some(value) = value {
            backend.set(score, Some(Value::I64(value)));
        }
        backend.set(json, Some(Value::Json(serde_json::json!({"score": nested}))));
        let event = ankurah_core::test_helpers::commit_backend(&backend, id)?;
        let mut entity = state(id, 1);
        entity.payload.state.state_buffers = StateBuffers(BTreeMap::from([("lww".into(), backend.to_state_buffer()?)]));
        entity.payload.state.head = Clock::singleton(&event);
        entity.payload.state.memberships.insert(model);
        write(engine, &entity).await?;
        seeded.push(entity);
    }
    let mut nested = PropertyPath::from(json);
    nested.subpath.push("score".into());
    let select = |order_by, limit| Selection { predicate: Predicate::MemberOf(model), order_by, limit };
    let mut cases =
        vec![select(None, Some(2)), select(Some(vec![OrderByItem { path: PropertyPath::id(), direction: OrderDirection::Desc }]), Some(2))];
    for path in [PropertyPath::from(score), PropertyPath::from(json)] {
        for direction in [OrderDirection::Asc, OrderDirection::Desc] {
            cases.push(select(Some(vec![OrderByItem { path: path.clone(), direction }]), Some(3)));
        }
    }
    cases.push(select(None, Some(0)));
    for (case, selection) in cases.iter().enumerate() {
        let fetched = engine.fetch_states(selection).await?;
        let mut snapshot = engine.state_snapshot(StateScan::Selection(selection)).await?;
        let selected = selected_pages(&mut *snapshot).await?;
        assert_eq!(selected, fetched, "selection {selection:?}");
        let ids: Vec<_> = selected.iter().map(|state| state.payload.entity_id.to_bytes()[0]).collect();
        match case {
            0 => assert_eq!(ids, [1, 2], "default order compares ID bytes, not base64 text"),
            1 => assert_eq!(ids, [240, 200], "an explicit descending ID order remains descending"),
            2 => assert_eq!(ids, [1, 2, 240], "ascending null placement and ID tie-break"),
            3 => assert_eq!(ids, [200, 240, 1], "descending null placement and ascending ID tie-break"),
            _ => {}
        }
        snapshot.finish().await?;
    }
    assert!(engine.state_snapshot(StateScan::Selection(&select(None, None))).await.is_err());
    let unsupported = select(Some(vec![OrderByItem { path: nested, direction: OrderDirection::Asc }]), Some(3));
    let fetch_error = engine.fetch_states(&unsupported).await.unwrap_err();
    let scan_error = engine.state_snapshot(StateScan::Selection(&unsupported)).await.err().expect("the same unsupported query");
    assert_eq!(scan_error.to_string(), fetch_error.to_string(), "unsupported JSON-path ordering preserves the normal query error");

    let selection = &cases[2];
    let fetched = engine.fetch_states(selection).await?;
    let mut snapshot = engine.state_snapshot(StateScan::Selection(selection)).await?;
    let mut changed = seeded[0].clone();
    changed.payload.state.head = Clock::genesis(EventId::from_bytes([99; 32]));
    write(engine, &changed).await?;
    assert_eq!(selected_pages(&mut *snapshot).await?, fetched, "selected pages retain the original states");
    assert_eq!(snapshot.get_states(&[changed.payload.entity_id]).await?[0].as_ref(), Some(&seeded[0]));
    let cursor = snapshot.finish().await?;
    assert_eq!(collect(engine, Some(&cursor)).await?.0.len(), 1);

    // The selected query uses a materialization table, but its relation lock
    // must not survive collection and prevent reset from dropping that table.
    let mut snapshot = engine.state_snapshot(StateScan::Selection(selection)).await?;
    tokio::time::timeout(Duration::from_secs(5), engine.delete_all()).await??;
    assert_eq!(snapshot.get_states(&[changed.payload.entity_id]).await?[0].as_ref(), Some(&changed));
    assert_eq!(selected_pages(&mut *snapshot).await?.len(), 3);
    let cursor = snapshot.finish().await?;
    assert!(matches!(engine.state_snapshot(StateScan::Changes(Some(&cursor))).await, Err(RetrievalError::InvalidStateCursor)));

    write(engine, &seeded[0]).await?;
    reset_during_selection_is_retryable(engine, selection, model, true).await?;
    engine.delete_all().await?;
    write(engine, &seeded[0]).await?;
    reset_during_selection_is_retryable(engine, selection, model, false).await?;

    engine.delete_all().await?;
    write(engine, &seeded[0]).await?;
    let table = engine.materialization(&model).await?;
    let column = table.query_columns(&[score]).await?[&score].clone();
    engine.pool.dedicated_connection().await?.batch_execute(&format!(r#"ALTER TABLE "{}" DROP COLUMN "{column}""#, table.table())).await?;
    let fetch_error = engine.fetch_states(selection).await.unwrap_err();
    let scan_error = engine.state_snapshot(StateScan::Selection(selection)).await.err().expect("the query's column is absent");
    assert_eq!(scan_error.to_string(), fetch_error.to_string(), "a SQL error without a reset preserves its cause");
    assert!(!matches!(scan_error, RetrievalError::MutationError(error) if matches!(*error, MutationError::WriteConflict)));
    Ok(())
}

async fn reset_during_selection_is_retryable(
    engine: &Postgres,
    selection: &Selection<Resolved>,
    model: ankurah_proto::ModelId,
    before_boundary: bool,
) -> anyhow::Result<()> {
    let table = engine.materialization(&model).await?.table().to_owned();
    let blocker = engine.pool.dedicated_connection().await?;
    let inspector = engine.pool.dedicated_connection().await?;
    let locked = if before_boundary { ENTITY_TABLE } else { &table };
    blocker.batch_execute(&format!(r#"BEGIN; LOCK TABLE "{locked}" IN ACCESS EXCLUSIVE MODE"#)).await?;
    let mut opening = Box::pin(engine.state_snapshot(StateScan::Selection(selection)));
    // Resolving an ORDER BY property's type can read canonical state while
    // preparing the query, before the explicit source-boundary lock.
    let waiting_query = if before_boundary { "%_ankurah_entity%" } else { "SELECT \"id\" FROM (%" };
    tokio::select! {
        result = &mut opening => panic!("snapshot did not wait for its source query: {:?}", result.err()),
        result = query_waiters(&inspector, 1, waiting_query) => result?,
    }
    // Reset can retire planned names either before the source boundary is
    // captured, or after it, while the materialization SELECT waits for a lock.
    blocker.batch_execute(&format!(r#"DELETE FROM "{ENTITY_MODEL_TABLE}"; DELETE FROM "{ENTITY_TABLE}"; DROP TABLE "{table}""#)).await?;
    reset(&blocker).await?;
    blocker.batch_execute("COMMIT").await?;
    assert!(matches!(opening.await, Err(RetrievalError::MutationError(error)) if matches!(*error, MutationError::WriteConflict)));
    Ok(())
}

async fn selection_planning_retries_only_for_newly_visible_states(engine: &Postgres) -> anyhow::Result<()> {
    use ankurah_proto::ModelId;

    // Cover a new writer, an older writer that commits late, and unrelated DDL
    // that advances transaction IDs without changing canonical state.
    for (late_writer, publish_state) in [(false, true), (true, true), (true, false)] {
        engine.delete_all().await?;
        let blocker = engine.pool.dedicated_connection().await?;
        let inspector = engine.pool.dedicated_connection().await?;
        engine.ensure_shared_tables(&blocker).await?;
        let model = ModelId::EntityId(EntityId::from_bytes([111; 32]));
        let mut entity = state(77, 1);
        entity.payload.state.memberships.insert(model);
        let selection = Selection { predicate: Predicate::MemberOf(model), order_by: None, limit: Some(2) };

        blocker.batch_execute(&format!(r#"BEGIN; LOCK TABLE "{ENTITY_TABLE}" IN ACCESS EXCLUSIVE MODE"#)).await?;
        if late_writer {
            let older: i64 = blocker.query_one("SELECT txid_current()", &[]).await?.get(0);
            let newer: i64 = inspector.query_one("SELECT txid_current()", &[]).await?.get(0);
            assert!(older < newer, "the unfinished writer precedes the preparation boundary's upper bound");
        }
        let mut opening = Box::pin(engine.state_snapshot(StateScan::Selection(&selection)));
        tokio::select! {
            result = &mut opening => panic!("snapshot did not pause after planning: {:?}", result.err()),
            result = snapshot_waiters(&inspector, 1) => result?,
        }

        // The query has planned against an absent model. Create its projection
        // after planning, then atomically publish it with the canonical state
        // on the transaction that releases the source-boundary lock.
        let projection = engine.materialization(&model).await?.prepare_state(&entity).await?;
        if publish_state {
            blocker
                .execute(
                    &format!(r#"INSERT INTO "{ENTITY_TABLE}" ("id", "state_buffer", "head", "attestations") VALUES ($1, $2, $3, $4)"#),
                    &[
                        &entity.payload.entity_id,
                        &bincode::serialize(&entity.payload.state.state_buffers)?,
                        &entity.payload.state.head,
                        &Vec::<Vec<u8>>::new(),
                    ],
                )
                .await?;
            blocker
                .execute(
                    &format!(r#"INSERT INTO "{ENTITY_MODEL_TABLE}" ("entity_id", "model_key") VALUES ($1, $2)"#),
                    &[&entity.payload.entity_id, &bincode::serialize(&model)?],
                )
                .await?;
            projection.write(&blocker).await?;
        }
        blocker.batch_execute("COMMIT").await?;

        if publish_state {
            assert!(matches!(opening.await, Err(RetrievalError::MutationError(error)) if matches!(*error, MutationError::WriteConflict)));
            let mut retried = engine.state_snapshot(StateScan::Selection(&selection)).await?;
            assert_eq!(selected_pages(&mut *retried).await?, vec![entity], "retry plans against the newly visible model");
            retried.finish().await?;
        } else {
            let mut snapshot = opening.await?;
            assert!(selected_pages(&mut *snapshot).await?.is_empty(), "unrelated transaction IDs do not invalidate the query plan");
            snapshot.finish().await?;
        }
    }
    Ok(())
}

async fn cursor_reopen_reset_and_migration(engine: &Postgres, pool: &bb8::Pool<PostgresConnectionManager<NoTls>>) -> anyhow::Result<()> {
    write(engine, &state(1, 1)).await?;
    let (_, cursor) = collect(engine, None).await?;
    let reopened = Postgres::new(pool.clone()).await?;
    let held = engine.state_snapshot(StateScan::Changes(None)).await?;
    tokio::time::timeout(Duration::from_secs(5), collect(&reopened, Some(&cursor))).await??;
    held.finish().await?;
    assert!(collect(&reopened, Some(&cursor)).await?.0.is_empty());

    let mut oversized_string = CURSOR_FORMAT.to_vec();
    oversized_string.extend(u64::MAX.to_le_bytes());
    let mut trailing_bytes = cursor.as_bytes().to_vec();
    trailing_bytes.push(0);
    for bytes in [Vec::new(), b"another engine".to_vec(), CURSOR_FORMAT.to_vec(), oversized_string, trailing_bytes] {
        assert!(matches!(
            engine.state_snapshot(StateScan::Changes(Some(&StateCursor::new(bytes)))).await,
            Err(RetrievalError::InvalidStateCursor)
        ));
    }
    let (incarnation, _): (String, String) = bincode::deserialize(cursor.as_bytes().strip_prefix(CURSOR_FORMAT).unwrap())?;
    let mut future = CURSOR_FORMAT.to_vec();
    future.extend(bincode::serialize(&(incarnation, "999999999:999999999:".to_owned()))?);
    assert!(matches!(
        engine.state_snapshot(StateScan::Changes(Some(&StateCursor::new(future)))).await,
        Err(RetrievalError::InvalidStateCursor)
    ));

    engine.delete_all().await?;
    assert!(matches!(reopened.state_snapshot(StateScan::Changes(Some(&cursor))).await, Err(RetrievalError::InvalidStateCursor)));
    assert!(matches!(engine.state_snapshot(StateScan::Changes(Some(&cursor))).await, Err(RetrievalError::InvalidStateCursor)));
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
    let mut opening = Box::pin(engine.state_snapshot(StateScan::Changes(None)));
    tokio::select! {
        result = &mut opening => panic!("snapshot did not wait for the table lock: {:?}", result.err()),
        result = snapshot_waiters(&inspector, 1) => result?,
    }
    drop(opening);
    snapshot_waiters(&inspector, 0).await?;

    // A wait for a relation lock must not freeze the read boundary before
    // that lock is granted. The write that unblocks it belongs in the view.
    let mut opening = Box::pin(engine.state_snapshot(StateScan::Changes(None)));
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
    query_waiters(inspector, expected, "%LOCK TABLE%_ankurah_entity%IN ACCESS SHARE MODE%").await
}

async fn query_waiters(inspector: &Client, expected: i64, query: &str) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let count: i64 = inspector
                .query_one("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE $1", &[&query])
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
    let mut snapshot = engine.state_snapshot(StateScan::Changes(None)).await?;
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
    assert!(matches!(engine.state_snapshot(StateScan::Changes(Some(&cursor))).await, Err(RetrievalError::InvalidStateCursor)));
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
