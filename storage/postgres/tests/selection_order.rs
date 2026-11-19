use std::sync::Arc;

use ankurah_core::test_helpers::selection_conformance;

mod common;

#[tokio::test]
async fn numeric_order_matches_digest_trees() -> anyhow::Result<()> {
    let (_container, engine) = common::create_postgres_container().await?;
    selection_conformance::numeric_ordering(Arc::new(engine)).await
}

#[tokio::test]
async fn json_order_matches_digest_trees() -> anyhow::Result<()> {
    let (_container, engine) = common::create_postgres_container().await?;
    selection_conformance::json_ordering(Arc::new(engine)).await
}

#[tokio::test]
async fn json_path_order_matches_digest_trees() -> anyhow::Result<()> {
    let (_container, engine) = common::create_postgres_container().await?;
    selection_conformance::json_path_ordering(Arc::new(engine)).await
}

#[tokio::test]
async fn mixed_order_matches_digest_trees() -> anyhow::Result<()> {
    let (_container, engine) = common::create_postgres_container().await?;
    selection_conformance::mixed_ordering(Arc::new(engine)).await
}

#[tokio::test]
async fn changes_and_refill_share_a_snapshot() -> anyhow::Result<()> {
    let (_container, engine) = common::create_postgres_container().await?;
    selection_conformance::selection_changes(Arc::new(engine)).await
}

#[tokio::test]
async fn non_c_locale_rejects_json_order_before_limit() -> anyhow::Result<()> {
    use ankql::ast::{OrderByItem, OrderDirection, Predicate, PropertyPath, Selection};
    use ankurah_core::{
        property::backend::{LWWBackend, PropertyBackend},
        storage::{StateScan, StorageEngine, StorageTransaction},
        value::Value,
    };
    use ankurah_proto::{Attested, Clock, EntityId, EntityState, ModelId, PropertyId, State, StateBuffers};

    let (_container, engine, pool) = common::create_postgres_container_with_locale("en_US.utf8").await?;
    let engine = Arc::new(engine);
    // Qualify the case-sensitive text order independently of Ankurah's query builder.
    let client = pool.get().await?;
    let labels: Vec<String> = client
        .query("SELECT value FROM (VALUES ('a'), ('b'), ('Z')) AS fixture(value) ORDER BY value COLLATE \"C\"", &[])
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(labels, ["Z", "a", "b"]);
    drop(client);
    // Typed text can explicitly request C collation even in this database.
    selection_conformance::mixed_ordering(engine.clone()).await?;

    // An entirely absent JSON column is constant NULL, so it does not invoke
    // JSONB's locale-dependent comparator. Materialize a real JSON value first.
    let document = PropertyId::EntityId(EntityId::from_bytes([203; 32]));
    let backend = LWWBackend::new();
    backend.set(document, Some(Value::Json(serde_json::json!({"rank": 1}))));
    let event = ankurah_core::test_helpers::commit_backend(&backend, 251)?;
    let state = Attested::opt(
        EntityState {
            entity_id: EntityId::from_bytes([251; 32]),
            state: State {
                state_buffers: StateBuffers([("lww".into(), backend.to_state_buffer()?)].into()),
                memberships: [ModelId::EntityId(EntityId::from_bytes([100; 32]))].into(),
                head: Clock::singleton(&event),
            },
        },
        None,
    );
    let mut transaction = engine.transaction();
    transaction.set_state(&Clock::default(), &state).await?;
    transaction.commit().await?.committed()?;

    let mut path = PropertyPath::from(document);
    path.subpath.push("rank".into());
    for limit in [0, 1] {
        let selection = Selection {
            predicate: Predicate::True,
            order_by: Some(vec![OrderByItem { path: path.clone(), direction: OrderDirection::Asc }]),
            limit: Some(limit),
        };
        let error = engine.fetch_states(&selection).await.unwrap_err();
        assert!(error.to_string().contains("JSON ordering requires") && error.to_string().contains("C/POSIX collation"), "{error}");
        let error = match engine.state_snapshot(StateScan::Selection(&selection)).await {
            Err(error) => error,
            Ok(_) => panic!("unsupported JSON collation must be rejected before opening the snapshot"),
        };
        assert!(error.to_string().contains("JSON ordering requires") && error.to_string().contains("C/POSIX collation"), "{error}");
    }
    Ok(())
}
