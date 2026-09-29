//! Engine-level cases mirrored from `storage/sqlite/src/engine.rs`'s unit
//! tests, through the crate's public API: physical names, the exact-head
//! commit, materialization refresh, and JSONB behaviour on node:sqlite.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use ankql::ast::Resolved;
use ankurah_core::{
    property::backend::{lww::LWWBackend, PropertyBackend},
    schema::CatalogResolver,
    storage::{StorageCommitOutcome, StorageEngine, StorageTransaction},
    value::Value,
};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, Event, EventId, ModelId, PropertyId, State, StateBuffers, SystemModel};
use ankurah_storage_common::ColumnPath;
use ankurah_storage_sqlite_node::{SqliteNodeError, SqliteNodeStorageEngine, SqliteValue};
use wasm_bindgen_test::wasm_bindgen_test;

#[derive(Default)]
struct TestResolver {
    model_names: BTreeMap<ModelId, String>,
    property_names: BTreeMap<PropertyId, String>,
}

#[async_trait::async_trait]
impl CatalogResolver for TestResolver {
    async fn get_model_label(&self, model: &ModelId) -> Option<String> { self.model_names.get(model).cloned() }

    async fn get_property_label(&self, property: &PropertyId) -> Option<String> { self.property_names.get(property).cloned() }
}

fn entity_id(byte: u8) -> EntityId { EntityId::from_bytes([byte; EntityId::BYTE_LEN]) }

fn state_with_strings(entity_id: EntityId, event_byte: u8, values: &[(PropertyId, &str)]) -> Attested<EntityState> {
    let backend = LWWBackend::new();
    for (property, value) in values {
        backend.set(*property, Some(Value::String((*value).to_owned())));
    }
    let operations = backend.to_operations().unwrap().expect("state has values");
    let event_id = EventId::from_bytes([event_byte; 32]);
    backend.apply_operations_with_event(&operations, event_id.clone()).unwrap();
    Attested::opt(
        EntityState {
            entity_id,
            state: State {
                state_buffers: StateBuffers(BTreeMap::from([("lww".to_owned(), backend.to_state_buffer().unwrap())])),
                memberships: BTreeSet::new(),
                head: Clock::from(vec![event_id]),
            },
        },
        None,
    )
}

fn state_for_model(mut state: Attested<EntityState>, model: ModelId) -> Attested<EntityState> {
    state.payload.state.memberships = [model].into();
    state
}

fn state_for_models(mut state: Attested<EntityState>, models: impl IntoIterator<Item = ModelId>) -> Attested<EntityState> {
    state.payload.state.memberships = models.into_iter().collect();
    state
}

async fn commit_canonical_state(engine: &SqliteNodeStorageEngine, expected_head: Clock, state: Attested<EntityState>) {
    let mut transaction = engine.transaction();
    transaction.set_state(&expected_head, &state).await.unwrap();
    let outcome = transaction.commit().await.unwrap();
    assert!(matches!(outcome, StorageCommitOutcome::Committed(_)));
}

async fn commit_state(engine: &SqliteNodeStorageEngine, expected_head: Clock, model: ModelId, state: Attested<EntityState>) {
    commit_canonical_state(engine, expected_head, state_for_model(state, model)).await;
}

fn install_resolver(engine: &SqliteNodeStorageEngine, resolver: Arc<dyn CatalogResolver>) -> Arc<dyn CatalogResolver> {
    engine.set_catalog_resolver(Arc::downgrade(&resolver));
    resolver
}

fn equals(property: PropertyId, value: &str) -> ankql::ast::Selection<Resolved> {
    ankql::ast::Selection {
        predicate: ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Path(property.into())),
            operator: ankql::ast::ComparisonOperator::Equal,
            right: Box::new(ankql::ast::Expr::Literal(Value::String(value.into()))),
        },
        order_by: None,
        limit: None,
    }
}

fn everything() -> ankql::ast::Selection<Resolved> {
    ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None }
}

/// The physical names of a model's table and of one property's column, read
/// from the engine's own durable maps.
async fn physical_names(engine: &SqliteNodeStorageEngine, model: ModelId, property: PropertyId) -> (String, String) {
    let model_key = bincode::serialize(&model).unwrap();
    let property_key = serde_json::to_string(&property).unwrap();
    engine
        .with_executor(move |executor| {
            let table = executor
                .query_row(
                    r#"SELECT "materialization_table_name" FROM "_ankurah_sqlite_model_map" WHERE "model_key" = ?"#,
                    &[SqliteValue::Blob(model_key.clone())],
                )?
                .expect("the model is registered")
                .text(0)?;
            let column = executor
                .query_row(
                    r#"SELECT "column_name" FROM "_ankurah_sqlite_column_map" WHERE "model_key" = ? AND "property_key" = ?"#,
                    &[SqliteValue::Blob(model_key), SqliteValue::Text(property_key)],
                )?
                .expect("the property has a column")
                .text(0)?;
            Ok((table, column))
        })
        .await
        .unwrap()
}

#[wasm_bindgen_test]
async fn fallback_names_survive_late_labels_and_engine_reopen() {
    let engine = SqliteNodeStorageEngine::open_in_memory().await.unwrap();
    let model = ModelId::EntityId(entity_id(0xe1));
    let property = PropertyId::EntityId(entity_id(0xe2));
    let entity = entity_id(0xe3);
    let resolver = install_resolver(&engine, Arc::new(TestResolver::default()));
    let initial = state_for_model(state_with_strings(entity, 1, &[(property, "before")]), model);
    commit_canonical_state(&engine, Clock::default(), initial.clone()).await;
    let (table, column) = physical_names(&engine, model, property).await;
    assert!(table.starts_with("m_"));
    assert!(column.starts_with("p_"));
    drop(resolver);

    // Reopen over the same database object, as a host restarting its node would.
    let reopened = SqliteNodeStorageEngine::from_database(engine.database().clone()).await.unwrap();
    drop(engine);
    let _resolver = install_resolver(
        &reopened,
        Arc::new(TestResolver {
            model_names: BTreeMap::from([(model, "LateModelLabel".into())]),
            property_names: BTreeMap::from([(property, "LatePropertyLabel".into())]),
        }),
    );
    let updated = state_for_model(state_with_strings(entity, 2, &[(property, "after")]), model);
    commit_canonical_state(&reopened, initial.payload.state.head, updated.clone()).await;
    assert_eq!(physical_names(&reopened, model, property).await, (table, column));
    let selection = equals(property, "after");
    let found = reopened.fetch_states(&selection.clone().and_member_of(model)).await.unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].payload.entity_id, entity);

    let other = PropertyId::EntityId(entity_id(0xe8));
    let removed = state_for_model(state_with_strings(entity, 3, &[(other, "remaining")]), model);
    commit_canonical_state(&reopened, updated.payload.state.head, removed).await;
    assert!(
        reopened.fetch_states(&selection.and_member_of(model)).await.unwrap().is_empty(),
        "removed properties must not retain old values"
    );
}

#[wasm_bindgen_test]
async fn concurrent_first_writes_share_ddl_coordination() {
    let engine = SqliteNodeStorageEngine::open_in_memory().await.unwrap();
    let model = ModelId::EntityId(entity_id(0xe4));
    let property = PropertyId::EntityId(entity_id(0xe5));
    futures_util::join!(
        commit_state(&engine, Clock::default(), model, state_with_strings(entity_id(0xe6), 1, &[(property, "first")])),
        commit_state(&engine, Clock::default(), model, state_with_strings(entity_id(0xe7), 2, &[(property, "second")])),
    );
    assert_eq!(engine.fetch_states(&everything().and_member_of(model)).await.unwrap().len(), 2);
}

#[wasm_bindgen_test]
async fn open_in_memory_starts_empty() {
    let engine = SqliteNodeStorageEngine::open_in_memory().await.unwrap();
    assert!(engine.fetch_states(&everything().and_member_of(ModelId::System(SystemModel::System))).await.unwrap().is_empty());
}

#[wasm_bindgen_test]
async fn list_materializations_reads_durable_registrations() {
    let engine = SqliteNodeStorageEngine::open_in_memory().await.unwrap();
    assert!(engine.list_materializations().await.unwrap().is_empty());

    let property = PropertyId::EntityId(entity_id(0xf1));
    let expected = [SystemModel::System, SystemModel::Model, SystemModel::Property].map(ModelId::System);
    for (index, model) in expected.iter().enumerate() {
        commit_state(&engine, Clock::default(), *model, state_with_strings(entity_id(0xf2 + index as u8), 1, &[(property, "x")])).await;
    }

    let mut found = engine.list_materializations().await.unwrap();
    found.sort();
    let mut expected = expected.to_vec();
    expected.sort();
    assert_eq!(found, expected);
}

/// Human labels only seed first-use physical assignments. Equal labels
/// remain distinct by durable identity, and every generated identifier is
/// normalized to lowercase.
#[wasm_bindgen_test]
async fn colliding_model_and_property_labels_get_distinct_lowercase_names() {
    let engine = SqliteNodeStorageEngine::open_in_memory().await.unwrap();
    engine
        .with_executor(|executor| {
            executor.execute(r#"CREATE TABLE "sales_report" ("application_value" TEXT)"#, &[])?;
            Ok(())
        })
        .await
        .unwrap();
    let model_a = ModelId::EntityId(entity_id(0x11));
    let model_b = ModelId::EntityId(entity_id(0x22));
    let property_a = PropertyId::EntityId(entity_id(0x33));
    let property_b = PropertyId::EntityId(entity_id(0x44));
    let resolver: Arc<dyn CatalogResolver> = Arc::new(TestResolver {
        model_names: BTreeMap::from([(model_a, "Sales Report".to_owned()), (model_b, "Sales Report".to_owned())]),
        property_names: BTreeMap::from([(property_a, "Display Name".to_owned()), (property_b, "Display Name".to_owned())]),
    });
    let _resolver = install_resolver(&engine, resolver);

    let state = state_with_strings(entity_id(0x55), 1, &[(property_a, "alpha"), (property_b, "beta")]);
    commit_state(&engine, Clock::default(), model_a, state).await;
    let state = state_with_strings(entity_id(0x56), 2, &[(property_a, "alpha"), (property_b, "beta")]);
    commit_state(&engine, Clock::default(), model_b, state).await;
    let (first, column_a) = physical_names(&engine, model_a, property_a).await;
    let (second, column_b) = physical_names(&engine, model_a, property_b).await;
    let (second_table, _) = physical_names(&engine, model_b, property_a).await;
    assert_eq!(first, second, "one model, one table");

    assert_ne!(first, second_table);
    assert_ne!(first, "sales_report");
    assert_ne!(second_table, "sales_report");
    assert!(first.starts_with("sales_report"));
    assert!(second_table.starts_with("sales_report"));
    assert_eq!(first, first.to_ascii_lowercase());
    assert_eq!(second_table, second_table.to_ascii_lowercase());

    assert_ne!(column_a, column_b);
    assert!(column_a.starts_with("display_name") && column_b.starts_with("display_name"));
    assert_eq!(column_a, column_a.to_ascii_lowercase());
    assert_eq!(column_b, column_b.to_ascii_lowercase());
}

/// A canonical write refreshes every materialization named by its explicit
/// canonical membership set.
#[wasm_bindgen_test]
async fn write_refreshes_every_canonical_membership_materialization() {
    let engine = SqliteNodeStorageEngine::open_in_memory().await.unwrap();
    let model_a = ModelId::EntityId(entity_id(0x61));
    let model_b = ModelId::EntityId(entity_id(0x62));
    let property_a = PropertyId::EntityId(entity_id(0x71));
    let property_b = PropertyId::EntityId(entity_id(0x72));
    let resolver: Arc<dyn CatalogResolver> = Arc::new(TestResolver {
        model_names: BTreeMap::from([(model_a, "Alpha".to_owned()), (model_b, "Beta".to_owned())]),
        property_names: BTreeMap::from([(property_a, "alpha".to_owned()), (property_b, "beta".to_owned())]),
    });
    let _resolver = install_resolver(&engine, resolver);
    let entity = entity_id(0x73);

    let initial = state_for_models(state_with_strings(entity, 1, &[(property_a, "a1"), (property_b, "b1")]), [model_a, model_b]);
    commit_canonical_state(&engine, Clock::default(), initial.clone()).await;
    let updated = state_for_models(state_with_strings(entity, 2, &[(property_a, "a2"), (property_b, "b2")]), [model_a, model_b]);
    commit_canonical_state(&engine, initial.payload.state.head, updated).await;

    let found = engine.fetch_states(&equals(property_b, "b2").and_member_of(model_b)).await.unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].payload.entity_id, entity);
}

#[wasm_bindgen_test]
async fn stale_head_rolls_back_the_complete_batch() {
    let engine = SqliteNodeStorageEngine::open_in_memory().await.unwrap();
    let model_a = ModelId::EntityId(entity_id(0x81));
    let model_b = ModelId::EntityId(entity_id(0x82));
    let property = PropertyId::EntityId(entity_id(0x83));
    let resolver: Arc<dyn CatalogResolver> = Arc::new(TestResolver {
        model_names: BTreeMap::from([(model_a, "Alpha".to_owned()), (model_b, "Beta".to_owned())]),
        property_names: BTreeMap::from([(property, "value".to_owned())]),
    });
    let _resolver = install_resolver(&engine, resolver);
    let first_id = entity_id(0x84);
    let second_id = entity_id(0x85);
    let first = state_with_strings(first_id, 1, &[(property, "first-old")]);
    let second = state_with_strings(second_id, 2, &[(property, "second-old")]);
    commit_state(&engine, Clock::default(), model_a, first.clone()).await;
    commit_state(&engine, Clock::default(), model_a, second.clone()).await;

    let mut transaction = engine.transaction();
    transaction
        .set_state(&first.payload.state.head, &state_for_model(state_with_strings(first_id, 3, &[(property, "first-new")]), model_b))
        .await
        .unwrap();
    transaction
        .set_state(&Clock::default(), &state_for_model(state_with_strings(second_id, 4, &[(property, "second-new")]), model_b))
        .await
        .unwrap();
    let outcome = transaction.commit().await.unwrap();
    let StorageCommitOutcome::Conflict { observed } = outcome else {
        panic!("one stale expected head must reject the complete batch");
    };
    assert_eq!(observed[&first_id].as_ref().unwrap().payload.state.head, first.payload.state.head);
    assert_eq!(observed[&second_id].as_ref().unwrap().payload.state.head, second.payload.state.head);
    assert_eq!(engine.get_state(first_id).await.unwrap().payload.state.head, first.payload.state.head);
    assert_eq!(engine.get_state(second_id).await.unwrap().payload.state.head, second.payload.state.head);

    assert!(
        engine.fetch_states(&everything().and_member_of(model_b)).await.unwrap().is_empty(),
        "a rejected batch must not publish associations or projections"
    );
}

#[wasm_bindgen_test]
fn sane_names() {
    assert!(SqliteNodeStorageEngine::sane_name("test_collection"));
    assert!(SqliteNodeStorageEngine::sane_name("test.collection"));
    assert!(SqliteNodeStorageEngine::sane_name("test:collection"));
    assert!(!SqliteNodeStorageEngine::sane_name("test;collection"));
    assert!(!SqliteNodeStorageEngine::sane_name("test'collection"));
}

/// node:sqlite's SQLite has the JSONB functions the engine relies on:
/// `jsonb()`, `json_extract()` with typed results, and numeric comparison.
#[wasm_bindgen_test]
async fn jsonb_functions_are_available() -> Result<(), SqliteNodeError> {
    let engine = SqliteNodeStorageEngine::open_in_memory().await.map_err(|e| SqliteNodeError::DDL(e.to_string()))?;

    let result =
        engine.with_executor(|executor| executor.query_row("SELECT jsonb('{\"key\": \"value\"}')", &[])?.expect("one row").blob(0)).await?;
    assert!(!result.is_empty(), "jsonb() function should return a non-empty BLOB");

    let result = engine
        .with_executor(|executor| {
            executor
                .query_row(r#"SELECT json_extract(jsonb('{"territory": "US", "count": 10}'), '$.territory')"#, &[])?
                .expect("one row")
                .text(0)
        })
        .await?;
    assert_eq!(result, "US", "JSON path extraction should return the SQL value");

    let result = engine
        .with_executor(|executor| {
            executor
                .query_row(
                    r#"SELECT json_extract(jsonb('{"count": 9}'), '$.count') > json_extract(jsonb('{"count": 10}'), '$.count')"#,
                    &[],
                )?
                .expect("one row")
                .integer(0)
        })
        .await?;
    assert_eq!(result, 0, "Numeric comparison: 9 > 10 should be false");

    Ok(())
}

#[wasm_bindgen_test]
async fn events_and_states_rollback_together_on_write_failure() -> anyhow::Result<()> {
    let engine = SqliteNodeStorageEngine::open_in_memory().await?;
    let property = PropertyId::System(ankurah_proto::SystemProperty::Name);
    let initial =
        [state_with_strings(entity_id(0x91), 1, &[(property, "before")]), state_with_strings(entity_id(0x92), 2, &[(property, "before")])];
    for state in &initial {
        commit_canonical_state(&engine, Clock::default(), state.clone()).await;
    }
    let mut events = Vec::new();
    let mut writes = Vec::new();
    for old in &initial {
        let event =
            Event::update(old.payload.entity_id, old.payload.state.head.clone(), ankurah_proto::AuthorId::Unknown, Default::default());
        let mut state = state_with_strings(old.payload.entity_id, 3, &[(property, "after")]);
        state.payload.state.head = event.id().into();
        events.push(Attested::opt(event, None));
        writes.push((old.payload.state.head.clone(), state));
    }
    let rejected = initial[1].payload.entity_id.to_base64();
    engine
        .with_executor(move |executor| {
            executor.execute(
                &format!(
                    r#"CREATE TRIGGER reject_second BEFORE UPDATE ON "_ankurah_entity"
                    WHEN NEW.id = '{rejected}' BEGIN SELECT RAISE(ABORT, 'test write failure'); END"#
                ),
                &[],
            )?;
            Ok(())
        })
        .await?;

    let mut transaction = engine.transaction();
    transaction.add_events(&events).await?;
    for (expected_head, state) in &writes {
        transaction.set_state(expected_head, state).await?;
    }
    assert!(transaction.commit().await.is_err());
    assert!(engine.get_events(events.iter().map(|event| event.payload.id()).collect(), &ankql::ast::Predicate::True).await?.is_empty());
    for state in &initial {
        assert_eq!(engine.get_state(state.payload.entity_id).await?.payload.state, state.payload.state);
    }

    engine
        .with_executor(|executor| {
            executor.execute("DROP TRIGGER reject_second", &[])?;
            Ok(())
        })
        .await?;
    let mut transaction = engine.transaction();
    transaction.add_events(&events).await?;
    for (expected_head, state) in &writes {
        transaction.set_state(expected_head, state).await?;
    }
    assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));
    assert_eq!(engine.get_events(events.iter().map(|event| event.payload.id()).collect(), &ankql::ast::Predicate::True).await?.len(), 2);
    for (_, state) in writes {
        assert_eq!(engine.get_state(state.payload.entity_id).await?.payload.state, state.payload.state);
    }
    Ok(())
}

/// The SQL builder emits json_extract() for JSON paths, the form node:sqlite
/// compares by SQL value.
#[wasm_bindgen_test]
fn json_path_query_uses_json_extract() -> anyhow::Result<()> {
    use ankql::parser::parse_selection;
    use ankurah_storage_sqlite_node::sql_builder::SqlBuilder;

    let selection = parse_selection(r#"data.status = 'active'"#).expect("Failed to parse query");
    let mut builder = SqlBuilder::with_fields(vec!["id", "state_buffer"]);
    builder.table_name("test_table");
    builder
        .selection(&ankql::selection::map_references(
            &selection,
            &|path| ColumnPath::new(path.steps[0].clone(), path.steps[1..].to_vec()),
            &|model| *model.as_id().expect("model ID in physical-column fixture"),
        ))
        .map_err(|e| SqliteNodeError::SqlGeneration(e.to_string()))?;

    let (sql, _params) = builder.build().map_err(|e| SqliteNodeError::SqlGeneration(e.to_string()))?;
    assert!(sql.contains(r#"json_extract("data", '$.status')"#), "SQL should extract from data column with $.status path: {}", sql);
    Ok(())
}

/// The full cycle the engine relies on: store JSONB through a `jsonb(?)`
/// placeholder, query it through json_extract() with a bound parameter.
#[wasm_bindgen_test]
async fn jsonb_storage_and_parameterized_query() -> Result<(), SqliteNodeError> {
    let engine = SqliteNodeStorageEngine::open_in_memory().await.map_err(|e| SqliteNodeError::DDL(e.to_string()))?;

    engine
        .with_executor(|executor| {
            executor.execute(r#"CREATE TABLE test_jsonb (id TEXT PRIMARY KEY, data BLOB)"#, &[])?;
            let json_text = r#"{"territory": "US", "count": 10}"#;
            executor.execute(
                r#"INSERT INTO test_jsonb (id, data) VALUES (?, jsonb(?))"#,
                &[SqliteValue::Text("1".into()), SqliteValue::Text(json_text.into())],
            )?;

            let count = executor.query_row("SELECT COUNT(*) FROM test_jsonb", &[])?.expect("one row").integer(0)?;
            assert_eq!(count, 1, "Should have 1 row");

            let data_type = executor.query_row("SELECT typeof(data) FROM test_jsonb WHERE id = '1'", &[])?.expect("one row").text(0)?;
            assert_eq!(data_type, "blob");

            let extracted = executor
                .query_row(r#"SELECT json_extract(data, '$.territory') FROM test_jsonb WHERE id = '1'"#, &[])?
                .expect("one row")
                .text(0)?;
            assert_eq!(extracted, "US");

            let result = executor
                .query_row(r#"SELECT id FROM test_jsonb WHERE json_extract(data, '$.territory') = ?"#, &[SqliteValue::Text("US".into())])?
                .map(|row| row.text(0))
                .transpose()?;
            assert_eq!(result.as_deref(), Some("1"), "Should find the row with territory = US");
            Ok(())
        })
        .await
}
