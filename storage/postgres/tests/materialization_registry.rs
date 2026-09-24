mod common;

use ankql::ast::PropertyId;
use ankurah::core::{
    property::backend::{lww::LWWBackend, PropertyBackend},
    schema::CatalogResolver,
    storage::{StorageCommitOutcome, StorageEngine, StorageTransaction},
    value::Value,
};
use ankurah::proto::{Attested, Clock, EntityId, EntityState, EventId, ModelId, State, StateBuffers};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

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

async fn commit_canonical_state(
    engine: &ankurah_storage_postgres::Postgres,
    expected_head: Clock,
    state: Attested<EntityState>,
) -> anyhow::Result<()> {
    let mut transaction = engine.transaction();
    transaction.set_state(&expected_head, &state).await?;
    let outcome = transaction.commit().await?;
    assert!(matches!(outcome, StorageCommitOutcome::Committed(_)));
    Ok(())
}

async fn commit_state(
    engine: &ankurah_storage_postgres::Postgres,
    expected_head: Clock,
    model: ModelId,
    state: Attested<EntityState>,
) -> anyhow::Result<()> {
    commit_canonical_state(engine, expected_head, state_for_model(state, model)).await
}

#[tokio::test]
async fn colliding_labels_remain_distinct_and_lowercase() -> anyhow::Result<()> {
    let (_container, engine, pool) = common::create_postgres_container_with_pool().await?;
    {
        let client = pool.get().await?;
        client.execute(r#"CREATE TABLE "sales_report" ("application_value" TEXT)"#, &[]).await?;
    }
    let model_a = ModelId::EntityId(entity_id(0x11));
    let model_b = ModelId::EntityId(entity_id(0x22));
    let property_a = PropertyId::EntityId(entity_id(0x33));
    let property_b = PropertyId::EntityId(entity_id(0x44));
    let resolver: Arc<dyn CatalogResolver> = Arc::new(TestResolver {
        model_names: BTreeMap::from([(model_a, "Sales Report".to_owned()), (model_b, "Sales Report".to_owned())]),
        property_names: BTreeMap::from([(property_a, "Display Name".to_owned()), (property_b, "Display Name".to_owned())]),
    });
    engine.set_catalog_resolver(Arc::downgrade(&resolver));

    commit_state(
        &engine,
        Clock::default(),
        model_a,
        state_with_strings(entity_id(0x55), 1, &[(property_a, "alpha"), (property_b, "beta")]),
    )
    .await?;
    commit_state(&engine, Clock::default(), model_b, state_with_strings(entity_id(0x56), 2, &[(property_a, "other")])).await?;

    let client = pool.get().await?;
    let rows = client
        .query(
            r#"SELECT "materialization_table_name"
               FROM "_ankurah_postgres_model_map"
               ORDER BY "materialization_table_name""#,
            &[],
        )
        .await?;
    let tables: Vec<String> = rows.into_iter().map(|row| row.get(0)).collect();
    assert_eq!(tables.len(), 2);
    assert_ne!(tables[0], tables[1]);
    assert!(tables.iter().all(|table| table != "sales_report"), "an existing application table also occupies its physical name");
    assert!(tables.iter().all(|table| table.starts_with("sales_report")));
    assert!(tables.iter().all(|table| table == &table.to_ascii_lowercase()));

    let model_key = bincode::serialize(&model_a)?;
    let rows = client
        .query(
            r#"SELECT "column_name"
               FROM "_ankurah_postgres_column_map"
               WHERE "model_key" = $1 AND "column_name" != 'id'
               ORDER BY "column_name""#,
            &[&model_key],
        )
        .await?;
    let columns: Vec<String> = rows.into_iter().map(|row| row.get(0)).collect();
    assert_eq!(columns.len(), 2);
    assert_ne!(columns[0], columns[1]);
    assert!(columns.iter().all(|column| column.starts_with("display_name")));
    assert!(columns.iter().all(|column| column == &column.to_ascii_lowercase()));
    Ok(())
}

#[tokio::test]
async fn long_labels_remain_distinct_within_postgres_identifier_limit() -> anyhow::Result<()> {
    let (_container, engine, pool) = common::create_postgres_container_with_pool().await?;
    let model_a = ModelId::EntityId(entity_id(0x11));
    let model_b = ModelId::EntityId(entity_id(0x22));
    let property_a = PropertyId::EntityId(entity_id(0x33));
    let property_b = PropertyId::EntityId(entity_id(0x44));
    let model_prefix = "shared_model_prefix_".repeat(5);
    let property_prefix = "shared_property_prefix_".repeat(4);
    let resolver: Arc<dyn CatalogResolver> = Arc::new(TestResolver {
        model_names: BTreeMap::from([(model_a, format!("{model_prefix}alpha")), (model_b, format!("{model_prefix}beta"))]),
        property_names: BTreeMap::from([(property_a, format!("{property_prefix}alpha")), (property_b, format!("{property_prefix}beta"))]),
    });
    engine.set_catalog_resolver(Arc::downgrade(&resolver));

    let entity_a = entity_id(0x55);
    let entity_b = entity_id(0x66);
    commit_state(&engine, Clock::default(), model_a, state_with_strings(entity_a, 1, &[(property_a, "alpha"), (property_b, "beta")]))
        .await?;
    commit_state(&engine, Clock::default(), model_b, state_with_strings(entity_b, 2, &[(property_a, "hidden")])).await?;

    let all = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
    let model_a_states = engine.fetch_states(&all.clone().and_member_of(model_a)).await?;
    let model_b_states = engine.fetch_states(&all.clone().and_member_of(model_b)).await?;
    assert_eq!(model_a_states.iter().map(|state| state.payload.entity_id).collect::<Vec<_>>(), vec![entity_a]);
    assert_eq!(model_b_states.iter().map(|state| state.payload.entity_id).collect::<Vec<_>>(), vec![entity_b]);

    let client = pool.get().await?;
    let rows = client
        .query(
            r#"SELECT "materialization_table_name"
               FROM "_ankurah_postgres_model_map"
               ORDER BY "materialization_table_name""#,
            &[],
        )
        .await?;
    let tables: Vec<String> = rows.into_iter().map(|row| row.get(0)).collect();
    assert_eq!(tables.len(), 2);
    assert_ne!(tables[0], tables[1]);
    assert!(tables.iter().all(|table| table.len() <= 63));

    let existing_rows = client.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", &[]).await?;
    let existing: BTreeSet<String> = existing_rows.into_iter().map(|row| row.get(0)).collect();
    assert!(tables.iter().all(|table| existing.contains(table)));

    let model_key = bincode::serialize(&model_a)?;
    let rows = client
        .query(
            r#"SELECT "column_name"
               FROM "_ankurah_postgres_column_map"
               WHERE "model_key" = $1 AND "column_name" != 'id'
               ORDER BY "column_name""#,
            &[&model_key],
        )
        .await?;
    let columns: Vec<String> = rows.into_iter().map(|row| row.get(0)).collect();
    assert_eq!(columns.len(), 2);
    assert_ne!(columns[0], columns[1]);
    assert!(columns.iter().all(|column| column.len() <= 63));
    Ok(())
}

#[tokio::test]
async fn write_refreshes_every_canonical_membership_materialization() -> anyhow::Result<()> {
    let (_container, engine, _pool) = common::create_postgres_container_with_pool().await?;
    let model_a = ModelId::EntityId(entity_id(0x61));
    let model_b = ModelId::EntityId(entity_id(0x62));
    let property_a = PropertyId::EntityId(entity_id(0x71));
    let property_b = PropertyId::EntityId(entity_id(0x72));
    // The second model and property must work without human-readable labels.
    let resolver: Arc<dyn CatalogResolver> = Arc::new(TestResolver {
        model_names: BTreeMap::from([(model_a, "Alpha".to_owned())]),
        property_names: BTreeMap::from([(property_a, "alpha".to_owned())]),
    });
    engine.set_catalog_resolver(Arc::downgrade(&resolver));
    let entity = entity_id(0x73);

    let initial = state_for_models(state_with_strings(entity, 1, &[(property_a, "a1"), (property_b, "b1")]), [model_a, model_b]);
    commit_canonical_state(&engine, Clock::default(), initial.clone()).await?;
    let updated = state_for_models(state_with_strings(entity, 2, &[(property_a, "a2"), (property_b, "b2")]), [model_a, model_b]);
    commit_canonical_state(&engine, initial.payload.state.head, updated.clone()).await?;

    let selection: ankql::ast::Selection<ankql::ast::Resolved> = ankql::ast::Predicate::Comparison {
        left: Box::new(ankql::ast::Expr::Path(property_b.into())),
        operator: ankql::ast::ComparisonOperator::Equal,
        right: Box::new(ankql::ast::Expr::Literal(Value::String("b2".into()))),
    }
    .into();
    let found = engine.fetch_states(&selection.clone().and_member_of(model_b)).await?;
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].payload.entity_id, entity);
    let removed = state_for_models(state_with_strings(entity, 3, &[(property_a, "a3")]), [model_a, model_b]);
    commit_canonical_state(&engine, updated.payload.state.head, removed).await?;
    assert!(engine.fetch_states(&selection.clone().and_member_of(model_b)).await?.is_empty(), "removed properties must not retain old values");

    Ok(())
}

#[tokio::test]
async fn stale_head_rolls_back_the_complete_batch() -> anyhow::Result<()> {
    let (_container, engine, _pool) = common::create_postgres_container_with_pool().await?;
    let model_a = ModelId::EntityId(entity_id(0x81));
    let model_b = ModelId::EntityId(entity_id(0x82));
    let property = PropertyId::EntityId(entity_id(0x83));
    let resolver: Arc<dyn CatalogResolver> = Arc::new(TestResolver {
        model_names: BTreeMap::from([(model_a, "Alpha".to_owned()), (model_b, "Beta".to_owned())]),
        property_names: BTreeMap::from([(property, "value".to_owned())]),
    });
    engine.set_catalog_resolver(Arc::downgrade(&resolver));
    let first_id = entity_id(0x84);
    let second_id = entity_id(0x85);
    let first = state_with_strings(first_id, 1, &[(property, "first-old")]);
    let second = state_with_strings(second_id, 2, &[(property, "second-old")]);
    commit_state(&engine, Clock::default(), model_a, first.clone()).await?;
    commit_state(&engine, Clock::default(), model_a, second.clone()).await?;

    let mut transaction = engine.transaction();
    transaction.set_state(
        &first.payload.state.head,
        &state_for_model(state_with_strings(first_id, 3, &[(property, "first-new")]), model_b),
    ).await?;
    transaction.set_state(
        &Clock::default(),
        &state_for_model(state_with_strings(second_id, 4, &[(property, "second-new")]), model_b),
    ).await?;
    let outcome = transaction.commit().await?;
    let StorageCommitOutcome::Conflict { observed } = outcome else {
        anyhow::bail!("one stale expected head must reject the complete batch");
    };
    assert_eq!(observed[&first_id].as_ref().unwrap().payload.state.head, first.payload.state.head);
    assert_eq!(observed[&second_id].as_ref().unwrap().payload.state.head, second.payload.state.head);
    assert_eq!(engine.get_state(first_id).await?.payload.state.head, first.payload.state.head);
    assert_eq!(engine.get_state(second_id).await?.payload.state.head, second.payload.state.head);

    let all = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
    assert!(engine.fetch_states(&all.clone().and_member_of(model_b)).await?.is_empty(), "a rejected batch must not publish associations or projections");
    Ok(())
}

#[tokio::test]
async fn independent_engines_cannot_commit_the_same_expected_head() -> anyhow::Result<()> {
    let (_container, first, pool) = common::create_postgres_container_with_pool().await?;
    let second = ankurah_storage_postgres::Postgres::new(pool).await?;
    let model = ModelId::EntityId(entity_id(0xa1));
    let property = PropertyId::EntityId(entity_id(0xa2));
    let id = entity_id(0xa3);
    let mut expected = Clock::default();

    // Exercise both a competing insert (no row to lock) and a competing update.
    for round in [0, 1] {
        let left = state_for_model(state_with_strings(id, 10 + round * 2, &[(property, "left")]), model);
        let right = state_for_model(state_with_strings(id, 11 + round * 2, &[(property, "right")]), model);
        let mut left_trx = first.transaction();
        left_trx.set_state(&expected, &left).await?;
        let mut right_trx = second.transaction();
        right_trx.set_state(&expected, &right).await?;

        let (left_result, right_result) = tokio::join!(left_trx.commit(), right_trx.commit());
        let (winner, observed) = match (left_result?, right_result?) {
            (StorageCommitOutcome::Committed(_), StorageCommitOutcome::Conflict { observed }) => (left, observed),
            (StorageCommitOutcome::Conflict { observed }, StorageCommitOutcome::Committed(_)) => (right, observed),
            outcomes => panic!("exactly one writer must commit: {outcomes:?}"),
        };
        assert_eq!(observed[&id].as_ref().unwrap(), &winner);
        assert_eq!(first.get_state(id).await?, winner);
        assert_eq!(second.get_state(id).await?, winner);
        let projected = second.fetch_states(&ankql::ast::Predicate::MemberOf(model).into()).await?;
        assert_eq!(projected, vec![winner.clone()]);
        expected = winner.payload.state.head;
    }
    Ok(())
}

#[tokio::test]
async fn events_and_states_rollback_together_on_write_failure() -> anyhow::Result<()> {
    let (_container, engine, pool) = common::create_postgres_container_with_pool().await?;
    let property = PropertyId::System(ankurah::proto::SystemProperty::Name);
    let initial =
        [state_with_strings(entity_id(0x91), 1, &[(property, "before")]), state_with_strings(entity_id(0x92), 2, &[(property, "before")])];
    for state in &initial {
        commit_canonical_state(&engine, Clock::default(), state.clone()).await?;
    }
    let mut events = Vec::new();
    let mut writes = Vec::new();
    for old in &initial {
        let event = ankurah::proto::Event::update(
            old.payload.entity_id,
            old.payload.state.head.clone(),
            ankurah::proto::AuthorId::Unknown,
            Default::default(),
        );
        let mut state = state_with_strings(old.payload.entity_id, 3, &[(property, "after")]);
        state.payload.state.head = event.id().into();
        events.push(Attested::opt(event, None));
        writes.push((old.payload.state.head.clone(), state));
    }
    let connection = pool.get().await?;
    let rejected = initial[1].payload.entity_id.to_base64();
    connection
        .batch_execute(&format!(
            r#"
        CREATE FUNCTION reject_second() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.id = '{rejected}' THEN
                RAISE EXCEPTION 'test write failure';
            END IF;
            RETURN NEW;
        END $$;
        CREATE TRIGGER reject_second BEFORE UPDATE ON "_ankurah_entity"
            FOR EACH ROW EXECUTE FUNCTION reject_second();
    "#
        ))
        .await?;
    let mut transaction = engine.transaction();
    transaction.add_events(&events).await?;
    for (expected_head, state) in &writes {
        transaction.set_state(expected_head, state).await?;
    }
    let error = transaction.commit().await.unwrap_err();
    assert!(format!("{error:?}").contains("test write failure"), "{error:?}");
    assert!(engine.get_events(events.iter().map(|event| event.payload.id()).collect(), &ankql::ast::Predicate::True).await?.is_empty());
    for state in &initial {
        assert_eq!(engine.get_state(state.payload.entity_id).await?.payload.state, state.payload.state);
    }

    connection.batch_execute(r#"DROP TRIGGER reject_second ON "_ankurah_entity"; DROP FUNCTION reject_second();"#).await?;
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
