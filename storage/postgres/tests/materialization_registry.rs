mod common;

use ankql::ast::PropertyId;
use ankurah::core::{
    property::backend::{lww::LWWBackend, PropertyBackend},
    schema::CatalogResolver,
    storage::{CommitBatchOutcome, PreparedEntityWrite, StorageEngine, StorageWriteBatch},
    value::{Value, ValueType},
};
use ankurah::proto::{Attested, Clock, EntityId, EntityState, EventId, ModelId, State, StateBuffers};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Default)]
struct TestResolver {
    model_names: BTreeMap<ModelId, String>,
    model_properties: BTreeMap<ModelId, Vec<PropertyId>>,
    property_names: BTreeMap<PropertyId, String>,
}

impl CatalogResolver for TestResolver {
    fn resolve_model_property(&self, model: &ModelId, name: &str) -> anyhow::Result<Option<PropertyId>> {
        Ok(self
            .model_properties
            .get(model)
            .into_iter()
            .flatten()
            .find_map(|property| (self.property_names.get(property).map(String::as_str) == Some(name)).then_some(*property)))
    }

    fn model_properties(&self, model: &ModelId) -> anyhow::Result<Vec<PropertyId>> {
        Ok(self.model_properties.get(model).cloned().unwrap_or_default())
    }

    fn model_name(&self, model: &ModelId) -> anyhow::Result<String> {
        self.model_names.get(model).cloned().ok_or_else(|| anyhow::anyhow!("unknown model {model}"))
    }

    fn property_name(&self, property: &PropertyId) -> anyhow::Result<String> {
        self.property_names.get(property).cloned().ok_or_else(|| anyhow::anyhow!("unknown property {property}"))
    }

    fn property_value_type(&self, property: &PropertyId) -> anyhow::Result<ValueType> {
        self.property_names
            .contains_key(property)
            .then_some(ValueType::String)
            .ok_or_else(|| anyhow::anyhow!("unknown property {property}"))
    }
}

fn entity_id(byte: u8) -> EntityId { EntityId::from_bytes([byte; 16]) }

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
                head: Clock::from(vec![event_id]),
            },
        },
        None,
    )
}

async fn commit_state(
    engine: &ankurah_storage_postgres::Postgres,
    expected_head: Clock,
    model: ModelId,
    state: Attested<EntityState>,
) -> anyhow::Result<()> {
    let outcome = engine.commit_batch(StorageWriteBatch::new(vec![PreparedEntityWrite::new(expected_head, state, [model])])).await?;
    assert!(matches!(outcome, CommitBatchOutcome::Committed(_)));
    Ok(())
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
        model_properties: BTreeMap::from([(model_a, vec![property_a, property_b]), (model_b, Vec::new())]),
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
    // A query touch assigns the second model's durable materialization name
    // without creating an entity association.
    engine.fetch_states(&model_b, &ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None }).await?;

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
async fn write_refreshes_every_associated_model_materialization() -> anyhow::Result<()> {
    let (_container, engine, _pool) = common::create_postgres_container_with_pool().await?;
    let model_a = ModelId::EntityId(entity_id(0x61));
    let model_b = ModelId::EntityId(entity_id(0x62));
    let property_a = PropertyId::EntityId(entity_id(0x71));
    let property_b = PropertyId::EntityId(entity_id(0x72));
    let resolver: Arc<dyn CatalogResolver> = Arc::new(TestResolver {
        model_names: BTreeMap::from([(model_a, "Alpha".to_owned()), (model_b, "Beta".to_owned())]),
        model_properties: BTreeMap::from([(model_a, vec![property_a]), (model_b, vec![property_b])]),
        property_names: BTreeMap::from([(property_a, "alpha".to_owned()), (property_b, "beta".to_owned())]),
    });
    engine.set_catalog_resolver(Arc::downgrade(&resolver));
    let entity = entity_id(0x73);

    let initial = state_with_strings(entity, 1, &[(property_a, "a1"), (property_b, "b1")]);
    commit_state(&engine, Clock::default(), model_a, initial.clone()).await?;
    commit_state(&engine, initial.payload.state.head.clone(), model_b, initial.clone()).await?;
    commit_state(&engine, initial.payload.state.head, model_a, state_with_strings(entity, 2, &[(property_a, "a2"), (property_b, "b2")]))
        .await?;

    let selection = ankql::parser::parse_selection("beta = 'b2'")?.resolve_names(&model_b, resolver.as_ref())?;
    let found = engine.fetch_states(&model_b, &selection).await?;
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].payload.entity_id, entity);
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
        model_properties: BTreeMap::from([(model_a, vec![property]), (model_b, vec![property])]),
        property_names: BTreeMap::from([(property, "value".to_owned())]),
    });
    engine.set_catalog_resolver(Arc::downgrade(&resolver));
    let first_id = entity_id(0x84);
    let second_id = entity_id(0x85);
    let first = state_with_strings(first_id, 1, &[(property, "first-old")]);
    let second = state_with_strings(second_id, 2, &[(property, "second-old")]);
    commit_state(&engine, Clock::default(), model_a, first.clone()).await?;
    commit_state(&engine, Clock::default(), model_a, second.clone()).await?;

    let outcome = engine
        .commit_batch(StorageWriteBatch::new(vec![
            PreparedEntityWrite::new(
                first.payload.state.head.clone(),
                state_with_strings(first_id, 3, &[(property, "first-new")]),
                [model_b],
            ),
            PreparedEntityWrite::new(Clock::default(), state_with_strings(second_id, 4, &[(property, "second-new")]), [model_b]),
        ]))
        .await?;
    let CommitBatchOutcome::Conflict { observed } = outcome else {
        anyhow::bail!("one stale expected head must reject the complete batch");
    };
    assert_eq!(observed[&first_id].as_ref().unwrap().payload.state.head, first.payload.state.head);
    assert_eq!(observed[&second_id].as_ref().unwrap().payload.state.head, second.payload.state.head);
    assert_eq!(engine.get_state(first_id).await?.payload.state.head, first.payload.state.head);
    assert_eq!(engine.get_state(second_id).await?.payload.state.head, second.payload.state.head);

    let all = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
    assert!(engine.fetch_states(&model_b, &all).await?.is_empty(), "a rejected batch must not publish associations or projections");
    Ok(())
}
