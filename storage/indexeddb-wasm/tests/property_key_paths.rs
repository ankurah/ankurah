use ankurah_core::{
    indexing::{IndexKeyPart, KeySpec},
    property::backend::{lww::LWWBackend, PropertyBackend},
    schema::catalog::resolver::{resolve_selection, ModelResolutionError, ModelResolver, ResolvedProperty},
    storage::{CatalogResolver, StorageCommitOutcome, StorageEngine, StorageTransaction},
    value::{Value, ValueType},
};
use ankurah_proto::{EntityId, EntityState, EventId, ModelId, PropertyId, State, StateBuffers};
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use std::sync::Arc;
use wasm_bindgen_test::*;
wasm_bindgen_test_configure!(run_in_browser);

struct FixtureResolver {
    value_id: EntityId,
    value_type: ValueType,
}

fn rank_id() -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([0x61; 32])) }

impl ModelResolver for FixtureResolver {
    fn resolve_property(&self, _model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
        Ok(match name {
            "value" => Some(ResolvedProperty { id: PropertyId::EntityId(self.value_id), value_type: self.value_type }),
            "rank" => Some(ResolvedProperty { id: rank_id(), value_type: ValueType::I32 }),
            _ => None,
        })
    }
}

#[async_trait::async_trait]
impl CatalogResolver for FixtureResolver {
    async fn get_model_label(&self, model: &ModelId) -> Option<String> { Some(format!("key_paths_{model}")) }
    async fn get_property_label(&self, property: &PropertyId) -> Option<String> {
        if *property == PropertyId::EntityId(self.value_id) {
            Some(property.to_string())
        } else if *property == rank_id() {
            Some("rank".into())
        } else {
            None
        }
    }
}

async fn commit(engine: &IndexedDBStorageEngine, state: EntityState) -> anyhow::Result<()> {
    let mut transaction = engine.transaction();
    transaction.set_state(&Default::default(), &state.into()).await?;
    let result = transaction.commit().await?;
    assert!(matches!(result, StorageCommitOutcome::Committed(_)));
    Ok(())
}

fn state(model: &ModelId, marker: u8, value_id: EntityId, value: Option<Value>, rank: i32) -> anyhow::Result<EntityState> {
    let backend = LWWBackend::new();
    if let Some(value) = value {
        backend.set(PropertyId::EntityId(value_id), Some(value));
    }
    backend.set(rank_id(), Some(Value::I32(rank)));
    let event_id = EventId::from_bytes([marker; 32]);
    let operations = backend.to_operations()?.expect("fixture has writes");
    backend.apply_operations_with_event(&operations, event_id.clone())?;
    Ok(EntityState {
        entity_id: EntityId::from_bytes([marker; 32]),
        state: State {
            state_buffers: StateBuffers([(String::from("lww"), backend.to_state_buffer()?)].into()),
            head: vec![event_id].into(),
            memberships: [*model].into(),
            ..State::default()
        },
    })
}

#[wasm_bindgen_test]
async fn invalid_display_names_use_valid_native_indexes() -> anyhow::Result<()> {
    console_error_panic_hook::set_once();
    let leading_digit = EntityId::from_bytes([0xd0; 32]);
    let mut hyphen_bytes = [0; 32];
    hyphen_bytes[1] = 0x0f;
    hyphen_bytes[2] = 0x80;
    let inner_hyphen = EntityId::from_bytes(hyphen_bytes);
    assert!(PropertyId::EntityId(leading_digit).to_string().starts_with('0'));
    let rendered = PropertyId::EntityId(inner_hyphen).to_string();
    assert!(rendered.starts_with('A') && rendered.contains('-'));

    for value_id in [leading_digit, inner_hyphen] {
        let db_name = format!("test_property_key_path_{}", ulid::Ulid::new());
        let engine = IndexedDBStorageEngine::open(&db_name).await?;
        let model_id = ModelId::EntityId(EntityId::from_bytes([0x77; 32]));
        let resolver = Arc::new(FixtureResolver { value_id, value_type: ValueType::String });
        let catalog: Arc<dyn CatalogResolver> = resolver.clone();
        engine.set_catalog_resolver(Arc::downgrade(&catalog));
        for (marker, value, rank) in [(1, Some("match"), 20), (2, Some("match"), 10), (3, Some("miss"), 99), (4, None, 100)] {
            let value = value.map(|value| Value::String(value.into()));
            commit(&engine, state(&model_id, marker, value_id, value, rank)?).await?;
        }
        let other_id = ModelId::EntityId(EntityId::from_bytes([0x78; 32]));
        commit(&engine, state(&other_id, 5, value_id, Some(Value::String("match".into())), 0)?).await?;

        let resolve =
            |query: &str| resolve_selection(&model_id, resolver.as_ref(), ankql::parser::parse_selection(query).unwrap()).unwrap();
        // The assigned column must support a native index, including with an invalid key-path seed.
        let matches = engine.fetch_states(&resolve("value = 'match'").and_member_of(model_id)).await?;
        let ids: std::collections::BTreeSet<_> = matches.iter().map(|row| row.payload.entity_id).collect();
        assert_eq!(ids, [EntityId::from_bytes([1; 32]), EntityId::from_bytes([2; 32])].into());
        {
            let transaction = engine.db.get_connection().await.transaction_with_str("materializations").unwrap();
            let store = transaction.object_store("materializations").unwrap();
            let column = ankurah_storage_common::naming::sanitize(&value_id.to_string());
            let index =
                KeySpec::new(vec![IndexKeyPart::asc("__materialization", ValueType::String), IndexKeyPart::asc(column, ValueType::String)]);
            assert!(store.index_names().contains(&index.name_with("", "__")));
            assert!(!store.index_names().contains("__materialization asc"), "invalid display names must not require a fallback scan");
        }

        // Compound indexes use the same assignment and apply LIMIT after filtering.
        let first = engine.fetch_states(&resolve("value = 'match' ORDER BY rank ASC LIMIT 1").and_member_of(model_id)).await?;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].payload.entity_id, EntityId::from_bytes([2; 32]));
        let last = engine.fetch_states(&resolve("value = 'match' ORDER BY rank DESC LIMIT 1").and_member_of(model_id)).await?;
        assert_eq!(last.len(), 1);
        assert_eq!(last[0].payload.entity_id, EntityId::from_bytes([1; 32]));

        engine.db.close().await;
        drop(engine);
        IndexedDBStorageEngine::cleanup(&db_name).await?;
    }
    Ok(())
}
