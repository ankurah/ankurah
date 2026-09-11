use ankurah_core::{
    indexing::{IndexKeyPart, KeySpec},
    property::backend::{lww::LWWBackend, PropertyBackend},
    schema::resolver::{resolve_selection, ModelResolutionError, ModelResolver, ResolvedProperty},
    storage::StorageEngine,
    value::{Value, ValueType},
};
use ankurah_proto::{CollectionId, EntityId, EntityState, EventId, ModelId, PropertyId, State, StateBuffers};
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
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

fn state(collection: &CollectionId, marker: u8, value_id: EntityId, value: Option<Value>, rank: i32) -> anyhow::Result<EntityState> {
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
        collection: collection.clone(),
        state: State {
            state_buffers: StateBuffers([(String::from("lww"), backend.to_state_buffer()?)].into()),
            head: vec![event_id].into(),
            ..State::default()
        },
    })
}

#[wasm_bindgen_test]
fn builtin_property_columns_are_unchanged() {
    use ankql::ast::{OrderByItem, OrderDirection, Predicate, Resolved, Selection, SystemProperty};

    for property in [PropertyId::Id, PropertyId::System(SystemProperty::Item), PropertyId::System(SystemProperty::Name)] {
        let selection = Selection::<Resolved> {
            predicate: Predicate::True,
            order_by: Some(vec![OrderByItem { path: property.into(), direction: OrderDirection::Asc }]),
            limit: None,
        };
        let lowered = ankurah_storage_indexeddb_wasm::lower::lower(&selection, &"key_paths".into());
        assert_eq!(lowered.order_by.unwrap()[0].path.column, property.to_string());
    }
}

#[wasm_bindgen_test]
async fn invalid_property_key_paths_use_encoded_native_index() -> anyhow::Result<()> {
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
        let collection_id: CollectionId = "key_paths".into();
        let collection = engine.collection(&collection_id).await?;
        for (marker, value, rank) in [(1, Some("match"), 20), (2, Some("match"), 10), (3, Some("miss"), 99), (4, None, 100)] {
            let value = value.map(|value| Value::String(value.into()));
            collection.set_state(state(&collection_id, marker, value_id, value, rank)?.into()).await?;
        }
        let other_id: CollectionId = "key_paths_other".into();
        let other = engine.collection(&other_id).await?;
        other.set_state(state(&other_id, 5, value_id, Some(Value::String("match".into())), 0)?.into()).await?;

        let resolve = |query: &str| {
            resolve_selection(
                &ModelId::EntityId(EntityId::from_bytes([0x77; 32])),
                &FixtureResolver { value_id, value_type: ValueType::String },
                ankql::parser::parse_selection(query).unwrap(),
            )
            .unwrap()
        };
        // The first query must index the encoded property and exclude the
        // missing-field row natively, without changing predicate semantics.
        let matches = collection.fetch_states(&resolve("value = 'match'")).await?;
        let ids: std::collections::BTreeSet<_> = matches.iter().map(|row| row.payload.entity_id).collect();
        assert_eq!(ids, [EntityId::from_bytes([1; 32]), EntityId::from_bytes([2; 32])].into());
        {
            let transaction = engine.db.get_connection().await.transaction_with_str("entities").unwrap();
            let store = transaction.object_store("entities").unwrap();
            let encoded = format!("p${}", value_id.to_string().replace('-', "$"));
            let index =
                KeySpec::new(vec![IndexKeyPart::asc("__collection", ValueType::String), IndexKeyPart::asc(encoded, ValueType::String)]);
            assert!(store.index_names().contains(&index.name_with("", "__")));
            assert!(!store.index_names().contains("__collection asc"), "property IDs must not require a fallback scan");
        }

        // Compound indexes must use the same encoding, preserve ordering and
        // exclude missing/nonmatching rows before applying the limit.
        let first = collection.fetch_states(&resolve("value = 'match' ORDER BY rank ASC LIMIT 1")).await?;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].payload.entity_id, EntityId::from_bytes([2; 32]));
        let last = collection.fetch_states(&resolve("value = 'match' ORDER BY rank DESC LIMIT 1")).await?;
        assert_eq!(last.len(), 1);
        assert_eq!(last[0].payload.entity_id, EntityId::from_bytes([1; 32]));

        drop(other);
        drop(collection);
        drop(engine);
        IndexedDBStorageEngine::cleanup(&db_name).await?;
    }
    Ok(())
}
