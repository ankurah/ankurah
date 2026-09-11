use ankurah_core::{
    error::RetrievalError,
    policy::{PermissiveAgent, DEFAULT_CONTEXT},
    property::backend::{lww::LWWBackend, PropertyBackend},
    schema::{catalog::SysModelRowView, MODEL_COLLECTION_ID},
    storage::StorageEngine,
    value::Value,
    Node,
};
use ankurah_proto::{Attested, AuthorId, CollectionId, EntityState, Event, OperationSet, PropertyId, State, StateBuffers, SystemProperty};
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use std::sync::Arc;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

fn row(collection: &CollectionId, name: &str) -> anyhow::Result<Attested<EntityState>> {
    let backend = LWWBackend::new();
    backend.set(PropertyId::System(SystemProperty::Name), Some(Value::String(name.into())));
    backend.set(PropertyId::System(SystemProperty::Label), Some(Value::String(name.into())));
    let operations = backend.to_operations()?.unwrap();
    let event = Event::genesis(
        collection.clone(),
        None,
        AuthorId::Unknown,
        OperationSet::from_backends([("lww".into(), operations.clone())].into()),
    );
    backend.apply_operations_with_event(&operations, event.id())?;
    let state = EntityState {
        entity_id: event.entity_id,
        collection: collection.clone(),
        state: State {
            state_buffers: StateBuffers([("lww".into(), backend.to_state_buffer()?)].into()),
            head: vec![event.id()].into(),
            ..State::default()
        },
    };
    Ok(state.into())
}

#[wasm_bindgen_test]
async fn catalog_get_cannot_relabel_a_nonresident_private_entity() -> anyhow::Result<()> {
    console_error_panic_hook::set_once();
    let name = format!("test_catalog_get_boundary_{}", ulid::Ulid::new());
    let engine = Arc::new(IndexedDBStorageEngine::open(&name).await?);
    let private = row(&"private".into(), "secret")?;
    let catalog = row(&CollectionId::fixed_name(MODEL_COLLECTION_ID), "public")?;
    engine.collection(&private.payload.collection).await?.set_state(private.clone()).await?;
    engine.collection(&catalog.payload.collection).await?.set_state(catalog.clone()).await?;
    {
        let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
        node.system.create().await?;
        node.wait_ready().await?;
        let context = node.context(DEFAULT_CONTEXT)?;
        assert!(matches!(context.get::<SysModelRowView>(private.payload.entity_id).await, Err(RetrievalError::EntityNotFound(_))));
        assert_eq!(context.get::<SysModelRowView>(catalog.payload.entity_id).await?.name()?, "public");
    }
    drop(engine);
    IndexedDBStorageEngine::cleanup(&name).await?;
    Ok(())
}
