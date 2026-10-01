use ankurah_core::{
    error::RetrievalError,
    model::Model,
    policy::{PermissiveAgent, DEFAULT_CONTEXT},
    schema::catalog::SysModelRowView,
    Node,
};
use ankurah_proto::ModelId;
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[derive(ankurah::Model, Debug, Serialize, Deserialize)]
#[model(no_ffi)]
pub struct Private {
    name: String,
}

#[wasm_bindgen_test]
async fn catalog_get_cannot_relabel_a_nonresident_private_entity() -> anyhow::Result<()> {
    console_error_panic_hook::set_once();
    let name = format!("test_catalog_get_boundary_{}", ulid::Ulid::new());
    let engine = Arc::new(IndexedDBStorageEngine::open(&name).await?);
    {
        let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
        node.system.create().await?;
        node.wait_ready().await?;
        let context = node.context(DEFAULT_CONTEXT)?;
        let private_id = {
            let trx = context.begin();
            let id = trx.create(&Private { name: "secret".into() }).await?.id();
            trx.commit().await?;
            id
        };
        assert!(matches!(context.get::<SysModelRowView>(private_id).await, Err(RetrievalError::MissingComponent { .. })));
        let ModelId::EntityId(model_id) = Private::descriptor().resolved.get(node.system.system_epoch().unwrap()).unwrap() else {
            unreachable!("user model")
        };
        assert_eq!(context.get::<SysModelRowView>(model_id).await?.name()?, "Private");
    }
    engine.db.close().await;
    drop(engine);
    IndexedDBStorageEngine::cleanup(&name).await?;
    Ok(())
}
