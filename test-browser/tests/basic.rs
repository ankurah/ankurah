#![cfg(target_arch = "wasm32")]

mod common;
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use anyhow::Result;
use common::*;
use std::sync::Arc;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn test_wasm_indexeddb() -> Result<()> {
    let storage_engine = IndexedDBStorageEngine::open("ankurah_test").await?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new()).context(c);

    let trx = node.begin();
    let _album = trx.create(&Album { name: "The rest of the owl".to_owned(), year: "2024".to_owned() }).await?;

    trx.commit().await?;

    Ok(())
}
