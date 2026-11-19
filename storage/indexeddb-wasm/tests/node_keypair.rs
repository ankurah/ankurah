//! A node's keypair on IndexedDB, which refuses the commit of a tab that
//! loses a race instead of making it wait.

mod common;

use std::sync::Arc;

use ankurah::{Node, PermissiveAgent};
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use wasm_bindgen_test::wasm_bindgen_test;

/// Two storage engines open one new database, as two browser tabs do, and
/// start nodes at once; both nodes end with one node id.
#[wasm_bindgen_test]
async fn two_storage_engines_that_open_one_new_database_end_with_one_keypair() -> anyhow::Result<()> {
    common::setup();
    let name = format!("node_keypair_{}", ulid::Ulid::new());
    let tabs = [IndexedDBStorageEngine::open(&name).await?, IndexedDBStorageEngine::open(&name).await?];
    let [here, there] = tabs.map(|engine| Node::new(Arc::new(engine), PermissiveAgent::new()));

    let (here_id, there_id) = futures::join!(here.wait_id(), there.wait_id());
    assert_eq!(here_id?, there_id?);

    drop((here, there));
    IndexedDBStorageEngine::cleanup(&name).await?;
    Ok(())
}
