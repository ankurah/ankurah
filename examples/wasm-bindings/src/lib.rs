use std::{panic, sync::Arc};

use ankurah::{changes::ChangeSet, ResultSet, WasmSignal};
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
pub use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
pub use ankurah_websocket_client_wasm::WebsocketClient;
use example_model::*;
use lazy_static::lazy_static;
use once_cell::sync::OnceCell;
use tracing::{error, info};
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};

lazy_static! {
    static ref NODE: OnceCell<Node<PermissiveAgent>> = OnceCell::new();
    static ref NOTIFY: tokio::sync::Notify = tokio::sync::Notify::new();
}

#[wasm_bindgen(start)]
pub async fn start() -> Result<(), JsValue> {
    tracing_wasm::set_as_global_default();
    panic::set_hook(Box::new(console_error_panic_hook::hook));
    let _ = any_spawner::Executor::init_wasm_bindgen();

    let storage_engine = IndexedDBStorageEngine::open("ankurah_example_app").await.map_err(|e| JsValue::from_str(&e.to_string()))?;
    let node: Node<PermissiveAgent> = Node::new(Arc::new(storage_engine), PermissiveAgent::new());
    if let Err(_) = NODE.set(node) {
        error!("Failed to set node");
    }
    NOTIFY.notify_waiters();

    Ok(())
}
pub async fn get_node() -> Node<PermissiveAgent> {
    if NODE.get().is_none() {
        NOTIFY.notified().await;
    }
    NODE.get().unwrap().clone()
}

#[wasm_bindgen]
pub async fn create_client() -> Result<WebsocketClient, JsValue> {
    let node = get_node().await;

    let connector = WebsocketClient::new(node.clone(), "ws://127.0.0.1:9797")?;

    info!("Waiting for client to connect");

    Ok(connector)
}

#[wasm_bindgen]
pub async fn fetch_test_items(client: &WebsocketClient) -> Result<Vec<SessionView>, JsValue> {
    client.ready().await;
    let node = get_node().await;
    let sessions: ResultSet<SessionView> =
        node.fetch("date_connected = '2024-01-01'").await.map_err(|e| JsValue::from_str(&e.to_string()))?;
    Ok(sessions.into())
}

#[wasm_bindgen]
pub fn subscribe_test_items(client: &WebsocketClient) -> Result<TestResultSetSignal, JsValue> {
    let (signal, rwsignal) = reactive_graph::signal::RwSignal::new(TestResultSet::default()).split();

    let client = client.clone();
    wasm_bindgen_futures::spawn_local(async move {
        client.ready().await;
        let node = get_node().await;

        use reactive_graph::traits::Set;
        match node
            .subscribe("date_connected = '2024-01-01'", move |changeset: ChangeSet<SessionView>| {
                rwsignal.set(TestResultSet(Arc::new(changeset.resultset.clone())));
                // let mut received = received_changesets_clone.lock().unwrap();
                // received.push(changeset);
            })
            .await
        {
            Ok(handle) => {
                // HACK
                std::mem::forget(handle);
            }
            Err(e) => {
                error!("Failed to subscribe to changes: {}", e);
            }
        }
    });

    Ok(signal.into())
}

#[wasm_bindgen]
pub async fn create_test_entity() -> Result<(), JsValue> {
    let node = get_node().await;
    let trx = node.begin(c);
    let _session = trx
        .create(&Session { date_connected: "2024-01-01".to_string(), ip_address: "127.0.0.1".to_string(), node_id: node.id.clone().into() })
        .await;
    trx.commit().await.unwrap();
    Ok(())
}

#[wasm_bindgen]
#[derive(WasmSignal, Clone, Default)]
pub struct TestResultSet(Arc<ResultSet<SessionView>>);

#[wasm_bindgen]
impl TestResultSet {
    pub fn resultset(&self) -> Vec<SessionView> { self.0.items.to_vec() }
}
