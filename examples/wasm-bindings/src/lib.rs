use std::{panic, sync::Arc};

pub use ankurah_core::Node;
use ankurah_web_client::ConnectionState;
pub use ankurah_web_client::{indexeddb::IndexedDBStorageEngine, WebsocketClient};
use example_model::*;
use reactive_graph::effect::Effect;
use tracing::info;
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};

const API_SERVER: &str = "ws://localhost:8080";

#[wasm_bindgen(start)]
pub async fn start() -> Result<(), JsValue> {
    tracing_wasm::set_as_global_default();
    panic::set_hook(Box::new(console_error_panic_hook::hook));
    let _ = any_spawner::Executor::init_wasm_bindgen();
    Ok(())
}

#[wasm_bindgen]
pub async fn create_client() -> Result<WebsocketClient, JsValue> {
    let storage_engine = IndexedDBStorageEngine::open("ankurah_example_app")
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let node = Arc::new(Node::new(Box::new(storage_engine)));
    let connector = WebsocketClient::new(node.clone(), "ws://127.0.0.1:9797")?;
    use reactive_graph::prelude::*;
    info!("Waiting for client to connect");
    let state = connector.connection_state();

    Ok(connector)
}

use ankurah_core::resultset::ResultSet;
#[wasm_bindgen]
pub async fn fetch_test_records(client: &WebsocketClient) -> Vec<SessionRecord> {
    let sessions: ResultSet<SessionRecord> = client
        .node()
        .fetch::<SessionRecord>("date_connected = '2024-01-01'")
        .await
        .unwrap();
    sessions.into()

    // .into_iter()
    // .map(|r| {
    //     format!(
    //         "Date: {}, IP: {}, Node: {}",
    //         r.date_connected(),
    //         r.ip_address(),
    //         r.node_id()
    //     )
    // })
    // .collect()
}

#[wasm_bindgen]
pub async fn create_test_record(client: &WebsocketClient) -> Result<(), JsValue> {
    let trx = client.node().begin();
    let _session = trx
        .create(&Session {
            date_connected: "2024-01-01".to_string(),
            ip_address: "127.0.0.1".to_string(),
            node_id: client.node().id.clone().into(),
        })
        .await;
    trx.commit().await.unwrap();
    Ok(())
}
