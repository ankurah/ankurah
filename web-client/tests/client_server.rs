// server / initiator
// #[cfg(not(target_arch = "wasm32"))]
// mod initiator {
//     #[cfg(not(target_arch = "wasm32"))]
//     use std::sync::Arc;

//     use ankurah_core::{storage::SledStorageEngine, Node};
//     use ankurah_server::ws_server::WebsocketServer;
//     use tracing::Level;

//     #[tokio::test]
//     async fn client_server() -> Result<(), Box<dyn std::error::Error>> {
//         tracing_subscriber::fmt().with_max_level(Level::INFO).init();
//         let storage = SledStorageEngine::new_test().unwrap();
//         let node = Arc::new(Node::new(Box::new(storage)));

//         // Create and start the websocket server
//         let server = WebsocketServer::new(node);
//         let handle = tokio::spawn(async move { server.run("0.0.0.0:9797").await });

//         // run the wasm_bindgen_test client_server_client by shelling out to wasm-pack test --chrome --headless
//         // let output = std::process::Command::new("wasm-pack")
//         //     .arg("test")
//         //     .arg("--chrome")
//         //     .arg("--headless")
//         //     .env("WASM_BINDGEN_TEST_FILTER", "client_server_client")
//         //     .output()?;

//         // // Print the output
//         // if !output.stdout.is_empty() {
//         //     println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
//         // }
//         // if !output.stderr.is_empty() {
//         //     eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
//         // }

//         // // Cleanup: abort the server
//         // handle.abort();

//         // assert!(output.status.success());

//         Ok(())
//     }
// }

// #[cfg(target_arch = "wasm32")]
mod client {

    use std::sync::Arc;

    use ankurah_web_client::indexeddb::IndexedDBStorageEngine;
    use wasm_bindgen_test::*;
    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    async fn client_server_client() {
        console_error_panic_hook::set_once();
        let _ = tracing_wasm::try_set_as_global_default();
        tracing::debug!("Test setup complete");

        // let client = ankurah_web_client::WebsocketClient::new("ws://localhost:9797", "test_db");
        // TODO: Add actual WebSocket communication tests here
        // For now, we're just testing that the client can be created and connected
        // assert!(true);
    }
}
