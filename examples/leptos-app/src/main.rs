use ankurah::{policy::PermissiveAgent, Node};
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use ankurah_websocket_client_wasm::WebsocketClient;
use leptos::prelude::*;
use std::sync::Arc;

fn main() {
    console_error_panic_hook::set_once();
    mount_to_body(|| view! { <App /> })
}

#[allow(unused)]
async fn create_client() -> WebsocketClient {
    let storage = IndexedDBStorageEngine::open("test_db").await.unwrap();
    let node = NodeBuilder::new(Arc::new(storage).build_ephemeral(), PermissiveAgent::new());
    WebsocketClient::new(node, "localhost:9797").unwrap()
}

#[component]
fn App() -> impl IntoView {
    view! {
        <div>Broken</div>
        // <Suspense>
        // {move || Suspend::new(async move {
        //     let client = SendWrapper::new(create_client().await);
        //     view! { <Home client=client/> }
        // })}
        // </Suspense>
    }
}

// #[component]
// fn Home(client: SendWrapper<WebsocketClient>) -> impl IntoView {
//     let sessions = signal(Vec::new());

//     spawn_local(async move {
//         loop {
//             sessions.set(client.fetch_example_r_ecords().await);
//             sleep(Duration::from_secs(5)).await;
//         }
//     });
//     let create_entity = move |_| {
//         spawn_local(async move {
//             if let Ok(_) = create_test_entity(&client).await {
//                 log!("Session created");
//                 sessions_resource.refetch();
//             }
//         });
//     };

//     view! {
//         <div class="container">
//             <h1>"Ankurah Example App"</h1>
//             <div class="card">
//                 <div class="connection-status">
//                     "Connection State: "
//                     <span>{move || client.connection_state().to_string()}</span>
//                 </div>

//                 <div class="button-container">
//                     <button on:click=create_entity>
//                         "Create"
//                     </button>
//                 </div>

//                 <div class="sessions-header">"Sessions:"</div>
//                 <table>
//                     <thead>
//                         <tr>
//                             <th>"ID"</th>
//                             <th>"Date Connected"</th>
//                             <th>"IP Address"</th>
//                             <th>"Node ID"</th>
//                         </tr>
//                     </thead>
//                     <tbody>
//                         <For
//                             each=sessions
//                             key=|session| session.id().as_string()
//                             children=move |session| {
//                                 view! {
//                                     <tr>
//                                         <td>{session.id().as_string()}</td>
//                                         <td>{session.date_connected()}</td>
//                                         <td>{session.ip_address()}</td>
//                                         <td>{session.node_id()}</td>
//                                     </tr>
//                                 }
//                             }
//                         />
//                     </tbody>
//                 </table>
//             </div>
//         </div>

//         <style>
//             ".container {
//                 display: flex;
//                 flex-direction: column;
//                 align-items: center;
//                 justify-content: center;
//                 width: 100vw;
//                 min-height: 100vh;
//                 padding: 20px;
//                 position: absolute;
//                 left: 0;
//                 top: 0;
//             }

//             .card {
//                 width: 100%;
//                 max-width: 1200px;
//                 background-color: #f0f0f0;
//                 padding: 2rem;
//                 border-radius: 8px;
//                 box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
//             }

//             table {
//                 width: 100%;
//                 border-collapse: collapse;
//                 margin-top: 1rem;
//             }

//             th {
//                 border: 1px solid #ddd;
//                 padding: 8px;
//                 text-align: left;
//                 background-color: #f4f4f4;
//                 font-weight: bold;
//             }

//             td {
//                 border: 1px solid #ddd;
//                 padding: 8px;
//                 text-align: left;
//             }

//             tr:nth-child(even) {
//                 background-color: #f8f8f8;
//             }

//             tr:hover {
//                 background-color: #f0f0f0;
//             }

//             .connection-status {
//                 text-align: center;
//             }

//             .button-container {
//                 text-align: center;
//                 margin: 20px 0;
//             }

//             .sessions-header {
//                 text-align: center;
//                 margin-bottom: 10px;
//             }
//             "
//         </style>
//     }
// }
