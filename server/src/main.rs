mod appstate;
mod error;
mod handler;
mod query;
mod signal;
mod storage;

use axum::extract::ws::CloseFrame;
use axum::extract::{connect_info::ConnectInfo, State};
use core::panic;
use error::AppError;
use futures_util::stream::SplitSink;
use handler::ingress::fetch_ingress_logs;
use std::{borrow::Cow, net::SocketAddr, ops::ControlFlow};

use appstate::AppState;

use anyhow::Result;
use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};

use axum_extra::{headers, TypedHeader};
use bincode::{deserialize, serialize};
use futures_util::{SinkExt, StreamExt};
use hydra_proto as proto;
use tower::ServiceBuilder;
use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::{info, Level};

#[tokio::main]
async fn main() -> Result<()> {
    // initialize tracing
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    let state = AppState::new()?;

    // build our application with a route and middleware
    let app = Router::new()
        .route("/", get(root))
        .route("/ingress", post(handler::ingress::capture))
        .route("/ws", get(ws_handler))
        .with_state(state)
        .layer(
            ServiceBuilder::new()
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                        .on_request(DefaultOnRequest::new().level(Level::INFO))
                        .on_response(DefaultOnResponse::new().level(Level::INFO)),
                )
                .into_inner(),
        );

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:9797").await.unwrap();
    tracing::debug!("listening on {}", listener.local_addr().unwrap());

    // axum::serve(listener, app).await.unwrap();
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();

    Ok(())
}

pub async fn root() -> Result<String, StatusCode> {
    Ok("Hello, world!".to_string())
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    info!("Upgrading connection");
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };
    println!("`{user_agent}` at {addr} connected.");
    // finalize the upgrade process by returning upgrade callback.
    // we can customize the callback by sending additional info such as address.
    ws.on_upgrade(move |socket| handle_socket(socket, addr, state))
}

/// Actual websocket statemachine (one will be spawned per connection)
async fn handle_socket(mut socket: WebSocket, who: SocketAddr, state: AppState) {
    println!("Connected to {}", who);

    // Send a ping (unsupported by some browsers) just to kick things off
    if socket.send(Message::Ping(vec![1, 2, 3])).await.is_ok() {
        println!("Pinged {who}...");
    } else {
        println!("Could not send ping to {who}!");
        return;
    }

    let (mut sender, mut receiver) = socket.split();

    // Process each incoming message
    while let Some(msg) = receiver.next().await {
        if let Ok(msg) = msg {
            if process_message(msg, who, &sender, &state).await.is_break() {
                break;
            }
        } else {
            println!("client {who} abruptly disconnected");
            break;
        }
    }

    println!("Websocket context {who} destroyed");
}

/// helper to print contents of messages to stdout. Has special treatment for Close.
async fn process_message(
    msg: Message,
    who: SocketAddr,
    sender: &SplitSink<WebSocket, Message>,
    state: &AppState,
) -> ControlFlow<(), ()> {
    match msg {
        Message::Text(t) => {
            println!(">>> {who} sent str: {t:?}");
        }
        Message::Binary(d) => {
            println!(">>> {} sent {} bytes: {:?}", who, d.len(), d);

            // Deserialize the binary message into a Message enum
            if let Ok(message) = deserialize::<proto::Message>(&d) {
                match message {
                    proto::Message::Request(request) => {
                        handle_request(request, sender, state);
                    }
                    proto::Message::Response(_) => {
                        println!("Unexpected response message from client");
                    }
                }
            } else {
                println!("Failed to deserialize message");
            }
        }
        Message::Close(c) => {
            if let Some(cf) = c {
                println!(
                    ">>> {} sent close with code {} and reason `{}`",
                    who, cf.code, cf.reason
                );
            } else {
                println!(">>> {who} somehow sent close message without CloseFrame");
            }
            return ControlFlow::Break(());
        }

        Message::Pong(v) => {
            println!(">>> {who} sent pong with {v:?}");
        }
        // You should never need to manually handle Message::Ping, as axum's websocket library
        // will do so for you automagically by replying with Pong and copying the v according to
        // spec. But if you need the contents of the pings you can see them here.
        Message::Ping(v) => {
            println!(">>> {who} sent ping with {v:?}");
        }
    }
    ControlFlow::Continue(())
}

async fn handle_request(
    request: proto::Request,
    sender: &SplitSink<WebSocket, Message>,
    state: &AppState,
) -> proto::Response {
    let response_payload = match request.payload {
        proto::RequestPayload::FetchIngressLogs(fetch_request) => {
            match fetch_ingress_logs(fetch_request, state, sender) {
                Ok(fetch_response) => proto::ResponsePayload::FetchIngressLogs(fetch_response),
                Err(e) => {
                    println!("Error fetching ingress logs: {:?}", e);
                    // You might want to define an error variant for ResponsePayload
                    // to handle this case more gracefully
                    return proto::Response {
                        request_id: request.id,
                        payload: proto::ResponsePayload::Error(format!("{:?}", e)),
                    };
                }
            }
        }
    };

    proto::Response {
        request_id: request.id,
        payload: response_payload,
    }
}
