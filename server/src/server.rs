use anyhow::Result;
use axum::extract::{connect_info::ConnectInfo, State};
use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use axum_extra::{headers, TypedHeader};
use bincode::deserialize;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use std::{net::SocketAddr, ops::ControlFlow, sync::Arc};
use tower::ServiceBuilder;
use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::{info, Level};

use crate::state::ServerState;
use ankurah_core::{node::Node, storage::StorageEngine};
use ankurah_proto as proto;

pub struct Server {
    bind_address: String,
    state: ServerState,
}

impl Server {
    pub fn builder() -> ServerBuilder {
        ServerBuilder::default()
    }

    pub async fn run(self) -> Result<()> {
        let app = Router::new()
            .route("/ws", get(ws_handler))
            .with_state(self.state)
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

        let listener = tokio::net::TcpListener::bind(&self.bind_address).await?;
        info!("listening on {}", listener.local_addr()?);

        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await?;

        Ok(())
    }
}

#[derive(Default)]
pub struct ServerBuilder {
    bind_address: Option<String>,
    storage: Option<Box<dyn StorageEngine>>,
}


impl ServerBuilder {
    pub fn bind_address(mut self, addr: impl Into<String>) -> Self {
        self.bind_address = Some(addr.into());
        self
    }

    pub fn with_storage(mut self, storage: impl StorageEngine + 'static) -> Self {
        self.storage = Some(Box::new(storage));
        self
    }

    pub fn build(self) -> Result<Server> {
        let bind_address = self
            .bind_address
            .ok_or_else(|| anyhow::anyhow!("bind_address is required"))?;

        let storage = self
            .storage
            .ok_or_else(|| anyhow::anyhow!("storage is required"))?;

        let node = Arc::new(Node::new(storage));
        let state = ServerState::new(node);

        Ok(Server {
            bind_address,
            state,
        })
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    info!("Upgrading connection");
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };
    println!("`{user_agent}` at {addr} connected.");
    ws.on_upgrade(move |socket| handle_socket(socket, addr, state))
}

async fn handle_socket(mut socket: WebSocket, who: SocketAddr, state: ServerState) {
    println!("Connected to {}", who);

    if socket.send(Message::Ping(vec![1, 2, 3])).await.is_ok() {
        println!("Pinged {who}...");
    } else {
        println!("Could not send ping to {who}!");
        return;
    }

    let (sender, mut receiver) = socket.split();

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

async fn process_message(
    msg: Message,
    who: SocketAddr,
    sender: &SplitSink<WebSocket, Message>,
    state: &ServerState,
) -> ControlFlow<(), ()> {
    match msg {
        Message::Text(t) => {
            println!(">>> {who} sent str: {t:?}");
        }
        Message::Binary(d) => {
            println!(">>> {} sent {} bytes: {:?}", who, d.len(), d);

            if let Ok(message) = deserialize::<proto::Message>(&d) {
                match message {
                    proto::Message::Request(request) => {
                        println!("Received request");
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
        Message::Ping(v) => {
            println!(">>> {who} sent ping with {v:?}");
        }
    }
    ControlFlow::Continue(())
}
