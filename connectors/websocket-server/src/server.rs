use ankurah_proto as proto;
use anyhow::Result;
use axum::extract::{connect_info::ConnectInfo, State};
use axum::{
    extract::ws::{WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use axum_extra::{headers, TypedHeader};
use bincode::deserialize;
use futures_util::StreamExt;
use std::{net::SocketAddr, ops::ControlFlow, sync::Arc};
use tower::ServiceBuilder;
use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::{info, warn, Level};

use ankurah_core::node::Node;

use super::state::Connection;

pub struct WebsocketServer {
    node: Arc<Node>,
}

impl WebsocketServer {
    pub fn new(node: Arc<Node>) -> Self { Self { node } }

    pub async fn run(&self, bind_address: &str) -> Result<()> {
        let app = Router::new().route("/ws", get(ws_handler)).with_state(self.node.clone()).layer(
            ServiceBuilder::new()
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                        .on_request(DefaultOnRequest::new().level(Level::INFO))
                        .on_response(DefaultOnResponse::new().level(Level::INFO)),
                )
                .into_inner(),
        );

        let listener = tokio::net::TcpListener::bind(bind_address).await?;
        info!("listening on {}", listener.local_addr()?);

        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;

        Ok(())
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(node): State<Arc<Node>>,
) -> impl IntoResponse {
    info!("Upgrading connection");
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent { user_agent.to_string() } else { String::from("Unknown browser") };
    println!("`{user_agent}` at {addr} connected.");
    ws.on_upgrade(move |socket| handle_socket(socket, addr, node))
}

async fn handle_socket(socket: WebSocket, who: SocketAddr, node: Arc<Node>) {
    println!("Connected to {}", who);

    let (sender, mut receiver) = socket.split();
    let mut conn = Connection::Initial(Some(sender));

    // Immediately send server presence after connection
    if let Err(e) = conn.send(proto::Message::Presence(proto::Presence { node_id: node.id.clone(), durable: node.durable })).await {
        println!("Error sending presence to {who}: {:?}", e);
        return;
    }

    while let Some(msg) = receiver.next().await {
        if let Ok(msg) = msg {
            if process_message(msg, who, &mut conn, node.clone()).await.is_break() {
                break;
            }
        } else {
            println!("client {who} abruptly disconnected");
            break;
        }
    }

    // Clean up peer registration if we had registered one
    if let Connection::Established(peer_sender) = conn {
        use ankurah_core::connector::PeerSender;
        node.deregister_peer(peer_sender.recipient_node_id());
    }

    println!("Websocket context {who} destroyed");
}

async fn process_message(msg: axum::extract::ws::Message, who: SocketAddr, state: &mut Connection, node: Arc<Node>) -> ControlFlow<(), ()> {
    match msg {
        axum::extract::ws::Message::Binary(d) => {
            println!(">>> {} sent {} bytes", who, d.len());

            if let Ok(message) = deserialize::<proto::Message>(&d) {
                match message {
                    proto::Message::Presence(presence) => {
                        match state {
                            Connection::Initial(sender) => {
                                if let Some(sender) = sender.take() {
                                    println!("Received client presence from {}", who);

                                    use super::sender::WebSocketClientSender;
                                    // Register peer sender for this client
                                    let sender = WebSocketClientSender::new(presence.node_id.clone(), sender);

                                    node.register_peer(presence, Box::new(sender.clone()));
                                    *state = Connection::Established(sender);
                                }
                            }
                            _ => warn!("Received presence from {} but already have a peer sender - ignoring", who),
                        }
                    }
                    proto::Message::PeerMessage(msg) => {
                        if let Connection::Established(_) = state {
                            tokio::spawn(async move {
                                if let Err(e) = node.handle_message(msg).await {
                                    println!("Error handling message from {}: {:?}", who, e);
                                }
                            });
                        } else {
                            warn!("Received peer message from {} but not connected as a peer", who);
                        }
                    }
                }
            } else {
                println!("Failed to deserialize message from {}", who);
            }
        }
        axum::extract::ws::Message::Text(t) => {
            println!(">>> {who} sent str: {t:?}");
        }
        axum::extract::ws::Message::Close(c) => {
            if let Some(cf) = c {
                println!(">>> {} sent close with code {} and reason `{}`", who, cf.code, cf.reason);
            } else {
                println!(">>> {who} somehow sent close message without CloseFrame");
            }
            return ControlFlow::Break(());
        }
        axum::extract::ws::Message::Pong(v) => {
            println!(">>> {who} sent pong with {v:?}");
        }
        axum::extract::ws::Message::Ping(v) => {
            println!(">>> {who} sent ping with {v:?}");
        }
    }
    ControlFlow::Continue(())
}
