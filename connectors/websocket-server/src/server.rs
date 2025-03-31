use ankurah_core::storage::StorageEngine;
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
use std::{net::SocketAddr, ops::ControlFlow};
use tower::ServiceBuilder;
use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::{debug, info, warn, Level};

use ankurah_core::{node::Node, policy::PolicyAgent};

use super::state::Connection;

pub struct WebsocketServer<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    node: Option<Node<SE, PA>>,
}

impl<SE, PA> WebsocketServer<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub fn new(node: Node<SE, PA>) -> Self { Self { node: Some(node) } }

    pub async fn run(&mut self, bind_address: &str) -> Result<()> {
        let Some(node) = self.node.take() else {
            return Err(anyhow::anyhow!("Already been run"));
        };
        let app = Router::new().route("/ws", get(ws_handler)).with_state(node).layer(
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
        info!("Websocket server listening on {}", listener.local_addr()?);

        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;

        Ok(())
    }
}

async fn ws_handler<SE, PA>(
    ws: WebSocketUpgrade,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(node): State<Node<SE, PA>>,
) -> impl IntoResponse
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    debug!("Websocket server upgrading connection");
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent { user_agent.to_string() } else { String::from("Unknown browser") };
    debug!("`{user_agent}` at {addr} connected.");
    ws.on_upgrade(move |socket| handle_socket(socket, addr, node))
}

async fn handle_socket<SE, PA>(socket: WebSocket, who: SocketAddr, node: Node<SE, PA>)
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    info!("Websocket server connected to {}", who);

    let (sender, mut receiver) = socket.split();
    let mut conn = Connection::Initial(Some(sender));

    // Immediately send server presence after connection
    if let Err(e) = conn.send(proto::Message::Presence(proto::Presence { node_id: node.id.clone(), durable: node.durable })).await {
        debug!("Error sending presence to {who}: {:?}", e);
        return;
    }

    while let Some(msg) = receiver.next().await {
        if let Ok(msg) = msg {
            if process_message(msg, who, &mut conn, node.clone()).await.is_break() {
                break;
            }
        } else {
            debug!("client {who} abruptly disconnected");
            break;
        }
    }

    // Clean up peer registration if we had registered one
    if let Connection::Established(peer_sender) = conn {
        use ankurah_core::connector::PeerSender;
        node.deregister_peer(peer_sender.recipient_node_id());
    }

    debug!("Websocket context {who} destroyed");
}

async fn process_message<SE, PA>(
    msg: axum::extract::ws::Message,
    who: SocketAddr,
    state: &mut Connection,
    node: Node<SE, PA>,
) -> ControlFlow<(), ()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    match msg {
        axum::extract::ws::Message::Binary(d) => {
            debug!(">>> {} sent {} bytes", who, d.len());

            if let Ok(message) = deserialize::<proto::Message>(&d) {
                match message {
                    proto::Message::Presence(presence) => {
                        match state {
                            Connection::Initial(sender) => {
                                if let Some(sender) = sender.take() {
                                    debug!("Received client presence from {}", who);

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
                                    error!("Error handling message from {}: {:?}", who, e);
                                }
                            });
                        } else {
                            warn!("Received peer message from {} but not connected as a peer", who);
                        }
                    }
                }
            } else {
                error!("Failed to deserialize message from {}", who);
            }
        }
        axum::extract::ws::Message::Text(t) => {
            debug!(">>> {who} sent str: {t:?}");
        }
        axum::extract::ws::Message::Close(c) => {
            if let Some(cf) = c {
                debug!(">>> {} sent close with code {} and reason `{}`", who, cf.code, cf.reason);
            } else {
                debug!(">>> {who} somehow sent close message without CloseFrame");
            }
            return ControlFlow::Break(());
        }
        axum::extract::ws::Message::Pong(v) => {
            debug!(">>> {who} sent pong with {v:?}");
        }
        axum::extract::ws::Message::Ping(v) => {
            debug!(">>> {who} sent ping with {v:?}");
        }
    }
    ControlFlow::Continue(())
}
