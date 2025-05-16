use ankurah_core::storage::StorageEngine;
use ankurah_proto as proto;
use anyhow::Result;
use axum::extract::State;
use axum::{
    extract::ws::{WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use axum_extra::{headers, TypedHeader};
use bincode::deserialize;
use futures_util::StreamExt;
use std::net::IpAddr;
use std::{net::SocketAddr, ops::ControlFlow};
use tower::ServiceBuilder;
use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};
#[cfg(feature = "instrument")]
use tracing::instrument;
use tracing::{debug, error, info, warn, Level};

use ankurah_core::{node::Node, policy::PolicyAgent};

use crate::client_ip::SmartClientIp;

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
    SmartClientIp(client_ip): SmartClientIp,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    State(node): State<Node<SE, PA>>,
) -> impl IntoResponse
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    debug!("Websocket server upgrading connection");
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent { user_agent.to_string() } else { String::from("Unknown browser") };
    debug!("`{user_agent}` at {client_ip} connected.");
    ws.on_upgrade(move |socket| handle_websocket(socket, client_ip, node))
}

#[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(client_ip = %client_ip)))]
async fn handle_websocket<SE, PA>(socket: WebSocket, client_ip: IpAddr, node: Node<SE, PA>)
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    info!("Websocket server connected to {}", client_ip);

    let (sender, mut receiver) = socket.split();
    let mut conn = Connection::Initial(Some(sender));

    // Immediately send server presence after connection
    if let Err(e) = conn
        .send(proto::Message::Presence(proto::Presence { node_id: node.id, durable: node.durable, system_root: node.system.root() }))
        .await
    {
        debug!("Error sending presence to {client_ip}: {:?}", e);
        return;
    }

    while let Some(msg) = receiver.next().await {
        if let Ok(msg) = msg {
            if process_message(msg, client_ip, &mut conn, node.clone()).await.is_break() {
                break;
            }
        } else {
            debug!("client {client_ip} abruptly disconnected");
            break;
        }
    }

    // Clean up peer registration if we had registered one
    if let Connection::Established(peer_sender) = conn {
        use ankurah_core::connector::PeerSender;
        node.deregister_peer(peer_sender.recipient_node_id());
    }

    debug!("Websocket context {client_ip} destroyed");
}

#[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(client_ip = %client_ip)))]
async fn process_message<SE, PA>(
    msg: axum::extract::ws::Message,
    client_ip: IpAddr,
    state: &mut Connection,
    node: Node<SE, PA>,
) -> ControlFlow<(), ()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    match msg {
        axum::extract::ws::Message::Binary(d) => {
            debug!(">>> {} sent {} bytes", client_ip, d.len());

            if let Ok(message) = deserialize::<proto::Message>(&d) {
                match message {
                    proto::Message::Presence(presence) => {
                        match state {
                            Connection::Initial(sender) => {
                                if let Some(sender) = sender.take() {
                                    debug!("Received client presence from {}", client_ip);

                                    use super::sender::WebSocketClientSender;
                                    // Register peer sender for this client
                                    let sender = WebSocketClientSender::new(presence.node_id, sender);

                                    node.register_peer(presence, Box::new(sender.clone()));
                                    *state = Connection::Established(sender);
                                }
                            }
                            _ => warn!("Received presence from {} but already have a peer sender - ignoring", client_ip),
                        }
                    }
                    proto::Message::PeerMessage(msg) => {
                        if let Connection::Established(_) = state {
                            tokio::spawn(async move {
                                if let Err(e) = node.handle_message(msg).await {
                                    error!("Error handling message from {}: {:?}", client_ip, e);
                                }
                            });
                        } else {
                            warn!("Received peer message from {} but not connected as a peer", client_ip);
                        }
                    }
                }
            } else {
                error!("Failed to deserialize message from {}", client_ip);
            }
        }
        axum::extract::ws::Message::Text(t) => {
            debug!(">>> {client_ip} sent str: {t:?}");
        }
        axum::extract::ws::Message::Close(c) => {
            if let Some(cf) = c {
                debug!(">>> {} sent close with code {} and reason `{}`", client_ip, cf.code, cf.reason);
            } else {
                debug!(">>> {client_ip} somehow sent close message without CloseFrame");
            }
            return ControlFlow::Break(());
        }
        axum::extract::ws::Message::Pong(v) => {
            debug!(">>> {client_ip} sent pong with {v:?}");
        }
        axum::extract::ws::Message::Ping(v) => {
            debug!(">>> {client_ip} sent ping with {v:?}");
        }
    }
    ControlFlow::Continue(())
}
