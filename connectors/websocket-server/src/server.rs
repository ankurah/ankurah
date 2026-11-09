use ankurah_core::storage::StorageEngine;
use ankurah_core::{
    connector::{PeerConnectionError, PeerProvenance},
    signals::Wait,
};
use ankurah_proto as proto;
use anyhow::Result;
use axum::{
    extract::{
        ws::{WebSocket, WebSocketUpgrade},
        State,
    },
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use axum_extra::{headers, TypedHeader};
use bincode::deserialize;
use futures_util::StreamExt;
use std::net::IpAddr;
use std::{future::Future, net::SocketAddr, ops::ControlFlow, pin::Pin, time::Duration};
use tower::ServiceBuilder;
use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};
#[cfg(feature = "instrument")]
use tracing::instrument;
use tracing::{debug, error, info, warn, Level};

use ankurah_core::{node::Node, policy::PolicyAgent};

use crate::{client_ip::SmartClientIp, OptionalUserAgent};

use super::state::Connection;

// A peer that cannot decode our handshake will never send its Presence, so the
// pre-establishment state must not be allowed to live for the socket lifetime.
const INITIAL_PRESENCE_TIMEOUT: Duration = Duration::from_secs(10);

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

    pub fn route_handler(
        &self,
    ) -> impl Clone + Send + 'static + Fn(WebSocketUpgrade, SmartClientIp, OptionalUserAgent) -> Pin<Box<dyn Future<Output = Response> + Send>>
    {
        let node = self.node.as_ref().expect("websocket server cannot produce a route after being run").clone();

        move |ws: WebSocketUpgrade, SmartClientIp(client_ip): SmartClientIp, OptionalUserAgent(user_agent)| {
            let node = node.clone();
            Box::pin(async move { upgrade_connection(ws, client_ip, user_agent, node) })
        }
    }

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
    let user_agent = user_agent.map(|TypedHeader(user_agent)| user_agent.to_string());
    upgrade_connection(ws, client_ip, user_agent, node)
}

fn upgrade_connection<SE, PA>(ws: WebSocketUpgrade, client_ip: IpAddr, user_agent: Option<String>, node: Node<SE, PA>) -> Response
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    debug!("Websocket server upgrading connection");
    let user_agent = format_user_agent(user_agent);
    debug!("`{user_agent}` at {client_ip} connected.");
    ws.on_upgrade(move |socket| handle_websocket(socket, client_ip, node))
}

fn format_user_agent(user_agent: Option<String>) -> String { user_agent.unwrap_or_else(|| String::from("Unknown browser")) }

#[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(client_ip = %client_ip)))]
async fn handle_websocket<SE, PA>(socket: WebSocket, client_ip: IpAddr, node: Node<SE, PA>)
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    info!("Websocket server connected to {}", client_ip);

    let (sender, mut receiver) = socket.split();
    let handshake = node.begin_handshake();
    let opening = proto::Message::Open(handshake.opening());
    let mut conn = Connection::Initial { sender: Some(sender), handshake: Some(handshake) };
    let initial_presence_deadline = tokio::time::Instant::now() + INITIAL_PRESENCE_TIMEOUT;

    // The client's presence must answer our challenge; ours answers the client's.
    match tokio::time::timeout_at(initial_presence_deadline, conn.send(opening)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            debug!("Error sending opening to {client_ip}: {:?}", e);
            return;
        }
        Err(_) => {
            warn!("Peer at {client_ip} did not take our opening within {INITIAL_PRESENCE_TIMEOUT:?}; closing");
            return;
        }
    }

    let node_state = node.state();
    let receive = async {
        loop {
            let next = if matches!(conn, Connection::Initial { .. }) {
                match tokio::time::timeout_at(initial_presence_deadline, receiver.next()).await {
                    Ok(next) => next,
                    Err(_) => {
                        warn!("Peer at {client_ip} did not send presence within {INITIAL_PRESENCE_TIMEOUT:?}; closing");
                        break;
                    }
                }
            } else {
                receiver.next().await
            };
            let Some(msg) = next else { break };

            if let Ok(msg) = msg {
                if process_message(msg, client_ip, &mut conn, node.clone(), initial_presence_deadline).await.is_break() {
                    break;
                }
            } else {
                debug!("client {client_ip} abruptly disconnected");
                break;
            }
        }
    };
    tokio::select! {
        biased;
        _ = node_state.wait_for(|state| state.halt_reason().is_some()) => {}
        _ = receive => {}
    }

    // Clean up peer registration if we had registered one
    if let Connection::Established(peer_sender) = conn {
        use ankurah_core::connector::PeerSender;
        node.deregister_peer(peer_sender.recipient_node_id());
    }

    debug!("Websocket context {client_ip} destroyed");
}

/// When `frame` is the peer's first, read it as its opening and answer it with this node's
/// presence, or with the versions this node speaks when the two share none. `None` for any
/// later frame.
async fn answer_opening<SE, PA>(
    frame: &[u8],
    client_ip: IpAddr,
    state: &mut Connection,
    node: &Node<SE, PA>,
    handshake_deadline: tokio::time::Instant,
) -> Option<ControlFlow<(), ()>>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let Connection::Initial { handshake: Some(handshake), .. } = state else { return None };
    if handshake.opened() {
        return None;
    }
    let answer = match handshake.read_opening(frame) {
        Ok(()) => node.presence(handshake).await.map(proto::Message::Presence),
        Err(refusal) => Err(refusal),
    };
    // Within the handshake deadline, so a peer that stops reading cannot hold the connection open.
    let (answer, outcome) = match answer {
        Ok(presence) => (presence, ControlFlow::Continue(())),
        Err(PeerConnectionError::NoCommonVersion { ours, theirs }) => {
            warn!("Refusing peer at {client_ip}: it speaks protocol versions {theirs} and this node speaks {ours}");
            (proto::Message::NoCommonVersion(ours), ControlFlow::Break(()))
        }
        Err(refusal) => {
            warn!("Refusing peer at {client_ip}: {refusal}");
            return Some(ControlFlow::Break(()));
        }
    };
    Some(match tokio::time::timeout_at(handshake_deadline, state.send(answer)).await {
        Ok(Ok(())) => outcome,
        Ok(Err(error)) => {
            warn!("Closing websocket peer at {client_ip}: failed to answer its opening: {error:?}");
            ControlFlow::Break(())
        }
        Err(_) => {
            warn!("Peer at {client_ip} did not take our answer to its opening within {INITIAL_PRESENCE_TIMEOUT:?}; closing");
            ControlFlow::Break(())
        }
    })
}

#[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(client_ip = %client_ip)))]
async fn process_message<SE, PA>(
    msg: axum::extract::ws::Message,
    client_ip: IpAddr,
    state: &mut Connection,
    node: Node<SE, PA>,
    handshake_deadline: tokio::time::Instant,
) -> ControlFlow<(), ()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    match msg {
        axum::extract::ws::Message::Binary(d) => {
            debug!(">>> {} sent {} bytes", client_ip, d.len());

            if let Some(outcome) = answer_opening(&d, client_ip, state, &node, handshake_deadline).await {
                return outcome;
            }
            if let Ok(message) = deserialize::<proto::Message>(&d) {
                match message {
                    proto::Message::Presence(presence) => {
                        match state {
                            Connection::Initial { sender, handshake } => {
                                if let (Some(sender), Some(handshake)) = (sender.take(), handshake.take()) {
                                    debug!("Received client presence from {}", client_ip);

                                    use super::sender::WebSocketClientSender;
                                    // Register peer sender for this client
                                    let sender = WebSocketClientSender::new(presence.claims.node_id, sender);

                                    // Outside the deadline: cancelling registration partway could leave the peer registered.
                                    match node.register_peer(presence, handshake, PeerProvenance::Inbound, Box::new(sender.clone())).await {
                                        Ok(()) => *state = Connection::Established(sender),
                                        Err(rejection) => {
                                            warn!("Refusing peer at {client_ip}: {rejection}");
                                            return ControlFlow::Break(());
                                        }
                                    }
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
                    proto::Message::NoCommonVersion(versions) => {
                        warn!("Peer at {client_ip} speaks protocol versions {versions}, none of which this node speaks; closing");
                        return ControlFlow::Break(());
                    }
                    proto::Message::Open(_) => {
                        if let Connection::Initial { .. } = state {
                            warn!("Peer at {client_ip} sent a second opening; closing");
                            return ControlFlow::Break(());
                        }
                        warn!("Received an opening from {} after its presence - ignoring", client_ip);
                    }
                }
            } else if let Connection::Initial { .. } = state {
                // A peer whose handshake we cannot read will never establish;
                // close instead of leaving the connection dangling.
                error!("Failed to deserialize handshake message from {client_ip}; closing");
                return ControlFlow::Break(());
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
