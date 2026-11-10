use ankurah_core::storage::StorageEngine;
use ankurah_core::{
    connector::{Handshake, PeerConnectionError, PeerProvenance},
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

use futures_util::{SinkExt, StreamExt};
use std::net::IpAddr;
use std::{future::Future, net::SocketAddr, ops::ControlFlow, pin::Pin, time::Duration};
use tower::ServiceBuilder;
use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};
#[cfg(feature = "instrument")]
use tracing::instrument;
use tracing::{debug, error, info, warn, Level};

use ankurah_core::{node::Node, policy::PolicyAgent};

use crate::{client_ip::SmartClientIp, OptionalUserAgent};

use super::sender::{Socket, WebSocketClientSender};
use super::state::Connection;

// From the connection's start, this bounds the whole accepting handshake: sending this node's
// opening, reading the dialer's opening and presence, registering the dialer with every plugin's
// hook, signing this node's presence with every plugin's announcement and sending it, or sending
// the frame that refuses the dialer. A connection not established by then closes, so neither a
// dialer that never presents nor a slow plugin holds it open.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

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

    let (socket, mut receiver) = socket.split();
    let handshake = node.begin_handshake(PeerProvenance::Inbound);
    let opening = proto::Message::Open(handshake.opening());
    let mut conn = Connection::Initial { socket: Some(socket), handshake: Some(handshake) };
    let handshake_deadline = tokio::time::Instant::now() + HANDSHAKE_TIMEOUT;

    // The dialer's presence must answer our challenge, and ours answers the dialer's.
    match tokio::time::timeout_at(handshake_deadline, conn.send(opening)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            debug!("Error sending opening to {client_ip}: {:?}", e);
            return;
        }
        Err(_) => {
            warn!("Peer at {client_ip} did not take our opening within {HANDSHAKE_TIMEOUT:?}; closing");
            return;
        }
    }

    let node_state = node.state();
    let receive = async {
        loop {
            let next = if matches!(conn, Connection::Initial { .. }) {
                match tokio::time::timeout_at(handshake_deadline, receiver.next()).await {
                    Ok(next) => next,
                    Err(_) => {
                        warn!("Peer at {client_ip} did not send presence within {HANDSHAKE_TIMEOUT:?}; closing");
                        break;
                    }
                }
            } else {
                receiver.next().await
            };
            let Some(msg) = next else { break };

            if let Ok(msg) = msg {
                if process_message(msg, client_ip, &mut conn, node.clone(), handshake_deadline).await.is_break() {
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
    if let Connection::Established { registration, .. } = conn {
        node.deregister_peer(registration);
    }

    debug!("Websocket context {client_ip} destroyed");
}

/// When `frame` is the peer's first, read it as its opening, answering with the versions this
/// node speaks when the two share none. `None` for any later frame.
async fn read_opening(
    frame: &[u8],
    client_ip: IpAddr,
    state: &mut Connection,
    handshake_deadline: tokio::time::Instant,
) -> Option<ControlFlow<(), ()>> {
    let Connection::Initial { handshake: Some(handshake), .. } = state else { return None };
    if handshake.opened() {
        return None;
    }
    let ours = match handshake.read_opening(frame) {
        Ok(()) => return Some(ControlFlow::Continue(())),
        Err(PeerConnectionError::NoCommonVersion { ours, theirs }) => {
            warn!(
                "Peer at {client_ip} speaks protocol versions {theirs}, none of which this node speaks; answering with {ours} and closing"
            );
            ours
        }
        Err(refusal) => {
            warn!("Refusing peer at {client_ip}: {refusal}");
            return Some(ControlFlow::Break(()));
        }
    };
    // Within the handshake deadline, so a peer that stops reading cannot hold the connection open.
    match tokio::time::timeout_at(handshake_deadline, state.send(proto::Message::NoCommonVersion(ours))).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!("Failed to send peer at {client_ip} the versions this node speaks: {error:?}"),
        Err(_) => warn!("Peer at {client_ip} did not take the versions this node speaks within {HANDSHAKE_TIMEOUT:?}"),
    }
    Some(ControlFlow::Break(()))
}

/// Register the dialer whose `presence` arrived on this connection, then answer it with this
/// node's own presence, so this node signs nothing for a dialer it has not registered.
async fn answer_presence<SE, PA>(
    presence: proto::Presence,
    mut socket: Socket,
    mut handshake: Handshake,
    client_ip: IpAddr,
    state: &mut Connection,
    node: &Node<SE, PA>,
    handshake_deadline: tokio::time::Instant,
) -> ControlFlow<(), ()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    debug!("Received client presence from {}", client_ip);
    let (sender, writer) = WebSocketClientSender::new(presence.claims.node_id);
    match tokio::time::timeout_at(handshake_deadline, node.register_peer(presence, &mut handshake, Box::new(sender.clone()))).await {
        // Held by the connection from here, so the peer is deregistered however the connection ends.
        Ok(Ok(registration)) => *state = Connection::Established { sender, registration },
        Ok(Err(refusal)) => return refuse(&mut socket, refusal, client_ip, handshake_deadline).await,
        // A registration cut off before it finished registered nothing.
        Err(_) => {
            warn!("Registering peer at {client_ip} did not finish within {HANDSHAKE_TIMEOUT:?}; closing");
            return ControlFlow::Break(());
        }
    }
    let answer = match tokio::time::timeout_at(handshake_deadline, node.presence(&handshake)).await {
        Ok(Ok(presence)) => proto::Message::Presence(presence),
        Ok(Err(refusal)) => return refuse(&mut socket, refusal, client_ip, handshake_deadline).await,
        Err(_) => {
            warn!("This node's presence for peer at {client_ip} was not ready within {HANDSHAKE_TIMEOUT:?}; closing");
            return ControlFlow::Break(());
        }
    };
    match tokio::time::timeout_at(handshake_deadline, socket.send(axum::extract::ws::Message::Binary(answer.encode().into()))).await {
        Ok(Ok(())) if writer.send(socket).is_ok() => ControlFlow::Continue(()),
        Ok(Ok(())) => {
            warn!("Closing websocket peer at {client_ip}: its frames have no writer");
            ControlFlow::Break(())
        }
        Ok(Err(error)) => {
            warn!("Closing websocket peer at {client_ip}: failed to answer its presence: {error:?}");
            ControlFlow::Break(())
        }
        Err(_) => {
            warn!("Peer at {client_ip} did not take our answer to its presence within {HANDSHAKE_TIMEOUT:?}; closing");
            ControlFlow::Break(())
        }
    }
}

/// Refuse the dialer for `refusal` and close: unless this node itself has halted, tell the dialer
/// why first, best effort and within the handshake deadline.
async fn refuse(
    socket: &mut Socket,
    refusal: PeerConnectionError,
    client_ip: IpAddr,
    handshake_deadline: tokio::time::Instant,
) -> ControlFlow<(), ()> {
    warn!("Refusing peer at {client_ip}: {refusal}");
    if !matches!(refusal, PeerConnectionError::NodeHalted(_)) {
        let frame = axum::extract::ws::Message::Binary(proto::Message::Refused(refusal.to_string()).encode().into());
        // Best effort: the connection closes either way.
        let _ = tokio::time::timeout_at(handshake_deadline, socket.send(frame)).await;
    }
    ControlFlow::Break(())
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

            if let Some(outcome) = read_opening(&d, client_ip, state, handshake_deadline).await {
                return outcome;
            }
            match proto::Message::decode(&d) {
                Ok(message) => match message {
                    proto::Message::Presence(presence) => match state {
                        Connection::Initial { socket, handshake } => {
                            if let (Some(socket), Some(handshake)) = (socket.take(), handshake.take()) {
                                return answer_presence(presence, socket, handshake, client_ip, state, &node, handshake_deadline).await;
                            }
                        }
                        _ => warn!("Received presence from {} but already have a peer sender - ignoring", client_ip),
                    },
                    proto::Message::PeerMessage(msg) => {
                        if let Connection::Established { registration, .. } = state {
                            let registration = *registration;
                            tokio::spawn(async move {
                                if let Err(e) = node.handle_message(registration, msg).await {
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
                    proto::Message::Refused(reason) => {
                        warn!("Peer at {client_ip} refused this node: {reason}; closing");
                        return ControlFlow::Break(());
                    }
                    proto::Message::Open(_) => {
                        if let Connection::Initial { .. } = state {
                            warn!("Peer at {client_ip} sent a second opening; closing");
                            return ControlFlow::Break(());
                        }
                        warn!("Received an opening from {} after its presence - ignoring", client_ip);
                    }
                },
                Err(unreadable) if unreadable.closes_connection(matches!(state, Connection::Established { .. })) => {
                    error!("Closing peer at {client_ip}: it sent {unreadable}");
                    return ControlFlow::Break(());
                }
                Err(unreadable) => error!("Dropping a frame from {client_ip}: {unreadable}"),
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
