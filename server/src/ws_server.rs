use ankurah_core::connector::PeerSender;
use ankurah_proto as proto;
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
use futures_util::StreamExt;
use std::{net::SocketAddr, ops::ControlFlow, sync::Arc};
use tower::ServiceBuilder;
use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::{info, Level};

use ankurah_core::node::Node;

use crate::peer_sender::SenderKind;

pub struct WebsocketServer {
    node: Arc<Node>,
}

impl WebsocketServer {
    pub fn new(node: Arc<Node>) -> Self {
        Self { node }
    }

    pub async fn run(&self, bind_address: &str) -> Result<()> {
        let app = Router::new()
            .route("/ws", get(ws_handler))
            .with_state(self.node.clone())
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

        let listener = tokio::net::TcpListener::bind(bind_address).await?;
        info!("listening on {}", listener.local_addr()?);

        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await?;

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
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };
    println!("`{user_agent}` at {addr} connected.");
    ws.on_upgrade(move |socket| handle_socket(socket, addr, node))
}

async fn handle_socket(socket: WebSocket, who: SocketAddr, node: Arc<Node>) {
    println!("Connected to {}", who);

    let (mut sender, mut receiver) = socket.split();

    // Send server presence immediately after connection
    let presence = proto::ServerMessage::Presence(proto::Presence {
        node_id: node.id.clone(),
        durable: node.durable,
    });
    use futures_util::SinkExt;
    if let Ok(data) = bincode::serialize(&presence) {
        if sender.send(Message::Binary(data)).await.is_ok() {
            println!("Sent presence to {who}");
        } else {
            println!("Could not send presence to {who}!");
            return;
        }
    }

    // Create shared state for the client's node ID
    let mut sender = SenderKind::Initial(Some(sender));
    while let Some(msg) = receiver.next().await {
        if let Ok(msg) = msg {
            if process_message(msg, who, &mut sender, node.clone())
                .await
                .is_break()
            {
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
    sender: &mut SenderKind,
    node: Arc<Node>,
) -> ControlFlow<(), ()> {
    match msg {
        Message::Binary(d) => {
            println!(">>> {} sent {} bytes", who, d.len());

            if let Ok(message) = deserialize::<proto::ClientMessage>(&d) {
                match message {
                    // TODO - what happens if they send us a second presence message?
                    // is that actually useful?
                    proto::ClientMessage::Presence(presence) => {
                        if let SenderKind::Initial(opt) = sender {
                            if let Some(ws_sender) = opt.take() {
                                // take the value out of the option
                                println!("Received client presence from {}", who);

                                use crate::peer_sender::WebSocketPeerSender;
                                // Register peer sender for this client
                                let peer_sender = WebSocketPeerSender::new(ws_sender);

                                node.register_peer(presence, Box::new(peer_sender.clone()))
                                    .await;
                                *sender = SenderKind::Peer(peer_sender);
                            }
                        }
                    }
                    proto::ClientMessage::PeerMessage(msg) => {
                        tokio::spawn(async move {
                            if let Err(e) = node.handle_message(msg).await {
                                println!("Error handling message from {}: {:?}", who, e);
                            }
                        });
                    }
                }
            } else {
                println!("Failed to deserialize message from {}", who);
            }
        }
        Message::Text(t) => {
            println!(">>> {who} sent str: {t:?}");
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
