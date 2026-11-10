//! The connection handshake through the websocket connectors: each side opens with the versions
//! it speaks and its challenge, the client presents, the server registers the client and only
//! then presents, and each side refuses what it cannot take; before or after the handshake, it
//! closes on a message of a kind it does not know. Each test drives a real server from a raw
//! socket, or a real client from a raw server.

use ankurah::core::node::NodeKey;
use futures_util::{SinkExt, StreamExt};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio_tungstenite::{tungstenite::Message as WsMessage, WebSocketStream};

#[path = "../common.rs"]
mod common;
use common::proto;

mod accepting;
mod dialing;

fn versions(lowest: u32, highest: u32) -> proto::ProtocolVersions { proto::ProtocolVersions { lowest, highest } }

const SPOKEN: proto::ProtocolVersions = proto::ProtocolVersions::SPOKEN;

/// The frame bytes of `message`.
fn frame(message: &proto::Message) -> WsMessage { WsMessage::Binary(message.encode().into()) }

/// What a peer holding only `key` claims answering `opening`, after offering `offered`.
fn claims_answering(key: &NodeKey, opening: &proto::Opening, offered: proto::ProtocolVersions, durable: bool) -> proto::PresenceClaims {
    proto::PresenceClaims {
        node_id: key.node_id(),
        durable,
        challenge: opening.challenge,
        timestamp: 0,
        negotiation: proto::VersionNegotiation {
            offered,
            peer_offered: opening.versions,
            chosen: offered.highest_shared(opening.versions).expect("the two offers share a version"),
        },
        features: vec![],
        announcements: BTreeMap::new(),
    }
}

/// The next frame `socket` receives, decoded; `None` once the other side closes. Panics after ten seconds.
async fn receive<S>(socket: &mut WebSocketStream<S>) -> Option<proto::Message>
where S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {
    loop {
        match tokio::time::timeout(Duration::from_secs(10), socket.next()).await.expect("the other side neither sent nor closed") {
            None | Some(Ok(WsMessage::Close(_))) | Some(Err(_)) => return None,
            Some(Ok(WsMessage::Binary(data))) => return Some(proto::Message::decode(&data).expect("a frame this build reads")),
            Some(Ok(_)) => continue,
        }
    }
}

/// Send `peer` a request from `from` over `socket`, and read frames until its response arrives;
/// `None` if the connection closes first. A connected node sends requests of its own meanwhile.
async fn request_answered<S>(socket: &mut WebSocketStream<S>, from: proto::NodeId, peer: proto::NodeId) -> Option<proto::NodeResponse>
where S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {
    let body = proto::CarriedBody::encode(&proto::NodeRequestBody::Get { ids: vec![] });
    let request = proto::NodeRequest { id: proto::RequestId::new(), to: peer, from, body };
    let request_id = request.id.clone();
    socket.send(frame(&proto::Message::PeerMessage(proto::NodeMessage::Request { auth: vec![Default::default()], request }))).await.ok()?;
    loop {
        if let proto::Message::PeerMessage(proto::NodeMessage::Response(response)) = receive(socket).await? {
            if response.request_id == request_id {
                return Some(response);
            }
        }
    }
}

/// Variant 99 of `Message`, then variant 99 of `NodeMessage` inside a peer message: kinds a later
/// version may define.
const UNKNOWN_KINDS: [&[u8]; 2] = [&[99, 0, 0, 0], &[3, 0, 0, 0, 99, 0, 0, 0]];
