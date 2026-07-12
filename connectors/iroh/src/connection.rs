//! Shared connection pump used by both the dialer ([`crate::IrohClient`]) and the
//! acceptor ([`crate::IrohServer`]).
//!
//! Wire format: one bidirectional QUIC stream per connection, carrying
//! length-delimited bincode-serialized [`proto::Message`] frames. The handshake
//! mirrors the websocket connectors: each side sends its own `Presence`
//! unconditionally as the first frame, and registers the remote peer with the
//! local `Node` upon receiving the other side's `Presence`. The peer is
//! deregistered when the pump exits, however it exits.

use ankurah_core::{connector::PeerSender, policy::PolicyAgent, storage::StorageEngine, Node};
use ankurah_proto as proto;
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use iroh::endpoint::{RecvStream, SendStream};
use tokio::{
    select,
    sync::{mpsc, Notify},
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, warn};

use crate::sender::IrohPeerSender;

/// Maximum size of a single length-delimited frame. Matches the 64 MiB default
/// message cap used by the websocket connectors (tungstenite's default).
const MAX_FRAME_LENGTH: usize = 64 * 1024 * 1024;

fn codec() -> LengthDelimitedCodec { LengthDelimitedCodec::builder().max_frame_length(MAX_FRAME_LENGTH).new_codec() }

/// Run one established connection to completion.
///
/// Sends our own `Presence` first, then pumps frames in both directions until the
/// stream closes, an error occurs, or `shutdown` is notified (when provided).
/// Registers the remote peer on receiving its `Presence` (invoking `on_peer_presence`
/// afterwards) and deregisters it on exit.
pub(crate) async fn run_connection<SE, PA>(
    node: &Node<SE, PA>,
    send_stream: SendStream,
    recv_stream: RecvStream,
    shutdown: Option<&Notify>,
    mut on_peer_presence: impl FnMut(&proto::Presence),
) -> Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let mut writer = FramedWrite::new(send_stream, codec());
    let mut reader = FramedRead::new(recv_stream, codec());

    // Send our presence immediately, before processing any inbound frames
    let presence = proto::Message::Presence(proto::Presence { node_id: node.id, durable: node.durable, system_root: node.system.root() });
    writer.send(bincode::serialize(&presence)?.into()).await?;
    debug!("Sent presence");

    let mut peer_sender: Option<IrohPeerSender> = None;
    let mut outgoing_rx: Option<mpsc::UnboundedReceiver<proto::NodeMessage>> = None;

    let result = loop {
        select! {
            _ = wait_shutdown(shutdown) => {
                debug!("Connection received shutdown signal");
                break Ok(());
            }
            msg = next_outgoing(&mut outgoing_rx) => {
                let Some(node_message) = msg else {
                    // All sender handles dropped - the peer was deregistered out from under us
                    // (e.g. replaced by a newer connection), so this connection is done.
                    debug!("Outgoing channel closed");
                    break Ok(());
                };
                match bincode::serialize(&proto::Message::PeerMessage(node_message)) {
                    Ok(data) => {
                        if let Err(e) = writer.send(data.into()).await {
                            break Err(e.into());
                        }
                    }
                    Err(e) => warn!("Failed to serialize outgoing message: {}", e),
                }
            }
            frame = reader.next() => {
                match frame {
                    Some(Ok(data)) => handle_frame(node, &data, &mut peer_sender, &mut outgoing_rx, &mut on_peer_presence),
                    Some(Err(e)) => break Err(e.into()),
                    None => {
                        debug!("Stream closed by remote");
                        break Ok(());
                    }
                }
            }
        }
    };

    // Signal that we will send nothing further; harmless if the stream already failed.
    let _ = writer.get_mut().finish();

    if let Some(sender) = peer_sender {
        node.deregister_peer(sender.recipient_node_id());
        debug!("Deregistered peer {}", sender.recipient_node_id());
    }

    result
}

fn handle_frame<SE, PA>(
    node: &Node<SE, PA>,
    data: &[u8],
    peer_sender: &mut Option<IrohPeerSender>,
    outgoing_rx: &mut Option<mpsc::UnboundedReceiver<proto::NodeMessage>>,
    on_peer_presence: &mut impl FnMut(&proto::Presence),
) where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    match bincode::deserialize::<proto::Message>(data) {
        Ok(proto::Message::Presence(presence)) => {
            if peer_sender.is_some() {
                warn!("Received presence from {} but already have a peer sender - ignoring", presence.node_id);
                return;
            }
            debug!("Received peer presence: {}", presence);

            let (sender, rx) = IrohPeerSender::new(presence.node_id);
            node.register_peer(presence.clone(), Box::new(sender.clone()));

            *outgoing_rx = Some(rx);
            *peer_sender = Some(sender);
            on_peer_presence(&presence);
        }
        Ok(proto::Message::PeerMessage(message)) => {
            if peer_sender.is_none() {
                warn!("Received peer message before presence exchange - ignoring");
                return;
            }
            let node = node.clone();
            tokio::spawn(async move {
                if let Err(e) = node.handle_message(message).await {
                    warn!("Error handling peer message: {}", e);
                }
            });
        }
        Err(e) => warn!("Failed to deserialize message: {}", e),
    }
}

async fn wait_shutdown(shutdown: Option<&Notify>) {
    match shutdown {
        Some(notify) => notify.notified().await,
        None => std::future::pending().await,
    }
}

async fn next_outgoing(rx: &mut Option<mpsc::UnboundedReceiver<proto::NodeMessage>>) -> Option<proto::NodeMessage> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}
