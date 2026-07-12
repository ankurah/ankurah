//! Shared connection pump used by both the dialer ([`crate::IrohClient`]) and the
//! acceptor ([`crate::IrohServer`]).
//!
//! Wire format: one bidirectional QUIC stream per connection, carrying
//! length-delimited bincode-serialized [`proto::Message`] frames. The handshake
//! mirrors the websocket connectors: each side sends its own `Presence`
//! unconditionally as the first frame, and registers the remote peer with the
//! local `Node` upon receiving the other side's `Presence`. The peer is
//! deregistered when the pump exits, however it exits.

use ankurah_core::{policy::PolicyAgent, storage::StorageEngine, Node};
use ankurah_proto as proto;
use anyhow::{anyhow, bail, Result};
use futures_util::{SinkExt, StreamExt};
use iroh::endpoint::{RecvStream, SendStream};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{select, time::timeout};
use tokio_util::{
    codec::{FramedRead, FramedWrite, LengthDelimitedCodec},
    sync::CancellationToken,
};
use tracing::{debug, warn};

use crate::sender::{ConnectionControl, IrohPeerSender, OutgoingFrame};

/// Maximum size of an established-connection frame. Matches the 64 MiB default
/// message cap used by the websocket connectors (tungstenite's default).
pub(crate) const MAX_FRAME_LENGTH: usize = 64 * 1024 * 1024;

/// Presence is the only legal first frame. Keeping its cap much smaller than the
/// data-frame cap prevents an unregistered peer from reserving 64 MiB simply by
/// sending a length prefix and then stalling.
const MAX_PRESENCE_FRAME_LENGTH: usize = 64 * 1024;

/// Once QUIC is established, Presence should be immediately available. This
/// timeout also bounds peers that open a stream but never complete the Ankurah
/// handshake.
pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

fn codec(max_frame_length: usize) -> LengthDelimitedCodec { LengthDelimitedCodec::builder().max_frame_length(max_frame_length).new_codec() }

/// Coordinates registrations created by one connector instance. Registration
/// and conditional deregistration share this lock, so cleanup from an older
/// connection cannot remove a newer registration for the same peer.
///
/// This is intentionally connector-local. Coordinating Iroh with other
/// transports, or multiple independent connector instances on the same Node,
/// requires registration identity in the core Node API.
#[derive(Default)]
pub(crate) struct RegistrationRegistry {
    active: Mutex<HashMap<proto::EntityId, Arc<ConnectionControl>>>,
}

impl RegistrationRegistry {
    fn register_peer<SE, PA>(
        self: &Arc<Self>,
        node: &Node<SE, PA>,
        presence: proto::Presence,
        sender: IrohPeerSender,
        control: Arc<ConnectionControl>,
    ) -> PeerRegistration<SE, PA>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let peer_id = presence.node_id;
        let mut active = self.active.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(previous) = active.insert(peer_id, control.clone()) {
            previous.stop_connection();
        }
        node.register_peer(presence, Box::new(sender));

        PeerRegistration { node: node.clone(), peer_id, control, registry: self.clone() }
    }

    fn deregister_peer_if_current<SE, PA>(&self, node: &Node<SE, PA>, peer_id: proto::EntityId, control: &Arc<ConnectionControl>) -> bool
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        self.remove_if_current(peer_id, control, || node.deregister_peer(peer_id))
    }

    /// Remove `control` and run its Node cleanup as one registry critical
    /// section. The callback keeps the registry independent of Node generics
    /// and lets tests pause the critical section at a deterministic point.
    fn remove_if_current(&self, peer_id: proto::EntityId, control: &Arc<ConnectionControl>, cleanup: impl FnOnce()) -> bool {
        let mut active = self.active.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let is_current = active.get(&peer_id).is_some_and(|current| Arc::ptr_eq(current, control));
        if !is_current {
            return false;
        }

        active.remove(&peer_id);
        cleanup();
        true
    }
}

/// Owns one connector-local Node peer registration. Deregistration lives in
/// Drop so task abort, router cancellation, and outer `select!` cancellation
/// cannot bypass it.
struct PeerRegistration<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    node: Node<SE, PA>,
    peer_id: proto::EntityId,
    control: Arc<ConnectionControl>,
    registry: Arc<RegistrationRegistry>,
}

impl<SE, PA> Drop for PeerRegistration<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if self.registry.deregister_peer_if_current(&self.node, self.peer_id, &self.control) {
            debug!("Deregistered peer {}", self.peer_id);
        } else {
            debug!("Skipped deregistering replaced peer {}", self.peer_id);
        }
    }
}

/// Run one established connection to completion.
///
/// Sends our own `Presence` first, requires the remote `Presence` as the first
/// inbound frame, then raises the codec limit and pumps frames in both
/// directions until the stream closes, an error occurs, or `shutdown` is
/// cancelled. Registration cleanup is cancellation-safe.
pub(crate) async fn run_connection<SE, PA>(
    node: &Node<SE, PA>,
    registrations: &Arc<RegistrationRegistry>,
    send_stream: SendStream,
    recv_stream: RecvStream,
    shutdown: Option<&CancellationToken>,
    mut on_peer_presence: impl FnMut(&proto::Presence),
) -> Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let mut writer = FramedWrite::new(send_stream, codec(MAX_PRESENCE_FRAME_LENGTH));
    let mut reader = FramedRead::new(recv_stream, codec(MAX_PRESENCE_FRAME_LENGTH));

    // Sending data is what makes a newly opened QUIC stream visible to the
    // acceptor, so both roles send Presence before waiting for the other side.
    let presence = proto::Message::Presence(proto::Presence { node_id: node.id, durable: node.durable, system_root: node.system.root() });
    match await_handshake_send(writer.send(bincode::serialize(&presence)?.into()), shutdown, HANDSHAKE_TIMEOUT).await {
        Ok(Some(Ok(()))) => {}
        Ok(Some(Err(e))) => return Err(e.into()),
        Ok(None) => return Ok(()),
        Err(_) => bail!("timed out sending local presence"),
    }
    debug!("Sent presence");

    let Some(frame) = wait_for_presence_frame(&mut reader, shutdown).await? else {
        return Ok(());
    };
    let peer_presence = match bincode::deserialize::<proto::Message>(&frame) {
        Ok(proto::Message::Presence(presence)) => presence,
        Ok(proto::Message::PeerMessage(_)) => bail!("received peer message before presence exchange"),
        Err(e) => return Err(anyhow!("failed to deserialize peer presence: {}", e)),
    };
    debug!("Received peer presence: {}", peer_presence);

    let (sender, mut outgoing_rx, connection_control) = IrohPeerSender::new(peer_presence.node_id);
    let _registration = registrations.register_peer(node, peer_presence.clone(), sender, connection_control.clone());
    on_peer_presence(&peer_presence);

    // Only registered peers may use the larger application-message cap.
    reader.decoder_mut().set_max_frame_length(MAX_FRAME_LENGTH);
    writer.encoder_mut().set_max_frame_length(MAX_FRAME_LENGTH);

    let result = loop {
        select! {
            _ = wait_shutdown(shutdown) => {
                debug!("Connection received shutdown signal");
                break Ok(());
            }
            _ = connection_control.stopped() => {
                debug!("Connection stop requested");
                break Ok(());
            }
            msg = outgoing_rx.recv() => {
                let Some(OutgoingFrame { data, _permit }) = msg else {
                    debug!("Outgoing channel closed");
                    break Ok(());
                };
                match await_send_or_stop(writer.send(data.into()), _permit, shutdown, &connection_control).await {
                    Some(Ok(())) => {}
                    Some(Err(e)) => break Err(e.into()),
                    None => {
                        debug!("Connection stopped while writing an outgoing frame");
                        break Ok(());
                    }
                }
            }
            frame = reader.next() => {
                match frame {
                    Some(Ok(data)) => handle_frame(node, &data),
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
    result
}

/// Bound the first outbound frame just like the first inbound frame. A peer can
/// otherwise complete QUIC while advertising too little receive credit for our
/// Presence and keep shutdown stuck before registration has even begun.
async fn await_handshake_send<F>(
    send: F,
    shutdown: Option<&CancellationToken>,
    timeout_after: Duration,
) -> std::result::Result<Option<F::Output>, tokio::time::error::Elapsed>
where
    F: std::future::Future,
{
    let timed_send = timeout(timeout_after, send);
    tokio::pin!(timed_send);

    match shutdown {
        Some(shutdown) => {
            select! {
                biased;
                _ = shutdown.cancelled() => Ok(None),
                result = &mut timed_send => result.map(Some),
            }
        }
        None => timed_send.await.map(Some),
    }
}

/// Await one write while retaining its byte-budget permit. QUIC flow control
/// can leave a large write pending indefinitely when the peer stops reading, so
/// shutdown and connection replacement must be able to cancel the write itself
/// rather than waiting for the outer pump loop to regain control.
async fn await_send_or_stop<F, T>(
    send: F,
    _retained: T,
    shutdown: Option<&CancellationToken>,
    connection_control: &ConnectionControl,
) -> Option<F::Output>
where
    F: std::future::Future,
{
    select! {
        biased;
        _ = wait_shutdown(shutdown) => None,
        _ = connection_control.stopped() => None,
        result = send => Some(result),
    }
}

async fn wait_for_presence_frame(
    reader: &mut FramedRead<RecvStream, LengthDelimitedCodec>,
    shutdown: Option<&CancellationToken>,
) -> Result<Option<tokio_util::bytes::BytesMut>> {
    let next = timeout(HANDSHAKE_TIMEOUT, reader.next());
    tokio::pin!(next);

    let frame = match shutdown {
        Some(shutdown) => {
            select! {
                _ = shutdown.cancelled() => return Ok(None),
                frame = &mut next => frame,
            }
        }
        None => next.await,
    }
    .map_err(|_| anyhow!("timed out waiting for peer presence"))?;

    match frame {
        Some(Ok(data)) => Ok(Some(data)),
        Some(Err(e)) => Err(e.into()),
        None => Err(anyhow!("stream closed before peer presence")),
    }
}

fn handle_frame<SE, PA>(node: &Node<SE, PA>, data: &[u8])
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    match bincode::deserialize::<proto::Message>(data) {
        Ok(proto::Message::Presence(presence)) => {
            warn!("Received duplicate presence from {} - ignoring", presence.node_id);
        }
        Ok(proto::Message::PeerMessage(message)) => {
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

async fn wait_shutdown(shutdown: Option<&CancellationToken>) {
    match shutdown {
        Some(shutdown) => shutdown.cancelled().await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah::PermissiveAgent;
    use ankurah_storage_sled::SledStorageEngine;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::sync_channel,
        Arc,
    };
    use tokio_util::bytes::{BufMut, BytesMut};
    use tokio_util::codec::Decoder;

    #[test]
    fn presence_codec_rejects_oversized_first_frame_from_its_header() {
        let mut codec = codec(MAX_PRESENCE_FRAME_LENGTH);
        let mut header = BytesMut::new();
        header.put_u32((MAX_PRESENCE_FRAME_LENGTH + 1) as u32);

        assert!(codec.decode(&mut header).is_err());
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) { self.0.store(true, Ordering::Release) }
    }

    #[tokio::test]
    async fn stalled_presence_write_is_interrupted_by_shutdown() {
        let shutdown = CancellationToken::new();
        let mut send =
            Box::pin(await_handshake_send(std::future::pending::<Result<(), std::io::Error>>(), Some(&shutdown), HANDSHAKE_TIMEOUT));

        assert!(timeout(Duration::from_millis(10), &mut send).await.is_err());
        shutdown.cancel();
        assert!(timeout(Duration::from_secs(1), send)
            .await
            .expect("stalled presence write should stop")
            .expect("shutdown should beat the handshake timeout")
            .is_none());
    }

    #[tokio::test]
    async fn stalled_presence_write_obeys_handshake_timeout() {
        let result = await_handshake_send(std::future::pending::<Result<(), std::io::Error>>(), None, Duration::from_millis(10)).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn stalled_write_is_interrupted_by_connection_stop() {
        let (_sender, _rx, control) = IrohPeerSender::new(proto::EntityId::new());
        let retained_dropped = Arc::new(AtomicBool::new(false));
        let mut send = Box::pin(await_send_or_stop(
            std::future::pending::<Result<(), std::io::Error>>(),
            DropFlag(retained_dropped.clone()),
            None,
            &control,
        ));

        assert!(timeout(Duration::from_millis(10), &mut send).await.is_err());
        assert!(!retained_dropped.load(Ordering::Acquire));

        control.stop_connection();
        assert!(timeout(Duration::from_secs(1), send).await.expect("stalled write should stop").is_none());
        assert!(retained_dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn stalled_write_is_interrupted_by_shutdown() {
        let (_sender, _rx, control) = IrohPeerSender::new(proto::EntityId::new());
        let shutdown = CancellationToken::new();
        let retained_dropped = Arc::new(AtomicBool::new(false));
        let mut send = Box::pin(await_send_or_stop(
            std::future::pending::<Result<(), std::io::Error>>(),
            DropFlag(retained_dropped.clone()),
            Some(&shutdown),
            &control,
        ));

        assert!(timeout(Duration::from_millis(10), &mut send).await.is_err());
        assert!(!retained_dropped.load(Ordering::Acquire));

        shutdown.cancel();
        assert!(timeout(Duration::from_secs(1), send).await.expect("stalled write should stop").is_none());
        assert!(retained_dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn replacement_stops_old_connection_and_stale_cleanup_skips_new_peer() -> Result<()> {
        let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
        let registry = Arc::new(RegistrationRegistry::default());
        let peer_id = proto::EntityId::new();
        let presence = proto::Presence { node_id: peer_id, durable: true, system_root: None };

        let (old_sender, _old_rx, old_control) = IrohPeerSender::new(peer_id);
        let _retained_old_sender = old_sender.clone();
        let old_registration = registry.register_peer(&node, presence.clone(), old_sender, old_control.clone());
        assert!(!old_control.is_stopped());

        let (replacement_sender, _replacement_rx, replacement_control) = IrohPeerSender::new(peer_id);
        let replacement_registration = registry.register_peer(&node, presence, replacement_sender, replacement_control.clone());
        assert!(old_control.is_stopped());
        assert!(!replacement_control.is_stopped());

        // The old pump may clean up after its replacement is installed, but
        // its stale registry identity must leave the replacement intact.
        drop(old_registration);
        assert_eq!(node.get_durable_peers(), vec![peer_id]);
        assert!(!replacement_control.is_stopped());

        // The active replacement still owns cleanup for this peer.
        drop(replacement_registration);
        assert!(node.get_durable_peers().is_empty());
        Ok(())
    }

    #[test]
    fn cleanup_and_replacement_cannot_interleave_check_then_remove() -> Result<()> {
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?;
        let _runtime_context = runtime.enter();
        let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
        let registry = Arc::new(RegistrationRegistry::default());
        let peer_id = proto::EntityId::new();
        let presence = proto::Presence { node_id: peer_id, durable: true, system_root: None };

        let (old_sender, _old_rx, old_control) = IrohPeerSender::new(peer_id);
        let old_registration = registry.register_peer(&node, presence.clone(), old_sender, old_control.clone());

        let (cleanup_checked_tx, cleanup_checked_rx) = sync_channel(1);
        let (continue_cleanup_tx, continue_cleanup_rx) = sync_channel(1);
        let cleanup_registry = registry.clone();
        let cleanup_node = node.clone();
        let cleanup_control = old_control.clone();
        let cleanup_runtime = runtime.handle().clone();
        let cleanup_thread = std::thread::spawn(move || {
            let _runtime_context = cleanup_runtime.enter();
            cleanup_registry.remove_if_current(peer_id, &cleanup_control, || {
                cleanup_checked_tx.send(()).expect("test should still be waiting for cleanup checkpoint");
                continue_cleanup_rx.recv().expect("test should release paused cleanup");
                cleanup_node.deregister_peer(peer_id);
            })
        });

        cleanup_checked_rx.recv_timeout(Duration::from_secs(1))?;

        let (replacement_started_tx, replacement_started_rx) = sync_channel(1);
        let (replacement_done_tx, replacement_done_rx) = sync_channel(1);
        let replacement_registry = registry.clone();
        let replacement_node = node.clone();
        let replacement_runtime = runtime.handle().clone();
        let replacement_thread = std::thread::spawn(move || {
            let _runtime_context = replacement_runtime.enter();
            let (sender, rx, control) = IrohPeerSender::new(peer_id);
            replacement_started_tx.send(()).expect("test should wait for replacement to start");
            let registration = replacement_registry.register_peer(&replacement_node, presence, sender, control.clone());
            replacement_done_tx.send(()).expect("test should observe replacement completion");
            (registration, rx, control)
        });

        replacement_started_rx.recv_timeout(Duration::from_secs(1))?;
        let replacement_completed_while_cleanup_paused = replacement_done_rx.recv_timeout(Duration::from_millis(200)).is_ok();

        // Release and join both threads before asserting, so a regression fails
        // cleanly instead of leaving either side blocked on a test channel.
        continue_cleanup_tx.send(()).expect("cleanup task should still be paused");
        assert!(cleanup_thread.join().expect("cleanup thread should not panic"));
        let (replacement_registration, _replacement_rx, replacement_control) =
            replacement_thread.join().expect("replacement thread should not panic");

        assert!(!replacement_completed_while_cleanup_paused, "replacement bypassed in-progress cleanup's registry lock");
        assert_eq!(node.get_durable_peers(), vec![peer_id]);
        assert!(!replacement_control.is_stopped());

        // The manually exercised cleanup removed the old registry entry; the
        // old guard is now stale and must also leave the replacement alone.
        drop(old_registration);
        assert_eq!(node.get_durable_peers(), vec![peer_id]);

        drop(replacement_registration);
        assert!(node.get_durable_peers().is_empty());
        Ok(())
    }
}
