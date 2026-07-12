//! Shared connection pump used by both the dialer ([`crate::IrohClient`]) and the
//! acceptor ([`crate::IrohServer`]).
//!
//! Wire format: one bidirectional QUIC stream per connection, carrying
//! length-delimited, exactly encoded [`proto::Message`] frames. Each side first
//! sends a fresh challenge, answers the remote challenge with a signed Presence,
//! and then admits only signed, sequenced frames for that authenticated session.

use ankurah_core::{connector::PeerHandshake, policy::PolicyAgent, storage::StorageEngine, Node};
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

/// Maximum established frame size is owned by proto so every transport and the
/// deserializer enforce one ceiling.
pub(crate) const MAX_FRAME_LENGTH: usize = proto::MAX_WIRE_MESSAGE_BYTES;

/// Handshake frames are deliberately small so an unregistered peer cannot
/// reserve the established connection's full frame allowance.
const MAX_HANDSHAKE_FRAME_LENGTH: usize = 64 * 1024;

/// Once QUIC is established, Presence should be immediately available. This
/// timeout also bounds peers that open a stream but never complete the Ankurah
/// handshake.
pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

fn codec(max_frame_length: usize) -> LengthDelimitedCodec { LengthDelimitedCodec::builder().max_frame_length(max_frame_length).new_codec() }

/// Coordinates registrations created by one connector instance. Registration
/// and conditional deregistration share this lock, so cleanup from an older
/// connection cannot remove a newer registration for the same peer.
///
/// This registry is intentionally connector-local and stops superseded iroh
/// pumps eagerly. Core's session-conditional deregistration is the final guard
/// across transports and independent connector instances.
#[derive(Default)]
pub(crate) struct RegistrationRegistry {
    active: Mutex<HashMap<proto::NodeId, Arc<ConnectionControl>>>,
}

impl RegistrationRegistry {
    fn register_peer<SE, PA>(
        self: &Arc<Self>,
        node: &Node<SE, PA>,
        presence: proto::Presence,
        handshake: PeerHandshake,
        outgoing_session: proto::HandshakeChallenge,
        sender: IrohPeerSender,
        control: Arc<ConnectionControl>,
    ) -> std::result::Result<PeerRegistration<SE, PA>, proto::PresenceRefusal>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let peer_id = presence.node_id;
        let incoming_session = handshake.challenge();
        let mut active = self.active.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        node.register_peer(presence, handshake, outgoing_session, Box::new(sender))?;
        if let Some(previous) = active.insert(peer_id, control.clone()) {
            previous.stop_connection();
        }

        Ok(PeerRegistration { node: node.clone(), peer_id, incoming_session, control, registry: self.clone() })
    }

    fn deregister_peer_if_current<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        peer_id: proto::NodeId,
        incoming_session: proto::HandshakeChallenge,
        control: &Arc<ConnectionControl>,
    ) -> bool
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        self.remove_if_current(peer_id, control, || {
            node.deregister_peer_session(peer_id, incoming_session);
        })
    }

    /// Remove `control` and run its Node cleanup as one registry critical
    /// section. The callback keeps the registry independent of Node generics
    /// and lets tests pause the critical section at a deterministic point.
    fn remove_if_current(&self, peer_id: proto::NodeId, control: &Arc<ConnectionControl>, cleanup: impl FnOnce()) -> bool {
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
    peer_id: proto::NodeId,
    incoming_session: proto::HandshakeChallenge,
    control: Arc<ConnectionControl>,
    registry: Arc<RegistrationRegistry>,
}

impl<SE, PA> Drop for PeerRegistration<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if self.registry.deregister_peer_if_current(&self.node, self.peer_id, self.incoming_session, &self.control) {
            debug!("Deregistered peer {}", self.peer_id);
        } else {
            debug!("Skipped deregistering replaced peer {}", self.peer_id);
        }
    }
}

/// Run one established connection to completion.
///
/// The authenticated QUIC `remote_id` is authoritative. The challenge issuer,
/// Presence node id, signed frame key, and every NodeMessage sender must all
/// agree with it. Registration cleanup is cancellation-safe and conditional on
/// this connection's receiver-generated session.
pub(crate) async fn run_connection<SE, PA>(
    node: &Node<SE, PA>,
    registrations: &Arc<RegistrationRegistry>,
    remote_id: iroh::EndpointId,
    send_stream: SendStream,
    recv_stream: RecvStream,
    shutdown: Option<&CancellationToken>,
    mut on_peer_presence: impl FnMut(&proto::Presence),
) -> Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let mut writer = FramedWrite::new(send_stream, codec(MAX_HANDSHAKE_FRAME_LENGTH));
    let mut reader = FramedRead::new(recv_stream, codec(MAX_HANDSHAKE_FRAME_LENGTH));
    let remote_node_id = crate::endpoint_node_id(remote_id);

    let handshake = node.begin_peer_handshake();
    let incoming_session = handshake.challenge();
    if !send_handshake_message(&mut writer, proto::Message::HandshakeChallenge(incoming_session), shutdown).await? {
        return Ok(());
    }
    debug!("Sent handshake challenge");

    let outgoing_session = match read_handshake_message(&mut reader, shutdown, "peer challenge").await? {
        Some(proto::Message::HandshakeChallenge(challenge)) if challenge.issuer() == remote_node_id => challenge,
        Some(proto::Message::HandshakeChallenge(_)) => {
            bail!("peer challenge issuer does not match authenticated iroh endpoint {remote_id}")
        }
        Some(other) => bail!("received {other} before peer challenge"),
        None => return Ok(()),
    };

    if !send_handshake_message(&mut writer, proto::Message::Presence(node.presence(outgoing_session)), shutdown).await? {
        return Ok(());
    }
    debug!("Sent signed presence");

    let peer_presence = match read_handshake_message(&mut reader, shutdown, "peer presence").await? {
        Some(proto::Message::Presence(presence)) => presence,
        Some(proto::Message::PresenceRejected(rejection)) => bail!("peer refused our presence: {rejection}"),
        Some(other) => bail!("received {other} before peer presence"),
        None => return Ok(()),
    };
    validate_remote_presence(remote_node_id, &peer_presence)?;
    if shutdown.is_some_and(CancellationToken::is_cancelled) {
        return Ok(());
    }
    debug!("Received peer presence: {}", peer_presence);

    let (sender, mut outgoing_rx, connection_control) = IrohPeerSender::new(peer_presence.node_id);
    let _registration =
        match registrations.register_peer(node, peer_presence.clone(), handshake, outgoing_session, sender, connection_control.clone()) {
            Ok(registration) => registration,
            Err(proto::PresenceRefusal::IncompatibleVersion(rejection)) => {
                let _ = send_handshake_message(&mut writer, proto::Message::PresenceRejected(rejection.clone()), shutdown).await;
                return Err(rejection.into());
            }
            Err(error) => return Err(error.into()),
        };
    if shutdown.is_some_and(CancellationToken::is_cancelled) {
        return Ok(());
    }
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
                    Some(Ok(data)) => {
                        let message = match verify_frame(node, remote_node_id, &data) {
                            Ok(message) => message,
                            Err(error) => break Err(error),
                        };
                        match await_dispatch_or_stop(
                            node.handle_verified_peer_message(message),
                            shutdown,
                            &connection_control,
                        ).await {
                            Some(Ok(())) => {}
                            Some(Err(error)) => {
                                // Application/storage failures are not wire-authentication
                                // failures. Match the websocket connectors: surface the
                                // error but keep the authenticated session alive.
                                warn!("Error handling verified peer message from {remote_node_id}: {error}");
                            }
                            None => {
                                debug!("Connection stopped while dispatching an incoming frame");
                                break Ok(());
                            }
                        }
                    }
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

fn validate_remote_presence(remote_node_id: proto::NodeId, presence: &proto::Presence) -> Result<()> {
    if presence.node_id != remote_node_id {
        bail!("Presence node identity {} does not match authenticated iroh endpoint {}", presence.node_id, remote_node_id);
    }
    Ok(())
}

async fn send_handshake_message(
    writer: &mut FramedWrite<SendStream, LengthDelimitedCodec>,
    message: proto::Message,
    shutdown: Option<&CancellationToken>,
) -> Result<bool> {
    let data = proto::encode_message(&message)?;
    match await_handshake_send(writer.send(data.into()), shutdown, HANDSHAKE_TIMEOUT).await {
        Ok(Some(Ok(()))) => Ok(true),
        Ok(Some(Err(error))) => Err(error.into()),
        Ok(None) => Ok(false),
        Err(_) => bail!("timed out sending handshake frame"),
    }
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

/// Keep incoming dispatch strictly ordered while allowing shutdown and session
/// replacement to cancel a handler stalled in policy or storage code.
async fn await_dispatch_or_stop<F>(
    dispatch: F,
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
        result = dispatch => Some(result),
    }
}

async fn read_handshake_message(
    reader: &mut FramedRead<RecvStream, LengthDelimitedCodec>,
    shutdown: Option<&CancellationToken>,
    expected: &str,
) -> Result<Option<proto::Message>> {
    let next = timeout(HANDSHAKE_TIMEOUT, reader.next());
    tokio::pin!(next);

    let frame = match shutdown {
        Some(shutdown) => {
            select! {
                biased;
                _ = shutdown.cancelled() => return Ok(None),
                frame = &mut next => frame,
            }
        }
        None => next.await,
    }
    .map_err(|_| anyhow!("timed out waiting for {expected}"))?;

    match frame {
        Some(Ok(data)) => proto::decode_message(&data).map(Some).map_err(|error| anyhow!("failed to decode {expected}: {error}")),
        Some(Err(e)) => Err(e.into()),
        None => Err(anyhow!("stream closed before {expected}")),
    }
}

fn verify_frame<SE, PA>(
    node: &Node<SE, PA>,
    authenticated_peer: proto::NodeId,
    data: &[u8],
) -> Result<ankurah_core::connector::VerifiedPeerMessage>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    match proto::decode_message(data)? {
        proto::Message::PeerMessage(frame) => Ok(node.verify_peer_message(authenticated_peer, frame)?),
        proto::Message::Presence(presence) => bail!("received duplicate Presence from {}", presence.node_id),
        proto::Message::HandshakeChallenge(_) => bail!("received duplicate handshake challenge"),
        proto::Message::PresenceRejected(rejection) => bail!("peer rejected established connection: {rejection}"),
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
        let mut codec = codec(MAX_HANDSHAKE_FRAME_LENGTH);
        let mut header = BytesMut::new();
        header.put_u32((MAX_HANDSHAKE_FRAME_LENGTH + 1) as u32);

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
        let (_sender, _rx, control) = IrohPeerSender::new(proto::NodeId::from_bytes([7; 32]));
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
        let (_sender, _rx, control) = IrohPeerSender::new(proto::NodeId::from_bytes([7; 32]));
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
    async fn stalled_dispatch_is_interrupted_by_connection_replacement() {
        let (_sender, _rx, control) = IrohPeerSender::new(proto::NodeId::from_bytes([7; 32]));
        let mut dispatch = Box::pin(await_dispatch_or_stop(std::future::pending::<Result<()>>(), None, &control));

        assert!(timeout(Duration::from_millis(10), &mut dispatch).await.is_err());
        control.stop_connection();
        assert!(timeout(Duration::from_secs(1), dispatch).await.expect("stalled dispatch should stop").is_none());
    }

    #[tokio::test]
    async fn stalled_dispatch_is_interrupted_by_shutdown() {
        let (_sender, _rx, control) = IrohPeerSender::new(proto::NodeId::from_bytes([7; 32]));
        let shutdown = CancellationToken::new();
        let mut dispatch = Box::pin(await_dispatch_or_stop(std::future::pending::<Result<()>>(), Some(&shutdown), &control));

        assert!(timeout(Duration::from_millis(10), &mut dispatch).await.is_err());
        shutdown.cancel();
        assert!(timeout(Duration::from_secs(1), dispatch).await.expect("stalled dispatch should stop").is_none());
    }

    type TestNode = Node<SledStorageEngine, PermissiveAgent>;

    fn signed_presence(
        node: &TestNode,
        peer_key: &ed25519_dalek::SigningKey,
        nonce: u8,
    ) -> (proto::Presence, PeerHandshake, proto::HandshakeChallenge) {
        use ed25519_dalek::Signer;

        let handshake = node.begin_peer_handshake();
        let challenge = handshake.challenge();
        let peer_id = proto::NodeId::from(peer_key.verifying_key());
        let outgoing_session = proto::HandshakeChallenge::new(peer_id, [nonce; 32]);
        let claims = proto::PresenceClaims {
            node_id: peer_id,
            durable: false,
            system_root: None,
            challenge,
            timestamp: u64::from(nonce),
            protocol_version: proto::PROTOCOL_VERSION,
        };
        let signature = peer_key.sign(&proto::Presence::signable_bytes(&claims)).into();
        (
            proto::Presence {
                node_id: peer_id,
                durable: false,
                system_root: None,
                challenge,
                timestamp: claims.timestamp,
                signature,
                protocol_version: proto::PROTOCOL_VERSION,
            },
            handshake,
            outgoing_session,
        )
    }

    fn signed_frame(peer_key: &ed25519_dalek::SigningKey, session: proto::HandshakeChallenge, sequence: u64) -> proto::SignedPeerMessage {
        use ed25519_dalek::Signer;

        let from = proto::NodeId::from(peer_key.verifying_key());
        let message = proto::NodeMessage::UnsubscribeQuery { from, query_id: proto::QueryId::new() };
        let signature = peer_key.sign(&proto::SignedPeerMessage::signable_bytes(session, sequence, &message)).into();
        proto::SignedPeerMessage { session, sequence, message, signature }
    }

    #[tokio::test]
    async fn presence_must_match_authenticated_iroh_remote() {
        let node = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
        let peer_key = ed25519_dalek::SigningKey::from_bytes(&[31; 32]);
        let (presence, _, _) = signed_presence(&node, &peer_key, 1);
        let different_remote = proto::NodeId::from_bytes([99; 32]);
        assert!(validate_remote_presence(different_remote, &presence).is_err());
        assert!(validate_remote_presence(presence.node_id, &presence).is_ok());
    }

    #[tokio::test]
    async fn captured_presence_cannot_authenticate_fresh_handshake() -> Result<()> {
        let node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
        let peer_key = ed25519_dalek::SigningKey::from_bytes(&[32; 32]);
        let peer_id = proto::NodeId::from(peer_key.verifying_key());
        let (captured, _, outgoing_session) = signed_presence(&node, &peer_key, 1);
        let fresh_handshake = node.begin_peer_handshake();
        let (sender, _rx, _control) = IrohPeerSender::new(peer_id);
        let error = node.register_peer(captured, fresh_handshake, outgoing_session, Box::new(sender)).unwrap_err();
        assert!(matches!(error, proto::PresenceRefusal::UnexpectedChallenge(id) if id == peer_id));
        Ok(())
    }

    #[tokio::test]
    async fn bad_replayed_and_stale_signed_frames_are_rejected() -> Result<()> {
        let node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
        let peer_key = ed25519_dalek::SigningKey::from_bytes(&[33; 32]);
        let bad_key = ed25519_dalek::SigningKey::from_bytes(&[34; 32]);
        let peer_id = proto::NodeId::from(peer_key.verifying_key());
        let (presence, handshake, outgoing_session) = signed_presence(&node, &peer_key, 2);
        let incoming_session = handshake.challenge();
        let (sender, _rx, _control) = IrohPeerSender::new(peer_id);
        node.register_peer(presence, handshake, outgoing_session, Box::new(sender))?;

        let mut bad = signed_frame(&peer_key, incoming_session, 1);
        bad.signature = signed_frame(&bad_key, incoming_session, 1).signature;
        assert!(matches!(node.verify_peer_message(peer_id, bad), Err(ankurah_core::connector::PeerFrameError::InvalidSignature { .. })));

        let frame = signed_frame(&peer_key, incoming_session, 10);
        node.verify_peer_message(peer_id, frame)?;
        assert!(matches!(
            node.verify_peer_message(peer_id, signed_frame(&peer_key, incoming_session, 10)),
            Err(ankurah_core::connector::PeerFrameError::ReplayedSequence { .. })
        ));
        node.verify_peer_message(peer_id, signed_frame(&peer_key, incoming_session, 5000))?;
        let wrong_session = proto::HandshakeChallenge::new(node.id, [0xFF; 32]);
        assert!(matches!(
            node.verify_peer_message(peer_id, signed_frame(&peer_key, wrong_session, 5001)),
            Err(ankurah_core::connector::PeerFrameError::WrongSession { .. })
        ));
        assert!(matches!(
            node.verify_peer_message(peer_id, signed_frame(&peer_key, incoming_session, 0)),
            Err(ankurah_core::connector::PeerFrameError::StaleSequence { .. })
        ));
        Ok(())
    }

    #[test]
    fn exact_established_codec_rejects_trailing_bytes() {
        let peer_key = ed25519_dalek::SigningKey::from_bytes(&[35; 32]);
        let session = proto::HandshakeChallenge::new(proto::NodeId::from_bytes([36; 32]), [37; 32]);
        let mut bytes = proto::encode_message(&proto::Message::PeerMessage(signed_frame(&peer_key, session, 1))).unwrap();
        bytes.push(0);
        assert!(proto::decode_message(&bytes).is_err());
    }

    #[tokio::test]
    async fn replacement_stops_old_connection_and_stale_cleanup_skips_new_peer() -> Result<()> {
        let node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
        let registry = Arc::new(RegistrationRegistry::default());
        let peer_key = ed25519_dalek::SigningKey::from_bytes(&[38; 32]);
        let peer_id = proto::NodeId::from(peer_key.verifying_key());
        let (presence, handshake, outgoing_session) = signed_presence(&node, &peer_key, 1);

        let (old_sender, _old_rx, old_control) = IrohPeerSender::new(peer_id);
        let old_registration = registry.register_peer(&node, presence, handshake, outgoing_session, old_sender, old_control.clone())?;
        assert!(!old_control.is_stopped());

        let (presence, handshake, outgoing_session) = signed_presence(&node, &peer_key, 2);
        let replacement_incoming_session = handshake.challenge();
        let (replacement_sender, _replacement_rx, replacement_control) = IrohPeerSender::new(peer_id);
        let replacement_registration =
            registry.register_peer(&node, presence, handshake, outgoing_session, replacement_sender, replacement_control.clone())?;
        assert!(old_control.is_stopped());
        assert!(!replacement_control.is_stopped());

        drop(old_registration);
        assert!(registry.active.lock().unwrap().contains_key(&peer_id));
        assert!(!replacement_control.is_stopped());
        node.verify_peer_message(peer_id, signed_frame(&peer_key, replacement_incoming_session, 0))?;

        drop(replacement_registration);
        assert!(!registry.active.lock().unwrap().contains_key(&peer_id));
        assert!(matches!(
            node.verify_peer_message(peer_id, signed_frame(&peer_key, replacement_incoming_session, 1)),
            Err(ankurah_core::connector::PeerFrameError::PeerNotRegistered { .. })
        ));
        Ok(())
    }

    #[test]
    fn cleanup_and_replacement_cannot_interleave_check_then_remove() -> Result<()> {
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?;
        let _runtime_context = runtime.enter();
        let node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
        let registry = Arc::new(RegistrationRegistry::default());
        let peer_key = ed25519_dalek::SigningKey::from_bytes(&[39; 32]);
        let peer_id = proto::NodeId::from(peer_key.verifying_key());
        let (presence, handshake, outgoing_session) = signed_presence(&node, &peer_key, 1);
        let old_incoming_session = handshake.challenge();
        let (old_sender, _old_rx, old_control) = IrohPeerSender::new(peer_id);
        let old_registration = registry.register_peer(&node, presence, handshake, outgoing_session, old_sender, old_control.clone())?;

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
                cleanup_node.deregister_peer_session(peer_id, old_incoming_session);
            })
        });

        cleanup_checked_rx.recv_timeout(Duration::from_secs(1))?;

        let (replacement_presence, replacement_handshake, replacement_outgoing_session) = signed_presence(&node, &peer_key, 2);
        let (replacement_started_tx, replacement_started_rx) = sync_channel(1);
        let (replacement_done_tx, replacement_done_rx) = sync_channel(1);
        let replacement_registry = registry.clone();
        let replacement_node = node.clone();
        let replacement_runtime = runtime.handle().clone();
        let replacement_thread = std::thread::spawn(move || {
            let _runtime_context = replacement_runtime.enter();
            let (sender, rx, control) = IrohPeerSender::new(peer_id);
            replacement_started_tx.send(()).expect("test should wait for replacement to start");
            let registration = replacement_registry
                .register_peer(
                    &replacement_node,
                    replacement_presence,
                    replacement_handshake,
                    replacement_outgoing_session,
                    sender,
                    control.clone(),
                )
                .expect("replacement registration should authenticate");
            replacement_done_tx.send(()).expect("test should observe replacement completion");
            (registration, rx, control)
        });

        replacement_started_rx.recv_timeout(Duration::from_secs(1))?;
        let replacement_completed_while_cleanup_paused = replacement_done_rx.recv_timeout(Duration::from_millis(200)).is_ok();

        continue_cleanup_tx.send(()).expect("cleanup task should still be paused");
        assert!(cleanup_thread.join().expect("cleanup thread should not panic"));
        let (replacement_registration, _replacement_rx, replacement_control) =
            replacement_thread.join().expect("replacement thread should not panic");

        assert!(!replacement_completed_while_cleanup_paused, "replacement bypassed in-progress cleanup's registry lock");
        assert!(registry.active.lock().unwrap().contains_key(&peer_id));
        assert!(!replacement_control.is_stopped());

        drop(old_registration);
        assert!(registry.active.lock().unwrap().contains_key(&peer_id));

        drop(replacement_registration);
        assert!(!registry.active.lock().unwrap().contains_key(&peer_id));
        Ok(())
    }
}
