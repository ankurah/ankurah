use ankurah_proto::{self as proto, Attested, EntityState};
use async_trait::async_trait;

use crate::{policy::PolicyAgent, storage::StorageEngine, Node};

// TODO redesign this such that:
// - the sender and receiver are disconnected at the same time
// - a connection id or dyn Ord/Eq/Hash is used to identify the connection for deregistration
//   so that we can have multiple connections to the same node without things getting mixed up

#[async_trait]
pub trait PeerSender: Send + Sync {
    fn send_message(&self, message: proto::SignedPeerMessage) -> Result<(), SendError>;
    /// Terminate the exact registered transport session shared by every clone
    /// of this sender. Implementations must be idempotent and non-blocking.
    fn close(&self);
    /// The node ID of the recipient of this message
    fn recipient_node_id(&self) -> proto::NodeId;
    fn cloned(&self) -> Box<dyn PeerSender>;
}

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("Connection closed")]
    ConnectionClosed,
    #[error("Send timeout")]
    Timeout,
    #[error("Other error: {0}")]
    Other(#[from] anyhow::Error),
    #[error("Unknown error")]
    Unknown,
}

/// A peer sent an envelope claiming to come from a different node than the
/// signed Presence authenticated for its connection.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("authenticated peer {authenticated_peer} sent a message declaring sender {declared_sender}")]
pub struct PeerIdentityMismatch {
    pub authenticated_peer: proto::NodeId,
    pub declared_sender: proto::NodeId,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PeerFrameError {
    #[error(transparent)]
    IdentityMismatch(#[from] PeerIdentityMismatch),
    #[error("peer {peer} sent a frame for a different authenticated session")]
    WrongSession { peer: proto::NodeId },
    #[error("peer {peer} replayed frame sequence {sequence}")]
    ReplayedSequence { peer: proto::NodeId, sequence: u64 },
    #[error("peer {peer} sent stale frame sequence {sequence} outside the replay window")]
    StaleSequence { peer: proto::NodeId, sequence: u64 },
    #[error("peer frame signature invalid for node {peer}")]
    InvalidSignature { peer: proto::NodeId },
    #[error("peer {peer} is not registered")]
    PeerNotRegistered { peer: proto::NodeId },
    #[error("peer {peer} cannot send traffic until its system-root join is durable")]
    PeerNotReady { peer: proto::NodeId },
}

/// Opaque token proving a frame passed session, sequence, signature, and
/// declared-sender checks. Connectors can verify synchronously in wire order,
/// then dispatch the token asynchronously without exposing an unbound message
/// injection seam.
#[derive(Debug)]
pub struct VerifiedPeerMessage {
    pub(crate) message: proto::NodeMessage,
    pub(crate) authenticated_peer: proto::NodeId,
    pub(crate) incoming_session: proto::HandshakeChallenge,
    pub(crate) system_generation: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl VerifiedPeerMessage {
    pub fn message(&self) -> &proto::NodeMessage { &self.message }
}

/// Core-issued, single-use capability for authenticating one peer connection.
///
/// The wire sees only [`Self::challenge`]. Connectors must move this token into
/// `register_peer`, so a duplicate or out-of-order Presence has no verifier to
/// reuse even though the wire challenge itself is copyable.
pub struct PeerHandshake {
    challenge: proto::HandshakeChallenge,
}

impl PeerHandshake {
    pub(crate) fn new(challenge: proto::HandshakeChallenge) -> Self { Self { challenge } }

    pub fn challenge(&self) -> proto::HandshakeChallenge { self.challenge }
}

#[async_trait]
pub trait NodeComms: Send + Sync {
    fn id(&self) -> proto::NodeId;
    fn durable(&self) -> bool;
    fn system_root(&self) -> Option<Attested<EntityState>>;
    fn begin_peer_handshake(&self) -> PeerHandshake;
    fn presence(&self, challenge: proto::HandshakeChallenge) -> proto::Presence;
    fn register_peer(
        &self,
        presence: proto::Presence,
        handshake: PeerHandshake,
        outgoing_session: proto::HandshakeChallenge,
        sender: Box<dyn PeerSender>,
    ) -> Result<(), proto::PresenceRefusal>;
    fn deregister_peer(&self, node_id: proto::NodeId, incoming_session: proto::HandshakeChallenge) -> bool;
    fn verify_peer_message(
        &self,
        authenticated_peer: proto::NodeId,
        message: proto::SignedPeerMessage,
    ) -> Result<VerifiedPeerMessage, PeerFrameError>;
    async fn handle_verified_peer_message(&self, message: VerifiedPeerMessage) -> anyhow::Result<()>;
    fn cloned(&self) -> Box<dyn NodeComms>;
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> NodeComms for Node<SE, PA> {
    fn id(&self) -> proto::NodeId { self.id }
    fn durable(&self) -> bool { self.durable }
    fn system_root(&self) -> Option<Attested<EntityState>> { self.system.root() }
    fn begin_peer_handshake(&self) -> PeerHandshake { Node::begin_peer_handshake(self) }
    fn presence(&self, challenge: proto::HandshakeChallenge) -> proto::Presence { Node::presence(self, challenge) }
    fn register_peer(
        &self,
        presence: proto::Presence,
        handshake: PeerHandshake,
        outgoing_session: proto::HandshakeChallenge,
        sender: Box<dyn PeerSender>,
    ) -> Result<(), proto::PresenceRefusal> {
        //
        self.register_peer(presence, handshake, outgoing_session, sender)
    }
    fn deregister_peer(&self, node_id: proto::NodeId, incoming_session: proto::HandshakeChallenge) -> bool {
        self.deregister_peer_session(node_id, incoming_session)
    }
    fn verify_peer_message(
        &self,
        authenticated_peer: proto::NodeId,
        message: proto::SignedPeerMessage,
    ) -> Result<VerifiedPeerMessage, PeerFrameError> {
        //
        self.verify_peer_message(authenticated_peer, message)
    }
    async fn handle_verified_peer_message(&self, message: VerifiedPeerMessage) -> anyhow::Result<()> {
        self.handle_verified_peer_message(message).await
    }
    fn cloned(&self) -> Box<dyn NodeComms> { Box::new(self.clone()) }
}
