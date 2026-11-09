use ankurah_proto as proto;
use async_trait::async_trait;

use crate::{policy::PolicyAgent, storage::StorageEngine, Node, NodeState};

/// A peer rejected before its connection becomes available to the node.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum PeerConnectionError {
    #[error("{0}")]
    Protocol(#[from] proto::PresenceRejection),
    #[error("presence from node {0} answers a challenge this connection did not issue")]
    ChallengeMismatch(proto::NodeId),
    #[error("presence signature is not valid for node {0}")]
    InvalidSignature(proto::NodeId),
    #[error("node {0} refused a connection to itself")]
    SelfConnection(proto::NodeId),
    #[error("node halted: {0}")]
    NodeHalted(#[from] crate::error::NodeHaltReason),
}

/// How this node came to be connected to a peer. Trust in the peer's durable claim follows from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerProvenance {
    /// This node dialed the peer from its own configuration.
    Configured,
    /// The peer connected to this node.
    Inbound,
}

/// A challenge a node issued for one connection. Send its value to the peer, then
/// pass it to `register_peer` with the peer's presence, which spends it. Its nonce
/// is always drawn fresh, so it matches only a presence signed for this connection.
#[derive(Debug)]
pub struct IssuedChallenge(proto::HandshakeChallenge);

impl IssuedChallenge {
    /// A challenge from `issuer` with a fresh nonce.
    pub fn new(issuer: proto::NodeId) -> Self { Self(proto::HandshakeChallenge::new(issuer)) }

    /// The value to send the peer.
    pub fn challenge(&self) -> proto::HandshakeChallenge { self.0 }

    /// Check that `presence` answers this challenge, which `receiver` issued, under a valid
    /// signature from a node other than `receiver`.
    pub(crate) fn authenticate(self, receiver: proto::NodeId, presence: &proto::Presence) -> Result<(), PeerConnectionError> {
        let peer = presence.node_id;
        if self.0.issuer() != receiver || presence.challenge != self.0 {
            return Err(PeerConnectionError::ChallengeMismatch(peer));
        }
        if !presence.signature_is_valid() {
            return Err(PeerConnectionError::InvalidSignature(peer));
        }
        if peer == receiver {
            return Err(PeerConnectionError::SelfConnection(peer));
        }
        Ok(())
    }
}

// TODO redesign this such that:
// - the sender and receiver are disconnected at the same time
// - a connection id or dyn Ord/Eq/Hash is used to identify the connection for deregistration
//   so that we can have multiple connections to the same node without things getting mixed up

#[async_trait]
pub trait PeerSender: Send + Sync {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError>;
    /// The node ID of the recipient of this message
    fn recipient_node_id(&self) -> proto::NodeId;
    fn cloned(&self) -> Box<dyn PeerSender>;
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum SendError {
    #[error("Connection closed")]
    ConnectionClosed,
    #[error("Send timeout")]
    Timeout,
    #[error("Other error: {0}")]
    Other(#[source] std::sync::Arc<anyhow::Error>),
    #[error("Unknown error")]
    Unknown,
}

impl From<anyhow::Error> for SendError {
    fn from(error: anyhow::Error) -> Self { Self::Other(std::sync::Arc::new(error)) }
}

#[async_trait]
pub trait NodeComms: Send + Sync {
    fn id(&self) -> proto::NodeId;
    /// Issue a fresh challenge for one new connection.
    fn issue_challenge(&self) -> IssuedChallenge;
    /// Sign this node's presence answering a peer's challenge; a halted node signs none.
    async fn presence(&self, challenge: proto::HandshakeChallenge) -> Result<proto::Presence, crate::error::NodeHaltReason>;
    async fn register_peer(
        &self,
        presence: proto::Presence,
        challenge: IssuedChallenge,
        provenance: PeerProvenance,
        sender: Box<dyn PeerSender>,
    ) -> Result<(), PeerConnectionError>;
    fn state(&self) -> ankurah_signals::Read<NodeState>;
    fn deregister_peer(&self, node_id: proto::NodeId);
    async fn handle_message(&self, message: proto::NodeMessage) -> anyhow::Result<()>;
    fn cloned(&self) -> Box<dyn NodeComms>;
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> NodeComms for Node<SE, PA> {
    fn id(&self) -> proto::NodeId { self.id }
    fn issue_challenge(&self) -> IssuedChallenge { self.issue_challenge() }
    async fn presence(&self, challenge: proto::HandshakeChallenge) -> Result<proto::Presence, crate::error::NodeHaltReason> {
        self.presence(challenge).await
    }
    async fn register_peer(
        &self,
        presence: proto::Presence,
        challenge: IssuedChallenge,
        provenance: PeerProvenance,
        sender: Box<dyn PeerSender>,
    ) -> Result<(), PeerConnectionError> {
        self.register_peer(presence, challenge, provenance, sender).await
    }
    fn state(&self) -> ankurah_signals::Read<NodeState> { self.state() }
    fn deregister_peer(&self, node_id: proto::NodeId) {
        //
        self.deregister_peer(node_id);
    }
    async fn handle_message(&self, message: proto::NodeMessage) -> anyhow::Result<()> {
        //
        self.handle_message(message).await
    }
    fn cloned(&self) -> Box<dyn NodeComms> { Box::new(self.clone()) }
}
