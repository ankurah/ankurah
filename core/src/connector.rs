use ankurah_proto as proto;
use async_trait::async_trait;

use crate::{node::ConnectionRegistration, policy::PolicyAgent, storage::StorageEngine, Node, NodeState};

mod handshake;
pub use handshake::Handshake;

/// Why a connection's handshake does not make its peer available to the node.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum PeerConnectionError {
    #[error("this node speaks protocol versions {ours} and the peer speaks {theirs}, with none in common")]
    NoCommonVersion { ours: proto::ProtocolVersions, theirs: proto::ProtocolVersions },
    #[error("the peer's opening frame cannot be read: {0}")]
    UnreadableOpening(String),
    #[error("the peer sent a second opening frame")]
    RepeatedOpening,
    #[error("no opening frame has been received from the peer on this connection")]
    NoOpening,
    #[error("presence from node {0} answers a challenge this connection did not issue")]
    ChallengeMismatch(proto::NodeId),
    #[error("presence signature is not valid for node {0}")]
    InvalidSignature(proto::NodeId),
    /// The peer's opening carried another node's challenge, as a node relaying that node's
    /// handshake would send it.
    #[error("node {peer}'s opening carried a challenge issued by node {issuer}")]
    RelayedChallenge { peer: proto::NodeId, issuer: proto::NodeId },
    #[error("node {0} refused a connection to itself")]
    SelfConnection(proto::NodeId),
    #[error("presence from node {0} does not repeat the versions offered and chosen on this connection")]
    OfferMismatch(proto::NodeId),
    /// A second presence was registered on a handshake that has already registered its peer.
    #[error("this connection's handshake has already registered its peer")]
    RepeatedRegistration,
    /// This node was asked to present on a connection it accepted before it registered the dialer.
    #[error("this node accepted the connection and presents only once it has registered the dialer")]
    PresenceBeforeRegistration,
    #[error("{0}")]
    Refused(crate::node::PeerRefusal),
    #[error("{0}")]
    AnnouncementFailed(crate::node::AnnouncementFailure),
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

// TODO redesign this such that the sender and receiver are disconnected at the same time

#[async_trait]
pub trait PeerSender: Send + Sync {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError>;
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
    /// Begin this node's side of one new connection's handshake, reached as `provenance` says.
    fn begin_handshake(&self, provenance: PeerProvenance) -> Handshake;
    /// Sign this node's presence answering the peer on `handshake`'s connection; a halted node signs none.
    async fn presence(&self, handshake: &Handshake) -> Result<proto::Presence, PeerConnectionError>;
    /// Register the peer whose presence completes `handshake`; `sender` holds the frames it is
    /// given until the connector has written this node's presence.
    async fn register_peer(
        &self,
        presence: proto::Presence,
        handshake: &mut Handshake,
        sender: Box<dyn PeerSender>,
    ) -> Result<ConnectionRegistration, PeerConnectionError>;
    fn state(&self) -> ankurah_signals::Read<NodeState>;
    fn deregister_peer(&self, registration: ConnectionRegistration);
    /// Handle a frame that arrived on the connection `registration` registered.
    async fn handle_message(&self, registration: ConnectionRegistration, message: proto::NodeMessage) -> anyhow::Result<()>;
    fn cloned(&self) -> Box<dyn NodeComms>;
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> NodeComms for Node<SE, PA> {
    fn id(&self) -> proto::NodeId { self.id }
    fn begin_handshake(&self, provenance: PeerProvenance) -> Handshake { self.begin_handshake(provenance) }
    async fn presence(&self, handshake: &Handshake) -> Result<proto::Presence, PeerConnectionError> { self.presence(handshake).await }
    async fn register_peer(
        &self,
        presence: proto::Presence,
        handshake: &mut Handshake,
        sender: Box<dyn PeerSender>,
    ) -> Result<ConnectionRegistration, PeerConnectionError> {
        self.register_peer(presence, handshake, sender).await
    }
    fn state(&self) -> ankurah_signals::Read<NodeState> { self.state() }
    fn deregister_peer(&self, registration: ConnectionRegistration) { self.deregister_peer(registration); }
    async fn handle_message(&self, registration: ConnectionRegistration, message: proto::NodeMessage) -> anyhow::Result<()> {
        self.handle_message(registration, message).await
    }
    fn cloned(&self) -> Box<dyn NodeComms> { Box::new(self.clone()) }
}
