use crate::internal::prelude::*;
use std::{collections::BTreeSet, convert::Infallible, sync::Arc};

use ankurah_proto::{DecodeError, EntityId, EventId};
use thiserror::Error;

use crate::connector::SendError;
pub use crate::node::event_admissibility::InadmissibleEvent;

/// Why this node permanently stopped accepting work, not a rejected connection or operation.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum NodeHaltReason {
    #[error("failed to reconstruct the local system: {0}")]
    SystemLoad(String),
    #[error("failed to reconstruct the local catalog: {0}")]
    CatalogLoad(String),
    #[error("system replacement requires a new node (current {current}, proposed {proposed})")]
    SystemReplacement { current: EntityId, proposed: EntityId },
}

/// Why the node cannot accept work requiring completed initialization.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum NodeReadinessError {
    #[error("node initialization is not complete")]
    NotReady,
    #[error("node halted: {0}")]
    Halted(#[from] NodeHaltReason),
}

#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
#[error("Node has been dropped")]
pub struct NodeDropped;

#[derive(Error, Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Error))]
#[cfg_attr(feature = "uniffi", uniffi(flat_error))]
pub enum RetrievalError {
    #[error(transparent)]
    NodeDropped(#[from] NodeDropped),
    #[error("node initialization is not complete")]
    NodeNotReady,
    #[error("node halted: {0}")]
    NodeHalted(#[from] NodeHaltReason),
    #[error("access denied")]
    AccessDenied(AccessDenied),
    #[error("Parse error: {0}")]
    ParseError(ankql::error::ParseError),
    #[error("Entity not found: {0:?}")]
    EntityNotFound(EntityId),
    #[error("Event not found: {0:?}")]
    EventNotFound(EventId),
    #[error("Storage error: {0}")]
    StorageError(Arc<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Collection not found: {0}")]
    CollectionNotFound(CollectionId),
    #[error("Update failed: {0}")]
    FailedUpdate(Arc<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Deserialization error: {0}")]
    DeserializationError(Arc<bincode::ErrorKind>),
    #[error("No durable peers available for fetch operation")]
    NoDurablePeers,
    #[error("Other error: {0}")]
    Other(String),
    #[error("bucket name must only contain valid characters")]
    InvalidBucketName,
    #[error("ankql filter: {0}")]
    AnkqlFilter(crate::selection::filter::Error),
    #[error("Future join: {0}")]
    FutureJoin(Arc<tokio::task::JoinError>),
    #[error("{0}")]
    Anyhow(Arc<anyhow::Error>),
    #[error("Decode error: {0}")]
    DecodeError(Arc<DecodeError>),
    #[error("invalid stored state: {0}")]
    InvalidState(String),
    #[error("State error: {0}")]
    StateError(Arc<StateError>),
    #[error("Mutation error: {0}")]
    MutationError(Arc<MutationError>),
    #[error("Property error: {0}")]
    PropertyError(Arc<crate::property::PropertyError>),
    #[error("Request error: {0}")]
    RequestError(RequestError),
    #[error("Apply error: {0}")]
    ApplyError(Arc<ApplyError>),
    #[error("model '{label}' has no complete compatible binding in the local catalog; schema synchronization or registration is required")]
    UnboundDeclaration { label: String },
}

impl From<NodeReadinessError> for RetrievalError {
    fn from(error: NodeReadinessError) -> Self {
        match error {
            NodeReadinessError::NotReady => Self::NodeNotReady,
            NodeReadinessError::Halted(reason) => Self::NodeHalted(reason),
        }
    }
}

impl From<RequestError> for RetrievalError {
    fn from(err: RequestError) -> Self { RetrievalError::RequestError(err) }
}

impl From<crate::property::PropertyError> for RetrievalError {
    fn from(err: crate::property::PropertyError) -> Self { RetrievalError::PropertyError(Arc::new(err)) }
}

impl From<tokio::task::JoinError> for RetrievalError {
    fn from(err: tokio::task::JoinError) -> Self { RetrievalError::FutureJoin(Arc::new(err)) }
}

impl From<MutationError> for RetrievalError {
    fn from(err: MutationError) -> Self { RetrievalError::MutationError(Arc::new(err)) }
}

impl RetrievalError {
    pub fn storage(err: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>) -> Self {
        RetrievalError::StorageError(Arc::from(err.into()))
    }
}

impl From<bincode::Error> for RetrievalError {
    fn from(e: bincode::Error) -> Self { RetrievalError::DeserializationError(e.into()) }
}

impl From<crate::selection::filter::Error> for RetrievalError {
    fn from(err: crate::selection::filter::Error) -> Self { RetrievalError::AnkqlFilter(err) }
}

impl From<anyhow::Error> for RetrievalError {
    fn from(err: anyhow::Error) -> Self { RetrievalError::Anyhow(Arc::new(err)) }
}

impl From<Infallible> for RetrievalError {
    fn from(_: Infallible) -> Self { unreachable!("Infallible can never be constructed") }
}

#[derive(Error, Debug, Clone)]
pub enum RequestError {
    #[error("node initialization is not complete")]
    NodeNotReady,
    #[error("node halted: {0}")]
    NodeHalted(#[from] NodeHaltReason),
    #[error("Peer not connected")]
    PeerNotConnected,
    #[error("Connection lost")]
    ConnectionLost,
    #[error("System not ready")]
    SystemNotReady,
    #[error("Server error: {0}")]
    ServerError(String),
    #[error("Send error: {0}")]
    SendError(SendError),
    #[error("Internal channel closed")]
    InternalChannelClosed,
    #[error("Unexpected response: {0:?}")]
    UnexpectedResponse(ankurah_proto::NodeResponseBody),
    #[error("Access denied: {0}")]
    AccessDenied(AccessDenied),
}

impl From<NodeReadinessError> for RequestError {
    fn from(error: NodeReadinessError) -> Self {
        match error {
            NodeReadinessError::NotReady => Self::NodeNotReady,
            NodeReadinessError::Halted(reason) => Self::NodeHalted(reason),
        }
    }
}

impl From<AccessDenied> for RequestError {
    fn from(err: AccessDenied) -> Self { RequestError::AccessDenied(err) }
}

impl From<SendError> for RequestError {
    fn from(err: SendError) -> Self { RequestError::SendError(err) }
}

#[derive(Error, Debug)]
pub enum SubscriptionError {
    #[error("predicate not found")]
    PredicateNotFound,
    #[error("already subscribed to predicate")]
    PredicateAlreadySubscribed,
    #[error("subscription not found")]
    SubscriptionNotFound,
}

impl From<DecodeError> for RetrievalError {
    fn from(err: DecodeError) -> Self { RetrievalError::DecodeError(Arc::new(err)) }
}

#[derive(Error, Debug)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Error))]
#[cfg_attr(feature = "uniffi", uniffi(flat_error))]
pub enum MutationError {
    #[error(transparent)]
    NodeDropped(#[from] NodeDropped),
    #[error("node initialization is not complete")]
    NodeNotReady,
    #[error("the entity belongs to another node's system epoch")]
    ForeignEntity,
    #[error("node halted: {0}")]
    NodeHalted(#[from] NodeHaltReason),
    #[error("access denied")]
    AccessDenied(AccessDenied),
    #[error("already exists")]
    AlreadyExists,
    #[error("retrieval error: {0}")]
    RetrievalError(RetrievalError),
    #[error("state error: {0}")]
    StateError(StateError),
    #[error("failed update: {0}")]
    UpdateFailed(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("failed step: {0}: {1}")]
    FailedStep(&'static str, String),
    #[error("failed to set property: {0}: {1}")]
    FailedToSetProperty(&'static str, String),
    #[error("general error: {0}")]
    General(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("no durable peers available")]
    NoDurablePeers,
    #[error("decode error: {0}")]
    DecodeError(DecodeError),
    #[error("lineage error: {0}")]
    LineageError(LineageError),
    #[error("peer rejected transaction")]
    PeerRejected,
    #[error("invalid event")]
    InvalidEvent,
    #[error("malformed event: {0}")]
    EventStructure(ankurah_proto::EventStructureError),
    #[error("inadmissible event: {0}")]
    InadmissibleEvent(InadmissibleEvent),
    /// The node does not know its system root, so it cannot derive an entity
    /// id: a non-root genesis binds the root into its own id. A caller that
    /// reaches this on an ephemeral node can retry once the handshake with a
    /// durable peer has established the system.
    #[error("the node does not know its system root yet")]
    SystemNotReady,
    /// A commit-path invariant about an event's provenance, as opposed to
    /// [`MutationError::EventStructure`], which is about its shape. The string
    /// names which one.
    #[error("commit refused: {0}")]
    CommitInvariant(&'static str),
    /// An entity that was never created in this transaction and has no head to
    /// extend. Naming it is the point: the caller edited a view it obtained
    /// some way other than `Transaction::create`.
    #[error(
        "cannot commit phantom entity {0}: it has no genesis frozen by create() in this transaction and no head an update could extend"
    )]
    PhantomEntity(ankurah_proto::EntityId),
    #[error("invalid update: {0}")]
    InvalidUpdate(&'static str),
    #[error("property error: {0}")]
    PropertyError(crate::property::PropertyError),
    #[error("future join: {0}")]
    FutureJoin(tokio::task::JoinError),
    #[error("anyhow error: {0}")]
    Anyhow(anyhow::Error),
    #[error("TOCTOU attempts exhausted")]
    TOCTOUAttemptsExhausted,
}

impl From<NodeReadinessError> for MutationError {
    fn from(error: NodeReadinessError) -> Self {
        match error {
            NodeReadinessError::NotReady => Self::NodeNotReady,
            NodeReadinessError::Halted(reason) => Self::NodeHalted(reason),
        }
    }
}

impl From<ankurah_proto::EventStructureError> for MutationError {
    fn from(err: ankurah_proto::EventStructureError) -> Self { MutationError::EventStructure(err) }
}

impl From<InadmissibleEvent> for MutationError {
    fn from(err: InadmissibleEvent) -> Self { MutationError::InadmissibleEvent(err) }
}

impl From<tokio::task::JoinError> for MutationError {
    fn from(err: tokio::task::JoinError) -> Self { MutationError::FutureJoin(err) }
}

impl From<anyhow::Error> for MutationError {
    fn from(err: anyhow::Error) -> Self { MutationError::Anyhow(err) }
}

#[derive(Debug)]
pub enum LineageError {
    /// Proven different genesis events (single-root invariant violated)
    Disjoint,
    /// Recursion budget exceeded before determination could be made
    BudgetExceeded { original_budget: usize, subject_frontier: BTreeSet<EventId>, other_frontier: BTreeSet<EventId> },
}

impl std::fmt::Display for LineageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LineageError::Disjoint => write!(f, "disjoint (different genesis events)"),
            LineageError::BudgetExceeded { original_budget, subject_frontier, other_frontier } => {
                let subject: Vec<_> = subject_frontier.iter().map(|id| id.to_base64_short()).collect();
                let other: Vec<_> = other_frontier.iter().map(|id| id.to_base64_short()).collect();
                write!(f, "budget exceeded ({}): subject[{}] other[{}]", original_budget, subject.join(", "), other.join(", "))
            }
        }
    }
}

impl std::error::Error for LineageError {}

impl From<LineageError> for MutationError {
    fn from(err: LineageError) -> Self { MutationError::LineageError(err) }
}

impl From<DecodeError> for MutationError {
    fn from(err: DecodeError) -> Self { MutationError::DecodeError(err) }
}

#[cfg(feature = "wasm")]
impl From<MutationError> for wasm_bindgen::JsValue {
    fn from(err: MutationError) -> Self { err.to_string().into() }
}
#[cfg(feature = "wasm")]
impl From<RetrievalError> for wasm_bindgen::JsValue {
    fn from(err: RetrievalError) -> Self { err.to_string().into() }
}

impl From<AccessDenied> for MutationError {
    fn from(err: AccessDenied) -> Self { MutationError::AccessDenied(err) }
}

impl From<bincode::Error> for MutationError {
    fn from(e: bincode::Error) -> Self { MutationError::StateError(StateError::SerializationError(e)) }
}

impl From<RetrievalError> for MutationError {
    fn from(err: RetrievalError) -> Self {
        match err {
            RetrievalError::NodeDropped(error) => MutationError::NodeDropped(error),
            RetrievalError::NodeNotReady => MutationError::NodeNotReady,
            RetrievalError::NodeHalted(error) => MutationError::NodeHalted(error),
            RetrievalError::AccessDenied(a) => MutationError::AccessDenied(a),
            _ => MutationError::RetrievalError(err),
        }
    }
}
impl From<AccessDenied> for RetrievalError {
    fn from(err: AccessDenied) -> Self { RetrievalError::AccessDenied(err) }
}

impl From<SubscriptionError> for RetrievalError {
    fn from(err: SubscriptionError) -> Self { anyhow::anyhow!("Subscription error: {:?}", err).into() }
}

#[derive(Error, Debug)]
pub enum StateError {
    #[error("serialization error: {0}")]
    SerializationError(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("DDL error: {0}")]
    DDLError(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("DMLError: {0}")]
    DMLError(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl From<bincode::Error> for StateError {
    fn from(e: bincode::Error) -> Self { StateError::SerializationError(Box::new(e)) }
}

impl From<StateError> for MutationError {
    fn from(err: StateError) -> Self { MutationError::StateError(err) }
}

impl From<crate::property::PropertyError> for MutationError {
    fn from(err: crate::property::PropertyError) -> Self { MutationError::PropertyError(err) }
}

impl From<StateError> for RetrievalError {
    fn from(err: StateError) -> Self { RetrievalError::StateError(Arc::new(err)) }
}

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Deserialization error: {0}")]
    Deserialization(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Validation failed: {0}")]
    ValidationFailed(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Rejected: {0}")]
    Rejected(&'static str),
}

/// Error type for NodeApplier operations (applying remote deltas)
#[derive(Debug)]
pub enum ApplyError {
    Items(Vec<ApplyErrorItem>),
    CollectionNotFound(CollectionId),
    RetrievalError(Box<RetrievalError>),
    MutationError(Box<MutationError>),
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApplyError::Items(errors) => {
                write!(f, "Failed to apply {} delta(s)", errors.len())?;
                for (i, err) in errors.iter().enumerate() {
                    write!(f, "\n  [{}] {}", i + 1, err)?;
                }
                Ok(())
            }
            ApplyError::CollectionNotFound(id) => write!(f, "Collection not found: {}", id),
            ApplyError::RetrievalError(e) => write!(f, "Retrieval error: {}", e),
            ApplyError::MutationError(e) => write!(f, "Mutation error: {}", e),
        }
    }
}

impl std::error::Error for ApplyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ApplyError::RetrievalError(e) => Some(e),
            ApplyError::MutationError(e) => Some(e),
            _ => None,
        }
    }
}

/// Error applying a specific delta
#[derive(Debug)]
pub struct ApplyErrorItem {
    pub entity_id: EntityId,
    pub collection: CollectionId,
    pub cause: MutationError,
}

impl std::fmt::Display for ApplyErrorItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to apply delta for entity {} in collection {}: {}", self.entity_id.to_base64_short(), self.collection, self.cause)
    }
}

impl From<RetrievalError> for ApplyError {
    fn from(err: RetrievalError) -> Self { ApplyError::RetrievalError(Box::new(err)) }
}

impl From<MutationError> for ApplyError {
    fn from(err: MutationError) -> Self { ApplyError::MutationError(Box::new(err)) }
}

impl From<ApplyError> for RetrievalError {
    fn from(err: ApplyError) -> Self { RetrievalError::ApplyError(Arc::new(err)) }
}
