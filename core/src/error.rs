use std::{collections::BTreeSet, convert::Infallible};

use ankurah_proto::{CollectionId, DecodeError, EntityId, EventId};
use thiserror::Error;

use crate::{connector::SendError, policy::AccessDenied};

#[derive(Error, Debug)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Error))]
#[cfg_attr(feature = "uniffi", uniffi(flat_error))]
pub enum RetrievalError {
    #[error("access denied")]
    AccessDenied(AccessDenied),
    #[error("Parse error: {0}")]
    ParseError(ankql::error::ParseError),
    #[error("Entity not found: {0:?}")]
    EntityNotFound(EntityId),
    #[error("Event not found: {0:?}")]
    EventNotFound(EventId),
    #[error("Storage error: {0}")]
    StorageError(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Collection not found: {0}")]
    CollectionNotFound(CollectionId),
    #[error("Update failed: {0}")]
    FailedUpdate(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Deserialization error: {0}")]
    DeserializationError(bincode::Error),
    #[error("No durable peers available for fetch operation")]
    NoDurablePeers,
    #[error("Other error: {0}")]
    Other(String),
    #[error("bucket name must only contain valid characters")]
    InvalidBucketName,
    #[error("ankql filter: {0}")]
    AnkqlFilter(crate::selection::filter::Error),
    #[error("Future join: {0}")]
    FutureJoin(tokio::task::JoinError),
    #[error("{0}")]
    Anyhow(anyhow::Error),
    #[error("Decode error: {0}")]
    DecodeError(DecodeError),
    #[error("State error: {0}")]
    StateError(StateError),
    #[error("Mutation error: {0}")]
    MutationError(Box<MutationError>),
    #[error("Property error: {0}")]
    PropertyError(Box<crate::property::PropertyError>),
    #[error("Request error: {0}")]
    RequestError(RequestError),
    #[error("Apply error: {0}")]
    ApplyError(ApplyError),
}

impl From<RequestError> for RetrievalError {
    fn from(err: RequestError) -> Self { RetrievalError::RequestError(err) }
}

impl From<crate::property::PropertyError> for RetrievalError {
    fn from(err: crate::property::PropertyError) -> Self { RetrievalError::PropertyError(Box::new(err)) }
}

impl From<tokio::task::JoinError> for RetrievalError {
    fn from(err: tokio::task::JoinError) -> Self { RetrievalError::FutureJoin(err) }
}

impl From<MutationError> for RetrievalError {
    fn from(err: MutationError) -> Self { RetrievalError::MutationError(Box::new(err)) }
}

impl RetrievalError {
    pub fn storage(err: impl std::error::Error + Send + Sync + 'static) -> Self { RetrievalError::StorageError(Box::new(err)) }
}

impl From<bincode::Error> for RetrievalError {
    fn from(e: bincode::Error) -> Self { RetrievalError::DeserializationError(e) }
}

impl From<crate::selection::filter::Error> for RetrievalError {
    fn from(err: crate::selection::filter::Error) -> Self { RetrievalError::AnkqlFilter(err) }
}

impl From<anyhow::Error> for RetrievalError {
    fn from(err: anyhow::Error) -> Self { RetrievalError::Anyhow(err) }
}

impl From<Infallible> for RetrievalError {
    fn from(_: Infallible) -> Self { unreachable!("Infallible can never be constructed") }
}

#[derive(Error, Debug)]
pub enum RequestError {
    #[error("Peer not connected")]
    PeerNotConnected,
    #[error("Connection lost")]
    ConnectionLost,
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
    fn from(err: DecodeError) -> Self { RetrievalError::DecodeError(err) }
}

#[derive(Error, Debug)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Error))]
#[cfg_attr(feature = "uniffi", uniffi(flat_error))]
pub enum MutationError {
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
    #[error("invalid update")]
    InvalidUpdate(&'static str),
    #[error("property error: {0}")]
    PropertyError(crate::property::PropertyError),
    #[error("future join: {0}")]
    FutureJoin(tokio::task::JoinError),
    #[error("anyhow error: {0}")]
    Anyhow(anyhow::Error),
    #[error("TOCTOU attempts exhausted")]
    TOCTOUAttemptsExhausted,
    #[error("ingest: {0}")]
    Ingest(IngestError),
    /// The persist funnel refused a non-canonical Entity instance: the id
    /// resolves to a DIFFERENT live resident in the node's map (D2-6, the
    /// M4 codex follow-up remediation). An innocent duplicate-instance
    /// race, not a lineage rejection: the caller's item failure is the
    /// heal path (the sender redelivers and the redelivery lands on the
    /// canonical instance). Core-local; never wire-serialized.
    #[error("persist refused: {0} resolves to a different canonical resident instance")]
    StaleInstancePersistRefused(EntityId),
}

impl From<tokio::task::JoinError> for MutationError {
    fn from(err: tokio::task::JoinError) -> Self { MutationError::FutureJoin(err) }
}

impl From<anyhow::Error> for MutationError {
    fn from(err: anyhow::Error) -> Self { MutationError::Anyhow(err) }
}

/// The typed taxonomy at the application boundary (RFC #268 item 4).
///
/// Replaces stringly `General`/`InvalidUpdate` on the ingest paths so callers
/// and ack payloads can distinguish outcome classes without matching strings.
/// Budget is deliberately NOT a lineage rejection: a budget-exhausted
/// comparison is a resumable liveness anomaly carrying its frontiers, never a
/// verdict (C4-08); two nodes of different cache depth must not disagree on
/// rejections.
#[derive(Error, Debug)]
/// Several variants are forward-declared design surface (T7): `Contention`,
/// `Storage`, `Validation`, and `Unsupported`, plus
/// `LineageRejection::{CreationOverNonEmptyHead, BatchCycle}`, have no D1
/// construction site by design (red-team F5: later milestones migrate onto
/// complete types; D3's rejection horizon extends this surface).
pub enum IngestError {
    /// The PolicyAgent refused the event at admission or commit time.
    #[error("policy denied: {0}")]
    PolicyDenied(AccessDenied),
    /// The event's lineage makes it permanently unappliable here.
    #[error("lineage rejection: {0}")]
    Lineage(LineageRejection),
    /// Comparison exhausted its budget before reaching a verdict. Resumable:
    /// the event stays uncommitted and a retry with warmer caches (or, after
    /// D2, generation bounds) can succeed.
    #[error("budget exhausted before verdict (budget {original_budget})")]
    Budget { original_budget: usize, subject_frontier: BTreeSet<EventId>, other_frontier: BTreeSet<EventId> },
    /// Head-mutation retries exhausted under concurrent writers.
    #[error("contention: head mutation retries exhausted after {attempts}")]
    Contention { attempts: u32 },
    /// Durable storage failed while persisting an event or state.
    #[error("storage: {0}")]
    Storage(Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Structural validation of the payload failed before staging.
    #[error("validation: {0}")]
    Validation(Box<dyn std::error::Error + Send + Sync + 'static>),
    /// A wire shape the pipeline recognizes but does not implement.
    #[error("unsupported: {0}")]
    Unsupported(&'static str),
}

/// Permanent lineage-shaped rejections. Every variant is a function of
/// grounded graph facts, reachable only through completed exploration, never
/// through a budget-limited walk (268-B).
#[derive(Debug)]
pub enum LineageRejection {
    /// Proven different genesis events (single-root invariant violated).
    Disjoint,
    /// Non-creation event addressed to an entity with an empty head.
    NonCreationOverEmptyHead,
    /// Creation event for an entity that already has committed history.
    CreationOverNonEmptyHead,
    /// The batch's parent edges form a cycle (malformed or malicious input;
    /// impossible for honest content-addressed ids).
    BatchCycle,
    /// A claimed generation contradicts what this node can resolve. Two
    /// emitters (D2 M4): admission verification, where an event's claim
    /// fails the equation `generation == 1 + max(parent generations)`
    /// against parents resolved from the resident's MATERIALIZED head
    /// generations first (which on ephemeral nodes may be
    /// trust-envelope-carried rather than payload-verified) and local
    /// payload reads otherwise (genesis events must claim exactly 1); and
    /// state-adoption validation on definitive-storage nodes, where a wire
    /// annotation's per-tip value contradicts the locally held tip payload
    /// (a direct value comparison). The honest value is deterministic
    /// given the parents, so this is only ever reachable by a buggy or
    /// malicious writer; contained like any malformed input (plan REV 4
    /// D2-3, REV 5 section K).
    GenerationMismatch { event: EventId, claimed: u32, expected: u32 },
    /// A state's head-generation annotation does not cover exactly its
    /// head's tips (same ids, each exactly once). Structurally malformed
    /// input, rejected before adoption on every node flavor: an adopted
    /// mismatch could never stamp a commit (D2 M4, plan REV 5 section K).
    HeadGenerationsMismatch,
    /// A wire state carries a head-generation annotation for a tip whose
    /// event is not locally resolvable on this DEFINITIVE-storage node, so
    /// the carried value cannot be inspected against a payload. Amendment
    /// K's durable rule: a durable node never adopts a wire-carried
    /// generation uninspected; adopted, the unvalidated value would become
    /// rejection ground truth against honest descendants and commit-stamp
    /// input (M4 remediation item 4). Ephemeral nodes adopt inside the
    /// state's trust envelope and never produce this.
    UnresolvableHeadGenerationTip { event: EventId },
}

impl std::fmt::Display for LineageRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LineageRejection::Disjoint => write!(f, "disjoint (different genesis events)"),
            LineageRejection::NonCreationOverEmptyHead => write!(f, "non-creation event over an empty head"),
            LineageRejection::CreationOverNonEmptyHead => write!(f, "creation event over a non-empty head"),
            LineageRejection::BatchCycle => write!(f, "event batch contains a parent cycle"),
            LineageRejection::GenerationMismatch { event, claimed, expected } => {
                write!(f, "generation mismatch for event {event}: claimed {claimed}, expected {expected} from its parents")
            }
            LineageRejection::HeadGenerationsMismatch => {
                write!(f, "state head_generations does not annotate exactly the state head")
            }
            LineageRejection::UnresolvableHeadGenerationTip { event } => {
                write!(f, "carried head generation for tip {event} cannot be inspected: the tip's event is not locally resolvable on this definitive-storage node")
            }
        }
    }
}

impl From<IngestError> for MutationError {
    fn from(err: IngestError) -> Self { MutationError::Ingest(err) }
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
            RetrievalError::AccessDenied(a) => MutationError::AccessDenied(a),
            _ => MutationError::RetrievalError(err),
        }
    }
}
impl From<AccessDenied> for RetrievalError {
    fn from(err: AccessDenied) -> Self { RetrievalError::AccessDenied(err) }
}

impl From<SubscriptionError> for RetrievalError {
    fn from(err: SubscriptionError) -> Self { RetrievalError::Anyhow(anyhow::anyhow!("Subscription error: {:?}", err)) }
}

#[derive(Error, Debug)]
pub enum StateError {
    #[error("serialization error: {0}")]
    SerializationError(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("DDL error: {0}")]
    DDLError(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("DMLError: {0}")]
    DMLError(Box<dyn std::error::Error + Send + Sync + 'static>),
    /// The wire envelope carries a model-definition id (#330) and neither the
    /// well-known table nor the catalog can supply one for this collection.
    /// For a locally committed user collection this means
    /// commit-before-registration, a bug rather than a race.
    #[error("no model id for collection '{0}'")]
    UnknownModel(String),
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
    fn from(err: StateError) -> Self { RetrievalError::StateError(err) }
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
    /// The model-definition id the wire item carried (#330); the collection
    /// may be unresolvable, which is itself one of the reportable causes.
    pub model: EntityId,
    pub cause: MutationError,
}

impl std::fmt::Display for ApplyErrorItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Failed to apply delta for entity {} of model {}: {}",
            self.entity_id.to_base64_short(),
            self.model.to_base64_short(),
            self.cause
        )
    }
}

impl From<RetrievalError> for ApplyError {
    fn from(err: RetrievalError) -> Self { ApplyError::RetrievalError(Box::new(err)) }
}

impl From<MutationError> for ApplyError {
    fn from(err: MutationError) -> Self { ApplyError::MutationError(Box::new(err)) }
}

impl From<ApplyError> for RetrievalError {
    fn from(err: ApplyError) -> Self { RetrievalError::ApplyError(err) }
}
