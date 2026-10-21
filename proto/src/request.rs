use ankql::ast;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    auth::Attested, clock::Clock, data::Event, id::EntityId, subscription::QueryId, transaction::TransactionId, EntityState, EventId,
    KnownState, RegisterModel, RegisteredModel, StateAnswer, StateReply, StateRoundOutcome, StateStreamId,
};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize, Hash, Default)]
pub struct RequestId(Ulid);

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let id_str = self.0.to_string();
        write!(f, "R{}", &id_str[20..])
    }
}

impl RequestId {
    pub fn new() -> Self { Self(Ulid::new()) }
}

/// A request from one node to another
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeRequest {
    pub id: RequestId,
    pub to: EntityId,
    pub from: EntityId,
    pub body: NodeRequestBody,
}

/// Causal relation between two clocks: `subject` (local) vs `other`.
/// - A `Clock` is a normalized antichain frontier (a lattice point).
/// - `meet` is the GCA frontier: Max(Past(subject) ∩ Past(other)).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum CausalRelation {
    /// Identical lattice points.
    Equal,

    /// Subject strictly after other: Past(subject) ⊃ Past(other).
    /// Action: apply other's state directly.
    StrictDescends,

    /// Subject strictly before other: Past(subject) ⊂ Past(other).
    /// Action: no-op (keep subject).
    StrictAscends,

    /// Both sides have advanced since the meet (GCA).
    /// `subject`/`other` are minimal antichains after `meet`.
    DivergedSince {
        /// GCA frontier (meet).
        meet: Clock,
        /// Minimal subject frontier after `meet`.
        subject: Clock,
        /// Minimal other frontier after `meet`.
        other: Clock,
    },

    /// Proven different genesis events (single-root invariant).
    /// Optional `gca` records any common non-minimal ancestors discovered en route.
    Disjoint {
        /// Optional non-minimal common ancestors (if any were found).
        gca: Option<Clock>,
        /// Proven genesis of subject.
        subject_root: EventId,
        /// Proven genesis of other.
        other_root: EventId,
    },

    /// Traversal could not complete under budget; return current frontiers to resume.
    BudgetExceeded { subject: Clock, other: Clock },
}

// Not actually sent over the wire - but used for validating lineage attestations (and converted to/from EntityHeadRelationFragment)
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CausalAssertion {
    pub entity_id: EntityId,
    pub subject: Clock,
    pub other: Clock,
    // Directionality: subject CausalRelations other
    pub relation: CausalRelation,
}

/// Wire-minimal lineage attestation (omits heads that are reconstructible)
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CausalAssertionFragment {
    pub relation: CausalRelation,
    pub attestations: crate::auth::AttestationSet,
}

/// The body of a request from one node to another
#[derive(Debug, Serialize, Deserialize)]
pub enum NodeRequestBody {
    // Request that the Events to be committed on the remote node
    CommitTransaction {
        id: TransactionId,
        events: Vec<Attested<Event>>,
    },
    // Request entities by identity, distinguishing missing from denied reads.
    Get {
        ids: Vec<EntityId>,
    },
    GetEvents {
        event_ids: Vec<EventId>,
    },
    Fetch {
        selection: ast::Selection<ast::Resolved>,
        known_state: KnownState,
    },
    SubscribeQuery {
        query_id: QueryId,
        selection: ast::Selection<ast::Resolved>,
        version: u32,
        known_state: KnownState,
    },
    /// Register one model's schema: an UPSERT the durable node
    /// executes under a process-local mutex. Carries everything the durable
    /// side needs: the receiver policy-checks, looks each definition up by
    /// its lookup key, allocates a fresh EntityId on miss, emits ordinary
    /// events, persists, and responds with
    /// [`NodeResponseBody::SchemaRegistered`] carrying the resolved
    /// definition. Idempotent as an upsert: a repeat registration finds
    /// every key, emits zero events, and returns the same ids. The catalog
    /// collections are not writable any other way.
    RegisterSchema {
        model: RegisterModel,
    },
    /// Return flow credit and answer questions without waiting for another page.
    /// Responses are Success; further numbered frames arrive as NodeUpdate.
    /// Retrying the same request id is idempotent. A processed prefix may contain
    /// explicit pending NeedRepair; it is not the global Applied acknowledgment.
    ContinueState {
        stream: StateStreamId,
        /// Answers belong to this round; None addresses live-target repair.
        /// Credit-only feedback needs no active round.
        round: Option<u64>,
        processed_through: u64,
        /// Actual causal coverage and local publication through a complete live
        /// revision, distinct from frame receipt credit. Releases retained live
        /// repair targets.
        applied_through: Option<u64>,
        answers: Vec<StateAnswer>,
    },
    /// Compare another frozen offer on the standing stream. One round at a time;
    /// the number must be the previous round plus one. The responder sends an
    /// ordered BeginRound marker and returns Success, without an extra page poll.
    RefreshState {
        stream: StateStreamId,
        round: u64,
        known_state: KnownState,
    },
    /// Release only the named round, preserving a subscription's live delivery.
    /// Fetch retires its stream when its sole round finishes. Repeats are safe.
    /// The response is Success; readiness does not wait for the Applied reply.
    FinishStateRound {
        stream: StateStreamId,
        round: u64,
        outcome: StateRoundOutcome,
    },
}

/// A response from one node to another
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeResponse {
    pub request_id: RequestId,
    pub from: EntityId,
    pub to: EntityId,
    pub body: NodeResponseBody,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum NodeResponseBody {
    // Response to CommitEvents
    CommitComplete {
        id: TransactionId,
    },
    Fetch(StateReply),
    /// One outcome per requested identity, in request order.
    Get(Vec<GetResult>),
    GetEvents(Vec<Attested<Event>>),
    QuerySubscribed {
        query_id: QueryId,
        /// Subscription establishment and initial transfer completion are distinct.
        /// Live delivery must preserve the handoff from this captured snapshot.
        state: StateReply,
    },
    /// The resolved schema, with build ids echoed for descriptor binding.
    SchemaRegistered {
        model: RegisteredModel,
    },
    Success,
    Error(String),
    /// The initial Fetch or SubscribeQuery capture could not keep up with
    /// concurrent source changes. No stream was created; retry after backoff.
    StateCaptureBusy,
    /// The state stream expired or was invalidated. Restart the original Fetch or
    /// SubscribeQuery; previously received pages cannot complete a new comparison.
    StateStreamUnavailable {
        stream: StateStreamId,
    },
}

/// A known-ID read reports denial separately from absence, without returning denied state.
#[derive(Debug, Serialize, Deserialize, Clone, strum::Display)]
pub enum GetResult {
    #[strum(transparent)]
    Found(Attested<EntityState>),
    #[strum(to_string = "{0}: not found")]
    NotFound(EntityId),
    #[strum(to_string = "{0}: access denied")]
    AccessDenied(EntityId),
}

impl GetResult {
    pub fn entity_id(&self) -> EntityId {
        match self {
            Self::Found(state) => state.payload.entity_id,
            Self::NotFound(id) | Self::AccessDenied(id) => *id,
        }
    }
}

/// Failed known-ID read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GetFailure {
    NotFound(EntityId),
    AccessDenied(EntityId),
}

impl TryFrom<GetResult> for Attested<EntityState> {
    type Error = GetFailure;

    fn try_from(result: GetResult) -> Result<Self, Self::Error> {
        match result {
            GetResult::Found(state) => Ok(state),
            GetResult::NotFound(id) => Err(GetFailure::NotFound(id)),
            GetResult::AccessDenied(id) => Err(GetFailure::AccessDenied(id)),
        }
    }
}

impl std::fmt::Display for NodeRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Request {} from {}->{}: {}", self.id, self.from, self.to, self.body)
    }
}

impl std::fmt::Display for NodeResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Response({}) {}->{} {}", self.request_id, self.from, self.to, self.body)
    }
}

impl std::fmt::Display for NodeRequestBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRequestBody::CommitTransaction { id, events, .. } => {
                write!(f, "CommitTransaction {id} [{}]", events.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", "))
            }
            NodeRequestBody::Get { ids } => {
                write!(f, "Get {}", ids.iter().map(|id| id.to_base64_short()).collect::<Vec<_>>().join(", "))
            }
            NodeRequestBody::GetEvents { event_ids } => {
                write!(f, "GetEvents {}", event_ids.iter().map(|id| id.to_base64_short()).collect::<Vec<_>>().join(", "),)
            }
            NodeRequestBody::Fetch { selection: query, known_state } => {
                write!(f, "Fetch {query} spans:{}", known_state.spans.len())
            }
            NodeRequestBody::SubscribeQuery { query_id, selection: query, version, known_state } => {
                write!(f, "Subscribe {query_id} {query} v{version} spans:{}", known_state.spans.len())
            }
            NodeRequestBody::RegisterSchema { model } => {
                write!(f, "RegisterSchema {} properties:{}", model.label, model.properties.len())
            }
            NodeRequestBody::ContinueState { stream, processed_through, answers, .. } => {
                write!(f, "ContinueState {stream} through:{processed_through} answers:{}", answers.len())
            }
            NodeRequestBody::RefreshState { stream, round, known_state } => {
                write!(f, "RefreshState {stream} round:{round} spans:{}", known_state.spans.len())
            }
            NodeRequestBody::FinishStateRound { stream, round, outcome } => write!(f, "FinishStateRound {stream}/{round} {outcome:?}"),
        }
    }
}
impl std::fmt::Display for NodeResponseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeResponseBody::CommitComplete { id } => write!(f, "CommitComplete {id}"),
            NodeResponseBody::Fetch(state) => {
                write!(f, "Fetch {} frame:{}", state.frame.stream, state.frame.sequence)
            }
            NodeResponseBody::Get(states) => {
                write!(f, "Get [{}]", states.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(", "))
            }
            NodeResponseBody::GetEvents(events) => {
                write!(f, "GetEvents [{}]", events.iter().map(|e| e.payload.to_string()).collect::<Vec<_>>().join(", "))
            }
            NodeResponseBody::QuerySubscribed { query_id, state } => {
                write!(f, "Subscribed {query_id} {} frame:{}", state.frame.stream, state.frame.sequence)
            }
            NodeResponseBody::SchemaRegistered { model } => {
                write!(f, "SchemaRegistered {} properties:{}", model.label, model.properties.len())
            }
            NodeResponseBody::Success => write!(f, "Success"),
            NodeResponseBody::Error(e) => write!(f, "Error: {e}"),
            NodeResponseBody::StateCaptureBusy => write!(f, "StateCaptureBusy"),
            NodeResponseBody::StateStreamUnavailable { stream } => write!(f, "StateStreamUnavailable {stream}"),
        }
    }
}
