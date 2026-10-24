use ankql::ast;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    auth::Attested, clock::Clock, data::Event, id::EntityId, subscription::QueryId, transaction::TransactionId, EntityState, EventId,
    FeedbackAnswers, KnownState, RegisterModel, RegisteredModel, StateReply, StateRoundOutcome, StateStreamId,
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
    /// Returns frame credit and the requester's answers on a state stream. The
    /// responder replies Success; the frames it sends next arrive as
    /// NodeMessage::StateFrame. A retry with the same request ID and contents is
    /// harmless. Credit counts frames processed, not applied: only FinishStateRound
    /// reports a round applied.
    StateFeedback {
        stream: StateStreamId,
        /// The last frame the requester has processed; frames through it stop counting
        /// against the responder's window.
        processed_through: u64,
        /// The last live publication the requester has applied and published, whose
        /// targets the responder may then forget.
        applied_through: Option<u64>,
        /// Answers, if any, named by what they respond to.
        answers: Option<FeedbackAnswers>,
    },
    /// Asks for another comparison round on a SubscribeQuery stream, comparing
    /// `known_state`. The round's number follows the previous round's, and only one
    /// round runs at a time. The responder replies Success and begins the round with a
    /// BeginRound frame.
    RefreshState {
        stream: StateStreamId,
        round: u64,
        known_state: KnownState,
    },
    /// Ends the named round with its outcome. A subscription's stream continues with
    /// live delivery; a Fetch's stream ends with its only round. The responder replies
    /// Success, and a repeat is harmless.
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
        /// The stream's opening reply. The subscription exists once it arrives; its
        /// first round completes later, and live publications follow the revision that
        /// round's snapshot reflects.
        state: StateReply,
    },
    /// The resolved schema, with build ids echoed for descriptor binding.
    SchemaRegistered {
        model: RegisteredModel,
    },
    Success,
    Error(String),
    /// The responder could not take a stable snapshot of the query result for an
    /// initial Fetch or SubscribeQuery while its source kept changing. No stream was
    /// created; retry after backoff.
    StateSnapshotBusy,
    /// The state stream expired or ended. The requester sends its Fetch or
    /// SubscribeQuery again; frames it received cannot complete a new round.
    StateStreamUnavailable,
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
            NodeRequestBody::StateFeedback { stream, processed_through, answers, .. } => {
                write!(f, "StateFeedback {stream} through:{processed_through} answers:{}", answers.as_ref().map_or(0, FeedbackAnswers::len))
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
            NodeResponseBody::StateSnapshotBusy => write!(f, "StateSnapshotBusy"),
            NodeResponseBody::StateStreamUnavailable => write!(f, "StateStreamUnavailable"),
        }
    }
}
