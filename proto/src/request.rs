use ankql::ast;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    auth::Attested, clock::Clock, data::Event, id::EntityId, subscription::QueryId, transaction::TransactionId, EntityState, EventFragment,
    EventId, KnownState, RegisterModel, RegisteredModel, SpanAnswer, SpanQuestion, StateFragment, TreeSpec,
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

/// An entity and the head its sender holds of it; an empty head means the sender
/// holds none.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct KnownEntity {
    pub entity_id: EntityId,
    pub head: Clock,
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

/// Content for entity initialization - either bridge, state, or attested state
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum DeltaContent {
    /// The full state snapshot, when no event bridge from the receiver's known head serves
    StateSnapshot { state: StateFragment },
    /// The events from the receiver's known head, across a small event gap
    EventBridge { events: Vec<EventFragment> },
    /// Across a large event gap from the receiver's known head; send state + causal assertion
    StateAndRelation { state: StateFragment, relation: CausalAssertionFragment },
}

/// Data that brings a receiver's copy of an entity to its sender's state
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EntityDelta {
    pub entity_id: EntityId,
    pub content: DeltaContent,
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
    /// Read a query's result once. `known_state` describes what the requester holds of
    /// the result; the responder compares it with a snapshot of its own result, answers
    /// with its account of that snapshot, and pushes on the update channel each entity
    /// the requester lacks or holds at another head. Nothing stays registered on the
    /// responder.
    Fetch {
        selection: ast::Selection<ast::Resolved>,
        known_state: KnownState,
    },
    /// Install a query in the responder's reactor subscription for this peer, then
    /// compare `known_state` with a snapshot of its result taken after the install, and
    /// push what the requester lacks as items tagged `(query_id, Initial)`. Changes after
    /// the install arrive as the subscription's ordinary updates.
    SubscribeQuery {
        query_id: QueryId,
        selection: ast::Selection<ast::Resolved>,
        version: u32,
        known_state: KnownState,
    },
    /// Ask for the responder's digests of spans of a query's result, or one page of its
    /// heads within a span, as the result stands now. Self-contained: a responder that
    /// kept nothing since the query opened can answer it.
    CompareSpans {
        selection: ast::Selection<ast::Resolved>,
        /// The tree the spans address: the spec of the responder's account.
        spec: TreeSpec,
        /// At most `MAX_OFFER_SPANS` questions, of which at most `MAX_HEAD_QUESTIONS`
        /// ask for heads.
        questions: Vec<SpanQuestion>,
    },
    /// Ask the responder to push its current state of each entity on the update
    /// channel, from the head the requester holds. The selection decides whether the
    /// request is a credentialless catalog read.
    SyncEntities {
        selection: ast::Selection<ast::Resolved>,
        /// At most `MAX_PAGE_HEADS` entities.
        entities: Vec<KnownEntity>,
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
    /// The responder's account of its result when it compared, which the Fetch's
    /// pushed data brings the requester to.
    Fetch(KnownState),
    /// One outcome per requested identity, in request order.
    Get(Vec<GetResult>),
    GetEvents(Vec<Attested<Event>>),
    /// The query is installed; `target` is the responder's account of its result when
    /// it compared, which the initial data brings the requester to.
    QuerySubscribed {
        query_id: QueryId,
        target: KnownState,
    },
    /// One answer per question, in the questions' order.
    SpansCompared {
        answers: Vec<SpanAnswer>,
    },
    /// For each entity the responder may send, the head it holds, which it pushes
    /// unless the requester's head already equals it; and the entities it holds no
    /// readable state of.
    EntitiesSynced {
        held: Vec<KnownEntity>,
        unavailable: Vec<EntityId>,
    },
    /// The resolved schema, with build ids echoed for descriptor binding.
    SchemaRegistered {
        model: RegisteredModel,
    },
    Success,
    Error(String),
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

impl TryFrom<GetResult> for EntityDelta {
    type Error = GetFailure;

    fn try_from(result: GetResult) -> Result<Self, Self::Error> {
        let state = Attested::<EntityState>::try_from(result)?;
        Ok(Self { entity_id: state.payload.entity_id, content: DeltaContent::StateSnapshot { state: state.into() } })
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
                write!(f, "Fetch {query} known spans:{}", known_state.spans.len())
            }
            NodeRequestBody::SubscribeQuery { query_id, selection: query, version, known_state } => {
                write!(f, "Subscribe {query_id} {query} v{version} known spans:{}", known_state.spans.len())
            }
            NodeRequestBody::CompareSpans { selection: query, questions, .. } => {
                write!(f, "CompareSpans {query} questions:{}", questions.len())
            }
            NodeRequestBody::SyncEntities { selection: query, entities } => {
                write!(f, "SyncEntities {query} {}", entities.iter().map(|e| e.entity_id.to_base64_short()).collect::<Vec<_>>().join(", "))
            }
            NodeRequestBody::RegisterSchema { model } => {
                write!(f, "RegisterSchema {} properties:{}", model.label, model.properties.len())
            }
        }
    }
}
impl std::fmt::Display for NodeResponseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeResponseBody::CommitComplete { id } => write!(f, "CommitComplete {id}"),
            NodeResponseBody::Fetch(target) => write!(f, "Fetch target spans:{}", target.spans.len()),
            NodeResponseBody::Get(states) => {
                write!(f, "Get [{}]", states.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(", "))
            }
            NodeResponseBody::GetEvents(events) => {
                write!(f, "GetEvents [{}]", events.iter().map(|e| e.payload.to_string()).collect::<Vec<_>>().join(", "))
            }
            NodeResponseBody::QuerySubscribed { query_id, target } => {
                write!(f, "Subscribed {query_id} target spans:{}", target.spans.len())
            }
            NodeResponseBody::SpansCompared { answers } => write!(f, "SpansCompared answers:{}", answers.len()),
            NodeResponseBody::EntitiesSynced { held, unavailable } => {
                write!(f, "EntitiesSynced held:{} unavailable:{}", held.len(), unavailable.len())
            }
            NodeResponseBody::SchemaRegistered { model } => {
                write!(f, "SchemaRegistered {} properties:{}", model.label, model.properties.len())
            }
            NodeResponseBody::Success => write!(f, "Success"),
            NodeResponseBody::Error(e) => write!(f, "Error: {e}"),
        }
    }
}

impl std::fmt::Display for EntityDelta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.content {
            DeltaContent::StateSnapshot { state } => write!(f, "EntityDelta {}: StateSnapshot({})", self.entity_id, state),
            DeltaContent::EventBridge { events } => {
                let mut event_strs = Vec::new();
                for event in events {
                    let event = Attested::<Event>::from_parts(self.entity_id, event.clone());
                    event_strs.push(event.payload.to_string());
                }
                write!(f, "EntityDelta {}: EventBridge({})", self.entity_id, event_strs.join(", "))
            }
            DeltaContent::StateAndRelation { state, relation: _ } => {
                write!(f, "EntityDelta {}: StateAndRelation({})", self.entity_id, state)
            }
        }
    }
}
