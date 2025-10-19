use ankql::ast;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    auth::Attested, clock::Clock, collection::CollectionId, data::Event, id::EntityId, subscription::QueryId, transaction::TransactionId,
    EntityState, EventFragment, EventId, StateFragment,
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

/// Entity with known head for lineage attestation
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

    /// Proven different genesis events (single-root invariant) or empty clocks.
    /// Empty clocks (like SQL NULL) are always Disjoint from everything.
    /// Optional `gca` records any common non-minimal ancestors discovered en route.
    Disjoint {
        /// Optional non-minimal common ancestors (if any were found).
        gca: Option<Clock>,
        /// Proven genesis of subject (None if subject is empty clock).
        subject_root: Option<EventId>,
        /// Proven genesis of other (None if other is empty clock).
        other_root: Option<EventId>,
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
    /// Entity not in known_matches; send full state snapshot
    StateSnapshot { state: StateFragment },
    /// Entity present in known matches with a small event gap
    EventBridge { events: Vec<EventFragment> },
    /// Entity present in known matches with a large event gap; send state + causal assertion
    StateAndRelation { state: StateFragment, relation: CausalAssertionFragment },
}

/// Entity initialization data returned in QuerySubscribed and Fetch
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EntityDelta {
    pub entity_id: EntityId,
    pub collection: CollectionId,
    pub content: DeltaContent,
}

/// The body of a request from one node to another
#[derive(Debug, Serialize, Deserialize)]
pub enum NodeRequestBody {
    // Request that the Events to be committed on the remote node
    CommitTransaction { id: TransactionId, events: Vec<Attested<Event>> },
    // Request to fetch entities matching a predicate
    Get { collection: CollectionId, ids: Vec<EntityId> },
    GetEvents { collection: CollectionId, event_ids: Vec<EventId> },
    Fetch { collection: CollectionId, selection: ast::Selection, known_matches: Vec<KnownEntity> },
    SubscribeQuery { query_id: QueryId, collection: CollectionId, selection: ast::Selection, version: u32, known_matches: Vec<KnownEntity> },
}

/// A response from one node to another
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeResponse {
    pub request_id: RequestId,
    pub from: EntityId,
    pub to: EntityId,
    pub body: NodeResponseBody,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeResponseBody {
    // Response to CommitEvents
    CommitComplete { id: TransactionId },
    Fetch(Vec<EntityDelta>),
    Get(Vec<Attested<EntityState>>),
    GetEvents(Vec<Attested<Event>>),
    QuerySubscribed { query_id: QueryId, deltas: Vec<EntityDelta> },
    Success,
    Error(String),
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
            NodeRequestBody::CommitTransaction { id, events } => {
                write!(f, "CommitTransaction {id} [{}]", events.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", "))
            }
            NodeRequestBody::Get { collection, ids } => {
                write!(f, "Get {collection} {}", ids.iter().map(|id| id.to_base64_short()).collect::<Vec<_>>().join(", "))
            }
            NodeRequestBody::GetEvents { collection, event_ids } => {
                write!(f, "GetEvents {collection} {}", event_ids.iter().map(|id| id.to_base64_short()).collect::<Vec<_>>().join(", "),)
            }
            NodeRequestBody::Fetch { collection, selection: query, known_matches } => {
                write!(f, "Fetch {collection} {query} known:{}", known_matches.len())
            }
            NodeRequestBody::SubscribeQuery { query_id, collection, selection: query, version, known_matches } => {
                write!(f, "Subscribe {query_id} {collection} {query} v{version} known:{}", known_matches.len())
            }
        }
    }
}
impl std::fmt::Display for NodeResponseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeResponseBody::CommitComplete { id } => write!(f, "CommitComplete {id}"),
            NodeResponseBody::Fetch(deltas) => {
                write!(f, "Fetch [{}]", deltas.len()) // TODO display deltas
            }
            NodeResponseBody::Get(states) => {
                write!(f, "Get [{}]", states.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(", "))
            }
            NodeResponseBody::GetEvents(events) => {
                write!(f, "GetEvents [{}]", events.iter().map(|e| e.payload.to_string()).collect::<Vec<_>>().join(", "))
            }
            NodeResponseBody::QuerySubscribed { query_id, deltas: initial } => write!(f, "Subscribed {query_id} initial:{}", initial.len()),
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
                    let event = Attested::<Event>::from_parts(self.entity_id, self.collection.clone(), event.clone());
                    event_strs.push(event.payload.to_string());
                }
                write!(f, "EntityDelta {}: EventBridge({})", self.entity_id, event_strs.join(", "))
            }
            DeltaContent::StateAndRelation { state, relation } => write!(f, "EntityDelta {}: StateAndRelation({})", self.entity_id, state),
        }
    }
}
