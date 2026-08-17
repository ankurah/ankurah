use ankql::ast;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    auth::Attested, clock::Clock, collection::CollectionId, data::Event, id::EntityId, subscription::QueryId, transaction::TransactionId,
    EntityState, EventFragment, EventId, RegisterModel, RegisteredModel, StateFragment,
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
    CommitTransaction {
        id: TransactionId,
        events: Vec<Attested<Event>>,
    },
    // Request to fetch entities matching a predicate
    Get {
        collection: CollectionId,
        ids: Vec<EntityId>,
    },
    GetEvents {
        collection: CollectionId,
        event_ids: Vec<EventId>,
    },
    Fetch {
        collection: CollectionId,
        selection: ast::Selection,
        known_matches: Vec<KnownEntity>,
    },
    SubscribeQuery {
        query_id: QueryId,
        collection: CollectionId,
        selection: ast::Selection,
        version: u32,
        known_matches: Vec<KnownEntity>,
    },
    /// Register schema definitions: an UPSERT the durable node
    /// executes under a process-local mutex. Carries everything the durable
    /// side needs: the receiver policy-checks, looks each definition up by
    /// its lookup key, allocates a fresh EntityId on miss, emits ordinary
    /// events, persists, relays, and responds with
    /// [`NodeResponseBody::SchemaRegistered`] carrying the full resolved
    /// definitions. Idempotent as an upsert: a repeat registration finds
    /// every key, emits zero events, and returns the same ids. The catalog
    /// collections are not writable any other way.
    RegisterSchema {
        models: Vec<RegisterModel>,
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

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeResponseBody {
    // Response to CommitEvents
    CommitComplete {
        id: TransactionId,
    },
    Fetch(Vec<EntityDelta>),
    Get(Vec<Attested<EntityState>>),
    GetEvents(Vec<Attested<Event>>),
    QuerySubscribed {
        query_id: QueryId,
        deltas: Vec<EntityDelta>,
    },
    /// Response to RegisterSchema: the full resolved definitions,
    /// ids included -- allocated on this execution or already existing. The
    /// requester upserts these into its catalog map immediately on ack, so
    /// catalog maintenance proceeds without waiting for replication.
    SchemaRegistered {
        models: Vec<RegisteredModel>,
    },
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
            // `id` (a `TransactionId`) is a fresh ULID minted by `::new()` on
            // every send (including on every retry of the same commit); it
            // carries no content of its own. The sorted event ids are a
            // content hash of (entity_id, operations, parent), so they fully
            // and stably identify what is being committed, unlike `id`, which
            // would make two structurally identical commits render
            // differently. Both the simulation harness's determinism digest
            // (tests/src/sim/transport.rs) and a human scanning logs want the
            // latter.
            NodeRequestBody::CommitTransaction { events, .. } => {
                let mut ids: Vec<String> = events.iter().map(|e| e.payload.id().to_base64_short()).collect();
                ids.sort();
                write!(f, "CommitTransaction [{}]", ids.join(", "))
            }
            NodeRequestBody::Get { collection, ids } => {
                let mut ids: Vec<String> = ids.iter().map(|id| id.to_base64_short()).collect();
                ids.sort();
                write!(f, "Get {collection} {}", ids.join(", "))
            }
            NodeRequestBody::GetEvents { collection, event_ids } => {
                let mut ids: Vec<String> = event_ids.iter().map(|id| id.to_base64_short()).collect();
                ids.sort();
                write!(f, "GetEvents {collection} {}", ids.join(", "))
            }
            NodeRequestBody::Fetch { collection, selection: query, known_matches } => {
                write!(f, "Fetch {collection} {query} known:{}", known_matches.len())
            }
            // `query_id` is excluded for the same reason `CommitTransaction`'s
            // `id` is above: a fresh per-send ULID, not content.
            NodeRequestBody::SubscribeQuery { collection, selection: query, version, known_matches, .. } => {
                write!(f, "Subscribe {collection} {query} v{version} known:{}", known_matches.len())
            }
            NodeRequestBody::RegisterSchema { models } => {
                write!(f, "RegisterSchema models:{} properties:{}", models.len(), models.iter().map(|m| m.properties.len()).sum::<usize>())
            }
        }
    }
}
impl std::fmt::Display for NodeResponseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // See `NodeRequestBody::CommitTransaction`: `id` is a per-send ULID.
            NodeResponseBody::CommitComplete { .. } => write!(f, "CommitComplete"),
            NodeResponseBody::Fetch(deltas) => {
                write!(f, "Fetch [{}]", deltas.len()) // TODO display deltas
            }
            NodeResponseBody::Get(states) => {
                let mut ids: Vec<String> = states.iter().map(|s| s.payload.entity_id.to_base64_short()).collect();
                ids.sort();
                write!(f, "Get [{}]", ids.join(", "))
            }
            NodeResponseBody::GetEvents(events) => {
                let mut ids: Vec<String> = events.iter().map(|e| e.payload.id().to_base64_short()).collect();
                ids.sort();
                write!(f, "GetEvents [{}]", ids.join(", "))
            }
            // See `NodeRequestBody::SubscribeQuery`: `query_id` is a per-send ULID.
            NodeResponseBody::QuerySubscribed { deltas: initial, .. } => write!(f, "Subscribed initial:{}", initial.len()),
            NodeResponseBody::SchemaRegistered { models } => {
                write!(
                    f,
                    "SchemaRegistered models:{} properties:{}",
                    models.len(),
                    models.iter().map(|m| m.properties.len()).sum::<usize>()
                )
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
                    let event = Attested::<Event>::from_parts(self.entity_id, self.collection.clone(), event.clone());
                    event_strs.push(event.payload.to_string());
                }
                write!(f, "EntityDelta {}: EventBridge({})", self.entity_id, event_strs.join(", "))
            }
            DeltaContent::StateAndRelation { state, relation } => write!(f, "EntityDelta {}: StateAndRelation({})", self.entity_id, state),
        }
    }
}
