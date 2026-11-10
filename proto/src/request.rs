use ankql::ast;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    attestation::Attested, carried_body::CarriedBody, clock::Clock, event::Event, id::EntityId, identity::NodeId, subscription::QueryId,
    transaction::TransactionId, EntityState, EventFragment, EventId, RegisterModel, RegisteredModel, StateFragment,
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
    pub to: NodeId,
    pub from: NodeId,
    pub body: CarriedBody<NodeRequestBody>,
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
    pub attestations: crate::attestation::AttestationSet,
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
    Fetch {
        selection: ast::Selection<ast::Resolved>,
        known_matches: Vec<KnownEntity>,
    },
    SubscribeQuery {
        query_id: QueryId,
        selection: ast::Selection<ast::Resolved>,
        version: u32,
        known_matches: Vec<KnownEntity>,
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
    pub from: NodeId,
    pub to: NodeId,
    pub body: CarriedBody<NodeResponseBody>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum NodeResponseBody {
    // Response to CommitEvents
    CommitComplete {
        id: TransactionId,
    },
    Fetch(Vec<EntityDelta>),
    /// One outcome per requested identity, in request order.
    Get(Vec<GetResult>),
    GetEvents(Vec<Attested<Event>>),
    QuerySubscribed {
        query_id: QueryId,
        deltas: Vec<EntityDelta>,
    },
    /// The resolved schema, with build ids echoed for descriptor binding.
    SchemaRegistered {
        model: RegisteredModel,
    },
    Success,
    Error(String),
    /// The answer to a request whose body the responder cannot read.
    Unsupported,
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
            NodeRequestBody::Fetch { selection: query, known_matches } => {
                write!(f, "Fetch {query} known:{}", known_matches.len())
            }
            NodeRequestBody::SubscribeQuery { query_id, selection: query, version, known_matches } => {
                write!(f, "Subscribe {query_id} {query} v{version} known:{}", known_matches.len())
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
            NodeResponseBody::SchemaRegistered { model } => {
                write!(f, "SchemaRegistered {} properties:{}", model.label, model.properties.len())
            }
            NodeResponseBody::Success => write!(f, "Success"),
            NodeResponseBody::Error(e) => write!(f, "Error: {e}"),
            NodeResponseBody::Unsupported => write!(f, "Unsupported"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::encode;
    use crate::{State, StateFragment};

    fn selection() -> ast::Selection<ast::Resolved> { ast::Selection { predicate: ast::Predicate::True, order_by: None, limit: None } }

    fn state() -> StateFragment { StateFragment { state: State::default(), attestations: Default::default() } }

    fn index(value: &impl Serialize) -> [u8; 4] { encode(value)[..4].try_into().unwrap() }

    /// A body's first four bytes are its variant index, which names the request a peer is asked to serve.
    #[test]
    fn request_body_variant_order_is_pinned() {
        let model = RegisterModel { label: "m".into(), name: "m".into(), explicit_id: None, build_id: [0; 16], properties: vec![] };
        let bodies = [
            NodeRequestBody::CommitTransaction { id: TransactionId::new(), events: vec![] },
            NodeRequestBody::Get { ids: vec![] },
            NodeRequestBody::GetEvents { event_ids: vec![] },
            NodeRequestBody::Fetch { selection: selection(), known_matches: vec![] },
            NodeRequestBody::SubscribeQuery { query_id: QueryId::test(1), selection: selection(), version: 0, known_matches: vec![] },
            NodeRequestBody::RegisterSchema { model },
        ];
        let indices: Vec<_> = bodies.iter().map(index).collect();
        assert_eq!(indices, [[0, 0, 0, 0], [1, 0, 0, 0], [2, 0, 0, 0], [3, 0, 0, 0], [4, 0, 0, 0], [5, 0, 0, 0]]);
    }

    #[test]
    fn response_body_variant_order_is_pinned() {
        let model = RegisteredModel { id: EntityId::from_bytes([1; 32]), label: "m".into(), name: "m".into(), properties: vec![] };
        let bodies = [
            NodeResponseBody::CommitComplete { id: TransactionId::new() },
            NodeResponseBody::Fetch(vec![]),
            NodeResponseBody::Get(vec![]),
            NodeResponseBody::GetEvents(vec![]),
            NodeResponseBody::QuerySubscribed { query_id: QueryId::test(1), deltas: vec![] },
            NodeResponseBody::SchemaRegistered { model },
            NodeResponseBody::Success,
            NodeResponseBody::Error("e".into()),
            NodeResponseBody::Unsupported,
        ];
        let indices: Vec<_> = bodies.iter().map(index).collect();
        assert_eq!(
            indices,
            [[0, 0, 0, 0], [1, 0, 0, 0], [2, 0, 0, 0], [3, 0, 0, 0], [4, 0, 0, 0], [5, 0, 0, 0], [6, 0, 0, 0], [7, 0, 0, 0], [8, 0, 0, 0]]
        );
    }

    #[test]
    fn delta_get_result_and_causal_relation_variant_order_is_pinned() {
        let relation = CausalAssertionFragment { relation: CausalRelation::Equal, attestations: Default::default() };
        let deltas = [
            DeltaContent::StateSnapshot { state: state() },
            DeltaContent::EventBridge { events: vec![] },
            DeltaContent::StateAndRelation { state: state(), relation },
        ];
        assert_eq!(deltas.iter().map(index).collect::<Vec<_>>(), [[0, 0, 0, 0], [1, 0, 0, 0], [2, 0, 0, 0]]);

        let entity = EntityId::from_bytes([1; 32]);
        let found = Attested::from(EntityState { entity_id: entity, state: State::default() });
        let results = [GetResult::Found(found), GetResult::NotFound(entity), GetResult::AccessDenied(entity)];
        assert_eq!(results.iter().map(index).collect::<Vec<_>>(), [[0, 0, 0, 0], [1, 0, 0, 0], [2, 0, 0, 0]]);

        let (root, clock) = (EventId::from_bytes([2; 32]), Clock::default());
        let relations = [
            CausalRelation::Equal,
            CausalRelation::StrictDescends,
            CausalRelation::StrictAscends,
            CausalRelation::DivergedSince { meet: clock.clone(), subject: clock.clone(), other: clock.clone() },
            CausalRelation::Disjoint { gca: None, subject_root: root.clone(), other_root: root },
            CausalRelation::BudgetExceeded { subject: clock.clone(), other: clock },
        ];
        assert_eq!(
            relations.iter().map(index).collect::<Vec<_>>(),
            [[0, 0, 0, 0], [1, 0, 0, 0], [2, 0, 0, 0], [3, 0, 0, 0], [4, 0, 0, 0], [5, 0, 0, 0]]
        );
    }
}
