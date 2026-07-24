use ankql::ast;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use ulid::Ulid;

use crate::{
    auth::Attested, clock::Clock, data::Event, subscription::QueryId, transaction::TransactionId, EntityId, EntityState, EventFragment,
    EventId, ModelContext, ModelId, StateFragment,
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
    /// The model context under which this entity delta is being delivered.
    pub model: crate::ModelId,
    pub content: DeltaContent,
}

/// A model definition to register.
///
/// The `collection` field name is retained for source/API compatibility. Its
/// value is the model's registration label, not a physical collection or a
/// claim that canonical entities belong to this model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelDescriptor {
    /// Source-level model label and registration lookup key (RFC 5.1).
    ///
    /// Historical field name; canonical entities are model-independent.
    pub collection: String,
    /// Display name (initially the struct name); mutable metadata.
    pub name: String,
    /// Explicit binding (RFC 5.9): reference an EXISTING model entity
    /// instead of looking one up by label. Never mints; hard-fails if absent
    /// or if the bound entity's label differs. Properties and
    /// memberships in the SAME request resolve under the bound id, so a
    /// request touching an explicitly-bound model must include its
    /// ModelDescriptor.
    pub explicit_id: Option<EntityId>,
}

/// A property definition to register. Language-agnostic: `backend` and
/// `value_type` follow the normative mapping table (RFC section 4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertyDescriptor {
    /// Label of the minting model: the lookup scope (provenance, not
    /// ownership; RFC 5.1). Historical field name.
    pub minting_collection: String,
    /// Current display name; part of the upsert lookup key.
    pub name: String,
    /// Transient rename hint (RFC 5.8): the name this property carried
    /// before a rename. Applied by the executor before lookup-or-create,
    /// GUARDED (only when the current-name lookup misses and the old-name
    /// lookup hits); a no-op once applied or when nothing matches.
    /// Removable from source after every target system has seen it.
    pub renamed_from: Option<String>,
    /// Backend registry name, e.g. "lww", "yrs".
    pub backend: String,
    /// Language-agnostic value type, e.g. "string", "i64".
    pub value_type: String,
    /// For reference-typed properties: the target model, named by its
    /// source label (ids are the executor's to allocate or resolve; RFC 5.2).
    /// The executor resolves this against the catalog, allocating the model
    /// entity on miss. Mutable metadata, not identity.
    pub target_collection: Option<String>,
    /// Explicit binding (RFC 5.9): reference an EXISTING property entity
    /// instead of looking one up by name. Never mints; hard-fails if absent.
    pub explicit_id: Option<EntityId>,
}

/// How a membership names its property within a RegisterSchema request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PropertyRef {
    /// A property declared in this request for the same model, by current
    /// display name.
    Name(String),
    /// An existing (possibly shared) property entity, by explicit id.
    Id(EntityId),
}

/// A (model, property) contract-membership to register.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipDescriptor {
    /// The model's source label. Historical field name.
    pub collection: String,
    /// The property to attach, by a same-request name or durable identity.
    pub property: PropertyRef,
    /// PER CONTRACT: the same property may be required in one model and
    /// optional in another.
    pub optional: bool,
}

/// The body of a request from one node to another
#[derive(Debug, Serialize, Deserialize)]
pub enum NodeRequestBody {
    // Request that the Events to be committed on the remote node
    CommitTransaction {
        id: TransactionId,
        events: Vec<ModelContext<Attested<Event>>>,
    },
    // Request to fetch entities matching a predicate
    Get {
        model: ModelId,
        ids: Vec<EntityId>,
    },
    GetEvents {
        model: ModelId,
        event_ids: Vec<EventId>,
    },
    Fetch {
        model: ModelId,
        selection: ast::Selection,
        known_matches: Vec<KnownEntity>,
    },
    SubscribeQuery {
        query_id: QueryId,
        model: ModelId,
        selection: ast::Selection,
        version: u32,
        known_matches: Vec<KnownEntity>,
    },
    /// Register schema definitions (RFC 5.2): an UPSERT the durable node
    /// executes under a process-local mutex. Carries everything the durable
    /// side needs: the receiver policy-checks, looks each definition up by
    /// its lookup key, allocates a fresh EntityId on miss, emits ordinary
    /// events, persists, relays, and responds with
    /// [`NodeResponseBody::SchemaRegistered`] carrying the full resolved
    /// definitions. Idempotent as an upsert: a repeat registration finds
    /// every key, emits zero events, and returns the same ids. The catalog
    /// collections are not writable any other way.
    RegisterSchema {
        models: Vec<ModelDescriptor>,
        properties: Vec<PropertyDescriptor>,
        memberships: Vec<MembershipDescriptor>,
    },
}

impl NodeRequestBody {
    /// The single model scope carried by query/control-plane requests.
    pub fn target_model(&self) -> Option<&ModelId> {
        match self {
            Self::Get { model, .. } | Self::GetEvents { model, .. } | Self::Fetch { model, .. } | Self::SubscribeQuery { model, .. } => {
                Some(model)
            }
            Self::CommitTransaction { .. } | Self::RegisterSchema { .. } => None,
        }
    }
}

/// A resolved model definition, as returned by
/// [`NodeResponseBody::SchemaRegistered`]: the allocated (or existing)
/// entity id plus the definition state the catalog now holds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredModel {
    /// The model's allocated or previously registered catalog entity id.
    pub id: EntityId,
    /// The source-level model label used to look up this registration.
    /// Historical field name.
    pub collection: String,
    /// The model's current registered display name.
    pub name: String,
}

/// A resolved property definition (see [`RegisteredModel`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredProperty {
    /// The property's allocated or previously registered catalog entity id.
    pub id: EntityId,
    /// The model in whose scope this property was minted (provenance).
    pub model: EntityId,
    /// The property's current registered display name.
    pub name: String,
    /// The property's canonical state-backend identifier.
    pub backend: String,
    /// The property's canonical logical value-type spelling.
    pub value_type: String,
    /// Resolved target model id for reference-typed properties.
    pub target_model: Option<EntityId>,
}

/// A resolved contract-membership (see [`RegisteredModel`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredMembership {
    /// The membership's allocated or previously registered catalog entity id.
    pub id: EntityId,
    /// The model participating in the membership.
    pub model: EntityId,
    /// The property participating in the membership.
    pub property: EntityId,
    /// Whether this property is optional in this model contract.
    pub optional: bool,
}

/// A response from one node to another
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeResponse {
    pub request_id: RequestId,
    pub from: EntityId,
    pub to: EntityId,
    pub body: NodeResponseBody,
    /// Catalog definition entities the receiver needs to resolve this
    /// response's model ids (#330); see `NodeUpdate::schema` in update.rs.
    #[serde(default)]
    pub schema: Vec<ModelContext<Attested<EntityState>>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeResponseBody {
    // Response to CommitEvents
    CommitComplete {
        id: TransactionId,
    },
    Fetch(Vec<EntityDelta>),
    Get {
        model: ModelId,
        states: Vec<Attested<EntityState>>,
    },
    GetEvents {
        model: ModelId,
        events: Vec<Attested<Event>>,
    },
    QuerySubscribed {
        query_id: QueryId,
        deltas: Vec<EntityDelta>,
    },
    /// Response to RegisterSchema (RFC 5.2): the full resolved definitions,
    /// ids included -- allocated on this execution or already existing. The
    /// requester upserts these into its catalog map immediately on ack, so
    /// schema binding and id-keyed writes proceed without waiting for the
    /// catalog subscription.
    SchemaRegistered {
        models: Vec<RegisteredModel>,
        properties: Vec<RegisteredProperty>,
        memberships: Vec<RegisteredMembership>,
    },
    Success,
    Error(String),
}

impl NodeResponseBody {
    /// Model ids carried by entity data in this response. Senders use this to
    /// attach any catalog definitions the connection has not seen yet.
    pub fn referenced_models(&self) -> BTreeSet<crate::ModelId> {
        let mut models = BTreeSet::new();
        match self {
            Self::Fetch(deltas) | Self::QuerySubscribed { deltas, .. } => models.extend(deltas.iter().map(|delta| delta.model.clone())),
            Self::Get { model, .. } | Self::GetEvents { model, .. } => {
                models.insert(*model);
            }
            Self::CommitComplete { .. } | Self::SchemaRegistered { .. } | Self::Success | Self::Error(_) => {}
        }
        models
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
            NodeRequestBody::CommitTransaction { id, events } => {
                write!(
                    f,
                    "CommitTransaction {id} [{}]",
                    events.iter().map(|event| format!("{:#}: {}", event.model, event.value.payload)).collect::<Vec<_>>().join(", ")
                )
            }
            NodeRequestBody::Get { model, ids } => {
                write!(f, "Get {model} {}", ids.iter().map(|id| id.to_base64_short()).collect::<Vec<_>>().join(", "))
            }
            NodeRequestBody::GetEvents { model, event_ids } => {
                write!(f, "GetEvents {model} {}", event_ids.iter().map(|id| id.to_base64_short()).collect::<Vec<_>>().join(", "),)
            }
            NodeRequestBody::Fetch { model, selection: query, known_matches } => {
                write!(f, "Fetch {model} {query} known:{}", known_matches.len())
            }
            NodeRequestBody::SubscribeQuery { query_id, model, selection: query, version, known_matches } => {
                write!(f, "Subscribe {query_id} {model} {query} v{version} known:{}", known_matches.len())
            }
            NodeRequestBody::RegisterSchema { models, properties, memberships } => {
                write!(f, "RegisterSchema models:{} properties:{} memberships:{}", models.len(), properties.len(), memberships.len())
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
            NodeResponseBody::Get { states, .. } => {
                write!(f, "Get [{}]", states.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(", "))
            }
            NodeResponseBody::GetEvents { events, .. } => {
                write!(f, "GetEvents [{}]", events.iter().map(|e| e.payload.to_string()).collect::<Vec<_>>().join(", "))
            }
            NodeResponseBody::QuerySubscribed { query_id, deltas: initial } => write!(f, "Subscribed {query_id} initial:{}", initial.len()),
            NodeResponseBody::SchemaRegistered { models, properties, memberships } => {
                write!(f, "SchemaRegistered models:{} properties:{} memberships:{}", models.len(), properties.len(), memberships.len())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SystemModel;

    fn selection() -> ast::Selection { ankql::parser::parse_selection("true").unwrap() }

    #[test]
    fn query_request_arms_carry_tagged_model_ids_and_no_collection_field() {
        let allocated = ModelId::EntityId(EntityId::from_bytes([0x5a; 16]));
        let requests = [
            NodeRequestBody::Get { model: allocated.clone(), ids: Vec::new() },
            NodeRequestBody::GetEvents { model: ModelId::System(SystemModel::System), event_ids: Vec::new() },
            NodeRequestBody::Fetch { model: ModelId::System(SystemModel::Model), selection: selection(), known_matches: Vec::new() },
            NodeRequestBody::SubscribeQuery {
                query_id: QueryId::test(7),
                model: ModelId::System(SystemModel::Property),
                selection: selection(),
                version: 1,
                known_matches: Vec::new(),
            },
        ];

        for request in requests {
            assert!(request.target_model().is_some());
            let json = serde_json::to_value(&request).unwrap();
            let body = json.as_object().unwrap().values().next().unwrap().as_object().unwrap();
            assert!(body.contains_key("model"));
            assert!(!body.contains_key("collection"));
            let bytes = bincode::serialize(&request).unwrap();
            let round_trip: NodeRequestBody = bincode::deserialize(&bytes).unwrap();
            assert_eq!(round_trip.target_model(), request.target_model());
        }
    }

    #[test]
    fn get_wire_shape_pins_the_model_tag_before_ids() {
        let request = NodeRequestBody::Get { model: ModelId::System(SystemModel::ModelProperty), ids: Vec::new() };
        assert_eq!(
            bincode::serialize(&request).unwrap(),
            [
                1, 0, 0, 0, // Get
                1, 0, 0, 0, // ModelId::System
                3, 0, 0, 0, // SystemModel::ModelProperty
                0, 0, 0, 0, 0, 0, 0, 0, // empty ids
            ]
        );
    }
}
