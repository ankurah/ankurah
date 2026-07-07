use crate::util::Iterable;
use crate::{
    entity::Entity,
    error::ValidationError,
    node::{ContextData, Node, NodeInner, WeakNode},
    property::PropertyError,
    proto::{self},
    storage::StorageEngine,
};
use ankql::{ast::Predicate, error::ParseError};
use ankurah_proto::Attested;
use async_trait::async_trait;
use thiserror::Error;
use tracing::debug;
/// The result of a policy check. Currently just Allow/Deny, but will support Trace in the future
#[derive(Debug, Error)]
pub enum AccessDenied {
    #[error("Access denied by policy: {0}")]
    ByPolicy(&'static str),
    #[error("Access denied by collection: {0}")]
    CollectionDenied(proto::CollectionId),
    #[error("Access denied by property error: {0}")]
    PropertyError(Box<PropertyError>),
    #[error("Access denied by parse error: {0}")]
    ParseError(ParseError),
    #[error("Insufficient attestation")]
    InsufficientAttestation,
}

impl From<PropertyError> for AccessDenied {
    fn from(error: PropertyError) -> Self { AccessDenied::PropertyError(Box::new(error)) }
}
impl From<ParseError> for AccessDenied {
    fn from(error: ParseError) -> Self { AccessDenied::ParseError(error) }
}

#[cfg(feature = "wasm")]
impl From<AccessDenied> for wasm_bindgen::JsValue {
    fn from(error: AccessDenied) -> Self { wasm_bindgen::JsValue::from_str(&error.to_string()) }
}

impl AccessDenied {}

/// What a RegisterSchema request will ACTUALLY do, resolved by the
/// registration executor under the allocation mutex (RFC 5.7; plan
/// decision 26). Passed to [`PolicyAgent::check_schema_registration`]
/// before any event is emitted, so an agent can judge real creations and
/// metadata changes without performing its own catalog lookups:
/// `check_request` cannot know whether a descriptor already exists, and
/// `check_event` fires per event mid-commit. Core-side only; never
/// crosses the wire.
#[derive(Debug, Default)]
pub struct RegistrationPlan {
    /// Model entities this request will CREATE, with their would-be
    /// allocated ids (minted, not yet committed).
    pub creates_models: Vec<(proto::EntityId, proto::ModelDescriptor)>,
    /// Property entities this request will CREATE, with their would-be
    /// allocated ids. The descriptor's `minting_collection` names the
    /// owning scope (a created or existing model in this same plan).
    pub creates_properties: Vec<(proto::EntityId, proto::PropertyDescriptor)>,
    /// Contract memberships this request will CREATE, fully resolved.
    pub creates_memberships: Vec<PlannedMembership>,
    /// Metadata follow-ups this request will write on EXISTING entities
    /// (display-name changes including rename-hint applications, target
    /// retargets, membership `optional` flips).
    pub updates: Vec<PlannedUpdate>,
    /// Definitions that resolved to existing entities with no changes:
    /// pure no-ops, listed for context.
    pub existing: Vec<proto::EntityId>,
}

impl RegistrationPlan {
    /// Whether the plan writes anything at all (a re-registration of
    /// unchanged definitions is a pure no-op and skips the policy verb).
    pub fn is_noop(&self) -> bool {
        self.creates_models.is_empty()
            && self.creates_properties.is_empty()
            && self.creates_memberships.is_empty()
            && self.updates.is_empty()
    }
}

/// A membership creation in a [`RegistrationPlan`], resolved to ids.
#[derive(Debug, Clone)]
pub struct PlannedMembership {
    pub id: proto::EntityId,
    pub model: proto::EntityId,
    pub property: proto::EntityId,
    pub optional: bool,
}

/// A metadata follow-up on an existing catalog entity, in a
/// [`RegistrationPlan`].
#[derive(Debug, Clone)]
pub struct PlannedUpdate {
    /// Which catalog collection the entity lives in.
    pub collection: proto::CollectionId,
    pub entity: proto::EntityId,
    pub field: String,
    /// The current catalog value, when one exists.
    pub from: Option<crate::value::Value>,
    pub to: crate::value::Value,
}

/// PolicyAgents control access to resources, by:
/// - signing requests which are sent to other nodes - this may come in the form of a bearer token, or a signature, or some other arbitrary method of authentication as defined by the PolicyAgent
/// - checking access for requests. If approved, yield a ContextData
/// - attesting events for requests that were approved
/// - validating attestations for events
#[async_trait]
pub trait PolicyAgent: Clone + Send + Sync + 'static {
    /// The context type that will be used for all resource requests.
    /// This will typically represent a user or service account.
    type ContextData: ContextData;

    /// Called after the Node is fully constructed, giving the PolicyAgent a weak reference to its owning node.
    /// Use this to start background tasks (file watchers, policy subscriptions) that need the node.
    fn on_node_ready<SE: StorageEngine + Send + Sync + 'static>(&self, _node: WeakNode<SE, Self>) {}

    /// Create relevant auth data for a given request
    /// This could be a JWT or a cryptographic signature, or some other arbitrary method of authentication as defined by the PolicyAgent
    fn sign_request<SE: StorageEngine, C>(
        &self,
        node: &NodeInner<SE, Self>,
        cdata: &C,
        request: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied>
    where
        C: Iterable<Self::ContextData>;

    /// Reverse of sign_request. This will typically parse + validate the auth data and return a ContextData if valid
    /// optionally, the PolicyAgent may introspect the request directly for signature validation, or other policy checks
    /// Note that check_read and check_write will be called with the ContextData as well if the request is approved
    /// Meaning that the PolicyAgent need not necessarily introspect the request directly here if it doesn't want to.
    async fn check_request<SE: StorageEngine, A>(
        &self,
        node: &Node<SE, Self>,
        auth: &A,
        request: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ValidationError>
    where
        Self: Sized,
        A: Iterable<proto::AuthData> + Send + Sync;

    /// Check the event and optionally return an attestation
    /// This could be used to attest that the event has passed the policy check for a given context
    /// or you could just return None if you don't want to attest to the event
    /// entity_before: Entity state before the event is applied
    /// entity_after: Entity state after the event has been applied (allows inspection of resulting state)
    fn check_event<SE: StorageEngine>(
        &self,
        node: &Node<SE, Self>,
        cdata: &Self::ContextData,
        entity_before: &Entity,
        entity_after: &Entity,
        event: &proto::Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied>;

    /// Gate a schema registration on its RESOLVED effect (RFC 5.7; plan
    /// decision 26). Called by the registration executor after its lookup
    /// phase and before any event is emitted, still under the allocation
    /// mutex, with the request's actual consequences: what will be created,
    /// what will be updated, what already exists. Agents may discriminate
    /// on the principal (`cdata`: who may define schema) or on the object
    /// (the planned definitions themselves); both styles are first-class.
    /// Refusal fails the whole registration before anything is emitted.
    /// Every emitted event still passes [`Self::check_event`] afterwards,
    /// INDIVIDUALLY: the commit batch is not transactional, so an agent
    /// that allows the plan but denies a constituent event aborts the
    /// remainder and leaves earlier catalog events durable (accepted by
    /// maintainer ruling 2026-07-06; the allocator's storage-checked
    /// lookups keep identity convergent across such partials, and #313
    /// tracks the transactional upgrade). The default allows.
    fn check_schema_registration<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _cdata: &Self::ContextData,
        _plan: &RegistrationPlan,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    /// Validate an event attestation
    /// This could be used to validate that the event has sufficient attestation as to be trusted
    fn validate_received_event<SE: StorageEngine>(
        &self,
        node: &Node<SE, Self>,
        received_from_node: &proto::EntityId,
        event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied>;

    /// Attest a state which the caller asserts is valid. Implementation may return None if no attestation is required
    fn attest_state<SE: StorageEngine>(&self, node: &Node<SE, Self>, state: &proto::EntityState) -> Option<proto::Attestation>;

    fn validate_received_state<SE: StorageEngine>(
        &self,
        node: &Node<SE, Self>,
        received_from_node: &proto::EntityId,
        state: &Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied>;

    // For checking if a context can access a collection
    fn can_access_collection<C>(&self, data: &C, collection: &proto::CollectionId) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData>;

    /// Filter a predicate based on the context data
    fn filter_predicate<C>(&self, data: &C, collection: &proto::CollectionId, predicate: Predicate) -> Result<Predicate, AccessDenied>
    where C: Iterable<Self::ContextData>;

    /// Check if a context can read an entity
    /// If the policy agent wants to inspect the entity state, it can do so with either TemporaryEntity::new or entityset.with_state
    /// Optimization: Consider adding a common trait implemented by Entity and TemporaryEntity returned by entityset.get_evaluation_entity that
    /// returns a real entity if resident, falling back to a temporary entity if not. (as the former case would save cycles creating/populating the backends)
    fn check_read<C>(
        &self,
        data: &C,
        id: &proto::EntityId,
        collection: &proto::CollectionId,
        state: &proto::State,
    ) -> Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>;

    /// Check if a context can read an event
    fn check_read_event<C>(&self, data: &C, event: &Attested<proto::Event>) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData>;

    /// Check if a context can edit an entity
    fn check_write(&self, data: &Self::ContextData, entity: &Entity, event: Option<&proto::Event>) -> Result<(), AccessDenied>;

    /// Validate a lineage attestation from a peer
    /// This validates that the relation attestation correctly describes the lineage between two entity heads
    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        node: &Node<SE, Self>,
        peer_id: &proto::EntityId,
        head_relation: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied>;

    // fn check_write_event(&self, data: &Self::ContextData, entity: &Entity, event: &proto::Event) -> Result<(), AccessDenied>;

    // // For checking if a context can subscribe to changes
    // fn can_subscribe(&self, data: &Self::ContextData, collection: &CollectionId, predicate: &Predicate) -> AccessResult;

    // // For checking if a context can communicate with another node
    // fn can_communicate_with_node(&self, data: &Self::ContextData, node_id: &ID) -> AccessResult;
}

/// A policy agent that allows all operations
#[derive(Clone)]
pub struct PermissiveAgent {}

impl Default for PermissiveAgent {
    fn default() -> Self { Self::new() }
}

impl PermissiveAgent {
    pub fn new() -> Self { Self {} }
}

#[async_trait]
impl PolicyAgent for PermissiveAgent {
    type ContextData = &'static DefaultContext;

    /// Create relevant auth data for a given request
    fn sign_request<SE: StorageEngine, C>(
        &self,
        _node: &NodeInner<SE, Self>,
        cdata: &C,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        debug!("PermissiveAgent sign_request: {:?}", _request);
        // Create one AuthData per context (though PermissiveAgent doesn't really use them)
        Ok(cdata.iterable().map(|_| proto::AuthData(vec![])).collect())
    }

    /// Validate auth data and yield the context data if valid
    async fn check_request<SE: StorageEngine, A>(
        &self,
        _node: &Node<SE, Self>,
        auth: &A,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ValidationError>
    where
        A: Iterable<proto::AuthData> + Send + Sync,
    {
        // PermissiveAgent accepts all auth attempts and returns one context per auth
        Ok(auth.iterable().map(|_| DEFAULT_CONTEXT).collect())
    }

    /// Create an attestation for an event
    fn check_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _cdata: &Self::ContextData,
        _entity_before: &Entity,
        _entity_after: &Entity,
        _event: &proto::Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied> {
        Ok(None)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _from_node: &proto::EntityId,
        _event: &proto::Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _node: &Node<SE, Self>, _state: &proto::EntityState) -> Option<proto::Attestation> {
        // This PolicyAgent does not require attestation, so we return None
        // Client/Server policy agents may also return None and defer to the server identity to validate the received state
        None
    }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _from_node: &proto::EntityId,
        _state: &Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        // This PolicyAgent does not require validation, so we return Ok
        // Client/Server policy agents may use the _from_node to validate the received state rather than an attestation
        Ok(())
    }

    fn can_access_collection<C>(&self, _data: &C, _collection: &proto::CollectionId) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        // PermissiveAgent allows access if any context is provided
        Ok(())
    }

    fn check_read<C>(
        &self,
        _data: &C,
        _id: &proto::EntityId,
        _collection: &proto::CollectionId,
        _state: &proto::State,
    ) -> Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        // PermissiveAgent allows access if any context is provided
        Ok(())
    }

    fn check_read_event<C>(&self, _data: &C, _event: &Attested<proto::Event>) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        // PermissiveAgent allows access if any context is provided
        Ok(())
    }

    fn check_write(&self, _context: &Self::ContextData, _entity: &Entity, _event: Option<&proto::Event>) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _peer_id: &proto::EntityId,
        _head_relation: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        // PermissiveAgent trusts all causal assertions
        Ok(())
    }

    fn filter_predicate<C>(&self, _data: &C, _collection: &proto::CollectionId, predicate: Predicate) -> Result<Predicate, AccessDenied>
    where C: Iterable<Self::ContextData> {
        // PermissiveAgent allows access if any context is provided
        Ok(predicate)
    }

    // fn can_read_entity(&self, _context: &Self::ContextData, _entity: &Entity) -> AccessResult { AccessResult::Allow }

    // fn can_modify_entity(&self, _context: &Self::ContextData, _collection: &CollectionId, _id: &ID) -> AccessResult { AccessResult::Allow }

    // fn can_create_in_collection(&self, _context: &Self::ContextData, _collection: &CollectionId) -> AccessResult { AccessResult::Allow }

    // fn can_subscribe(&self, _context: &Self::ContextData, _collection: &CollectionId, _predicate: &Predicate) -> AccessResult {
    //     AccessResult::Allow
    // }

    // fn can_communicate_with_node(&self, _context: &Self::ContextData, _node_id: &ID) -> AccessResult { AccessResult::Allow }
}

/// A default context that is used when no context is needed

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DefaultContext {}
pub static DEFAULT_CONTEXT: &DefaultContext = &DefaultContext {};

impl Default for DefaultContext {
    fn default() -> Self { Self::new() }
}

impl DefaultContext {
    pub fn new() -> Self { Self {} }
}

#[async_trait]
impl ContextData for &'static DefaultContext {}
