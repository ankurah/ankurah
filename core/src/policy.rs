use crate::error::{NodeDropped, ValidationError};
use crate::internal::prelude::*;
use crate::util::Iterable;
use crate::{
    node::{ContextData, NodeInner},
    property::PropertyError,
};
use ankql::{
    ast::{Predicate, Resolved},
    error::ParseError,
};
use ankurah_proto::Attested;
use async_trait::async_trait;
use thiserror::Error;
use tracing::debug;

mod context;
pub use context::ContextPolicy;

/// The result of a policy check. Currently just Allow/Deny, but will support Trace in the future
#[derive(Debug, Error, Clone)]
pub enum AccessDenied {
    #[error("Access denied by policy: {0}")]
    ByPolicy(&'static str),
    #[error("Access denied by model: {0}")]
    ModelDenied(proto::ModelId),
    #[error("Access denied by property error: {0}")]
    PropertyError(std::sync::Arc<PropertyError>),
    #[error("Access denied by parse error: {0}")]
    ParseError(ParseError),
    #[error("Insufficient attestation")]
    InsufficientAttestation,
    #[error("Node has been dropped")]
    NodeDropped,
}

impl From<NodeDropped> for AccessDenied {
    fn from(_: NodeDropped) -> Self { Self::NodeDropped }
}

impl From<PropertyError> for AccessDenied {
    fn from(error: PropertyError) -> Self { AccessDenied::PropertyError(std::sync::Arc::new(error)) }
}
impl From<ParseError> for AccessDenied {
    fn from(error: ParseError) -> Self { AccessDenied::ParseError(error) }
}

#[cfg(feature = "wasm")]
impl From<AccessDenied> for wasm_bindgen::JsValue {
    fn from(error: AccessDenied) -> Self { wasm_bindgen::JsValue::from_str(&error.to_string()) }
}

impl AccessDenied {}

// The registration plan vocabulary (RegistrationPlan and friends) lives
// with its builder in crate::schema::registration and is re-exported here
// because it is part of this trait's surface.
pub use crate::schema::registration::{PlannedModelPropertyMembership, PlannedUpdate, RegistrationPlan};

/// PolicyAgents control access to resources, by:
/// - signing requests which are sent to other nodes - this may come in the form of a bearer token, or a signature, or some other arbitrary method of authentication as defined by the PolicyAgent
/// - checking access for requests. If approved, yield a ContextData
/// - attesting events for requests that were approved
/// - validating attestations for events
///
/// Read checks may receive several credentials, one, or none. Implementations
/// must decide the empty case explicitly and authorize actual entity memberships,
/// not a model supplied by the caller. `PermissiveAgent` allows all credentials.
#[async_trait]
pub trait PolicyAgent: Clone + Send + Sync + 'static {
    /// The context type that will be used for all resource requests.
    /// This will typically represent a user or service account.
    type ContextData: ContextData;

    /// Initialize policy after the system and catalog load, before ordinary contexts are issued.
    /// Load existing policy; installing or updating shared policy is a separate bootstrap operation.
    /// Bootstrap work must not wait for the node to finish this startup step.
    async fn start<SE: StorageEngine + Send + Sync + 'static>(&self, _node: WeakNode<SE, Self>) -> anyhow::Result<()> { Ok(()) }

    /// Load policy bindings for a registered model before synchronous access checks.
    /// This prepares policy data; it neither resolves names nor grants access.
    async fn preflight<SE: StorageEngine + Send + Sync + 'static>(
        &self,
        _node: &Node<SE, Self>,
        _model: proto::ModelId,
    ) -> Result<(), RetrievalError> {
        Ok(())
    }

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
    /// Read predicates and check_write will use the ContextData as well if the request is approved
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

    /// Authorize the complete registration plan; the executor commits privileged.
    fn check_schema_registration<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _cdata: &Self::ContextData,
        _plan: &RegistrationPlan,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    /// Stage policy bindings alongside an authorized schema registration, before its transaction commits.
    /// Replicas apply the committed records; they do not run this hook.
    /// The executor retains any returned authoring guard through the commit.
    async fn schema_registered<SE: StorageEngine + Send + Sync + 'static>(
        &self,
        _node: &Node<SE, Self>,
        _transaction: &Transaction,
        _plan: &RegistrationPlan,
    ) -> anyhow::Result<Option<tokio::sync::OwnedMutexGuard<()>>> {
        Ok(None)
    }

    /// Authorize a proposed event, seeing the transaction's original state
    /// and its state after applying the event; optionally return an attestation.
    fn check_write_event<SE: StorageEngine>(
        &self,
        node: &Node<SE, Self>,
        cdata: &Self::ContextData,
        entity_before: &Entity,
        entity_after: &Entity,
        event: &proto::Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied>;

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

    /// Entities these credentials may discover through queries and subscriptions.
    /// Include all membership and row restrictions; core chooses where to evaluate them.
    fn query_predicate<C>(&self, data: &C) -> Result<Predicate<Resolved>, AccessDenied>
    where
        C: Iterable<Self::ContextData>;

    /// Entities these credentials may retrieve by identity, including their events.
    /// Override when retrieval is permitted more broadly than discovery.
    fn retrieval_predicate<C>(&self, data: &C) -> Result<Predicate<Resolved>, AccessDenied>
    where C: Iterable<Self::ContextData> {
        self.query_predicate(data)
    }

    /// Additional restrictions on retrieved states, including cached reads and live updates.
    /// Return denials keyed by entity id; omitted ids are allowed. Cannot grant access outside the read predicate.
    fn check_reads<C>(
        &self,
        _data: &C,
        _states: &[(&proto::EntityId, &proto::State)],
    ) -> std::collections::HashMap<proto::EntityId, AccessDenied>
    where C: Iterable<Self::ContextData> {
        std::collections::HashMap::new()
    }

    /// Additional event restrictions after the entity passes the retrieval predicate.
    fn check_read_event<C>(&self, _data: &C, _event: &Attested<proto::Event>) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        Ok(())
    }

    /// Check if a context can edit an entity
    fn check_write(
        &self,
        data: &Self::ContextData,
        entity: &Entity,
        event: Option<&proto::Event>,
    ) -> Result<(), AccessDenied>;

    /// Validate a lineage attestation from a peer
    /// This validates that the relation attestation correctly describes the lineage between two entity heads
    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        node: &Node<SE, Self>,
        peer_id: &proto::EntityId,
        head_relation: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied>;

    // // For checking if a context can subscribe to changes
    // fn can_subscribe(&self, data: &Self::ContextData, collection: &ModelId, predicate: &Predicate) -> AccessResult;

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
    fn check_write_event<SE: StorageEngine>(
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

    fn check_write(
        &self,
        _context: &Self::ContextData,
        _entity: &Entity,
        _event: Option<&proto::Event>,
    ) -> Result<(), AccessDenied> {
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

    fn query_predicate<C>(&self, _data: &C) -> Result<Predicate<Resolved>, AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        // PermissiveAgent allows regardless of which credentials are supplied, including none
        Ok(Predicate::True)
    }

    // fn can_read_entity(&self, _context: &Self::ContextData, _entity: &Entity) -> AccessResult { AccessResult::Allow }

    // fn can_modify_entity(&self, _context: &Self::ContextData, _collection: &ModelId, _id: &ID) -> AccessResult { AccessResult::Allow }

    // fn can_create_in_collection(&self, _context: &Self::ContextData, _collection: &ModelId) -> AccessResult { AccessResult::Allow }

    // fn can_subscribe(&self, _context: &Self::ContextData, _collection: &ModelId, _predicate: &Predicate) -> AccessResult {
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
