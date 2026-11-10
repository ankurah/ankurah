use crate::connector::PeerProvenance;
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

/// A statement a policy agent asks its node to make about the event or state in hand: its kind
/// and payload. The node names the subject and seals the statement with its own key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttestationRequest {
    /// The entity id of the definition that says what the statement means and how to read `payload`.
    pub kind: proto::EntityId,
    /// The statement's content, read as `kind` says.
    pub payload: Vec<u8>,
}

/// What this node grants a connected peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerTrust {
    /// Whether this node treats the peer as durable: a relay target for local commits and a source
    /// for durable reads.
    pub durable: bool,
}

// The registration plan vocabulary (RegistrationPlan and friends) lives
// with its builder in crate::schema::registration and is re-exported here
// because it is part of this trait's surface.
pub use crate::schema::registration::{PlannedModelPropertyMembership, PlannedUpdate, RegistrationPlan};

/// PolicyAgents control access to resources, by:
/// - signing requests which are sent to other nodes - this may come in the form of a bearer token, or a signature, or some other arbitrary method of authentication as defined by the PolicyAgent
/// - checking access for requests. If approved, yield a ContextData
/// - deciding which events and served states this node attests, and what each attestation says
/// - deciding whether the attestations on a received event or state suffice
/// - deciding what a connected peer is trusted with
///
/// Core seals an attestation only when an agent requests one, and checks none: an agent
/// receives the attestations on an event or state exactly as they came, and unseals the ones
/// it cares about with [`proto::SealedAttestation::unseal`].
///
/// Read checks may receive several credentials, one, or none. Implementations
/// must decide the empty case explicitly and authorize actual entity memberships,
/// not a model supplied by the caller. `PermissiveAgent` allows all credentials.
#[async_trait]
pub trait PolicyAgent: Clone + Send + Sync + 'static {
    /// The context type that will be used for all resource requests.
    /// This will typically represent a user or service account.
    type ContextData: ContextData;

    /// Initialize policy after the catalog loads, before ordinary contexts are issued.
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
    /// and its state after applying the event; optionally request this node's attestation on it.
    fn check_write_event<SE: StorageEngine>(
        &self,
        node: &Node<SE, Self>,
        cdata: &Self::ContextData,
        entity_before: &Entity,
        entity_after: &Entity,
        event: &proto::Event,
    ) -> Result<Option<AttestationRequest>, AccessDenied>;

    /// Decide whether a received event's attestations suffice. They arrive as they came:
    /// unverified, and not necessarily naming this event.
    fn validate_received_event<SE: StorageEngine>(
        &self,
        node: &Node<SE, Self>,
        received_from_node: &proto::NodeId,
        event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied>;

    /// The statement this agent asks a durable node to make about `state` when it serves it; `None`
    /// asks for none.
    fn request_state_attestation<SE: StorageEngine>(&self, node: &Node<SE, Self>, state: &proto::EntityState)
        -> Option<AttestationRequest>;

    /// Decide whether a received state's attestations suffice. They arrive as they came:
    /// unverified, and not necessarily naming this state.
    fn validate_received_state<SE: StorageEngine>(
        &self,
        node: &Node<SE, Self>,
        received_from_node: &proto::NodeId,
        state: &Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied>;

    /// Entities these credentials may discover through queries and subscriptions.
    /// Include all membership and row restrictions; core chooses where to evaluate them.
    fn query_predicate<C>(&self, data: &C) -> Result<Predicate<Resolved>, AccessDenied>
    where C: Iterable<Self::ContextData>;

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
    where
        C: Iterable<Self::ContextData>,
    {
        std::collections::HashMap::new()
    }

    /// Additional event restrictions after the entity passes the retrieval predicate.
    fn check_read_event<C>(&self, _data: &C, _event: &Attested<proto::Event>) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        Ok(())
    }

    /// Check if a context can edit an entity
    fn check_write(&self, data: &Self::ContextData, entity: &Entity, event: Option<&proto::Event>) -> Result<(), AccessDenied>;

    /// Validate a lineage attestation from a peer
    /// This validates that the relation attestation correctly describes the lineage between two entity heads
    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        node: &Node<SE, Self>,
        peer_id: &proto::NodeId,
        head_relation: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied>;

    /// The trust this node grants a peer whose presence verified, from how the connection was
    /// reached and whether the peer claims to be durable; by default, durable only for a peer this
    /// node dialed. A verified presence does not show that the peer is the far end of the
    /// connection (docs/internals/node-identity.md, "What the handshake does not give").
    fn peer_trust(&self, _peer: &proto::NodeId, provenance: PeerProvenance, claimed_durable: bool) -> PeerTrust {
        match provenance {
            PeerProvenance::Configured => PeerTrust { durable: claimed_durable },
            PeerProvenance::Inbound => PeerTrust { durable: false },
        }
    }

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

    fn check_write_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _cdata: &Self::ContextData,
        _entity_before: &Entity,
        _entity_after: &Entity,
        _event: &proto::Event,
    ) -> Result<Option<AttestationRequest>, AccessDenied> {
        Ok(None)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _from_node: &proto::NodeId,
        _event: &proto::Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn request_state_attestation<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _state: &proto::EntityState,
    ) -> Option<AttestationRequest> {
        None
    }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _from_node: &proto::NodeId,
        _state: &Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        // This PolicyAgent does not require validation, so we return Ok
        // Client/Server policy agents may use the _from_node to validate the received state rather than an attestation
        Ok(())
    }

    fn check_write(&self, _context: &Self::ContextData, _entity: &Entity, _event: Option<&proto::Event>) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _peer_id: &proto::NodeId,
        _head_relation: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        // PermissiveAgent trusts all causal assertions
        Ok(())
    }

    fn query_predicate<C>(&self, _data: &C) -> Result<Predicate<Resolved>, AccessDenied>
    where C: Iterable<Self::ContextData> {
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
