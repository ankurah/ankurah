use crate::{
    entity::Entity,
    error::ValidationError,
    node::{ContextData, Node, NodeInner},
    property::PropertyError,
    proto::{self, CollectionId, ID},
    storage::StorageEngine,
};
use ankql::{ast::Predicate, error::ParseError};
use ankurah_proto::Attested;
use async_trait::async_trait;
use thiserror::Error;
use tracing::{debug, info};
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

impl AccessDenied {}

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

    /// Create relevant auth data for a given request
    /// This could be a JWT or a cryptographic signature, or some other arbitrary method of authentication as defined by the PolicyAgent
    fn sign_request<SE: StorageEngine>(
        &self,
        node: &NodeInner<SE, Self>,
        cdata: &Self::ContextData,
        request: &proto::NodeRequest,
    ) -> proto::AuthData;

    /// Reverse of sign_request. This will typically parse + validate the auth data and return a ContextData if valid
    /// optionally, the PolicyAgent may introspect the request directly for signature validation, or other policy checks
    /// Note that check_read and check_write will be called with the ContextData as well if the request is approved
    /// Meaning that the PolicyAgent need not necessarily introspect the request directly here if it doesn't want to.
    async fn check_request<SE: StorageEngine>(
        &self,
        node: &Node<SE, Self>,
        auth: &proto::AuthData,
        request: &proto::NodeRequest,
    ) -> Result<Self::ContextData, ValidationError>
    where
        Self: Sized;

    /// Check the event and optionally return an attestation
    /// This could be used to attest that the event has passed the policy check for a given context
    /// or you could just return None if you don't want to attest to the event
    fn check_event<SE: StorageEngine>(
        &self,
        node: &Node<SE, Self>,
        cdata: &Self::ContextData,
        entity: &Entity,
        event: &proto::Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied>;

    /// Validate an event attestation
    /// This could be used to validate that the event has sufficient attestation as to be trusted
    fn validate_received_event<SE: StorageEngine>(
        &self,
        node: &Node<SE, Self>,
        received_from_node: &proto::ID,
        event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied>;

    // For checking if a context can access a collection
    // For checking if a context can access a collection
    fn can_access_collection(&self, data: &Self::ContextData, collection: &proto::CollectionId) -> Result<(), AccessDenied>;

    /// Filter a predicate based on the context data
    fn filter_predicate(
        &self,
        data: &Self::ContextData,
        collection: &proto::CollectionId,
        predicate: Predicate,
    ) -> Result<Predicate, AccessDenied>;

    /// Check if a context can read an entity
    fn check_read(&self, data: &Self::ContextData, entity: &Entity) -> Result<(), AccessDenied>;

    /// Check if a context can edit an entity
    fn check_write(&self, data: &Self::ContextData, entity: &Entity, event: Option<&proto::Event>) -> Result<(), AccessDenied>;

    // fn check_write_event(&self, data: &Self::ContextData, entity: &Entity, event: &proto::Event) -> Result<(), AccessDenied>;

    // // For checking if a context can subscribe to changes
    // fn can_subscribe(&self, data: &Self::ContextData, collection: &CollectionId, predicate: &Predicate) -> AccessResult;

    // // For checking if a context can communicate with another node
    // fn can_communicate_with_node(&self, data: &Self::ContextData, node_id: &ID) -> AccessResult;
}

/// A policy agent that allows all operations
#[derive(Clone)]
pub struct PermissiveAgent {}

impl PermissiveAgent {
    pub fn new() -> Self { Self {} }
}

#[async_trait]
impl PolicyAgent for PermissiveAgent {
    type ContextData = &'static DefaultContext;

    /// Create relevant auth data for a given request
    fn sign_request<SE: StorageEngine>(
        &self,
        _node: &NodeInner<SE, Self>,
        _cdata: &Self::ContextData,
        _request: &proto::NodeRequest,
    ) -> proto::AuthData {
        debug!("PermissiveAgent sign_request: {:?}", _request);
        proto::AuthData(vec![])
    }

    /// Validate auth data and yield the context data if valid
    async fn check_request<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _auth: &proto::AuthData,
        _request: &proto::NodeRequest,
    ) -> Result<Self::ContextData, ValidationError> {
        debug!("PermissiveAgent check_request: {:?}", _request);
        Ok(DEFAULT_CONTEXT)
    }

    /// Create an attestation for an event
    fn check_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _cdata: &Self::ContextData,
        _entity: &Entity,
        _event: &proto::Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied> {
        info!("PermissiveAgent check_event: {:?}", _event);
        Ok(None)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _from_node: &proto::ID,
        _event: &proto::Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        info!("PermissiveAgent validate_received_event: {:?}", _event);
        Ok(())
    }

    fn can_access_collection(&self, _context: &Self::ContextData, _collection: &proto::CollectionId) -> Result<(), AccessDenied> {
        info!("PermissiveAgent can_access_collection: {:?}", _collection);
        Ok(())
    }

    fn check_read(&self, _context: &Self::ContextData, _entity: &Entity) -> Result<(), AccessDenied> {
        info!("PermissiveAgent check_read: {:?}", _entity);
        Ok(())
    }

    fn check_write(&self, _context: &Self::ContextData, _entity: &Entity, event: Option<&proto::Event>) -> Result<(), AccessDenied> {
        info!("PermissiveAgent check_write: {:?}", event);
        Ok(())
    }

    fn filter_predicate(
        &self,
        _context: &Self::ContextData,
        _collection: &proto::CollectionId,
        predicate: Predicate,
    ) -> Result<Predicate, AccessDenied> {
        info!("PermissiveAgent filter_predicate: {:?}", predicate);
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

pub struct DefaultContext {}
pub static DEFAULT_CONTEXT: &'static DefaultContext = &DefaultContext {};

impl DefaultContext {
    pub fn new() -> Self { Self {} }
}

#[async_trait]
impl ContextData for &'static DefaultContext {}
