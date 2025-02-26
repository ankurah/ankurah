use crate::{
    error::ValidationError,
    model::Entity,
    node::{ContextData, Node, NodeInner},
    property::PropertyError,
    proto,
    storage::StorageEngine,
};
use ankql::{ast::Predicate, error::ParseError};
use async_trait::async_trait;
use thiserror::Error;

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
}

impl From<PropertyError> for AccessDenied {
    fn from(error: PropertyError) -> Self { AccessDenied::PropertyError(Box::new(error)) }
}
impl From<ParseError> for AccessDenied {
    fn from(error: ParseError) -> Self { AccessDenied::ParseError(error) }
}

impl AccessDenied {}

/// Applications will implement this trait to control access to resources
/// (Entities and RPC calls) and the Node will be generic over this trait
#[async_trait]
pub trait PolicyAgent: Clone + Send + Sync + 'static {
    /// The context type that will be used for all resource requests.
    /// This will typically represent a user or service account.
    type ContextData: ContextData;

    /// Validate and convert from wire format to this context type
    async fn validate_request<SE: StorageEngine>(
        &self,
        node: &Node<SE, Self>,
        auth: proto::AuthData,
        request: &proto::NodeRequest,
    ) -> Result<Self::ContextData, ValidationError>
    where
        Self: Sized;

    /// Create relevant auth data for a given request
    fn sign_request<SE: StorageEngine>(
        &self,
        node: &NodeInner<SE, Self>,
        cdata: &Self::ContextData,
        request: &proto::NodeRequest,
    ) -> proto::AuthData;

    /// Create an attestation for an event
    fn attest_event<SE: StorageEngine>(
        &self,
        node: &Node<SE, Self>,
        cdata: &Self::ContextData,
        event: &proto::Event,
    ) -> Result<proto::Attestation, AccessDenied>;

    fn validate_event_attestation<SE: StorageEngine>(
        &self,
        node: &Node<SE, Self>,
        from_node: &proto::NodeId,
        event: &proto::Event,
        attestation: Option<&proto::Attestation>,
    ) -> Result<(), AccessDenied>;

    // For checking if a context can access a collection
    // For checking if a context can access a collection
    fn can_access_collection(&self, data: &Self::ContextData, collection: &proto::CollectionId) -> Result<(), AccessDenied>;

    fn filter_predicate(
        &self,
        data: &Self::ContextData,
        collection: &proto::CollectionId,
        predicate: Predicate,
    ) -> Result<Predicate, AccessDenied>;

    // // For checking if a context can read an entity
    // fn can_read_entity(&self, data: &Self::ContextData, entity: &Entity) -> AccessResult;

    // // For checking if a context can modify an entity
    // fn can_modify_entity(&self, data: &Self::ContextData, collection: &CollectionId, id: &ID) -> AccessResult;

    // // For checking if a context can create entities in a collection
    fn pre_create(&self, data: &Self::ContextData, entity: &Entity) -> Result<(), AccessDenied>;

    // TODO - figure out how we can convert pre-event actions and post-event actions into a single representation suitable for passing to pre_edit so we can policy-control discrete mutations
    fn pre_edit(&self, data: &Self::ContextData, entity: &Entity) -> Result<(), AccessDenied>;

    // // For checking if a context can subscribe to changes
    // fn can_subscribe(&self, data: &Self::ContextData, collection: &CollectionId, predicate: &Predicate) -> AccessResult;

    // // For checking if a context can communicate with another node
    // fn can_communicate_with_node(&self, data: &Self::ContextData, node_id: &NodeId) -> AccessResult;
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
        proto::AuthData(vec![])
    }

    /// Validate auth data and yield the context data if valid
    async fn validate_request<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _auth: proto::AuthData,
        _request: &proto::NodeRequest,
    ) -> Result<Self::ContextData, ValidationError> {
        Ok(DEFAULT_CONTEXT)
    }

    /// Create an attestation for an event
    fn attest_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _cdata: &Self::ContextData,
        _event: &proto::Event,
    ) -> Result<proto::Attestation, AccessDenied> {
        Ok(proto::Attestation(vec![]))
    }

    fn validate_event_attestation<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _from_node: &proto::NodeId,
        _event: &proto::Event,
        _attestation: Option<&proto::Attestation>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn can_access_collection(&self, _context: &Self::ContextData, _collection: &proto::CollectionId) -> Result<(), AccessDenied> { Ok(()) }

    fn pre_create(&self, _context: &Self::ContextData, _entity: &Entity) -> Result<(), AccessDenied> { Ok(()) }

    fn pre_edit(&self, _context: &Self::ContextData, _entity: &Entity) -> Result<(), AccessDenied> { Ok(()) }

    fn filter_predicate(
        &self,
        _context: &Self::ContextData,
        _collection: &proto::CollectionId,
        predicate: Predicate,
    ) -> Result<Predicate, AccessDenied> {
        Ok(predicate)
    }

    // fn can_read_entity(&self, _context: &Self::ContextData, _entity: &Entity) -> AccessResult { AccessResult::Allow }

    // fn can_modify_entity(&self, _context: &Self::ContextData, _collection: &CollectionId, _id: &ID) -> AccessResult { AccessResult::Allow }

    // fn can_create_in_collection(&self, _context: &Self::ContextData, _collection: &CollectionId) -> AccessResult { AccessResult::Allow }

    // fn can_subscribe(&self, _context: &Self::ContextData, _collection: &CollectionId, _predicate: &Predicate) -> AccessResult {
    //     AccessResult::Allow
    // }

    // fn can_communicate_with_node(&self, _context: &Self::ContextData, _node_id: &NodeId) -> AccessResult { AccessResult::Allow }
}

/// A default context that is used when no context is needed

pub struct DefaultContext {}
pub static DEFAULT_CONTEXT: &'static DefaultContext = &DefaultContext {};

impl DefaultContext {
    pub fn new() -> Self { Self {} }
}

#[async_trait]
impl ContextData for &'static DefaultContext {}
