use crate::{
    node::ContextData,
    proto::{CollectionId, ID},
};
use ankql::ast::Predicate;

/// The result of a policy check. Currently just Allow/Deny, but will support Trace in the future
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessResult {
    /// The operation is allowed
    Allow,
    /// The operation is denied
    Deny,
}

impl AccessResult {
    /// Convenience method to check if access is allowed
    pub fn is_allowed(&self) -> bool { matches!(self, AccessResult::Allow) }
}

/// Applications will implement this trait to control access to resources
/// (Entities and RPC calls) and the Node will be generic over this trait
pub trait PolicyAgent: Clone + Send + Sync + 'static {
    /// The context type that will be used for all resource requests.
    /// This will typically represent a user or service account.
    type ContextData: ContextData;

    // For checking if a context can access a collection
    fn can_access_collection(&self, data: &Self::ContextData, collection: &CollectionId) -> AccessResult;

    // For checking if a context can read an entity
    fn can_read_entity(&self, data: &Self::ContextData, collection: &CollectionId, id: &ID) -> AccessResult;

    // For checking if a context can modify an entity
    fn can_modify_entity(&self, data: &Self::ContextData, collection: &CollectionId, id: &ID) -> AccessResult;

    // For checking if a context can create entities in a collection
    fn can_create_in_collection(&self, data: &Self::ContextData, collection: &CollectionId) -> AccessResult;

    // For checking if a context can subscribe to changes
    fn can_subscribe(&self, data: &Self::ContextData, collection: &CollectionId, predicate: &Predicate) -> AccessResult;

    // For checking if a context can communicate with another node
    fn can_communicate_with_node(&self, data: &Self::ContextData, node_id: &ID) -> AccessResult;
}

/// A policy agent that allows all operations
#[derive(Clone)]
pub struct PermissiveAgent {}

impl PermissiveAgent {
    pub fn new() -> Self { Self {} }
}

impl PolicyAgent for PermissiveAgent {
    type ContextData = &'static DefaultContext;

    fn can_access_collection(&self, _context: &Self::ContextData, _collection: &CollectionId) -> AccessResult { AccessResult::Allow }

    fn can_read_entity(&self, _context: &Self::ContextData, _collection: &CollectionId, _id: &ID) -> AccessResult { AccessResult::Allow }

    fn can_modify_entity(&self, _context: &Self::ContextData, _collection: &CollectionId, _id: &ID) -> AccessResult { AccessResult::Allow }

    fn can_create_in_collection(&self, _context: &Self::ContextData, _collection: &CollectionId) -> AccessResult { AccessResult::Allow }

    fn can_subscribe(&self, _context: &Self::ContextData, _collection: &CollectionId, _predicate: &Predicate) -> AccessResult {
        AccessResult::Allow
    }

    fn can_communicate_with_node(&self, _context: &Self::ContextData, _node_id: &ID) -> AccessResult { AccessResult::Allow }
}

/// A default context that is used when no context is needed

pub struct DefaultContext {}
pub static DEFAULT_CONTEXT: &'static DefaultContext = &DefaultContext {};

impl DefaultContext {
    pub fn new() -> Self { Self {} }
}

impl ContextData for &'static DefaultContext {}
