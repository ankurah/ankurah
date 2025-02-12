use crate::{
    changes::ChangeSet,
    model::{Entity, Model},
    proto::{CollectionId, NodeId, ID},
    traits::{Context, PolicyAgent},
};
use ankql::ast::Predicate;
use std::collections::HashSet;

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

/// A policy agent that allows all operations
#[derive(Clone)]
pub struct PermissiveAgent {}

impl PermissiveAgent {
    pub fn new() -> Self { Self {} }
}

impl PolicyAgent for PermissiveAgent {
    type Context = &'static DefaultContext;

    fn can_access_collection(&self, _context: &Self::Context, _collection: &CollectionId) -> AccessResult { AccessResult::Allow }

    fn can_read_entity(&self, _context: &Self::Context, _collection: &CollectionId, _id: &ID) -> AccessResult { AccessResult::Allow }

    fn can_modify_entity(&self, _context: &Self::Context, _collection: &CollectionId, _id: &ID) -> AccessResult { AccessResult::Allow }

    fn can_create_in_collection(&self, _context: &Self::Context, _collection: &CollectionId) -> AccessResult { AccessResult::Allow }

    fn can_subscribe(&self, _context: &Self::Context, _collection: &CollectionId, _predicate: &Predicate) -> AccessResult {
        AccessResult::Allow
    }

    fn can_communicate_with_node(&self, _context: &Self::Context, _node_id: &NodeId) -> AccessResult { AccessResult::Allow }
}

/// A default context that is used when no context is needed

pub struct DefaultContext {}
pub static DEFAULT_CONTEXT: DefaultContext = DefaultContext {};

impl DefaultContext {
    pub fn new() -> Self { Self {} }
}

impl Context for &'static DefaultContext {}
