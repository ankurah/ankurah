use crate::{
    error::RetrievalError,
    policy::AccessResult,
    proto::{CollectionId, Event, NodeId, State, ID},
};
use ankql::ast::Predicate;
use async_trait::async_trait;
use std::sync::Arc;

/// Optional trait that allows storage operations to be scoped to a specific namespace.
/// Storage engines may implement namespace-aware storage to partition data.
pub trait Namespace {
    /// Returns the namespace for this context, if any
    fn namespace(&self) -> Option<&str>;
}

/// Applications will implement this trait to control access to resources
/// (Entities and RPC calls) and the Node will be generic over this trait
pub trait PolicyAgent {
    /// The context type that will be used for all resource requests.
    /// This will typically represent a user or service account.
    type Context: Context;

    // For checking if a context can access a collection
    fn can_access_collection(&self, context: &Self::Context, collection: &CollectionId) -> AccessResult;

    // For checking if a context can read an entity
    fn can_read_entity(&self, context: &Self::Context, collection: &CollectionId, id: &ID) -> AccessResult;

    // For checking if a context can modify an entity
    fn can_modify_entity(&self, context: &Self::Context, collection: &CollectionId, id: &ID) -> AccessResult;

    // For checking if a context can create entities in a collection
    fn can_create_in_collection(&self, context: &Self::Context, collection: &CollectionId) -> AccessResult;

    // For checking if a context can subscribe to changes
    fn can_subscribe(&self, context: &Self::Context, collection: &CollectionId, predicate: &Predicate) -> AccessResult;

    // For checking if a context can communicate with another node
    fn can_communicate_with_node(&self, context: &Self::Context, node_id: &NodeId) -> AccessResult;
}

/// Just a marker trait for now
pub trait Context {}

#[async_trait]
pub trait StorageEngine: Send + Sync {
    // Opens and/or creates a storage collection.
    async fn collection(&self, id: &CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError>;
}

#[async_trait]
pub trait StorageCollection: Send + Sync {
    // TODO - implement merge_states based on event history.
    // Consider whether to play events forward from a prior checkpoint (probably this)
    // or maybe to require PropertyBackends to be able to merge states.
    async fn set_state(&self, id: ID, state: &State) -> anyhow::Result<bool>;
    async fn get_state(&self, id: ID) -> Result<State, RetrievalError>;

    // Fetch raw entity states matching a predicate
    async fn fetch_states(&self, predicate: &ankql::ast::Predicate) -> Result<Vec<(ID, State)>, RetrievalError>;

    async fn set_states(&self, entities: Vec<(ID, &State)>) -> anyhow::Result<()> {
        for (id, state) in entities {
            self.set_state(id, state).await?;
        }
        Ok(())
    }

    // TODO:
    async fn add_event(&self, entity_event: &Event) -> anyhow::Result<bool>;
    async fn get_events(&self, id: ID) -> Result<Vec<Event>, crate::error::RetrievalError>;
}
