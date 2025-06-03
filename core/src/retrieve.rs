pub mod local;
pub mod localrefetch;

use ankurah_proto::CollectionId;

use crate::error::RetrievalError;

/// Trait for retrieving entities for subscriptions using different strategies
/// it's generic so we can use it for unit tests and Entities
#[async_trait::async_trait]
pub trait Fetch<T>: Send + Sync + 'static {
    /// Retrieve entities matching a predicate for a subscription
    async fn fetch(self, collection_id: &CollectionId, predicate: &ankql::ast::Predicate) -> Result<Vec<T>, RetrievalError>;
}

// TODO: consider rolling GetState and GetEvents traits into this file
