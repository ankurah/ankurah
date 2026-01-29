use crate::{
    filtering::FilteredStream,
    sorting::{LimitedStream, SortedStream, TopKStream},
};
use ankurah_core::{EntityId, error::StorageError};
use ankurah_proto::{Attested, EntityState};
use futures::Stream;

/// Stream of entity IDs - generic over any storage engine
pub trait EntityIdStream: Stream<Item = Result<EntityId, StorageError>> + Unpin + Sized {
    /// Limit the number of entity IDs returned
    fn limit(self, limit: Option<u64>) -> LimitedStream<Self> { LimitedStream::new(self, limit) }
}

/// Stream of entity states - generic over any storage engine
pub trait EntityStateStream: Stream<Item = Result<Attested<EntityState>, StorageError>> + Unpin {
    /// Collect states, failing fast on first error (async version)
    fn collect_states(self) -> impl std::future::Future<Output = Result<Vec<Attested<EntityState>>, StorageError>>
    where Self: Sized {
        use futures::StreamExt;
        async move {
            let mut results = Vec::new();
            let mut stream = std::pin::pin!(self);
            while let Some(item) = stream.next().await {
                match item {
                    Ok(state) => results.push(state),
                    Err(e) => return Err(e), // Fail fast on first error
                }
            }
            Ok(results)
        }
    }
}

// LimitExt removed - limit functionality moved to specific stream traits

/// Generic scan operations that can be implemented by any KV store
pub trait ScanExt: Sized {
    /// Associated type for the entity ID stream this scanner produces
    type EntityIdStream: EntityIdStream;

    /// Extract entity IDs from keys (e.g., index key suffix or collection key)
    fn extract_entity_ids(self) -> Self::EntityIdStream;
}

// Blanket implementations for common stream types
impl<S> EntityIdStream for S where S: Stream<Item = Result<EntityId, StorageError>> + Unpin {}
impl<S> EntityStateStream for S where S: Stream<Item = Result<Attested<EntityState>, StorageError>> + Unpin {}

/// GetPropertyValueStream: default combinators that construct wrapper streams
pub trait GetPropertyValueStream: Stream + Unpin + Sized {
    /// Filter: returns a FilteredStream over this stream
    fn filter_predicate(self, _predicate: &ankql::ast::Predicate) -> FilteredStream<Self> {
        todo!("construct FilteredStream(self, predicate.clone())");
    }

    /// Sort: returns a SortedStream over this stream (mutually exclusive with limit/top_k)
    fn sort_by(self, _order_by: &[ankql::ast::OrderByItem]) -> SortedStream<Self> {
        todo!("construct SortedStream(self, order_by.to_vec())");
    }

    /// Top-K: returns a TopKStream over this stream (mutually exclusive with sort/limit)
    fn top_k(self, _order_by: &[ankql::ast::OrderByItem], _k: usize) -> TopKStream<Self> {
        todo!("construct TopKStream(self, order_by.to_vec(), k)");
    }
}
