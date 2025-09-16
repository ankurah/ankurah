use crate::{
    filtering::FilteredStream,
    sorting::{SortedStream, TopKStream},
};
use ankurah_core::{EntityId, error::RetrievalError};
use ankurah_proto::{Attested, EntityState};

/// Stream of entity IDs - generic over any storage engine
pub trait EntityIdStream: Iterator<Item = Result<EntityId, RetrievalError>> {}

/// Stream of entity states - generic over any storage engine  
pub trait EntityStateStream: Iterator<Item = Result<Attested<EntityState>, RetrievalError>> {
    /// Collect states, failing fast on first error
    fn collect_states(self) -> Result<Vec<Attested<EntityState>>, RetrievalError>
    where Self: Sized {
        let mut results = Vec::new();
        for item in self {
            match item {
                Ok(state) => results.push(state),
                Err(e) => return Err(e), // Fail fast on first error
            }
        }
        Ok(results)
    }
}

/// Generic limit adapter for iterators used in the pipeline.
pub trait LimitExt: Sized {
    fn limit(self, _limit: Option<u64>) -> Self {
        todo!("limit: terminate upstream once satisfied");
    }
}

/// Generic scan operations that can be implemented by any KV store
pub trait ScanExt: Sized {
    /// Associated type for the entity ID stream this scanner produces
    type EntityIdStream: EntityIdStream;

    /// Extract entity IDs from keys (e.g., index key suffix or collection key)
    fn extract_entity_ids(self) -> Self::EntityIdStream;
}

// Blanket implementations for common iterator types
impl<I> EntityIdStream for I where I: Iterator<Item = Result<EntityId, RetrievalError>> {}
impl<I> EntityStateStream for I where I: Iterator<Item = Result<Attested<EntityState>, RetrievalError>> {}
impl<I, T> LimitExt for I where I: Iterator<Item = Result<T, RetrievalError>> {}

/// GetPropertyValueStream: default combinators that construct wrapper streams
pub trait GetPropertyValueStream: Iterator + Sized {
    /// Filter: returns a FilteredStream over this iterator
    fn filter_predicate(self, _predicate: &ankql::ast::Predicate) -> FilteredStream<Self> {
        todo!("construct FilteredStream(self, predicate.clone())");
    }

    /// Sort: returns a SortedStream over this iterator (mutually exclusive with limit/top_k)
    fn sort_by(self, _order_by: &[ankql::ast::OrderByItem]) -> SortedStream<Self> {
        todo!("construct SortedStream(self, order_by.to_vec())");
    }

    /// Top-K: returns a TopKStream over this iterator (mutually exclusive with sort/limit)
    fn top_k(self, _order_by: &[ankql::ast::OrderByItem], _k: usize) -> TopKStream<Self> {
        todo!("construct TopKStream(self, order_by.to_vec(), k)");
    }
}
