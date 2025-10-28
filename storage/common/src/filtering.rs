use ankql::ast::{OrderByItem, Predicate};
use ankurah_core::selection::filter::{Filterable, evaluate_predicate};

use crate::sorting::{LimitedStream, SortedStream, TopKStream};

/// Stream of items that can provide property values for filtering, sorting, and limiting
pub trait ValueSetStream: Iterator + Sized
where Self::Item: Filterable
{
    /// Filter stream items using a predicate
    fn filter_predicate(self, predicate: &Predicate) -> FilteredStream<Self> { FilteredStream::new(self, predicate.clone()) }

    /// Sort all items (mutually exclusive with limit/top_k)
    fn sort_by(self, order_by: &[OrderByItem]) -> SortedStream<Self> { SortedStream::new(self, order_by.to_vec()) }

    /// Limit stream to N items (mutually exclusive with sort_by/top_k)
    fn limit(self, limit: Option<u64>) -> LimitedStream<Self> { LimitedStream::new(self, limit) }

    /// Top-K with sort and limit (mutually exclusive with sort_by/limit)
    fn top_k(self, order_by: &[OrderByItem], k: usize) -> TopKStream<Self> { TopKStream::new(self, order_by.to_vec(), k) }

    /// Extract entity IDs from materialized values
    fn extract_ids(self) -> ExtractIdsStream<Self> { ExtractIdsStream::new(self) }
}

// Note: No blanket implementation - each concrete type implements  ValueSetStream individually

/// Wrapper for filtered streams - passes through items matching a predicate
pub struct FilteredStream<I> {
    pub inner: I,
    pub predicate: Predicate,
}

impl<I> FilteredStream<I> {
    pub fn new(inner: I, predicate: Predicate) -> Self { Self { inner, predicate } }
}

impl<I> Iterator for FilteredStream<I>
where
    I: Iterator,
    I::Item: Filterable,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.inner.next()?;

            // Evaluate predicate against the item
            match evaluate_predicate(&item, &self.predicate) {
                Ok(true) => return Some(item), // Item passes filter
                Ok(false) => continue,         // Item doesn't match, try next
                Err(_) => continue,            // Error evaluating, skip item (TODO: proper error handling)
            }
        }
    }
}

/// Wrapper for extracting entity IDs from materialized values
pub struct ExtractIdsStream<I> {
    pub inner: I,
}

impl<I> ExtractIdsStream<I> {
    pub fn new(inner: I) -> Self { Self { inner } }
}

impl<I> Iterator for ExtractIdsStream<I>
where
    I: Iterator,
    I::Item: HasEntityId,
{
    type Item = Result<ankurah_core::EntityId, ankurah_core::error::RetrievalError>;

    fn next(&mut self) -> Option<Self::Item> { self.inner.next().map(|item| Ok(item.entity_id())) }
}

/// Trait for types that can provide an EntityId
pub trait HasEntityId {
    fn entity_id(&self) -> ankurah_core::EntityId;
}

// ExtractIdsStream implements EntityIdStream (via blanket impl) since it yields Result<EntityId, RetrievalError>

// Blanket implementation for any Iterator with Filterable items
impl<I> ValueSetStream for I
where
    I: Iterator,
    I::Item: Filterable,
{
}
