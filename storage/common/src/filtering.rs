use ankql::ast::Predicate;
use ankurah_core::selection::filter::{Filterable, evaluate_predicate};
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::OrderByComponents;
use crate::sorting::{LimitedStream, SortedStream, TopKStream};

/// Stream of items that can provide property values for filtering, sorting, and limiting
pub trait ValueSetStream: Stream + Unpin + Sized
where Self::Item: Filterable
{
    /// Filter stream items using a predicate
    fn filter_predicate(self, predicate: &Predicate) -> FilteredStream<Self> { FilteredStream::new(self, predicate.clone()) }

    /// Sort all items by OrderByComponents (partition-aware when presort is non-empty)
    fn sort_by(self, order_by: OrderByComponents) -> SortedStream<Self> { SortedStream::new(self, order_by) }

    /// Limit stream to N items (mutually exclusive with sort_by/top_k)
    fn limit(self, limit: Option<u64>) -> LimitedStream<Self> { LimitedStream::new(self, limit) }

    /// Top-K with sort and limit (partition-aware when presort is non-empty)
    fn top_k(self, order_by: OrderByComponents, k: usize) -> TopKStream<Self> { TopKStream::new(self, order_by, k) }

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

impl<S: Unpin> Unpin for FilteredStream<S> {}

impl<S> Stream for FilteredStream<S>
where
    S: Stream + Unpin,
    S::Item: Filterable,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(item)) => match evaluate_predicate(&item, &self.predicate) {
                    Ok(true) => return Poll::Ready(Some(item)),
                    Ok(false) => continue,
                    Err(_) => continue,
                },
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
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

impl<S: Unpin> Unpin for ExtractIdsStream<S> {}

impl<S> Stream for ExtractIdsStream<S>
where
    S: Stream + Unpin,
    S::Item: HasEntityId,
{
    type Item = Result<ankurah_core::EntityId, ankurah_core::error::StorageError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(Ok(item.entity_id()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Trait for types that can provide an EntityId
pub trait HasEntityId {
    fn entity_id(&self) -> ankurah_core::EntityId;
}

// ExtractIdsStream implements EntityIdStream (via blanket impl) since it yields Result<EntityId, StorageError>

// Blanket implementation for any Stream with Filterable items
impl<S> ValueSetStream for S
where
    S: Stream + Unpin,
    S::Item: Filterable,
{
}
