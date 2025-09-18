use ankql::selection::filter::Filterable;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Helper function to sort items by ORDER BY clauses
fn sort_items_by_order<T: Filterable>(items: &mut [T], order_by: &[ankql::ast::OrderByItem]) {
    items.sort_by(|a, b| {
        for order_item in order_by {
            let property_name = match &order_item.identifier {
                ankql::ast::Identifier::Property(name) => name,
                ankql::ast::Identifier::CollectionProperty(_, name) => name,
            };

            let a_val = a.value(property_name).unwrap_or_default();
            let b_val = b.value(property_name).unwrap_or_default();

            let cmp = match order_item.direction {
                ankql::ast::OrderDirection::Asc => a_val.cmp(&b_val),
                ankql::ast::OrderDirection::Desc => b_val.cmp(&a_val),
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    });
}

/// Helper function to collect and sort all items from an iterator
fn collect_and_sort<I>(inner: I, order_by: &[ankql::ast::OrderByItem]) -> Vec<I::Item>
where
    I: Iterator,
    I::Item: Filterable,
{
    let mut items: Vec<_> = inner.collect();
    sort_items_by_order(&mut items, order_by);
    items
}

/// Helper function to collect top-K items using a bounded heap
fn collect_top_k<I>(inner: I, order_by: &[ankql::ast::OrderByItem], k: usize) -> Vec<I::Item>
where
    I: Iterator,
    I::Item: Filterable,
{
    // Use a bounded min-heap to maintain only the top K items
    let mut heap: BinaryHeap<HeapItem<I::Item>> = BinaryHeap::new();

    for item in inner {
        let heap_item = HeapItem { item, order_by: order_by.to_vec() };

        if heap.len() < k {
            // Heap not full, just add the item
            heap.push(heap_item);
        } else if let Some(smallest) = heap.peek() {
            // Heap is full, check if this item is better than the smallest
            if heap_item > *smallest {
                heap.pop(); // Remove smallest
                heap.push(heap_item); // Add new item
            }
        }
    }

    // Convert heap to sorted vector
    let mut top_k: Vec<_> = heap.into_iter().map(|h| h.item).collect();
    sort_items_by_order(&mut top_k, order_by);
    top_k
}

/// Wrapper for sorted streams - collects all items, sorts them, then iterates
pub struct SortedStream<I>
where I: Iterator
{
    pub inner: Option<I>, // None after collection
    pub order_by: Vec<ankql::ast::OrderByItem>,
    pub sorted_items: Option<std::vec::IntoIter<I::Item>>, // Lazy-initialized sorted iterator
}

impl<I> SortedStream<I>
where I: Iterator
{
    pub fn new(inner: I, order_by: Vec<ankql::ast::OrderByItem>) -> Self { Self { inner: Some(inner), order_by, sorted_items: None } }
}

impl<I> Iterator for SortedStream<I>
where
    I: Iterator,
    I::Item: Filterable,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        // Lazy initialization: collect and sort all items on first call
        if self.sorted_items.is_none()
            && let Some(inner) = self.inner.take()
        {
            let sorted_items = collect_and_sort(inner, &self.order_by);
            self.sorted_items = Some(sorted_items.into_iter());
        }

        // Return next item from sorted collection
        self.sorted_items.as_mut()?.next()
    }
}

/// Wrapper for limited streams - terminates after N items
pub struct LimitedStream<I> {
    pub inner: I,
    pub limit: Option<u64>,
    pub count: u64,
}

impl<I> LimitedStream<I> {
    pub fn new(inner: I, limit: Option<u64>) -> Self { Self { inner, limit, count: 0 } }
}

impl<I> Iterator for LimitedStream<I>
where I: Iterator
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        // Check if we've reached the limit
        if let Some(limit) = self.limit
            && self.count >= limit
        {
            return None;
        }

        // Get next item and increment counter
        match self.inner.next() {
            Some(item) => {
                self.count += 1;
                Some(item)
            }
            None => None,
        }
    }
}

/// Wrapper for heap-ordered items to enable custom comparison
struct HeapItem<T> {
    item: T,
    order_by: Vec<ankql::ast::OrderByItem>,
}

impl<T: Filterable> PartialEq for HeapItem<T> {
    fn eq(&self, other: &Self) -> bool { self.cmp(other) == Ordering::Equal }
}

impl<T: Filterable> Eq for HeapItem<T> {}

impl<T: Filterable> PartialOrd for HeapItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}

impl<T: Filterable> Ord for HeapItem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap, but we want min-heap behavior for TopK
        // So we reverse the comparison to get the smallest items at the top
        for order_item in &self.order_by {
            let property_name = match &order_item.identifier {
                ankql::ast::Identifier::Property(name) => name,
                ankql::ast::Identifier::CollectionProperty(_, name) => name,
            };

            let self_val = self.item.value(property_name).unwrap_or_default();
            let other_val = other.item.value(property_name).unwrap_or_default();

            let cmp = match order_item.direction {
                ankql::ast::OrderDirection::Asc => other_val.cmp(&self_val), // Reversed for min-heap
                ankql::ast::OrderDirection::Desc => self_val.cmp(&other_val), // Reversed for min-heap
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }
}

/// Wrapper for top-K streams - maintains bounded heap, returns sorted top K
pub struct TopKStream<I>
where I: Iterator
{
    pub inner: Option<I>, // None after collection
    pub order_by: Vec<ankql::ast::OrderByItem>,
    pub k: usize,
    pub top_k_items: Option<std::vec::IntoIter<I::Item>>, // Lazy-initialized top-K iterator
}

impl<I> TopKStream<I>
where I: Iterator
{
    pub fn new(inner: I, order_by: Vec<ankql::ast::OrderByItem>, k: usize) -> Self {
        Self { inner: Some(inner), order_by, k, top_k_items: None }
    }
}

impl<I> Iterator for TopKStream<I>
where
    I: Iterator,
    I::Item: Filterable,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        // Lazy initialization: use bounded heap to find top K efficiently
        if self.top_k_items.is_none()
            && let Some(inner) = self.inner.take()
        {
            let top_k_items = collect_top_k(inner, &self.order_by, self.k);
            self.top_k_items = Some(top_k_items.into_iter());
        }

        // Return next item from top-K collection
        self.top_k_items.as_mut()?.next()
    }
}

// All stream wrappers should implement GetPropertyValueStream for chaining
use crate::filtering::GetPropertyValueStream;

// impl<I> GetPropertyValueStream for SortedStream<I>
// where
//     I: Iterator,
//     I::Item: Filterable,
// {
// }

// impl<I> GetPropertyValueStream for LimitedStream<I>
// where
//     I: Iterator,
//     I::Item: Filterable,
// {
// }

// impl<I> GetPropertyValueStream for TopKStream<I>
// where
//     I: Iterator,
//     I::Item: Filterable,
// {
// }
