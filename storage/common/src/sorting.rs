use ankurah_core::selection::filter::Filterable;
use ankurah_core::value::Value;
use futures::Stream;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::OrderByComponents;

/// Helper function to sort items by ORDER BY clauses
fn sort_items_by_order<T: Filterable>(items: &mut [T], order_by: &[ankql::ast::OrderByItem]) {
    items.sort_by(|a, b| {
        for order_item in order_by {
            let property_name = order_item.path.property();

            let a_val = a.value(property_name);
            let b_val = b.value(property_name);

            // Handle None values: None sorts before Some
            let cmp = match (a_val, b_val, &order_item.direction) {
                (None, None, _) => Ordering::Equal,
                (None, Some(_), _) => Ordering::Less,
                (Some(_), None, _) => Ordering::Greater,
                (Some(a), Some(b), ankql::ast::OrderDirection::Asc) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
                (Some(a), Some(b), ankql::ast::OrderDirection::Desc) => b.partial_cmp(&a).unwrap_or(Ordering::Equal),
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    });
}

/// Extract partition key (presort column values) from an item
fn extract_partition_key<T: Filterable>(item: &T, presort: &[ankql::ast::OrderByItem]) -> Vec<Option<Value>> {
    presort.iter().map(|p| item.value(p.path.property())).collect()
}

/// Sorted stream with partition-aware support.
/// - When presort is empty: global sort by spill columns
/// - When presort is non-empty: partition-aware sort (sort within partitions defined by presort values)
pub struct SortedStream<S>
where S: Stream
{
    inner: Option<S>,
    order_by: OrderByComponents,
    // State for iteration
    current_partition: Vec<S::Item>,
    current_partition_key: Option<Vec<Option<Value>>>,
    sorted_partition: Option<std::vec::IntoIter<S::Item>>,
    exhausted: bool,
}

impl<S: Unpin> Unpin for SortedStream<S> where S: Stream {}

impl<S> SortedStream<S>
where S: Stream
{
    pub fn new(inner: S, order_by: OrderByComponents) -> Self {
        Self {
            inner: Some(inner),
            order_by,
            current_partition: Vec::new(),
            current_partition_key: None,
            sorted_partition: None,
            exhausted: false,
        }
    }
}

impl<S> Stream for SortedStream<S>
where
    S: Stream + Unpin,
    S::Item: Filterable,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // If presort is empty, do global sort (collect all, sort, emit)
        if this.order_by.presort.is_empty() {
            // First try to emit from sorted partition
            if let Some(ref mut sorted) = this.sorted_partition {
                if let Some(item) = sorted.next() {
                    return Poll::Ready(Some(item));
                }
                this.sorted_partition = None;
                return Poll::Ready(None);
            }

            // Collect items from inner until exhausted
            loop {
                let poll_result = {
                    let Some(inner) = this.inner.as_mut() else { break };
                    Pin::new(inner).poll_next(cx)
                };
                match poll_result {
                    Poll::Ready(Some(item)) => {
                        this.current_partition.push(item);
                    }
                    Poll::Ready(None) => {
                        // Inner exhausted - sort and prepare for emission
                        let mut items = std::mem::take(&mut this.current_partition);
                        sort_items_by_order(&mut items, &this.order_by.spill);
                        this.sorted_partition = Some(items.into_iter());
                        this.inner = None;
                        // Emit first item from sorted partition
                        return this.sorted_partition.as_mut().map_or(Poll::Ready(None), |iter| Poll::Ready(iter.next()));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
            return Poll::Ready(None);
        }

        // Partition-aware sorting
        loop {
            // First, try to emit from current sorted partition
            if let Some(ref mut sorted_iter) = this.sorted_partition {
                if let Some(item) = sorted_iter.next() {
                    return Poll::Ready(Some(item));
                }
                this.sorted_partition = None;
            }

            if this.exhausted {
                return Poll::Ready(None);
            }

            // Get next item from inner stream (scope borrow to allow other field access)
            let poll_result = {
                let Some(inner) = this.inner.as_mut() else {
                    return Poll::Ready(None);
                };
                Pin::new(inner).poll_next(cx)
            };

            match poll_result {
                Poll::Ready(Some(item)) => {
                    let item_key = extract_partition_key(&item, &this.order_by.presort);

                    match &this.current_partition_key {
                        None => {
                            // First item - start new partition
                            this.current_partition_key = Some(item_key);
                            this.current_partition.push(item);
                        }
                        Some(current_key) if *current_key == item_key => {
                            // Same partition - accumulate
                            this.current_partition.push(item);
                        }
                        Some(_) => {
                            // Partition changed - sort and emit previous partition
                            let mut partition = std::mem::take(&mut this.current_partition);
                            sort_items_by_order(&mut partition, &this.order_by.spill);
                            this.sorted_partition = Some(partition.into_iter());

                            // Start new partition with current item
                            this.current_partition_key = Some(item_key);
                            this.current_partition.push(item);
                        }
                    }
                }
                Poll::Ready(None) => {
                    // Inner exhausted - sort and emit final partition
                    this.exhausted = true;
                    this.inner = None;

                    if !this.current_partition.is_empty() {
                        let mut partition = std::mem::take(&mut this.current_partition);
                        sort_items_by_order(&mut partition, &this.order_by.spill);
                        this.sorted_partition = Some(partition.into_iter());
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
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

impl<S: Unpin> Unpin for LimitedStream<S> {}

impl<S> Stream for LimitedStream<S>
where S: Stream + Unpin
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Check if we've reached the limit
        if let Some(limit) = self.limit
            && self.count >= limit
        {
            return Poll::Ready(None);
        }

        // Get next item and increment counter
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(item)) => {
                self.count += 1;
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
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
        // TopK algorithm: keep K items that would appear first in the final sorted output.
        // We use a bounded heap and replace the "worst" candidate when a "better" one arrives.
        //
        // For ASC order (smallest K): use max-heap (largest candidate at top).
        //   - When new item < top, kick out top (it's worse than new).
        //   - Normal comparison: larger values are "greater".
        //
        // For DESC order (largest K): use min-heap (smallest candidate at top).
        //   - When new item > top, kick out top (it's worse than new).
        //   - Reversed comparison: smaller values are "greater" so BinaryHeap puts them at top.
        for order_item in &self.order_by {
            let property_name = order_item.path.property();

            let self_val = self.item.value(property_name);
            let other_val = other.item.value(property_name);

            // Handle None values: None is "worst" for the heap (will be kicked out first)
            // For ASC: None is smallest, so it should be "least" (kept longer in max-heap)
            // For DESC: None is smallest, so it should be "greatest" (at top of min-heap, kicked first)
            let cmp = match (self_val, other_val, &order_item.direction) {
                (None, None, _) => Ordering::Equal,
                (None, Some(_), ankql::ast::OrderDirection::Asc) => Ordering::Less, // None < Some for ASC max-heap
                (Some(_), None, ankql::ast::OrderDirection::Asc) => Ordering::Greater, // Some > None for ASC max-heap
                (None, Some(_), ankql::ast::OrderDirection::Desc) => Ordering::Greater, // None at top for DESC min-heap
                (Some(_), None, ankql::ast::OrderDirection::Desc) => Ordering::Less, // Some below None for DESC min-heap
                (Some(s), Some(o), ankql::ast::OrderDirection::Asc) => s.partial_cmp(&o).unwrap_or(Ordering::Equal), // Normal for max-heap
                (Some(s), Some(o), ankql::ast::OrderDirection::Desc) => o.partial_cmp(&s).unwrap_or(Ordering::Equal), // Reversed for min-heap
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }
}

/// TopK stream with partition-aware support.
/// - When presort is empty: global TopK by spill columns
/// - When presort is non-empty: partition-aware TopK (can stop early once K items emitted)
pub struct TopKStream<S>
where S: Stream
{
    inner: Option<S>,
    order_by: OrderByComponents,
    k: usize,
    emitted_count: usize,
    // State for iteration
    current_partition: Vec<S::Item>,
    current_partition_key: Option<Vec<Option<Value>>>,
    sorted_partition: Option<std::vec::IntoIter<S::Item>>,
    exhausted: bool,
}

impl<S: Unpin> Unpin for TopKStream<S> where S: Stream {}

impl<S> TopKStream<S>
where S: Stream
{
    pub fn new(inner: S, order_by: OrderByComponents, k: usize) -> Self {
        Self {
            inner: Some(inner),
            order_by,
            k,
            emitted_count: 0,
            current_partition: Vec::new(),
            current_partition_key: None,
            sorted_partition: None,
            exhausted: false,
        }
    }
}

impl<S> Stream for TopKStream<S>
where
    S: Stream + Unpin,
    S::Item: Filterable,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.emitted_count >= this.k {
            return Poll::Ready(None);
        }

        // If presort is empty, do global TopK (collect all, top-k, emit)
        if this.order_by.presort.is_empty() {
            // First try to emit from sorted partition
            if let Some(ref mut sorted) = this.sorted_partition {
                if let Some(item) = sorted.next() {
                    this.emitted_count += 1;
                    return Poll::Ready(Some(item));
                }
                this.sorted_partition = None;
                return Poll::Ready(None);
            }

            // Collect items from inner using top-k heap
            let mut heap: BinaryHeap<HeapItem<S::Item>> = BinaryHeap::new();
            loop {
                let poll_result = {
                    let Some(inner) = this.inner.as_mut() else { break };
                    Pin::new(inner).poll_next(cx)
                };
                match poll_result {
                    Poll::Ready(Some(item)) => {
                        let heap_item = HeapItem { item, order_by: this.order_by.spill.clone() };
                        if heap.len() < this.k {
                            heap.push(heap_item);
                        } else if let Some(worst) = heap.peek() {
                            if heap_item < *worst {
                                heap.pop();
                                heap.push(heap_item);
                            }
                        }
                    }
                    Poll::Ready(None) => {
                        // Inner exhausted - sort and prepare for emission
                        this.inner = None;
                        let mut top_k: Vec<_> = heap.into_iter().map(|h| h.item).collect();
                        sort_items_by_order(&mut top_k, &this.order_by.spill);
                        this.sorted_partition = Some(top_k.into_iter());
                        // Emit first item
                        return this.sorted_partition.as_mut().map_or(Poll::Ready(None), |iter| {
                            if let Some(item) = iter.next() {
                                this.emitted_count += 1;
                                Poll::Ready(Some(item))
                            } else {
                                Poll::Ready(None)
                            }
                        });
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
            return Poll::Ready(None);
        }

        // Partition-aware TopK
        loop {
            if this.emitted_count >= this.k {
                return Poll::Ready(None);
            }

            // First, try to emit from current sorted partition
            if let Some(ref mut sorted_iter) = this.sorted_partition {
                if let Some(item) = sorted_iter.next() {
                    this.emitted_count += 1;
                    return Poll::Ready(Some(item));
                }
                this.sorted_partition = None;
            }

            if this.exhausted {
                return Poll::Ready(None);
            }

            // Get next item from inner stream (scope borrow)
            let poll_result = {
                let Some(inner) = this.inner.as_mut() else {
                    return Poll::Ready(None);
                };
                Pin::new(inner).poll_next(cx)
            };

            match poll_result {
                Poll::Ready(Some(item)) => {
                    let item_key = extract_partition_key(&item, &this.order_by.presort);

                    match &this.current_partition_key {
                        None => {
                            // First item - start new partition
                            this.current_partition_key = Some(item_key);
                            this.current_partition.push(item);
                        }
                        Some(current_key) if *current_key == item_key => {
                            // Same partition - accumulate
                            this.current_partition.push(item);
                        }
                        Some(_) => {
                            // Partition changed - sort and emit previous partition
                            let mut partition = std::mem::take(&mut this.current_partition);
                            sort_items_by_order(&mut partition, &this.order_by.spill);
                            this.sorted_partition = Some(partition.into_iter());

                            // Start new partition with current item
                            this.current_partition_key = Some(item_key);
                            this.current_partition.push(item);
                        }
                    }
                }
                Poll::Ready(None) => {
                    // Inner exhausted - sort and emit final partition
                    this.exhausted = true;
                    this.inner = None;

                    if !this.current_partition.is_empty() {
                        let mut partition = std::mem::take(&mut this.current_partition);
                        sort_items_by_order(&mut partition, &this.order_by.spill);
                        this.sorted_partition = Some(partition.into_iter());
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::ast::{OrderByItem, OrderDirection, PathExpr};
    use futures::StreamExt;
    use std::collections::HashMap;

    /// Helper to collect stream items synchronously for testing
    fn collect_stream<S: Stream + Unpin>(stream: S) -> Vec<S::Item> { futures::executor::block_on(stream.collect()) }

    /// Helper to wrap items in a stream for testing
    fn stream_from<T>(items: Vec<T>) -> futures::stream::Iter<std::vec::IntoIter<T>> { futures::stream::iter(items) }

    /// Test item that implements Filterable for unit testing
    #[derive(Debug, Clone, PartialEq)]
    struct TestItem {
        values: HashMap<String, Value>,
    }

    impl TestItem {
        fn new(pairs: &[(&str, Value)]) -> Self {
            let mut values = HashMap::new();
            for (k, v) in pairs {
                values.insert(k.to_string(), v.clone());
            }
            Self { values }
        }

        fn int(pairs: &[(&str, i32)]) -> Self {
            let mut values = HashMap::new();
            for (k, v) in pairs {
                values.insert(k.to_string(), Value::I32(*v));
            }
            Self { values }
        }

        fn str(pairs: &[(&str, &str)]) -> Self {
            let mut values = HashMap::new();
            for (k, v) in pairs {
                values.insert(k.to_string(), Value::String(v.to_string()));
            }
            Self { values }
        }

        fn mixed(cat: &str, name: &str) -> Self {
            let mut values = HashMap::new();
            values.insert("cat".to_string(), Value::String(cat.to_string()));
            values.insert("name".to_string(), Value::String(name.to_string()));
            Self { values }
        }

        fn cat_val(cat: &str, val: i32) -> Self {
            let mut values = HashMap::new();
            values.insert("cat".to_string(), Value::String(cat.to_string()));
            values.insert("val".to_string(), Value::I32(val));
            Self { values }
        }

        fn cat_subcat_val(cat: &str, subcat: &str, val: i32) -> Self {
            let mut values = HashMap::new();
            values.insert("cat".to_string(), Value::String(cat.to_string()));
            values.insert("subcat".to_string(), Value::String(subcat.to_string()));
            values.insert("val".to_string(), Value::I32(val));
            Self { values }
        }
    }

    impl Filterable for TestItem {
        fn collection(&self) -> &str { "test" }

        fn value(&self, property: &str) -> Option<Value> { self.values.get(property).cloned() }
    }

    /// Helper to extract i32 from Value
    fn extract_i32(v: &Value) -> i32 {
        match v {
            Value::I32(n) => *n,
            _ => panic!("expected I32"),
        }
    }

    /// Helper to extract String from Value
    fn extract_string(v: &Value) -> String {
        match v {
            Value::String(s) => s.clone(),
            _ => panic!("expected String"),
        }
    }

    fn oby(col: &str, dir: OrderDirection) -> OrderByItem { OrderByItem { path: PathExpr::simple(col), direction: dir } }

    fn oby_asc(col: &str) -> OrderByItem { oby(col, OrderDirection::Asc) }

    fn oby_desc(col: &str) -> OrderByItem { oby(col, OrderDirection::Desc) }

    // ============================================================================
    // LimitedStream Tests
    // ============================================================================

    #[test]
    fn test_limited_stream_basic() {
        let items = vec![1, 2, 3, 4, 5];
        let limited: Vec<_> = collect_stream(LimitedStream::new(stream_from(items), Some(3)));
        assert_eq!(limited, vec![1, 2, 3]);
    }

    #[test]
    fn test_limited_stream_no_limit() {
        let items = vec![1, 2, 3, 4, 5];
        let limited: Vec<_> = collect_stream(LimitedStream::new(stream_from(items), None));
        assert_eq!(limited, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_limited_stream_limit_exceeds_items() {
        let items = vec![1, 2, 3];
        let limited: Vec<_> = collect_stream(LimitedStream::new(stream_from(items), Some(10)));
        assert_eq!(limited, vec![1, 2, 3]);
    }

    #[test]
    fn test_limited_stream_zero_limit() {
        let items = vec![1, 2, 3];
        let limited: Vec<_> = collect_stream(LimitedStream::new(stream_from(items), Some(0)));
        assert!(limited.is_empty());
    }

    #[test]
    fn test_limited_stream_empty_input() {
        let items: Vec<i32> = vec![];
        let limited: Vec<_> = collect_stream(LimitedStream::new(stream_from(items), Some(5)));
        assert!(limited.is_empty());
    }

    // ============================================================================
    // SortedStream Tests - Global Sort (empty presort)
    // ============================================================================

    #[test]
    fn test_sorted_stream_global_sort_asc() {
        let items = vec![TestItem::int(&[("x", 3)]), TestItem::int(&[("x", 1)]), TestItem::int(&[("x", 2)])];

        let order_by = OrderByComponents::new(vec![], vec![oby_asc("x")]);
        let sorted: Vec<_> = collect_stream(SortedStream::new(stream_from(items), order_by));

        let values: Vec<i32> = sorted.iter().map(|i| extract_i32(&i.value("x").unwrap())).collect();
        assert_eq!(values, vec![1, 2, 3]);
    }

    #[test]
    fn test_sorted_stream_global_sort_desc() {
        let items = vec![TestItem::int(&[("x", 1)]), TestItem::int(&[("x", 3)]), TestItem::int(&[("x", 2)])];

        let order_by = OrderByComponents::new(vec![], vec![oby_desc("x")]);
        let sorted: Vec<_> = collect_stream(SortedStream::new(stream_from(items), order_by));

        let values: Vec<i32> = sorted.iter().map(|i| extract_i32(&i.value("x").unwrap())).collect();
        assert_eq!(values, vec![3, 2, 1]);
    }

    #[test]
    fn test_sorted_stream_global_sort_multi_column() {
        let items = vec![TestItem::mixed("B", "Z"), TestItem::mixed("A", "Y"), TestItem::mixed("A", "X"), TestItem::mixed("B", "W")];

        let order_by = OrderByComponents::new(vec![], vec![oby_asc("cat"), oby_asc("name")]);
        let sorted: Vec<_> = collect_stream(SortedStream::new(stream_from(items), order_by));

        let names: Vec<String> = sorted.iter().map(|i| extract_string(&i.value("name").unwrap())).collect();
        assert_eq!(names, vec!["X", "Y", "W", "Z"]); // A-X, A-Y, B-W, B-Z
    }

    #[test]
    fn test_sorted_stream_empty_input() {
        let items: Vec<TestItem> = vec![];
        let order_by = OrderByComponents::new(vec![], vec![oby_asc("x")]);
        let sorted: Vec<_> = collect_stream(SortedStream::new(stream_from(items), order_by));
        assert!(sorted.is_empty());
    }

    #[test]
    fn test_sorted_stream_single_item() {
        let items = vec![TestItem::int(&[("x", 42)])];
        let order_by = OrderByComponents::new(vec![], vec![oby_asc("x")]);
        let sorted: Vec<_> = collect_stream(SortedStream::new(stream_from(items), order_by));
        assert_eq!(sorted.len(), 1);
    }

    // ============================================================================
    // SortedStream Tests - Partition-Aware Sort (non-empty presort)
    // ============================================================================

    #[test]
    fn test_sorted_stream_partition_aware_basic() {
        // Input is PRE-SORTED by presort column (category)
        let items = vec![TestItem::mixed("A", "Z"), TestItem::mixed("A", "X"), TestItem::mixed("B", "Y"), TestItem::mixed("B", "W")];

        // presort: cat ASC (already sorted), spill: name ASC
        let order_by = OrderByComponents::new(vec![oby_asc("cat")], vec![oby_asc("name")]);
        let sorted: Vec<_> = collect_stream(SortedStream::new(stream_from(items), order_by));

        let names: Vec<String> = sorted.iter().map(|i| extract_string(&i.value("name").unwrap())).collect();
        // Within A: X, Z (sorted)
        // Within B: W, Y (sorted)
        assert_eq!(names, vec!["X", "Z", "W", "Y"]);
    }

    #[test]
    fn test_sorted_stream_partition_aware_mixed_directions() {
        // Input PRE-SORTED by category ASC
        let items = vec![TestItem::mixed("A", "X"), TestItem::mixed("A", "Z"), TestItem::mixed("B", "W"), TestItem::mixed("B", "Y")];

        // presort: cat ASC, spill: name DESC
        let order_by = OrderByComponents::new(vec![oby_asc("cat")], vec![oby_desc("name")]);
        let sorted: Vec<_> = collect_stream(SortedStream::new(stream_from(items), order_by));

        let names: Vec<String> = sorted.iter().map(|i| extract_string(&i.value("name").unwrap())).collect();
        // Within A: Z, X (desc)
        // Within B: Y, W (desc)
        assert_eq!(names, vec!["Z", "X", "Y", "W"]);
    }

    #[test]
    fn test_sorted_stream_partition_aware_single_partition() {
        // All items in same partition
        let items = vec![TestItem::mixed("A", "Z"), TestItem::mixed("A", "X"), TestItem::mixed("A", "Y")];

        let order_by = OrderByComponents::new(vec![oby_asc("cat")], vec![oby_asc("name")]);
        let sorted: Vec<_> = collect_stream(SortedStream::new(stream_from(items), order_by));

        let names: Vec<String> = sorted.iter().map(|i| extract_string(&i.value("name").unwrap())).collect();
        assert_eq!(names, vec!["X", "Y", "Z"]);
    }

    #[test]
    fn test_sorted_stream_partition_aware_single_item_partitions() {
        // Single item per partition
        let items = vec![TestItem::mixed("A", "X"), TestItem::mixed("B", "Y"), TestItem::mixed("C", "Z")];

        let order_by = OrderByComponents::new(vec![oby_asc("cat")], vec![oby_asc("name")]);
        let sorted: Vec<_> = collect_stream(SortedStream::new(stream_from(items), order_by));

        let names: Vec<String> = sorted.iter().map(|i| extract_string(&i.value("name").unwrap())).collect();
        assert_eq!(names, vec!["X", "Y", "Z"]);
    }

    #[test]
    fn test_sorted_stream_partition_aware_empty_spill() {
        // When spill is empty but presort is non-empty, just pass through
        let items = vec![TestItem::mixed("A", "X"), TestItem::mixed("A", "Z"), TestItem::mixed("B", "Y")];

        // presort non-empty, spill empty
        let order_by = OrderByComponents::new(vec![oby_asc("cat")], vec![]);
        let sorted: Vec<_> = collect_stream(SortedStream::new(stream_from(items), order_by));

        // Items should maintain their order within partitions (no sorting needed)
        let names: Vec<String> = sorted.iter().map(|i| extract_string(&i.value("name").unwrap())).collect();
        assert_eq!(names, vec!["X", "Z", "Y"]);
    }

    // ============================================================================
    // TopKStream Tests - Global TopK (empty presort)
    // ============================================================================

    #[test]
    fn test_topk_stream_global_basic() {
        let items = vec![
            TestItem::int(&[("x", 5)]),
            TestItem::int(&[("x", 1)]),
            TestItem::int(&[("x", 3)]),
            TestItem::int(&[("x", 4)]),
            TestItem::int(&[("x", 2)]),
        ];

        let order_by = OrderByComponents::new(vec![], vec![oby_asc("x")]);
        let topk: Vec<_> = collect_stream(TopKStream::new(stream_from(items), order_by, 3));

        let values: Vec<i32> = topk.iter().map(|i| extract_i32(&i.value("x").unwrap())).collect();
        assert_eq!(values, vec![1, 2, 3]); // Top 3 smallest
    }

    #[test]
    fn test_topk_stream_global_desc() {
        let items = vec![
            TestItem::int(&[("x", 5)]),
            TestItem::int(&[("x", 1)]),
            TestItem::int(&[("x", 3)]),
            TestItem::int(&[("x", 4)]),
            TestItem::int(&[("x", 2)]),
        ];

        let order_by = OrderByComponents::new(vec![], vec![oby_desc("x")]);
        let topk: Vec<_> = collect_stream(TopKStream::new(stream_from(items), order_by, 3));

        let values: Vec<i32> = topk.iter().map(|i| extract_i32(&i.value("x").unwrap())).collect();
        assert_eq!(values, vec![5, 4, 3]); // Top 3 largest
    }

    #[test]
    fn test_topk_stream_global_k_exceeds_items() {
        let items = vec![TestItem::int(&[("x", 3)]), TestItem::int(&[("x", 1)])];

        let order_by = OrderByComponents::new(vec![], vec![oby_asc("x")]);
        let topk: Vec<_> = collect_stream(TopKStream::new(stream_from(items), order_by, 10));

        let values: Vec<i32> = topk.iter().map(|i| extract_i32(&i.value("x").unwrap())).collect();
        assert_eq!(values, vec![1, 3]);
    }

    #[test]
    fn test_topk_stream_global_k_zero() {
        let items = vec![TestItem::int(&[("x", 1)]), TestItem::int(&[("x", 2)])];

        let order_by = OrderByComponents::new(vec![], vec![oby_asc("x")]);
        let topk: Vec<_> = collect_stream(TopKStream::new(stream_from(items), order_by, 0));

        assert!(topk.is_empty());
    }

    #[test]
    fn test_topk_stream_global_empty_input() {
        let items: Vec<TestItem> = vec![];
        let order_by = OrderByComponents::new(vec![], vec![oby_asc("x")]);
        let topk: Vec<_> = collect_stream(TopKStream::new(stream_from(items), order_by, 5));
        assert!(topk.is_empty());
    }

    // ============================================================================
    // TopKStream Tests - Partition-Aware TopK (non-empty presort)
    // ============================================================================

    #[test]
    fn test_topk_stream_partition_aware_basic() {
        // Input PRE-SORTED by category
        let items = vec![
            TestItem::cat_val("A", 3),
            TestItem::cat_val("A", 1),
            TestItem::cat_val("A", 2),
            TestItem::cat_val("B", 6),
            TestItem::cat_val("B", 4),
            TestItem::cat_val("B", 5),
        ];

        // presort: cat, spill: val ASC, LIMIT 4
        let order_by = OrderByComponents::new(vec![oby_asc("cat")], vec![oby_asc("val")]);
        let topk: Vec<_> = collect_stream(TopKStream::new(stream_from(items), order_by, 4));

        // Should get A's sorted: 1, 2, 3, then B's sorted: 4
        let values: Vec<i32> = topk.iter().map(|i| extract_i32(&i.value("val").unwrap())).collect();
        assert_eq!(values, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_topk_stream_partition_aware_limit_within_partition() {
        // Input PRE-SORTED by category - A has 5 items, we only want 3
        let items = vec![
            TestItem::cat_val("A", 5),
            TestItem::cat_val("A", 1),
            TestItem::cat_val("A", 3),
            TestItem::cat_val("A", 2),
            TestItem::cat_val("A", 4),
            TestItem::cat_val("B", 10),
        ];

        let order_by = OrderByComponents::new(vec![oby_asc("cat")], vec![oby_asc("val")]);
        let topk: Vec<_> = collect_stream(TopKStream::new(stream_from(items), order_by, 3));

        // Should get A's smallest 3: 1, 2, 3
        let values: Vec<i32> = topk.iter().map(|i| extract_i32(&i.value("val").unwrap())).collect();
        assert_eq!(values, vec![1, 2, 3]);
    }

    #[test]
    fn test_topk_stream_partition_aware_mixed_directions() {
        // Input PRE-SORTED by category ASC
        let items = vec![
            TestItem::cat_val("A", 1),
            TestItem::cat_val("A", 3),
            TestItem::cat_val("A", 2),
            TestItem::cat_val("B", 4),
            TestItem::cat_val("B", 6),
        ];

        // presort: cat ASC, spill: val DESC
        let order_by = OrderByComponents::new(vec![oby_asc("cat")], vec![oby_desc("val")]);
        let topk: Vec<_> = collect_stream(TopKStream::new(stream_from(items), order_by, 4));

        // A sorted desc: 3, 2, 1 - B sorted desc: 6
        let values: Vec<i32> = topk.iter().map(|i| extract_i32(&i.value("val").unwrap())).collect();
        assert_eq!(values, vec![3, 2, 1, 6]);
    }

    // ============================================================================
    // NULL Handling Tests
    // ============================================================================

    #[test]
    fn test_sorted_stream_null_sorts_first_asc() {
        let items = vec![
            TestItem::int(&[("x", 2)]),
            TestItem::new(&[]), // x is NULL
            TestItem::int(&[("x", 1)]),
        ];

        let order_by = OrderByComponents::new(vec![], vec![oby_asc("x")]);
        let sorted: Vec<_> = collect_stream(SortedStream::new(stream_from(items), order_by));

        // NULL sorts first, then 1, 2
        let values: Vec<Option<i32>> = sorted.iter().map(|i| i.value("x").map(|v| extract_i32(&v))).collect();
        assert_eq!(values, vec![None, Some(1), Some(2)]);
    }

    #[test]
    fn test_sorted_stream_null_sorts_first_desc() {
        // Note: Current implementation has NULLs sort first regardless of direction
        let items = vec![
            TestItem::int(&[("x", 2)]),
            TestItem::new(&[]), // x is NULL
            TestItem::int(&[("x", 1)]),
        ];

        let order_by = OrderByComponents::new(vec![], vec![oby_desc("x")]);
        let sorted: Vec<_> = collect_stream(SortedStream::new(stream_from(items), order_by));

        // NULL sorts first even with DESC, then 2, 1
        let values: Vec<Option<i32>> = sorted.iter().map(|i| i.value("x").map(|v| extract_i32(&v))).collect();
        assert_eq!(values, vec![None, Some(2), Some(1)]);
    }

    #[test]
    fn test_sorted_stream_all_nulls() {
        let items = vec![TestItem::new(&[]), TestItem::new(&[]), TestItem::new(&[])];

        let order_by = OrderByComponents::new(vec![], vec![oby_asc("x")]);
        let sorted: Vec<_> = collect_stream(SortedStream::new(stream_from(items), order_by));

        assert_eq!(sorted.len(), 3);
    }

    // ============================================================================
    // Multi-Column Presort Tests
    // ============================================================================

    #[test]
    fn test_sorted_stream_multi_column_presort() {
        // Input PRE-SORTED by (cat, subcat)
        let items = vec![
            TestItem::cat_subcat_val("A", "X", 3),
            TestItem::cat_subcat_val("A", "X", 1),
            TestItem::cat_subcat_val("A", "Y", 5),
            TestItem::cat_subcat_val("A", "Y", 4),
            TestItem::cat_subcat_val("B", "X", 7),
            TestItem::cat_subcat_val("B", "X", 6),
        ];

        // presort: cat, subcat; spill: val ASC
        let order_by = OrderByComponents::new(vec![oby_asc("cat"), oby_asc("subcat")], vec![oby_asc("val")]);
        let sorted: Vec<_> = collect_stream(SortedStream::new(stream_from(items), order_by));

        // A-X: 1, 3
        // A-Y: 4, 5
        // B-X: 6, 7
        let values: Vec<i32> = sorted.iter().map(|i| extract_i32(&i.value("val").unwrap())).collect();
        assert_eq!(values, vec![1, 3, 4, 5, 6, 7]);
    }
}
