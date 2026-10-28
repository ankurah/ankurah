//! What the tree's tests share beyond the members: a tree's stored rows, the
//! comparison that tells whether two trees are the same tree, and a store
//! that pages its reads.

use std::ops::Bound;

use crate::common::{ItemStore, ItemTree};
use crate::{DigestGroup, Encode, MemoryStore, NodePrefix, RowKey, ScanDirection, Store};

impl<G: DigestGroup> ItemTree<G> {
    /// Every row the tree's store holds, encoded, in key order.
    pub(super) async fn stored_rows(&self) -> Vec<(RowKey, Vec<u8>)> {
        let rows = self.inner.store.range(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, usize::MAX).await.unwrap();
        rows.into_iter().map(|(key, row)| (key, row.encode())).collect()
    }

    /// The prefix of every node of the tree's trie.
    pub(super) async fn node_prefixes(&self) -> Vec<NodePrefix> {
        let rows = self.stored_rows().await;
        rows.into_iter()
            .filter_map(|(key, _)| match key {
                RowKey::Node(prefix) => Some(prefix),
                _ => None,
            })
            .collect()
    }

    /// The store, for tests that damage rows.
    pub(super) fn store(&self) -> &ItemStore<G> { &self.inner.store }
}

/// Whether two trees are the same tree: the same rows, metadata, members and
/// nodes alike, since a trie's shape follows from its members alone, and a
/// clean audit of each.
pub(super) async fn assert_same_tree<G: DigestGroup>(a: &ItemTree<G>, b: &ItemTree<G>) {
    assert_eq!(a.stored_rows().await, b.stored_rows().await, "rows");
    a.audit().await.unwrap();
    b.audit().await.unwrap();
}

/// A memory store that returns at most three rows per read, as a store that
/// pages its reads may: the tree must read on.
#[derive(Clone)]
pub(super) struct ShortPages<V>(pub(super) MemoryStore<RowKey, V>);

impl<V: Clone + Send + Sync + 'static> Store<RowKey, V> for ShortPages<V> {
    async fn get(&self, key: &RowKey) -> Result<Option<V>, crate::StoreError> { self.0.get(key).await }

    async fn range(
        &self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, V)>, crate::StoreError> {
        self.0.range(start, end, direction, limit.min(3)).await
    }

    async fn write(&self, items: Vec<crate::WriteItem<RowKey, V>>) -> Result<(), crate::StoreError> { self.0.write(items).await }
}
