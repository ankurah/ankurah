//! Trees the tree's tests share, and the comparison that tells whether two
//! trees hold the same members with the same digests.

use std::ops::Bound;

use super::{DigestTree, DigestTreeView};
use crate::fixtures::{asc, Id, Item, KeySpec, Tag};
use crate::{DigestGroup, Encode, MemoryStore, NodePrefix, Ristretto255, Row, RowKey, ScanDirection, Store};

/// A tree of the fixtures' members, in memory.
pub(super) type Tree<G = Ristretto255> = DigestTree<KeySpec, Id, Tag, G, MemoryStore<RowKey, Row<KeySpec, Tag, G>>>;

/// A view of such a tree.
pub(super) type View<G = Ristretto255> = DigestTreeView<KeySpec, Id, Tag, G, MemoryStore<RowKey, Row<KeySpec, Tag, G>>>;

/// A spec of one ascending integer part.
pub(super) fn integer_spec() -> KeySpec { KeySpec::new(vec![asc!("a")]) }

/// A spec of no parts: members ordered by id alone.
pub(super) fn by_id_spec() -> KeySpec { KeySpec::new(vec![]) }

/// A member under the integer spec, its one value `key`, missing when `None`:
/// `item!(1, Some(5), b"a")`.
macro_rules! item {
    ($id:expr, $key:expr, $payload:expr) => {
        $crate::fixtures::Item::new($id, [$key], $payload)
    };
}

pub(super) use item;

/// A tree in memory holding exactly `members`.
pub(super) async fn built<G: DigestGroup>(key_spec: KeySpec, members: impl IntoIterator<Item = Item>) -> Tree<G> {
    DigestTree::build(MemoryStore::new(), key_spec, members).await.unwrap()
}

impl<G: DigestGroup> Tree<G> {
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
    pub(super) fn store(&self) -> &MemoryStore<RowKey, Row<KeySpec, Tag, G>> { &self.inner.store }
}

/// Whether two trees are the same tree: the same rows, metadata, members and
/// nodes alike, since a trie's shape follows from its members alone, and a
/// clean audit of each.
pub(super) async fn assert_same_tree<G: DigestGroup>(a: &Tree<G>, b: &Tree<G>) {
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
