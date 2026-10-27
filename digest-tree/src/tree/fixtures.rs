//! Trees the tree's tests share, and the comparison that tells whether two
//! trees hold the same members with the same digests.

use std::ops::Bound;

use super::{DigestTree, DigestTreeView};
use crate::fixtures::{key_part, spec, Id, Item, Order, Spec, Tag};
use crate::{DigestGroup, Encode, MemoryStore, NodePrefix, Ristretto255, Row, RowKey, ScanDirection, Span, Store};

/// A tree of the fixtures' members, in memory.
pub(super) type Tree<G = Ristretto255> = DigestTree<Spec, Id, Tag, G, MemoryStore<RowKey, Row<Spec, Tag, G>>>;

/// A view of such a tree.
pub(super) type View<G = Ristretto255> = DigestTreeView<Spec, Id, Tag, G, MemoryStore<RowKey, Row<Spec, Tag, G>>>;

/// A spec of one ascending integer part.
pub(super) fn integer_spec() -> Spec { spec(&[Order::Ascending]) }

/// A spec of no parts: members ordered by id alone.
pub(super) fn by_id_spec() -> Spec { spec(&[]) }

/// The key of a member under the integer spec: `key` missing when `None`.
pub(super) fn integer_key(key: Option<i64>) -> Vec<u8> { key_part(key, Order::Ascending) }

/// A member under the integer spec: `key` missing when `None`.
pub(super) fn item(id: u32, key: Option<i64>, payload: &[u8]) -> Item { Item::new(id, [key], payload) }

/// A tree in memory holding exactly `members`.
pub(super) async fn built<G: DigestGroup>(key_spec: Spec, members: impl IntoIterator<Item = Item>) -> Tree<G> {
    DigestTree::build(MemoryStore::new(), key_spec, members).await.unwrap()
}

impl<G: DigestGroup> Tree<G> {
    /// Every row the tree's store holds, encoded, in key order.
    pub(super) async fn stored_rows(&self) -> Vec<(RowKey, Vec<u8>)> {
        let rows = self.inner.store.range(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, usize::MAX).await.unwrap();
        rows.into_iter().map(|(key, row)| (key, row.encode())).collect()
    }

    /// The rows that say what the tree holds: its metadata and members,
    /// every row but the nodes.
    pub(super) async fn member_rows(&self) -> Vec<(RowKey, Vec<u8>)> {
        self.stored_rows().await.into_iter().filter(|(key, _)| !matches!(key, RowKey::Node(_))).collect()
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
    pub(super) fn store(&self) -> &MemoryStore<RowKey, Row<Spec, Tag, G>> { &self.inner.store }
}

/// Whether two trees hold the same members with the same digests: equal
/// metadata and member rows, an equal digest and count over the span of
/// every node of either trie, and a clean audit of each. Their tries may
/// differ in shape, since a trie's shape depends on the order its members
/// came and went in.
pub(super) async fn assert_same_tree<G: DigestGroup>(a: &Tree<G>, b: &Tree<G>) {
    assert_eq!(a.member_rows().await, b.member_rows().await, "member rows");
    a.audit().await.unwrap();
    b.audit().await.unwrap();

    let (a_view, b_view) = (a.view(), b.view());
    let mut spans: Vec<Span> = a.node_prefixes().await.iter().chain(&b.node_prefixes().await).map(NodePrefix::span).collect();
    spans.push(Span::all());

    for span in spans {
        let (a_digest, b_digest) = (a_view.span_digest(&span).await.unwrap(), b_view.span_digest(&span).await.unwrap());
        assert_eq!(a_digest.digest, b_digest.digest, "{span:?}");
        assert_eq!(a_view.count(&span).await.unwrap(), a_digest.digest.count(), "{span:?}");
    }
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
