//! The digest tree: its spec and the memory-resident maps that hold its rows,
//! and the transactions that carry its operations.
//!
//! Every write is conditional on what its transaction read, so two changes to
//! one leaf, or to one tree node's base row, never overwrite each other: the
//! second meets [`TreeError::Conflict`], which fails its transaction whole, and
//! retries from fresh reads.
//!
//! A tree node's base row is written when the tree is built. Every later
//! change to the tree node is a delta row appended beside it, under a key of
//! its own, which readers sum, so no change rewrites a hot tree node's row and
//! changes do not serialize on the root; compaction folds the deltas into the
//! base, a bounded batch per transaction.

use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::ops::Bound;

use ankurah_core_types::EntityId;

use crate::digest::{Digest, DigestGroup, Ristretto255};
use crate::error::TreeError;
use crate::layout;
use crate::map::{MapAccess, MapName, MapTransaction};
use crate::memory::{MemoryMaps, MemoryTransaction};
use crate::prefix::NodePrefix;
use crate::rows::{leaf_address, LeafRow, NodeRow, TreeSpan};
use crate::spec::TreeSpec;

// Its bytes can only be made from the row they accompany.
pub(crate) struct PreparedLeaf<G: DigestGroup> {
    row: LeafRow<G>,
    value: Vec<u8>,
}

impl<G: DigestGroup> PreparedLeaf<G> {
    pub(crate) fn new(row: LeafRow<G>) -> Self {
        let value = layout::leaf_value(&row);
        Self { row, value }
    }

    pub(crate) fn into_row(self) -> LeafRow<G> { self.row }
}

/// A digest tree: the spec that defines which leaves it admits and the order
/// it keeps them in, and the memory-resident maps that hold its rows, with its
/// leaves summed in the group `G`, ristretto255 unless the tree names another.
///
/// Its operations run in transactions: [`DigestTree::begin`] gives one.
#[derive(Debug)]
pub struct DigestTree<G: DigestGroup = Ristretto255> {
    spec: TreeSpec,
    maps: MemoryMaps,
    group: PhantomData<G>,
}

impl<G: DigestGroup> DigestTree<G> {
    /// An empty tree defined by `spec`.
    pub fn new(spec: TreeSpec) -> Self { Self { spec, maps: MemoryMaps::new(), group: PhantomData } }

    /// The spec that defines the tree.
    pub fn spec(&self) -> &TreeSpec { &self.spec }

    /// Begin a transaction over the tree's maps as they stand now, with the
    /// tree's operations.
    pub fn begin(&self) -> TreeTransaction<G> { TreeTransaction::bind(&self.spec, self.maps.begin()) }
}

/// One transaction over a tree's maps, with the tree's operations, its leaves
/// summed in the group `G`.
///
/// Every operation runs within the transaction: its reads observe the state
/// the transaction began with and the transaction's own earlier writes, and
/// its writes land, all together, only when the owning transaction commits.
/// [`TreeTransaction::commit`] is available when this view owns its maps.
/// A borrowed view leaves commit to its outer owner.
/// A [`TreeError::Conflict`] from any operation or from the commit
/// fails the transaction whole, and the caller retries from fresh reads in a
/// new one. The operations below are the only writers of the maps' contents;
/// the layouts of the keys and values are in [`layout`].
#[derive(Debug)]
pub struct TreeTransaction<G: DigestGroup = Ristretto255, T = MemoryTransaction> {
    pub(crate) transaction: T,
    group: PhantomData<G>,
    pub(crate) spec: TreeSpec,
    pub(crate) failed: bool,
    pub(crate) generation: u64,
    pub(crate) snapshot: std::sync::Arc<()>,
}

impl<G: DigestGroup, T: MapTransaction> TreeTransaction<G, T> {
    /// Commit the owned maps together, if every condition still holds.
    /// Failure publishes nothing. Borrowed map views have no commit method.
    pub async fn commit(self) -> Result<(), TreeError> {
        if self.failed {
            return Err(TreeError::Conflict);
        }
        self.transaction.commit().await
    }
}

/// Keeps partial work from escaping through an outer transaction's commit,
/// including when a mutation future is dropped before returning a result.
pub(crate) struct Mutation<'a, G: DigestGroup, T: MapAccess> {
    tree: &'a mut TreeTransaction<G, T>,
    completed: bool,
}

impl<G: DigestGroup, T: MapAccess> Mutation<'_, G, T> {
    pub(crate) fn finish<R>(mut self, result: Result<R, TreeError>) -> Result<R, TreeError> {
        self.completed = result.is_ok();
        result
    }
}

impl<G: DigestGroup, T: MapAccess> std::ops::Deref for Mutation<'_, G, T> {
    type Target = TreeTransaction<G, T>;
    fn deref(&self) -> &Self::Target { self.tree }
}

impl<G: DigestGroup, T: MapAccess> std::ops::DerefMut for Mutation<'_, G, T> {
    fn deref_mut(&mut self) -> &mut Self::Target { self.tree }
}

impl<G: DigestGroup, T: MapAccess> Drop for Mutation<'_, G, T> {
    fn drop(&mut self) {
        if !self.completed {
            self.tree.failed = true;
            self.tree.transaction.poison();
        }
    }
}

/// How many entries a read of a whole range takes at a time.
const PAGE: usize = 1024;

/// How many deltas one compaction folds at most, so that the rows it reads and
/// the writes it buffers stay bounded however many deltas a tree node gathers.
pub const COMPACTION_BATCH: usize = 1024;

/// A read of a tree node's delta rows a page at a time, in key order, so that
/// no read holds them all at once: where the next page starts.
struct DeltaPages {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
    exhausted: bool,
}

impl DeltaPages {
    fn of(prefix: &NodePrefix) -> Self {
        let (first, past) = layout::delta_key_bounds(prefix);
        Self { start: Bound::Included(first), end: past.map_or(Bound::Unbounded, Bound::Excluded), exhausted: false }
    }

    /// The next page, empty once every delta has been read.
    async fn next<T: MapAccess>(&mut self, transaction: &mut T) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError> {
        if self.exhausted {
            return Ok(Vec::new());
        }
        let bounds = (self.start.as_ref().map(Vec::as_slice), self.end.as_ref().map(Vec::as_slice));
        let page = transaction.scan(MapName::Nodes, bounds, PAGE).await?;
        match page.last() {
            Some((last, _)) if page.len() == PAGE => self.start = Bound::Excluded(last.clone()),
            _ => self.exhausted = true,
        }
        Ok(page)
    }
}

/// A digest row as read: its key, its bytes, which a conditional write
/// expects to find unchanged, and the digest they hold.
struct DigestRow<G: DigestGroup> {
    value: Vec<u8>,
    digest: Digest<G>,
}

impl<G: DigestGroup, T: MapAccess> TreeTransaction<G, T> {
    /// Use these transaction-bound maps as the tree described by `spec`.
    /// The caller supplies this tree's maps and matching spec. Admission is
    /// still the caller's responsibility; the tree validates canonical keys.
    /// Passing a borrowed adapter leaves commit with the outer transaction.
    /// Scopes and cursors belong to this view and must be resolved again when
    /// a new view is opened, even within the same outer transaction.
    ///
    /// ```
    /// # async fn example() -> Result<(), ankurah_digest_tree::TreeError> {
    /// use ankurah_digest_tree::{MapTransaction, MemoryMaps, Ristretto255, TreeSpec, TreeTransaction};
    /// use ankurah_core_types::KeySpec;
    /// let spec = TreeSpec::new(KeySpec::new(vec![]), ankql::ast::Predicate::True);
    /// let maps = MemoryMaps::new();
    /// let mut owner = maps.begin();
    /// {
    ///     let mut tree = TreeTransaction::<Ristretto255, _>::bind(&spec, &mut owner);
    ///     tree.build([]).await?;
    /// }
    /// owner.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Borrowing maps does not grant permission to commit them:
    ///
    /// ```compile_fail
    /// # async fn example(spec: &ankurah_digest_tree::TreeSpec) {
    /// use ankurah_digest_tree::{MemoryMaps, Ristretto255, TreeTransaction};
    /// let maps = MemoryMaps::new();
    /// let mut owner = maps.begin();
    /// let tree = TreeTransaction::<Ristretto255, _>::bind(spec, &mut owner);
    /// tree.commit().await.unwrap();
    /// # }
    /// ```
    pub fn bind(spec: &TreeSpec, maps: T) -> Self {
        Self { transaction: maps, group: PhantomData, spec: spec.clone(), failed: false, generation: 0, snapshot: std::sync::Arc::new(()) }
    }

    pub(crate) fn mutation(&mut self) -> Mutation<'_, G, T> { Mutation { tree: self, completed: false } }

    /// The leaf row of the entity `entity_id` filed under `key`.
    pub async fn leaf(&mut self, key: &[u8], entity_id: EntityId) -> Result<Option<LeafRow<G>>, TreeError> {
        let address = leaf_address(key, entity_id);
        self.transaction.get(MapName::Leaves, &address).await?.map(|value| layout::leaf_row(&address, &value)).transpose()
    }

    /// Every leaf row of an entity, one per key it is filed under, in address
    /// order: how a change to the entity finds the leaves it must subtract or
    /// move, since the change names only the entity. A missing leaf named by
    /// the lookup is an error: omitting it would lose the old contribution
    /// that a change must subtract.
    pub async fn entity_leaves(&mut self, entity_id: EntityId) -> Result<Vec<LeafRow<G>>, TreeError> {
        let (_, keys) = self.keys_of(entity_id).await?;
        let mut rows = Vec::new();
        for key in keys {
            let row = self.leaf(&key, entity_id).await?.ok_or(TreeError::MissingLeaf { entity_id, key })?;
            rows.push(row);
        }
        // Key order is not address order where one key is a prefix of another.
        rows.sort_by_cached_key(LeafRow::address);
        Ok(rows)
    }

    /// The first `limit` leaf rows in `range`, in address order. Reading all
    /// of them from some address on, a page at a time, walks the leaves in
    /// order.
    pub async fn leaves(&mut self, range: &TreeSpan, limit: usize) -> Result<Vec<LeafRow<G>>, TreeError> {
        let rows = self.transaction.scan(MapName::Leaves, range.bounds(), limit).await?;
        rows.iter().map(|(address, value)| layout::leaf_row(address, value)).collect()
    }

    /// Write the leaf row at its address, if the row there now is `expected`,
    /// the one the writer read at that address (`None`: no row), and list its
    /// key under its entity. [`TreeError::Conflict`] when another change got
    /// there first.
    pub(crate) async fn put_leaf(&mut self, row: LeafRow<G>, expected: Option<&LeafRow<G>>) -> Result<(), TreeError> {
        self.put_prepared_leaf(&PreparedLeaf::new(row), expected).await
    }

    pub(crate) async fn put_prepared_leaf(&mut self, prepared: &PreparedLeaf<G>, expected: Option<&LeafRow<G>>) -> Result<(), TreeError> {
        let row = &prepared.row;
        debug_assert!(expected.is_none_or(|expected| expected.address() == row.address()), "the expectation is for the row's own address");
        let expected = expected.map(layout::leaf_value);
        self.transaction.put_if(MapName::Leaves, &row.address(), expected.as_deref(), &prepared.value).await?;
        let (listed, mut keys) = self.keys_of(row.entity_id).await?;
        if keys.insert(row.key.clone()) {
            if keys.len() == 2 {
                self.adjust_multiple_keys(true).await?;
            }
            self.transaction.put_if(MapName::Lookup, &row.entity_id.to_bytes(), listed.as_deref(), &layout::lookup_value(&keys)).await?;
        }
        Ok(())
    }

    /// Remove the leaf row `row`, as the writer read it, and its key from
    /// under its entity. [`TreeError::Conflict`] when the row there now is
    /// another, or none.
    pub(crate) async fn delete_leaf(&mut self, row: &LeafRow<G>) -> Result<(), TreeError> {
        self.transaction.delete_if(MapName::Leaves, &row.address(), &layout::leaf_value(row)).await?;
        let (listed, mut keys) = self.keys_of(row.entity_id).await?;
        if keys.remove(&row.key) {
            if keys.len() == 1 {
                self.adjust_multiple_keys(false).await?;
            }
            let listed = listed.as_deref().expect("the key was listed, so its entity has a lookup value");
            let id = row.entity_id.to_bytes();
            match keys.is_empty() {
                true => self.transaction.delete_if(MapName::Lookup, &id, listed).await?,
                false => self.transaction.put_if(MapName::Lookup, &id, Some(listed), &layout::lookup_value(&keys)).await?,
            }
        }
        Ok(())
    }

    /// The tree node at `prefix`: its base row plus the deltas appended
    /// since, summed a page at a time as they are read; `None` where it has
    /// neither.
    ///
    /// Deltas without a base report [`TreeError::IncompleteNode`]. This can
    /// happen in damaged or incompletely maintained rows: the remaining
    /// changes do not summarize the leaves still beneath the prefix.
    /// An empty baseline must be stored explicitly before appending
    /// changes to it. `None` means no stored summary, not an empty subtree.
    pub async fn node(&mut self, prefix: &NodePrefix) -> Result<Option<NodeRow<G>>, TreeError> {
        let base = self.base(prefix).await?;
        let exists = base.is_some();
        let mut digest = base.map_or(Digest::empty(), |base| base.digest);
        let mut deltas = DeltaPages::of(prefix);
        loop {
            let page = deltas.next(&mut self.transaction).await?;
            if page.is_empty() {
                return Ok(exists.then_some(NodeRow { digest }));
            }
            if !exists {
                return Err(TreeError::IncompleteNode(prefix.clone()));
            }
            for (_, value) in page {
                digest = digest.checked_add(layout::digest("delta value", &value)?)?;
            }
        }
    }

    /// The stored baseline without subsequent deltas; `None` where absent.
    /// Use [`TreeTransaction::node`] for the complete current summary.
    pub async fn node_base(&mut self, prefix: &NodePrefix) -> Result<Option<NodeRow<G>>, TreeError> {
        Ok(self.base(prefix).await?.map(|base| NodeRow { digest: base.digest }))
    }

    /// The nearest existing tree nodes beneath `prefix`, in prefix order:
    /// every tree node `prefix` properly contains with no existing tree node
    /// between them, so that no two overlap. Where path compression skipped
    /// levels, a child lies several levels down. Deltas without a base report
    /// [`TreeError::IncompleteNode`], as in [`TreeTransaction::node`].
    ///
    /// The scan over the subtree's keys meets delta rows too: the tree node's
    /// own, which are stepped over, and those of a tree node whose prefix ends
    /// on a whole byte of its encoding, which follow everything beneath that
    /// tree node. After each child the scan resumes past the child's own keys,
    /// not at the next prefix, so it also detects deltas whose base is missing.
    pub async fn children(&mut self, prefix: &NodePrefix) -> Result<Vec<(NodePrefix, NodeRow<G>)>, TreeError> {
        let mut children = Vec::new();
        for prefix in self.child_prefixes(prefix).await? {
            let node = self.node(&prefix).await?.ok_or_else(|| TreeError::IncompleteNode(prefix.clone()))?;
            children.push((prefix, node));
        }
        Ok(children)
    }

    /// Read topology without decoding summary values or their deltas.
    pub(crate) async fn child_prefixes(&mut self, prefix: &NodePrefix) -> Result<Vec<NodePrefix>, TreeError> {
        let (own_deltas_first, own_deltas_past) = layout::delta_key_bounds(prefix);
        let end = prefix.subtree_key_end();
        let mut start = Bound::Excluded(prefix.to_bytes());
        let mut children = Vec::new();
        loop {
            let bounds = (start.as_ref().map(Vec::as_slice), end.as_ref().map_or(Bound::Unbounded, |end| Bound::Excluded(end.as_slice())));
            let Some((key, _)) = self.transaction.scan(MapName::Nodes, bounds, 1).await?.pop() else { break };
            if key.starts_with(&own_deltas_first) {
                match &own_deltas_past {
                    Some(past) => start = Bound::Included(past.clone()),
                    None => break,
                }
                continue;
            }
            let child = layout::node_key_prefix(&key)?;
            if key != child.to_bytes() {
                return Err(TreeError::IncompleteNode(child));
            }
            children.push(child.clone());
            match child.subtree_key_end() {
                Some(end) => start = Bound::Included(end),
                None => break,
            }
        }
        Ok(children)
    }

    /// Write the tree node's base row for a build: inserted when `expected`
    /// is `None`, replaced when it is the base the writer read with
    /// [`TreeTransaction::node_base`]. Deltas appended to the tree node stay;
    /// a rebuild of the tree node alone deletes it first.
    /// [`TreeError::Conflict`] when the base there now is another.
    pub(crate) async fn put_node_base(
        &mut self,
        prefix: &NodePrefix,
        expected: Option<&NodeRow<G>>,
        node: NodeRow<G>,
    ) -> Result<(), TreeError> {
        let (key, value) = (prefix.to_bytes(), layout::digest_value(&node.digest));
        match expected {
            None => self.transaction.insert(MapName::Nodes, &key, &value).await,
            Some(expected) => self.transaction.put_if(MapName::Nodes, &key, Some(&layout::digest_value(&expected.digest)), &value).await,
        }
    }

    /// Record that a change moved the digest of the leaves beneath `prefix`
    /// by `delta`: a delta row is appended under a key of its own and the
    /// base row is left alone, so changes neither contend on it nor conflict
    /// with each other.
    pub(crate) async fn append_node_delta(&mut self, prefix: &NodePrefix, delta: Digest<G>) -> Result<(), TreeError> {
        let (key_prefix, _) = layout::delta_key_bounds(prefix);
        self.transaction.append(MapName::Nodes, &key_prefix, &layout::digest_value(&delta)).await
    }

    /// Fold up to [`COMPACTION_BATCH`] of the tree node's deltas, the first in
    /// key order, into its base row and remove them, so that reads sum fewer
    /// rows. Deltas without a base report [`TreeError::IncompleteNode`]
    /// without changing any rows. Returns whether deltas
    /// remain beyond the batch, which a later compaction, in a transaction of
    /// its own, folds. The base is written only if it is still the one read,
    /// and each delta removed only if it is still there:
    /// [`TreeError::Conflict`] otherwise, so the transaction fails whole and
    /// the compaction retries from fresh reads.
    ///
    /// Addition commutes, so the sum a reader gets is the same whichever
    /// deltas have been folded: a delta appended after compaction read is
    /// not among those it removes, stays in its row, and a later round folds
    /// it.
    /// Failure or cancellation prevents this transaction from committing.
    pub async fn compact_node(&mut self, prefix: &NodePrefix) -> Result<bool, TreeError> {
        let mut mutation = self.mutation();
        let result = mutation.compact_node_inner(prefix).await;
        mutation.finish(result)
    }

    async fn compact_node_inner(&mut self, prefix: &NodePrefix) -> Result<bool, TreeError> {
        let (first, past) = layout::delta_key_bounds(prefix);
        let bounds = (Bound::Included(first.as_slice()), past.as_deref().map_or(Bound::Unbounded, Bound::Excluded));
        let mut batch = self.transaction.scan(MapName::Nodes, bounds, COMPACTION_BATCH + 1).await?;
        let remain = batch.len() > COMPACTION_BATCH;
        batch.truncate(COMPACTION_BATCH);
        if batch.is_empty() {
            return Ok(false);
        }
        let base = self.base(prefix).await?.ok_or_else(|| TreeError::IncompleteNode(prefix.clone()))?;
        let mut digest = base.digest;
        for (_, value) in &batch {
            digest = digest.checked_add(layout::digest("delta value", value)?)?;
        }
        self.transaction.put_if(MapName::Nodes, &prefix.to_bytes(), Some(&base.value), &layout::digest_value(&digest)).await?;
        for (key, value) in batch {
            self.transaction.delete_if(MapName::Nodes, &key, &value).await?;
        }
        Ok(remain)
    }

    /// Remove the tree node: its base row and every delta, each only if still
    /// as read, [`TreeError::Conflict`] otherwise. Its descendants stay. A
    /// delta appended after the deletion read stays too: see
    /// [`TreeTransaction::node`].
    #[cfg(test)]
    pub(crate) async fn delete_node(&mut self, prefix: &NodePrefix) -> Result<(), TreeError> {
        if let Some(base) = self.base(prefix).await? {
            self.transaction.delete_if(MapName::Nodes, &prefix.to_bytes(), &base.value).await?;
        }
        let mut deltas = DeltaPages::of(prefix);
        loop {
            let page = deltas.next(&mut self.transaction).await?;
            if page.is_empty() {
                return Ok(());
            }
            for (key, value) in page {
                self.transaction.delete_if(MapName::Nodes, &key, &value).await?;
            }
        }
    }

    /// The keys the lookup lists for an entity, with the value they were read
    /// from, which a conditional write of the lookup expects to find.
    async fn keys_of(&mut self, entity_id: EntityId) -> Result<(Option<Vec<u8>>, BTreeSet<Vec<u8>>), TreeError> {
        let listed = self.transaction.get(MapName::Lookup, &entity_id.to_bytes()).await?;
        let keys = match &listed {
            Some(value) => layout::lookup_keys(value)?,
            None => BTreeSet::new(),
        };
        Ok((listed, keys))
    }

    /// Lookup's empty key is a count of entities filed under multiple keys.
    /// Entity lookup keys are always 32 bytes, so the metadata cannot collide.
    pub(crate) async fn multiple_key_entities(&mut self) -> Result<u64, TreeError> {
        match self.transaction.get(MapName::Lookup, layout::MULTIPLE_KEYS).await? {
            Some(value) => {
                Ok(u64::from_be_bytes(value.as_slice().try_into().map_err(|_| TreeError::Audit("invalid multi-key entity count"))?))
            }
            None => Ok(0),
        }
    }

    async fn adjust_multiple_keys(&mut self, add: bool) -> Result<(), TreeError> {
        let stored = self.transaction.get(MapName::Lookup, layout::MULTIPLE_KEYS).await?;
        let count = self.multiple_key_entities().await?;
        let count =
            if add { count.checked_add(1) } else { count.checked_sub(1) }.ok_or(TreeError::Audit("invalid multi-key entity count"))?;
        if count == 0 {
            self.transaction
                .delete_if(
                    MapName::Lookup,
                    layout::MULTIPLE_KEYS,
                    stored.as_deref().ok_or(TreeError::Audit("missing multi-key entity count"))?,
                )
                .await
        } else {
            self.transaction.put_if(MapName::Lookup, layout::MULTIPLE_KEYS, stored.as_deref(), &count.to_be_bytes()).await
        }
    }

    /// The tree node's base row.
    async fn base(&mut self, prefix: &NodePrefix) -> Result<Option<DigestRow<G>>, TreeError> {
        let key = prefix.to_bytes();
        let Some(value) = self.transaction.get(MapName::Nodes, &key).await? else { return Ok(None) };
        Ok(Some(DigestRow { digest: layout::digest("node value", &value)?, value }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest::{HeadHash, Leaf};
    use ankql::ast::Predicate;
    use ankurah_core_types::KeySpec;
    use async_trait::async_trait;
    use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    fn entity(n: u8) -> EntityId { EntityId::from_bytes([n; 32]) }

    /// The leaf row of `entity` under `key`, with a head of its own.
    fn row<G: DigestGroup>(key: &[u8], entity: EntityId) -> LeafRow<G> {
        let head = HeadHash::of([entity.to_bytes()]);
        LeafRow { key: key.to_vec(), entity_id: entity, head, element: Leaf::new(key, entity, head).element() }
    }

    /// The same row after a change to the entity: a new head and element.
    fn changed<G: DigestGroup>(row: &LeafRow<G>) -> LeafRow<G> {
        let head = HeadHash::of([row.head.as_bytes().map(|byte| byte ^ 0xff)]);
        LeafRow { head, element: Leaf::new(&row.key, row.entity_id, head).element(), ..row.clone() }
    }

    /// The digest of the one leaf of the entity `n` under the key `[n]`.
    fn leaf<G: DigestGroup>(n: u8) -> Digest<G> { Digest::from(row::<G>(&[n], entity(n)).element) }

    /// The tree node summing the leaves 1 to `leaves`.
    fn node<G: DigestGroup>(leaves: u8) -> NodeRow<G> { NodeRow { digest: (1..=leaves).map(leaf).sum() } }

    /// The prefix spelled by `text`, one character per bit.
    fn prefix(text: &str) -> NodePrefix { text.chars().fold(NodePrefix::root(), |prefix, bit| prefix.child(bit == '1')) }

    /// An empty tree. The tree files whatever addresses it is given, so its
    /// spec plays no part in these tests.
    fn empty_tree<G: DigestGroup>() -> DigestTree<G> { DigestTree::new(TreeSpec::new(KeySpec::new(vec![]), Predicate::True)) }

    async fn node_keys<G: DigestGroup, T: MapAccess>(maps: &mut TreeTransaction<G, T>) -> usize {
        maps.transaction.scan(MapName::Nodes, (Bound::Unbounded, Bound::Unbounded), usize::MAX).await.unwrap().len()
    }

    async fn delta_count<G: DigestGroup, T: MapAccess>(maps: &mut TreeTransaction<G, T>, prefix: &NodePrefix) -> usize {
        let (first, past) = layout::delta_key_bounds(prefix);
        let bounds = (Bound::Included(first.as_slice()), past.as_deref().map_or(Bound::Unbounded, Bound::Excluded));
        maps.transaction.scan(MapName::Nodes, bounds, usize::MAX).await.unwrap().len()
    }

    /// The prefixes of some tree nodes.
    fn prefixes<G: DigestGroup>(nodes: Vec<(NodePrefix, NodeRow<G>)>) -> Vec<NodePrefix> {
        nodes.into_iter().map(|(prefix, _)| prefix).collect()
    }

    /// A transaction that lets another commit land between two of its reads,
    /// as a concurrent transaction would: once, before its next scan of the
    /// nodes map, it awaits the meddle.
    struct Interleaved<T> {
        inner: T,
        before_next_nodes_scan: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    }

    #[async_trait]
    impl<T: MapAccess> MapAccess for Interleaved<T> {
        fn poison(&mut self) { self.inner.poison(); }
        fn watch_structure(&mut self) { self.inner.watch_structure(); }
        fn change_structure(&mut self) { self.inner.change_structure(); }
        async fn scan_reverse(
            &mut self,
            map: MapName,
            bounds: crate::map::ByteBounds<'_>,
            limit: usize,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError> {
            self.inner.scan_reverse(map, bounds, limit).await
        }
        async fn get(&mut self, map: MapName, key: &[u8]) -> Result<Option<Vec<u8>>, TreeError> { self.inner.get(map, key).await }

        async fn scan(
            &mut self,
            map: MapName,
            bounds: crate::map::ByteBounds<'_>,
            limit: usize,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError> {
            if map == MapName::Nodes {
                if let Some(meddle) = self.before_next_nodes_scan.take() {
                    meddle.await;
                }
            }
            self.inner.scan(map, bounds, limit).await
        }

        async fn insert(&mut self, map: MapName, key: &[u8], value: &[u8]) -> Result<(), TreeError> {
            self.inner.insert(map, key, value).await
        }

        async fn put_if(&mut self, map: MapName, key: &[u8], expected: Option<&[u8]>, value: &[u8]) -> Result<(), TreeError> {
            self.inner.put_if(map, key, expected, value).await
        }

        async fn delete_if(&mut self, map: MapName, key: &[u8], expected: &[u8]) -> Result<(), TreeError> {
            self.inner.delete_if(map, key, expected).await
        }

        async fn append(&mut self, map: MapName, key_prefix: &[u8], value: &[u8]) -> Result<(), TreeError> {
            self.inner.append(map, key_prefix, value).await
        }
    }

    #[async_trait]
    impl<T: MapTransaction> MapTransaction for Interleaved<T> {
        async fn commit(self) -> Result<(), TreeError> { self.inner.commit().await }
    }

    /// Runs each test below, generic over the group, once in each group, so
    /// that both pass the same suite.
    macro_rules! in_each_group {
        ($($test:ident),* $(,)?) => {
            mod ristretto255 {
                $(#[tokio::test]
                async fn $test() { super::$test::<crate::digest::Ristretto255>().await })*
            }
            mod lthash16 {
                $(#[tokio::test]
                async fn $test() { super::$test::<crate::digest::LtHash16>().await })*
            }
        };
    }

    in_each_group!(
        a_new_tree_is_empty,
        a_leaf_is_written_against_the_row_the_writer_read,
        the_leaves_of_one_entity_follow_its_keys,
        a_missing_leaf_in_the_lookup_is_an_error,
        leaves_are_read_by_address_range_a_page_at_a_time,
        the_range_under_a_prefix_reads_its_leaves,
        a_node_is_its_base_plus_its_deltas,
        compaction_folds_the_deltas_it_read_into_the_base,
        compaction_leaves_a_late_delta_and_fails_whole_on_a_removed_one,
        deleting_a_node_removes_its_base_and_its_deltas_and_leaves_its_descendants,
        a_delta_without_its_base_is_not_a_subtree_summary,
        children_skip_compressed_levels_and_delta_rows,
        children_reject_a_missing_base_at_seven_bits,
        children_reject_missing_bases_at_fourteen_bits,
        more_deltas_than_a_page_in_any_suffix_order_read_and_compact_whole,
        competing_lookup_updates_for_one_entity_leave_no_orphaned_row,
        competing_compactors_fold_each_delta_once,
        a_reader_crossing_a_compaction_sees_the_node_whole,
    );

    async fn a_new_tree_is_empty<G: DigestGroup>() {
        let spec = TreeSpec::new(KeySpec::new(vec![]), Predicate::True);
        let tree = DigestTree::<G>::new(spec.clone());
        assert_eq!(*tree.spec(), spec);
        let mut maps = tree.begin();
        assert_eq!(maps.node(&NodePrefix::root()).await.unwrap(), None, "no root row until a leaf arrives");
        assert_eq!(maps.leaves(&TreeSpan::all(), usize::MAX).await.unwrap(), []);
        assert_eq!(maps.entity_leaves(entity(1)).await.unwrap(), []);
        assert_eq!(maps.children(&NodePrefix::root()).await.unwrap(), []);
    }

    async fn a_leaf_is_written_against_the_row_the_writer_read<G: DigestGroup>() {
        let tree = empty_tree::<G>();
        let first = row::<G>(b"blue\0", entity(1));
        let mut maps = tree.begin();
        maps.put_leaf(first.clone(), None).await.unwrap();
        assert_eq!(maps.leaf(b"blue\0", entity(1)).await.unwrap(), Some(first.clone()));
        assert_eq!(maps.leaf(b"blue\0", entity(2)).await.unwrap(), None);
        assert_eq!(maps.leaf(b"red\0", entity(1)).await.unwrap(), None);
        maps.commit().await.unwrap();
        assert!(matches!(tree.begin().put_leaf(first.clone(), None).await, Err(TreeError::Conflict)), "a row is there now");

        // The next change to the entity replaces the row it read; a change
        // that read an older row conflicts.
        let later = changed(&first);
        let mut maps = tree.begin();
        maps.put_leaf(later.clone(), Some(&first)).await.unwrap();
        maps.commit().await.unwrap();
        let mut maps = tree.begin();
        assert_eq!(maps.leaf(b"blue\0", entity(1)).await.unwrap(), Some(later.clone()));
        assert_eq!(maps.leaves(&TreeSpan::all(), usize::MAX).await.unwrap(), std::slice::from_ref(&later));
        assert!(matches!(maps.put_leaf(changed(&later), Some(&first)).await, Err(TreeError::Conflict)), "stale");

        assert!(matches!(tree.begin().delete_leaf(&first).await, Err(TreeError::Conflict)), "stale");
        let mut maps = tree.begin();
        maps.delete_leaf(&later).await.unwrap();
        maps.commit().await.unwrap();
        assert_eq!(tree.begin().leaf(b"blue\0", entity(1)).await.unwrap(), None);
        assert!(matches!(tree.begin().delete_leaf(&later).await, Err(TreeError::Conflict)), "gone already");
    }

    /// The lookup from an entity to its rows follows every put and delete,
    /// and lists the rows in address order even where one key is a prefix
    /// of another.
    async fn the_leaves_of_one_entity_follow_its_keys<G: DigestGroup>() {
        let tree = empty_tree::<G>();
        // An id of 0xff bytes: against a longer key's next byte, the id's
        // first byte sorts after it, so address order is not key order.
        let moved = entity(0xff);
        let mut maps = tree.begin();
        assert_eq!(maps.entity_leaves(moved).await.unwrap(), []);
        for key in [b"ab".as_slice(), b"abc", b"a"] {
            maps.put_leaf(row(key, moved), None).await.unwrap();
        }
        maps.put_leaf(row(b"ab", entity(2)), None).await.unwrap();
        maps.commit().await.unwrap();

        let mut maps = tree.begin();
        let rows = maps.entity_leaves(moved).await.unwrap();
        let addresses: Vec<Vec<u8>> = rows.iter().map(LeafRow::address).collect();
        assert!(addresses.windows(2).all(|pair| pair[0] < pair[1]), "address order");
        let keys: Vec<&[u8]> = rows.iter().map(|row| row.key.as_slice()).collect();
        assert_eq!(keys, [b"abc".as_slice(), b"ab", b"a"]);

        // A change under a key the entity already has leaves the lookup as it is.
        let under_a = row(b"a", moved);
        maps.put_leaf(changed(&under_a), Some(&under_a)).await.unwrap();
        assert_eq!(maps.entity_leaves(moved).await.unwrap().len(), 3);

        // Moving the entity from one key to another.
        maps.delete_leaf(&row(b"ab", moved)).await.unwrap();
        maps.put_leaf(row(b"zz", moved), None).await.unwrap();
        let keys: Vec<Vec<u8>> = maps.entity_leaves(moved).await.unwrap().into_iter().map(|row| row.key).collect();
        assert_eq!(keys, [b"abc".to_vec(), b"a".to_vec(), b"zz".to_vec()]);
        assert_eq!(maps.entity_leaves(entity(2)).await.unwrap(), [row(b"ab", entity(2))], "another entity's rows are its own");
        maps.commit().await.unwrap();

        let mut maps = tree.begin();
        maps.delete_leaf(&changed(&under_a)).await.unwrap();
        for key in [b"abc".as_slice(), b"zz"] {
            maps.delete_leaf(&row(key, moved)).await.unwrap();
        }
        assert_eq!(maps.entity_leaves(moved).await.unwrap(), []);
        maps.commit().await.unwrap();
        let mut maps = tree.begin();
        assert_eq!(
            maps.transaction.get(MapName::Lookup, &moved.to_bytes()).await.unwrap(),
            None,
            "an entity with no rows leaves the lookup"
        );
        assert!(maps.transaction.get(MapName::Lookup, &entity(2).to_bytes()).await.unwrap().is_some());
    }

    async fn a_missing_leaf_in_the_lookup_is_an_error<G: DigestGroup>() {
        let tree = empty_tree::<G>();
        let leaf = row::<G>(b"blue", entity(1));
        let mut maps = tree.begin();
        maps.put_leaf(leaf.clone(), None).await.unwrap();
        maps.commit().await.unwrap();
        let mut damaged = tree.begin();
        damaged.transaction.delete_if(MapName::Leaves, &leaf.address(), &layout::leaf_value(&leaf)).await.unwrap();
        damaged.commit().await.unwrap();
        assert!(matches!(tree.begin().entity_leaves(leaf.entity_id).await,
            Err(TreeError::MissingLeaf { entity_id, key }) if entity_id == leaf.entity_id && key == leaf.key));
    }

    async fn leaves_are_read_by_address_range_a_page_at_a_time<G: DigestGroup>() {
        let tree = empty_tree::<G>();
        let rows: Vec<LeafRow<G>> = (1..=6).map(|n| row(&[n], entity(n))).collect();
        let mut maps = tree.begin();
        for row in rows.iter().rev() {
            maps.put_leaf(row.clone(), None).await.unwrap();
        }
        maps.commit().await.unwrap();

        let mut maps = tree.begin();
        assert_eq!(maps.leaves(&TreeSpan::all(), usize::MAX).await.unwrap(), rows, "address order, not arrival order");
        let range = TreeSpan::new(Bound::Included(vec![2]), Bound::Excluded(vec![6]));
        let first_page = maps.leaves(&range, 2).await.unwrap();
        assert_eq!(first_page, rows[1..3]);
        let second_page = maps.leaves(&range.after(&first_page[1].address()), 2).await.unwrap();
        assert_eq!(second_page, rows[3..5]);
        assert_eq!(maps.leaves(&range.after(&second_page[1].address()), 2).await.unwrap(), []);
        assert_eq!(maps.leaves(&TreeSpan::all(), 0).await.unwrap(), []);
    }

    /// The range under a prefix reads the leaves beneath that tree node.
    async fn the_range_under_a_prefix_reads_its_leaves<G: DigestGroup>() {
        let tree = empty_tree::<G>();
        let mut maps = tree.begin();
        for n in [0x00, 0x3f, 0x40, 0x7f, 0x80, 0xff] {
            maps.put_leaf(row(&[n], entity(n)), None).await.unwrap();
        }
        maps.commit().await.unwrap();

        let mut maps = tree.begin();
        let keys = |rows: Vec<LeafRow<G>>| rows.into_iter().map(|row| row.key[0]).collect::<Vec<u8>>();
        assert_eq!(keys(maps.leaves(&TreeSpan::under(&prefix("0")), usize::MAX).await.unwrap()), [0x00, 0x3f, 0x40, 0x7f]);
        assert_eq!(keys(maps.leaves(&TreeSpan::under(&prefix("01")), usize::MAX).await.unwrap()), [0x40, 0x7f]);
        assert_eq!(keys(maps.leaves(&TreeSpan::under(&prefix("1")), usize::MAX).await.unwrap()), [0x80, 0xff]);
        assert_eq!(keys(maps.leaves(&TreeSpan::under(&NodePrefix::root()), usize::MAX).await.unwrap()).len(), 6);
    }

    async fn a_node_is_its_base_plus_its_deltas<G: DigestGroup>() {
        let tree = empty_tree::<G>();
        let bucket = prefix("0110");
        let mut maps = tree.begin();
        maps.put_node_base(&bucket, None, node(2)).await.unwrap();
        assert_eq!(maps.node(&bucket).await.unwrap(), Some(node(2)));
        maps.commit().await.unwrap();
        assert!(matches!(tree.begin().put_node_base(&bucket, None, node(2)).await, Err(TreeError::Conflict)), "a base is there now");

        // Leaf 3 arrives, then leaf 1 leaves: a delta with a negative count.
        // Each append takes a key of its own, so both are summed.
        let mut maps = tree.begin();
        maps.append_node_delta(&bucket, leaf(3)).await.unwrap();
        maps.append_node_delta(&bucket, -leaf(1)).await.unwrap();
        maps.commit().await.unwrap();
        let mut maps = tree.begin();
        let summed = maps.node(&bucket).await.unwrap().unwrap();
        assert_eq!(summed.digest, leaf(2) + leaf(3));
        assert_eq!(summed.digest.count(), 2);
        assert_eq!(maps.node_base(&bucket).await.unwrap(), Some(node(2)), "the base alone");
        assert_eq!(node_keys(&mut maps).await, 3, "the base and two deltas");

        // An empty baseline is explicit.
        let fresh = prefix("1");
        maps.put_node_base(&fresh, None, node(0)).await.unwrap();
        maps.append_node_delta(&fresh, leaf(5)).await.unwrap();
        assert_eq!(maps.node(&fresh).await.unwrap(), Some(NodeRow { digest: leaf(5) }));
        assert_eq!(maps.node_base(&fresh).await.unwrap(), Some(node(0)));
        maps.commit().await.unwrap();

        // A base is replaced against the base the writer read, not against
        // the tree node's sum.
        let mut maps = tree.begin();
        let sum = maps.node(&bucket).await.unwrap().unwrap();
        assert!(matches!(maps.put_node_base(&bucket, Some(&sum), node(1)).await, Err(TreeError::Conflict)), "the sum is no base");
        let mut maps = tree.begin();
        let base = maps.node_base(&bucket).await.unwrap();
        maps.put_node_base(&bucket, base.as_ref(), node(1)).await.unwrap();
        maps.commit().await.unwrap();
        let mut maps = tree.begin();
        assert_eq!(maps.node(&bucket).await.unwrap().unwrap().digest, leaf(1) - leaf(1) + leaf(3));
        assert_eq!(node_keys(&mut maps).await, 5);
    }

    async fn compaction_folds_the_deltas_it_read_into_the_base<G: DigestGroup>() {
        let tree = empty_tree::<G>();
        let bucket = prefix("0110");
        let mut maps = tree.begin();
        maps.put_node_base(&bucket, None, node(2)).await.unwrap();
        for n in [3, 4, 5] {
            maps.append_node_delta(&bucket, leaf(n)).await.unwrap();
        }
        let before = maps.node(&bucket).await.unwrap();
        maps.commit().await.unwrap();

        let mut maps = tree.begin();
        assert!(!maps.compact_node(&bucket).await.unwrap(), "the deltas fit one batch");
        assert_eq!(maps.node(&bucket).await.unwrap(), before, "the sum is unchanged");
        maps.commit().await.unwrap();
        let mut maps = tree.begin();
        assert_eq!(maps.node(&bucket).await.unwrap(), before);
        assert_eq!(maps.node_base(&bucket).await.unwrap(), Some(node(5)));
        assert_eq!(node_keys(&mut maps).await, 1, "the base alone");
        assert!(!maps.compact_node(&bucket).await.unwrap());
        assert_eq!(node_keys(&mut maps).await, 1, "nothing to fold");

        // An explicit empty base can be compacted like any other.
        let fresh = prefix("1");
        maps.put_node_base(&fresh, None, node(0)).await.unwrap();
        maps.append_node_delta(&fresh, leaf(6)).await.unwrap();
        maps.compact_node(&fresh).await.unwrap();
        maps.commit().await.unwrap();
        let mut maps = tree.begin();
        assert_eq!(maps.node_base(&fresh).await.unwrap(), Some(NodeRow { digest: leaf(6) }));
        assert_eq!(node_keys(&mut maps).await, 2);
    }

    /// A delta appended after a compaction read is left in place, and a delta
    /// it read that another writer removed meanwhile fails the compaction
    /// whole: nothing it wrote lands.
    async fn compaction_leaves_a_late_delta_and_fails_whole_on_a_removed_one<G: DigestGroup>() {
        let tree = empty_tree::<G>();
        let bucket = prefix("0110");
        let mut maps = tree.begin();
        maps.put_node_base(&bucket, None, node(2)).await.unwrap();
        maps.append_node_delta(&bucket, leaf(3)).await.unwrap();
        maps.commit().await.unwrap();

        let mut compactor = tree.begin();
        compactor.compact_node(&bucket).await.unwrap();
        let mut late = tree.begin();
        late.append_node_delta(&bucket, leaf(4)).await.unwrap();
        late.commit().await.unwrap();
        compactor.commit().await.unwrap();
        let mut maps = tree.begin();
        assert_eq!(maps.node_base(&bucket).await.unwrap(), Some(node(3)), "what compaction read");
        assert_eq!(delta_count(&mut maps, &bucket).await, 1, "the late delta stays");
        assert_eq!(maps.node(&bucket).await.unwrap(), Some(node(4)));

        let mut compactor = tree.begin();
        compactor.compact_node(&bucket).await.unwrap();
        let mut remover = tree.begin();
        let (first, past) = layout::delta_key_bounds(&bucket);
        let bounds = (Bound::Included(first.as_slice()), past.as_deref().map_or(Bound::Unbounded, Bound::Excluded));
        let (key, value) = remover.transaction.scan(MapName::Nodes, bounds, 1).await.unwrap().remove(0);
        remover.transaction.delete_if(MapName::Nodes, &key, &value).await.unwrap();
        remover.commit().await.unwrap();
        assert!(matches!(compactor.commit().await, Err(TreeError::Conflict)));
        let mut maps = tree.begin();
        assert_eq!(maps.node_base(&bucket).await.unwrap(), Some(node(3)), "the base as it was");
        assert_eq!(delta_count(&mut maps, &bucket).await, 0, "as the remover left it");
        assert_eq!(maps.node(&bucket).await.unwrap(), Some(node(3)));
    }

    async fn deleting_a_node_removes_its_base_and_its_deltas_and_leaves_its_descendants<G: DigestGroup>() {
        let tree = empty_tree::<G>();
        let (parent, child) = (prefix("0"), prefix("0110"));
        let mut maps = tree.begin();
        maps.put_node_base(&parent, None, node(3)).await.unwrap();
        maps.append_node_delta(&parent, leaf(4)).await.unwrap();
        maps.put_node_base(&child, None, node(1)).await.unwrap();
        maps.append_node_delta(&child, leaf(4)).await.unwrap();
        assert_eq!(node_keys(&mut maps).await, 4);
        maps.commit().await.unwrap();

        let mut maps = tree.begin();
        maps.delete_node(&parent).await.unwrap();
        maps.commit().await.unwrap();
        let mut maps = tree.begin();
        assert_eq!(maps.node(&parent).await.unwrap(), None);
        assert_eq!(maps.node(&child).await.unwrap(), Some(NodeRow { digest: leaf(1) + leaf(4) }));
        assert_eq!(node_keys(&mut maps).await, 2);
        maps.delete_node(&parent).await.unwrap();
    }

    /// Removing a parent's summary leaves its leaves and children intact. A
    /// concurrent insertion can leave a delta behind, but that delta alone
    /// must never stand for the parent or be compacted into a base.
    async fn a_delta_without_its_base_is_not_a_subtree_summary<G: DigestGroup>() {
        let tree = empty_tree::<G>();
        let (root, parent, left, right) = (prefix(""), prefix("0"), prefix("00"), prefix("01"));
        let (a, b, c) = (row::<G>(b"", entity(0x01)), row::<G>(b"", entity(0x41)), row::<G>(b"", entity(0x02)));
        let (da, db, dc) = (Digest::from(a.element), Digest::from(b.element), Digest::from(c.element));
        let mut build = tree.begin();
        build.put_leaf(a, None).await.unwrap();
        build.put_leaf(b, None).await.unwrap();
        for (p, digest) in [(&root, da + db), (&parent, da + db), (&left, da), (&right, db)] {
            build.put_node_base(p, None, NodeRow { digest }).await.unwrap();
        }
        build.commit().await.unwrap();

        let (mut deletion, mut append) = (tree.begin(), tree.begin());
        deletion.delete_node(&parent).await.unwrap();
        append.put_leaf(c, None).await.unwrap();
        for p in [&root, &parent, &left] {
            append.append_node_delta(p, dc).await.unwrap();
        }
        append.commit().await.unwrap();
        deletion.commit().await.unwrap();

        let mut maps = tree.begin();
        let leaves: Digest<G> = maps.leaves(&TreeSpan::under(&parent), 10).await.unwrap().iter().map(|row| Digest::from(row.element)).sum();
        assert_eq!(leaves.count(), 3);
        let children: Digest<G> = maps.children(&parent).await.unwrap().iter().map(|(_, row)| row.digest).sum();
        assert_eq!(children, leaves);
        assert_eq!(maps.node(&root).await.unwrap().unwrap().digest, leaves);
        assert!(matches!(maps.node(&parent).await, Err(TreeError::IncompleteNode(p)) if p == parent));
        assert!(matches!(maps.children(&root).await, Err(TreeError::IncompleteNode(p)) if p == parent));
        assert!(matches!(maps.compact_node(&parent).await, Err(TreeError::IncompleteNode(p)) if p == parent));
        assert!(matches!(maps.commit().await, Err(TreeError::Conflict)));
        let mut maps = tree.begin();
        assert_eq!(maps.node_base(&parent).await.unwrap(), None, "compaction wrote no base");
        assert_eq!(delta_count(&mut maps, &parent).await, 1, "compaction consumed no delta");
    }

    /// Children are the nearest existing tree nodes beneath a prefix, with deltas
    /// on the parent and the children in the way, across both placements of a
    /// tree node's deltas: after its own base (a prefix of one bit) and after its
    /// descendants (the root, and a prefix of seven bits, whose descendants'
    /// encodings extend its bytes).
    async fn children_skip_compressed_levels_and_delta_rows<G: DigestGroup>() {
        let tree = empty_tree::<G>();
        let mut maps = tree.begin();
        // The root, its children 0 and 1; beneath 0 the seven-bit 0000000 and
        // 01; beneath 0000000 its child 00000000; 01 starts with an empty base.
        for (text, leaves) in [("", 6), ("0", 3), ("0000000", 2), ("00000000", 2), ("01", 0), ("1", 3)] {
            maps.put_node_base(&prefix(text), None, node(leaves)).await.unwrap();
        }
        for text in ["", "0", "0000000", "1", "01"] {
            maps.append_node_delta(&prefix(text), leaf(7)).await.unwrap();
        }
        maps.commit().await.unwrap();

        let mut maps = tree.begin();
        assert_eq!(prefixes(maps.children(&NodePrefix::root()).await.unwrap()), [prefix("0"), prefix("1")]);
        assert_eq!(prefixes(maps.children(&prefix("0")).await.unwrap()), [prefix("0000000"), prefix("01")]);
        assert_eq!(prefixes(maps.children(&prefix("00")).await.unwrap()), [prefix("0000000")], "beneath an unstored prefix too");
        assert_eq!(prefixes(maps.children(&prefix("0000000")).await.unwrap()), [prefix("00000000")]);
        assert_eq!(prefixes(maps.children(&prefix("00000000")).await.unwrap()), []);
        assert_eq!(prefixes(maps.children(&prefix("01")).await.unwrap()), []);
        assert_eq!(prefixes(maps.children(&prefix("1")).await.unwrap()), [], "the last prefix, with nothing after it");

        let (child, summary) = &maps.children(&prefix("0")).await.unwrap()[1];
        assert_eq!((child, summary), (&prefix("01"), &NodeRow { digest: leaf(7) }), "a child with an explicit empty baseline");
        assert_eq!(maps.node(&prefix("0")).await.unwrap(), Some(NodeRow { digest: node(3).digest + leaf(7) }));
    }

    /// A tree node whose prefix ends on a whole byte of its encoding, as one of
    /// seven bits does, keeps its deltas after everything beneath it. A
    /// missing base must still be detected after scanning its descendants.
    async fn children_reject_a_missing_base_at_seven_bits<G: DigestGroup>() {
        let tree = empty_tree::<G>();
        let deltas_only = prefix("0110100");
        let (leftmost, rightmost) = (prefix("01101000000000"), prefix("01101001111111"));
        let mut maps = tree.begin();
        for based in [prefix("00"), leftmost.clone(), rightmost.clone(), prefix("1")] {
            maps.put_node_base(&based, None, node(2)).await.unwrap();
        }
        maps.append_node_delta(&deltas_only, leaf(5)).await.unwrap();
        maps.commit().await.unwrap();

        let mut maps = tree.begin();
        assert!(matches!(maps.children(&NodePrefix::root()).await, Err(TreeError::IncompleteNode(p)) if p == deltas_only));
        assert!(matches!(maps.children(&prefix("0")).await, Err(TreeError::IncompleteNode(p)) if p == deltas_only));
        assert_eq!(prefixes(maps.children(&deltas_only).await.unwrap()), [leftmost, rightmost]);
        assert!(matches!(maps.node(&deltas_only).await, Err(TreeError::IncompleteNode(p)) if p == deltas_only));
    }

    /// The same at fourteen bits, the second whole byte, beneath a tree node
    /// with a base, and for one tree node of deltas beneath another.
    async fn children_reject_missing_bases_at_fourteen_bits<G: DigestGroup>() {
        let tree = empty_tree::<G>();
        let (parent, deltas_only) = (prefix("1010101"), prefix("10101010011001"));
        let (leftmost, rightmost) = (prefix("101010100110010"), prefix("101010100110011111111"));
        let (outer, inner) = (prefix("0011110"), prefix("00111101100110"));
        let (inner_leftmost, inner_rightmost) = (prefix("001111011001100000000"), prefix("001111011001101"));
        let mut maps = tree.begin();
        for based in [&parent, &leftmost, &rightmost, &inner_leftmost, &inner_rightmost] {
            maps.put_node_base(based, None, node(2)).await.unwrap();
        }
        for (deltas_only, n) in [(&deltas_only, 5), (&outer, 6), (&inner, 7)] {
            maps.append_node_delta(deltas_only, leaf(n)).await.unwrap();
        }
        maps.commit().await.unwrap();

        let mut maps = tree.begin();
        assert!(matches!(maps.children(&NodePrefix::root()).await, Err(TreeError::IncompleteNode(p)) if p == inner));
        assert!(matches!(maps.children(&parent).await, Err(TreeError::IncompleteNode(p)) if p == deltas_only));
        assert_eq!(prefixes(maps.children(&deltas_only).await.unwrap()), [leftmost, rightmost]);
        assert!(matches!(maps.children(&outer).await, Err(TreeError::IncompleteNode(p)) if p == inner));
        assert_eq!(prefixes(maps.children(&inner).await.unwrap()), [inner_leftmost, inner_rightmost]);
        assert!(matches!(maps.node(&outer).await, Err(TreeError::IncompleteNode(p)) if p == outer));
        assert!(matches!(maps.node(&inner).await, Err(TreeError::IncompleteNode(p)) if p == inner));
    }

    /// More deltas than a page and a compaction batch, filed under suffixes in
    /// no particular order: a read sums them all, and batched compaction folds
    /// them all, a batch per transaction, leaving a delta appended after a
    /// batch's read, whose key sorts among the batch's, for the next batch.
    async fn more_deltas_than_a_page_in_any_suffix_order_read_and_compact_whole<G: DigestGroup>() {
        let tree = empty_tree::<G>();
        let bucket = prefix("0110");
        let (first, _) = layout::delta_key_bounds(&bucket);
        let delta_key = |suffix: &[u8]| [first.as_slice(), suffix].concat();
        let leaves: Vec<Digest<G>> = (1..=5).map(leaf).collect();
        let mut suffixes: Vec<u32> = (0..1100).collect();
        suffixes.shuffle(&mut SmallRng::seed_from_u64(23));

        let mut maps = tree.begin();
        maps.put_node_base(&bucket, None, node(2)).await.unwrap();
        let mut expected = node::<G>(2).digest;
        for suffix in suffixes {
            let delta = leaves[suffix as usize % leaves.len()];
            expected += delta;
            maps.transaction.insert(MapName::Nodes, &delta_key(&suffix.to_be_bytes()), &layout::digest_value(&delta)).await.unwrap();
        }
        maps.commit().await.unwrap();
        let mut maps = tree.begin();
        assert_eq!(maps.node(&bucket).await.unwrap(), Some(NodeRow { digest: expected }));
        assert_eq!(expected.count(), 2 + 1100);
        assert_eq!(prefixes(maps.children(&NodePrefix::root()).await.unwrap()), std::slice::from_ref(&bucket));

        let mut compactor = tree.begin();
        assert!(compactor.compact_node(&bucket).await.unwrap(), "more deltas than a batch");
        // Suffix 5 and then a zero byte sorts between the suffixes 5 and 6,
        // among the batch's keys.
        let late_delta = leaf::<G>(6);
        let mut late = tree.begin();
        late.transaction.insert(MapName::Nodes, &delta_key(&[0, 0, 0, 5, 0]), &layout::digest_value(&late_delta)).await.unwrap();
        late.commit().await.unwrap();
        compactor.commit().await.unwrap();

        let mut maps = tree.begin();
        assert_eq!(maps.node(&bucket).await.unwrap(), Some(NodeRow { digest: expected + late_delta }), "nothing lost or folded twice");
        assert_eq!(delta_count(&mut maps, &bucket).await, 1100 - COMPACTION_BATCH + 1, "the late delta among them");
        assert!(!maps.compact_node(&bucket).await.unwrap(), "the rest fit one batch");
        maps.commit().await.unwrap();
        let mut maps = tree.begin();
        assert_eq!(delta_count(&mut maps, &bucket).await, 0);
        assert_eq!(maps.node_base(&bucket).await.unwrap(), Some(NodeRow { digest: expected + late_delta }));
    }

    /// Two transactions file one entity, with no lookup yet, under two keys:
    /// the second fails whole, leaving no row its lookup does not list, and
    /// its retry from fresh reads lands.
    async fn competing_lookup_updates_for_one_entity_leave_no_orphaned_row<G: DigestGroup>() {
        let tree = empty_tree::<G>();
        let filed = entity(7);
        let (blue, red) = (row::<G>(b"blue\0", filed), row::<G>(b"red\0", filed));
        let (mut first, mut second) = (tree.begin(), tree.begin());
        first.put_leaf(blue.clone(), None).await.unwrap();
        second.put_leaf(red.clone(), None).await.unwrap();
        first.commit().await.unwrap();
        assert!(matches!(second.commit().await, Err(TreeError::Conflict)), "it expected no lookup");

        let mut maps = tree.begin();
        assert_eq!(maps.entity_leaves(filed).await.unwrap(), std::slice::from_ref(&blue));
        assert_eq!(maps.leaf(b"red\0", filed).await.unwrap(), None, "no orphaned row");

        let mut retry = tree.begin();
        retry.put_leaf(red.clone(), None).await.unwrap();
        retry.commit().await.unwrap();
        assert_eq!(tree.begin().entity_leaves(filed).await.unwrap(), [blue, red]);
    }

    /// Two compactions of one tree node read the same deltas: the first folds
    /// them, and the second fails whole, so no delta is folded twice.
    async fn competing_compactors_fold_each_delta_once<G: DigestGroup>() {
        let tree = empty_tree::<G>();
        let bucket = prefix("0110");
        let mut maps = tree.begin();
        maps.put_node_base(&bucket, None, node(2)).await.unwrap();
        for n in [3, 4, 5] {
            maps.append_node_delta(&bucket, leaf(n)).await.unwrap();
        }
        maps.commit().await.unwrap();

        let (mut first, mut second) = (tree.begin(), tree.begin());
        first.compact_node(&bucket).await.unwrap();
        second.compact_node(&bucket).await.unwrap();
        first.commit().await.unwrap();
        assert!(matches!(second.commit().await, Err(TreeError::Conflict)));

        let mut maps = tree.begin();
        assert_eq!(maps.node(&bucket).await.unwrap(), Some(node(5)));
        assert_eq!(maps.node_base(&bucket).await.unwrap(), Some(node(5)));
        assert_eq!(delta_count(&mut maps, &bucket).await, 0);
    }

    /// A reader that reads a tree node's base, then its deltas, with a
    /// compaction landing in between, still sums the tree node it began with:
    /// the base and the delta, not the base alone.
    async fn a_reader_crossing_a_compaction_sees_the_node_whole<G: DigestGroup + 'static>() {
        let tree = Arc::new(empty_tree::<G>());
        let bucket = prefix("0110");
        let mut maps = tree.begin();
        maps.put_node_base(&bucket, None, node(2)).await.unwrap();
        maps.append_node_delta(&bucket, leaf(3)).await.unwrap();
        maps.commit().await.unwrap();

        let (compacting, compacted) = (tree.clone(), bucket.clone());
        let compaction = async move {
            let mut compactor = compacting.begin();
            compactor.compact_node(&compacted).await.unwrap();
            compactor.commit().await.unwrap();
        };
        let interleaved = Interleaved { inner: tree.maps.begin(), before_next_nodes_scan: Some(Box::pin(compaction)) };
        let mut reader = TreeTransaction::<G, _>::bind(tree.spec(), interleaved);
        assert_eq!(reader.node(&bucket).await.unwrap(), Some(node(3)), "the base and the delta it began with");

        let mut maps = tree.begin();
        assert_eq!(maps.node_base(&bucket).await.unwrap(), Some(node(3)), "the compaction landed");
        assert_eq!(maps.node(&bucket).await.unwrap(), Some(node(3)));
    }
}
