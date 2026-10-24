//! The tree node rows: a base row per tree node and the delta rows appended
//! beside it, which readers sum and compaction folds into the base.
//!
//! A read decodes the base and every delta: by the digest benchmark, about
//! 20 µs for a tree node with eight deltas in ristretto255 and about 1 µs in
//! LtHash16, so compaction keeps reads short.

use std::ops::Bound;

use super::{TreeTransaction, PAGE};
use crate::address::NodePrefix;
use crate::digest::{Digest, DigestGroup};
use crate::error::TreeError;
use crate::store::layout::{self, NodeKey};
use crate::store::{MapAccess, MapName};

/// How many deltas one compaction folds at most, so that the rows it reads and
/// the writes it buffers stay bounded however many deltas a tree node gathers.
const COMPACTION_BATCH: usize = 1024;

/// A read of a tree node's delta rows a page at a time, in key order, so that
/// no read holds them all at once: where the next page starts.
struct DeltaPages {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
    exhausted: bool,
}

impl DeltaPages {
    fn of(prefix: &NodePrefix) -> Self {
        let (first, past) = layout::delta_key_bounds(&prefix.to_bytes());
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

/// A tree node's base row as read: its bytes, which a conditional write
/// expects to find unchanged, and the digest they hold.
struct StoredBase<G: DigestGroup> {
    value: Vec<u8>,
    digest: Digest<G>,
}

impl<G: DigestGroup, T: MapAccess> TreeTransaction<G, T> {
    /// The digest of the tree node at `prefix`: its base row plus the deltas
    /// appended since, summed a page at a time as they are read; `None` where
    /// there is neither, which means no stored tree node, not an empty subtree.
    ///
    /// Deltas without a base report [`TreeError::IncompleteNode`]: the rows
    /// are damaged or incompletely maintained, and the deltas alone do not sum
    /// the leaves beneath the prefix. So the base of an empty tree node is
    /// stored, holding the empty digest, before any delta is appended to it.
    pub async fn node(&mut self, prefix: &NodePrefix) -> Result<Option<Digest<G>>, TreeError> {
        let base = self.base(prefix).await?;
        let exists = base.is_some();
        let mut digest = base.map_or(Digest::empty(), |base| base.digest);
        let mut deltas = DeltaPages::of(prefix);
        loop {
            let page = deltas.next(&mut self.transaction).await?;
            if page.is_empty() {
                return Ok(exists.then_some(digest));
            }
            if !exists {
                return Err(TreeError::IncompleteNode(prefix.clone()));
            }
            for (_, value) in page {
                digest = digest.checked_add(layout::digest("delta value", &value)?)?;
            }
        }
    }

    /// The tree nodes [`Self::child_prefixes`] finds beneath `prefix`, each
    /// with its digest.
    pub(crate) async fn children(&mut self, prefix: &NodePrefix) -> Result<Vec<(NodePrefix, Digest<G>)>, TreeError> {
        let mut children = Vec::new();
        for prefix in self.child_prefixes(prefix).await? {
            let node = self.node(&prefix).await?.ok_or_else(|| TreeError::IncompleteNode(prefix.clone()))?;
            children.push((prefix, node));
        }
        Ok(children)
    }

    /// The nearest existing tree nodes beneath `prefix`, in prefix order:
    /// every tree node `prefix` properly contains with no existing tree node
    /// between them, so that no two overlap, found without reading their
    /// digests. Where path compression skipped levels, a child lies several
    /// levels down. This transaction's own writes count. Deltas without a
    /// base report [`TreeError::IncompleteNode`], as in
    /// [`TreeTransaction::node`].
    ///
    /// The scan over the subtree's keys meets delta rows too: the tree node's
    /// own, which are stepped over, and those of a tree node whose prefix ends
    /// on a whole byte of its encoding, which follow everything beneath that
    /// tree node. After each child the scan resumes past the child's own keys,
    /// not at the next prefix, so it also detects deltas whose base is missing.
    pub async fn child_prefixes(&mut self, prefix: &NodePrefix) -> Result<Vec<NodePrefix>, TreeError> {
        let (encoding, end) = prefix.subtree_keys();
        let (own_deltas_first, own_deltas_past) = layout::delta_key_bounds(&encoding);
        let mut start = Bound::Excluded(encoding);
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
            let child = match layout::node_key(&key)? {
                NodeKey::Base(child) => child,
                NodeKey::Delta(child) => return Err(TreeError::IncompleteNode(child)),
            };
            let past_child = child.subtree_key_end();
            children.push(child);
            match past_child {
                Some(end) => start = Bound::Included(end),
                None => break,
            }
        }
        Ok(children)
    }

    /// Insert the tree node's base row, holding `digest`, where the
    /// transaction sees none: [`TreeError::Conflict`] when a base is there.
    pub(crate) async fn insert_node_base(&mut self, prefix: &NodePrefix, digest: Digest<G>) -> Result<(), TreeError> {
        self.transaction.insert(MapName::Nodes, &prefix.to_bytes(), &layout::digest_value(&digest)).await
    }

    /// Record that a change moved the digest of the leaves beneath each of
    /// `prefixes` by the same `delta`, encoded once for all of them: a delta
    /// row is appended under a key of its own and the base row is left alone,
    /// so changes neither contend on it nor conflict with each other. An empty
    /// delta moves nothing and appends no row.
    pub(crate) async fn append_node_deltas(&mut self, prefixes: &[NodePrefix], delta: Digest<G>) -> Result<(), TreeError> {
        if prefixes.is_empty() || delta == Digest::empty() {
            return Ok(());
        }
        let value = layout::digest_value(&delta);
        for prefix in prefixes {
            let (key_prefix, _) = layout::delta_key_bounds(&prefix.to_bytes());
            self.transaction.append(MapName::Nodes, &key_prefix, &value).await?;
        }
        Ok(())
    }

    /// Fold `difference` and every delta of the tree node at `prefix` into its
    /// base row, removing the deltas: one base write however many changes
    /// the tree node gathered. The base is written only if it is still the
    /// one read, and each delta removed only if it is still there:
    /// [`TreeError::Conflict`] otherwise. [`TreeError::IncompleteNode`] when
    /// the tree node has no base.
    pub(crate) async fn fold_into_base(&mut self, prefix: &NodePrefix, difference: Digest<G>) -> Result<(), TreeError> {
        let base = self.base(prefix).await?.ok_or_else(|| TreeError::IncompleteNode(prefix.clone()))?;
        let mut digest = base.digest.checked_add(difference)?;
        let mut deltas = DeltaPages::of(prefix);
        loop {
            let page = deltas.next(&mut self.transaction).await?;
            if page.is_empty() {
                break;
            }
            for (key, value) in page {
                digest = digest.checked_add(layout::digest("delta value", &value)?)?;
                self.transaction.delete_if(MapName::Nodes, &key, &value).await?;
            }
        }
        if digest == base.digest {
            return Ok(());
        }
        self.transaction.put_if(MapName::Nodes, &prefix.to_bytes(), Some(&base.value), &layout::digest_value(&digest)).await
    }

    /// Fold the tree node's first deltas in key order, a batch of at most
    /// 1,024, into its base row and remove them, so that reads sum fewer rows,
    /// and return whether deltas remain beyond the batch, for a later
    /// compaction, in a transaction of its own, to fold. Deltas without a base
    /// report [`TreeError::IncompleteNode`] without changing any rows. The base
    /// is written only if it is still the one read, and each delta removed only
    /// if it is still there: [`TreeError::Conflict`] otherwise, so the
    /// transaction fails whole and the compaction retries from fresh reads.
    /// Failure or cancellation prevents this transaction from committing.
    ///
    /// Addition commutes, so the sum a reader gets is the same whichever deltas
    /// have been folded: a delta appended after the compaction read is not
    /// among those it removes, stays in its row, and a later compaction folds
    /// it.
    pub async fn compact_node(&mut self, prefix: &NodePrefix) -> Result<bool, TreeError> {
        let mut guard = self.mutation_guard();
        let result = guard.fold_deltas(prefix).await;
        guard.finish(result)
    }

    async fn fold_deltas(&mut self, prefix: &NodePrefix) -> Result<bool, TreeError> {
        let (first, past) = layout::delta_key_bounds(&prefix.to_bytes());
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

    /// The tree node's base row.
    async fn base(&mut self, prefix: &NodePrefix) -> Result<Option<StoredBase<G>>, TreeError> {
        let key = prefix.to_bytes();
        let Some(value) = self.transaction.get(MapName::Nodes, &key).await? else { return Ok(None) };
        Ok(Some(StoredBase { digest: layout::digest("node value", &value)?, value }))
    }
}

#[cfg(test)]
mod tests;
