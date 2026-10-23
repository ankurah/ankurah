//! The tree node rows: a base row per tree node and the delta rows appended
//! beside it, which readers sum and compaction folds into the base.

use std::ops::Bound;

use super::{TreeTransaction, PAGE};
use crate::address::NodePrefix;
use crate::digest::{Digest, DigestGroup};
use crate::error::TreeError;
use crate::store::layout;
use crate::store::{MapAccess, MapName, NodeRow};

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

    /// The nearest stored descendants, in the same prefix order as [`Self::children`],
    /// without decoding or summing their digests. Includes pending topology changes
    /// and still rejects descendant delta rows whose base is missing.
    pub async fn child_prefixes(&mut self, prefix: &NodePrefix) -> Result<Vec<NodePrefix>, TreeError> {
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

    #[cfg(test)]
    pub(crate) async fn append_node_delta(&mut self, prefix: &NodePrefix, delta: Digest<G>) -> Result<(), TreeError> {
        self.append_node_deltas(std::slice::from_ref(prefix), delta).await
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
            let (key_prefix, _) = layout::delta_key_bounds(prefix);
            self.transaction.append(MapName::Nodes, &key_prefix, &value).await?;
        }
        Ok(())
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

    /// The tree node's base row.
    async fn base(&mut self, prefix: &NodePrefix) -> Result<Option<DigestRow<G>>, TreeError> {
        let key = prefix.to_bytes();
        let Some(value) = self.transaction.get(MapName::Nodes, &key).await? else { return Ok(None) };
        Ok(Some(DigestRow { digest: layout::digest("node value", &value)?, value }))
    }
}

#[cfg(test)]
mod tests;
