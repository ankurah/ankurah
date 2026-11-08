//! A tree's rows as one operation sees them: the rows it reads through, a
//! read of the store or the owner's transaction, under the changes the
//! operation has made and not yet written.
//!
//! A read makes no changes, and sees the rows as its read does. A commit makes
//! all its changes here, so that each change sees the ones before it, and then
//! writes them through the owner's transaction in one batch.

use std::ops::Bound;

use crate::store::{LeafRow, Meta, NodeRow, Overlay};
use crate::{DigestGroup, Error, NodePrefix, Payload, ReadTrx, Row, RowKey, ScanDirection, Span, WriteItem};

/// The rows an operation reads through, with its changes on top: what the
/// operation reads, and, once it is done, the batch it writes.
pub(super) struct TreeRows<K, L, G: DigestGroup, R> {
    rows: R,
    changes: Overlay<RowKey, Row<K, L, G>>,
}

impl<K, L, G, R> TreeRows<K, L, G, R>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    R: ReadTrx<RowKey, Row<K, L, G>>,
{
    pub(super) fn new(rows: R) -> Self { Self { rows, changes: Overlay::new() } }

    /// Replace the row under `key`.
    pub(super) fn put(&mut self, key: RowKey, row: Row<K, L, G>) { self.changes.put(key, row); }

    /// Remove any row under `key`.
    pub(super) fn delete(&mut self, key: RowKey) { self.changes.delete(key); }

    /// Every change, as one batch for the store.
    pub(super) fn into_writes(self) -> Vec<WriteItem<RowKey, Row<K, L, G>>> { self.changes.into_writes() }

    async fn get(&mut self, key: &RowKey) -> Result<Option<Row<K, L, G>>, Error> { Ok(self.changes.get(&mut self.rows, key).await?) }

    /// The first `limit` rows between the bounds, in key order or reversed.
    pub(super) async fn range(
        &mut self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, Row<K, L, G>)>, Error> {
        Ok(self.changes.range(&mut self.rows, start, end, direction, limit).await?)
    }

    /// The tree's metadata.
    pub(super) async fn meta(&mut self) -> Result<Option<Meta<K>>, Error> {
        match self.get(&RowKey::Meta).await? {
            None => Ok(None),
            Some(Row::Meta(meta)) => Ok(Some(meta)),
            Some(_) => Err(wrong_kind()),
        }
    }

    /// The row of the node at `prefix`, if the tree has one.
    pub(super) async fn node(&mut self, prefix: &NodePrefix) -> Result<Option<NodeRow<G>>, Error> {
        node_row(prefix, self.get(&RowKey::Node(prefix.clone())).await?)
    }

    /// The row of the node at each of `prefixes`, in order, each `None` where
    /// the tree has no node, in one read of many.
    pub(super) async fn nodes(&mut self, prefixes: &[NodePrefix]) -> Result<Vec<Option<NodeRow<G>>>, Error> {
        let keys: Vec<RowKey> = prefixes.iter().cloned().map(RowKey::Node).collect();
        let rows = self.changes.get_many(&mut self.rows, &keys).await?;

        prefixes.iter().zip(rows).map(|(prefix, row)| node_row(prefix, row)).collect()
    }

    /// The first `limit` members within `spans`, read in `direction`: the
    /// spans one after another in the order given, each in `direction`.
    pub(super) async fn first_leaves(
        &mut self,
        spans: &[Span],
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, LeafRow<L, G>)>, Error> {
        let mut leaves = Vec::new();
        for span in spans {
            if leaves.len() == limit {
                break;
            }

            leaves.extend(self.leaves(span, direction, limit - leaves.len()).await?);
        }

        Ok(leaves)
    }

    /// At most `limit` members within `span`, in address order, or reversed.
    pub(super) async fn leaves(
        &mut self,
        span: &Span,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, LeafRow<L, G>)>, Error> {
        // Leaf keys run from the empty address to the end of the rows.
        let start = match &span.start {
            Bound::Unbounded => Bound::Included(RowKey::Leaf(Vec::new())),
            Bound::Included(address) => Bound::Included(RowKey::Leaf(address.clone())),
            Bound::Excluded(address) => Bound::Excluded(RowKey::Leaf(address.clone())),
        };

        let end = match &span.end {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(address) => Bound::Included(RowKey::Leaf(address.clone())),
            Bound::Excluded(address) => Bound::Excluded(RowKey::Leaf(address.clone())),
        };

        self.range(start.as_ref(), end.as_ref(), direction, limit)
            .await?
            .into_iter()
            .map(|row| match row {
                (RowKey::Leaf(address), Row::Leaf(leaf)) => Ok((address, leaf)),
                _ => Err(wrong_kind()),
            })
            .collect()
    }
}

/// The node row a read under the key of the node at `prefix` found, if any.
/// Every step down the trie goes from a node to the children its record
/// names, so a record within the node makes each step longer than the last
/// and every walk ends; one outside it could lead back to the node.
fn node_row<K, L, G: DigestGroup>(prefix: &NodePrefix, row: Option<Row<K, L, G>>) -> Result<Option<NodeRow<G>>, Error> {
    match row {
        None => Ok(None),
        Some(Row::Node(node)) if node.branches_at.as_ref().is_some_and(|branches_at| !prefix.contains(branches_at)) => {
            Err(branches_outside())
        }
        Some(Row::Node(node)) => Ok(Some(node)),
        Some(_) => Err(wrong_kind()),
    }
}

/// A node's record of where it branches lies outside the node: the store's
/// rows are damaged.
pub(super) fn branches_outside() -> Error { Error::Inconsistent("a branch's record of where it branches lies outside it") }

/// A row of another kind than its key names: the store's rows are damaged.
pub(super) fn wrong_kind() -> Error { Error::Inconsistent("a row is of another kind than its key") }

#[cfg(test)]
mod tests;
