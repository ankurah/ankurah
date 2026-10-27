//! The trie over a tree's addresses: a binary trie with path compression, a
//! branch where the addresses beneath a node first differ, and a digest at
//! every node.
//!
//! A node with no children is a bucket, and holds at most
//! [`BUCKET_CAPACITY`] members. A node with children has exactly two, siblings
//! that differ in their last bit, and holds more than [`MERGE_AT`] members.
//! Every member beneath a node with children lies beneath one of them, and
//! every node but the root holds at least one member: a branch parts
//! members, so neither of its sides is empty.

use std::ops::Bound;

use super::rows::{wrong_kind, StagedRows};
use crate::{Digest, DigestGroup, Error, NodePrefix, Payload, Row, RowKey, ScanDirection, Store};

/// The most members a bucket holds: an insertion that takes a bucket past it
/// splits the bucket.
pub(super) const BUCKET_CAPACITY: u64 = 64;

/// A subtree with children that a removal leaves holding this many members
/// or fewer merges into one bucket. The gap below the capacity keeps a bucket
/// near it from splitting and merging over and over.
pub(super) const MERGE_AT: u64 = 32;

/// A node of the trie: its prefix, and the digest of the members beneath it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Node<G: DigestGroup> {
    pub(super) prefix: NodePrefix,
    pub(super) digest: Digest<G>,
}

/// The trie in a tree's rows, as an operation sees them.
pub(super) struct Trie<'r, 's, K, L, G: DigestGroup, S> {
    rows: &'r StagedRows<'s, K, L, G, S>,
}

impl<'r, 's, K, L, G, S> Trie<'r, 's, K, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    pub(super) fn new(rows: &'r StagedRows<'s, K, L, G, S>) -> Self { Self { rows } }

    /// The rows the trie is read from.
    pub(super) fn rows(&self) -> &'r StagedRows<'s, K, L, G, S> { self.rows }

    /// The root, which every tree has.
    pub(super) async fn root(&self) -> Result<Node<G>, Error> {
        let prefix = NodePrefix::root();
        let digest = self.rows.node(&prefix).await?.ok_or(Error::Inconsistent("the tree has no root node"))?;
        Ok(Node { prefix, digest })
    }

    /// The two children of the node at `prefix`, or `None` for a bucket.
    pub(super) async fn children(&self, prefix: &NodePrefix) -> Result<Option<[Node<G>; 2]>, Error> {
        // The first node beneath a branch in prefix order is its first child.
        let Some(first) = self.beneath(prefix, 1).await?.pop() else { return Ok(None) };
        let second = match first.prefix.bit_len().checked_sub(1).map(|last| first.prefix.bit(last)) {
            Some(false) => first.prefix.sibling().expect("a child is not the root"),
            _ => return Err(Error::Inconsistent("a branch's first child is a second child")),
        };

        let digest = self.rows.node(&second).await?.ok_or(Error::Inconsistent("a branch has one child"))?;
        Ok(Some([first, Node { prefix: second, digest }]))
    }

    /// The nearest nodes beneath `prefix`, which may lie anywhere: every node
    /// it properly contains with no node between them, in prefix order.
    /// Beneath a node, these are its children.
    pub(super) async fn nearest_beneath(&self, prefix: &NodePrefix) -> Result<Vec<Node<G>>, Error> {
        let mut nearest = Vec::new();
        let mut from = Bound::Excluded(prefix.clone());

        while let Some(node) = self.nodes_from(from.as_ref(), prefix, 1).await?.pop() {
            // The next nearest node lies past this one's subtree.
            let after = node.prefix.after_subtree();
            nearest.push(node);

            match after {
                Some(after) => from = Bound::Included(after),
                None => break,
            }
        }

        Ok(nearest)
    }

    /// At most `limit` of the nodes strictly beneath `prefix`, in prefix
    /// order.
    pub(super) async fn beneath(&self, prefix: &NodePrefix, limit: usize) -> Result<Vec<Node<G>>, Error> {
        self.nodes_from(Bound::Excluded(prefix), prefix, limit).await
    }

    /// At most `limit` nodes of the subtree at `subtree` from `start` on, in
    /// prefix order. A subtree's nodes are one run in prefix order, which ends
    /// at the first prefix past them, or with the node rows themselves.
    async fn nodes_from(&self, start: Bound<&NodePrefix>, subtree: &NodePrefix, limit: usize) -> Result<Vec<Node<G>>, Error> {
        let start = start.map(|prefix| RowKey::Node(prefix.clone()));

        let end = match subtree.after_subtree() {
            Some(after) => Bound::Excluded(RowKey::Node(after)),
            None => Bound::Excluded(RowKey::Leaf(Vec::new())),
        };

        self.rows
            .range(start.as_ref(), end.as_ref(), ScanDirection::Forward, limit)
            .await?
            .into_iter()
            .map(|row| match row {
                (RowKey::Node(prefix), Row::Node(digest)) => Ok(Node { prefix, digest }),
                _ => Err(wrong_kind()),
            })
            .collect()
    }
}

/// The nodes of the subtree at `prefix` laid out bottom-up over its members,
/// given in address order with their elements: the subtree's own node, and
/// beneath it, wherever more members lie than a bucket holds, a branch where
/// their addresses first differ. Each bucket is summed once, and each branch
/// from its children.
pub(super) fn lay_out<G: DigestGroup>(prefix: &NodePrefix, leaves: &[(&[u8], &G::Element)]) -> Result<Vec<Node<G>>, Error> {
    enum Step {
        /// Lay out the node at the prefix over these leaves.
        Build { prefix: NodePrefix, from: usize, to: usize },

        /// Sum the node at the prefix from its children, laid out just before.
        Join(NodePrefix),
    }

    let mut nodes = Vec::new();
    let mut steps = vec![Step::Build { prefix: prefix.clone(), from: 0, to: leaves.len() }];
    let mut completed = Vec::<Digest<G>>::new();

    while let Some(step) = steps.pop() {
        let (prefix, from, to) = match step {
            Step::Build { prefix, from, to } => (prefix, from, to),

            Step::Join(prefix) => {
                let right = completed.pop().expect("the second child is laid out before the join");
                let mut digest = completed.pop().expect("the first child is laid out before the join");
                digest.add(&right)?;
                nodes.push(Node { prefix, digest });
                completed.push(digest);

                continue;
            }
        };

        let members = &leaves[from..to];
        if members.len() as u64 <= BUCKET_CAPACITY {
            let mut digest = Digest::empty();
            for &(_, element) in members {
                digest.add_element(element)?;
            }

            nodes.push(Node { prefix, digest });
            completed.push(digest);

            continue;
        }

        let (first, last) = (members[0].0, members[members.len() - 1].0);
        let common = first.iter().zip(last).take_while(|(a, b)| a == b).count();

        if common == first.len().min(last.len()) {
            return Err(address_begins_another());
        }

        let depth = (common * 8) as u32 + (first[common] ^ last[common]).leading_zeros();
        let left = NodePrefix::of(first, depth + 1);

        let right = NodePrefix::of(last, depth + 1);
        let middle = from + members.partition_point(|(address, _)| left.contains_address(address));

        steps.push(Step::Join(prefix));
        steps.push(Step::Build { prefix: right, from: middle, to });
        steps.push(Step::Build { prefix: left, from, to: middle });
    }

    Ok(nodes)
}

/// One member's address begins another's, so no bit tells them apart and no
/// branch can hold them apart. No address the tree forms begins another, so
/// the tree's addresses are damaged.
pub(super) fn address_begins_another() -> Error { Error::Inconsistent("one member's address begins another's") }

#[cfg(test)]
mod tests;
