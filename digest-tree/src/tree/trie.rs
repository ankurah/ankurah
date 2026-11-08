//! The trie over a tree's addresses: a binary trie with path compression, a
//! branch where the addresses beneath a node first differ, and a digest at
//! every node.
//!
//! A node with no children is a bucket, and holds at most
//! [`BUCKET_CAPACITY`] members. A node with children has exactly two, siblings
//! that differ in their last bit, and holds more than [`BUCKET_CAPACITY`]
//! members. Every member beneath a node with children lies beneath one of
//! them, and every node but the root holds at least one member: a branch
//! parts members, so neither of its sides is empty. So the trie's shape
//! follows from its members alone, whatever order they came and went in.
//!
//! A node with children records in its row the prefix they share, the
//! longest prefix of the members beneath it, so that every step down the trie
//! reads a node by its key.

use std::ops::Bound;

use super::rows::{wrong_kind, TreeRows};
use crate::store::NodeRow;
use crate::{Digest, DigestGroup, Error, NodePrefix, Payload, ReadTrx, Row, RowKey, ScanDirection, Span};

/// The most members a bucket holds. An insertion that takes a bucket past it
/// splits the bucket, and a removal that leaves a subtree with children
/// holding this many or fewer merges the subtree into one bucket. Splitting
/// and merging at the same count keeps the trie's shape a matter of its
/// members alone, though a bucket that hovers at the count splits and merges
/// by turns.
pub(super) const BUCKET_CAPACITY: u64 = 64;

/// A node of the trie: its prefix, and its row, the digest of the members
/// beneath it and where it branches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Node<G: DigestGroup> {
    pub(super) prefix: NodePrefix,
    pub(super) row: NodeRow<G>,
}

/// The trie in a tree's rows, as an operation sees them.
pub(super) struct Trie<'r, K, L, G: DigestGroup, R> {
    rows: &'r mut TreeRows<K, L, G, R>,
}

impl<'r, K, L, G, R> Trie<'r, K, L, G, R>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    R: ReadTrx<RowKey, Row<K, L, G>>,
{
    pub(super) fn new(rows: &'r mut TreeRows<K, L, G, R>) -> Self { Self { rows } }

    /// The root, which every tree has.
    pub(super) async fn root(&mut self) -> Result<Node<G>, Error> {
        let prefix = NodePrefix::root();
        let row = self.rows.node(&prefix).await?.ok_or(Error::Inconsistent("the tree has no root node"))?;
        Ok(Node { prefix, row })
    }

    /// The two children of `node`, or `None` for a bucket: the nodes at the
    /// prefix where it branches followed by 0 and by 1, read by their keys in
    /// one read.
    pub(super) async fn children(&mut self, node: &Node<G>) -> Result<Option<[Node<G>; 2]>, Error> {
        let Some(branches_at) = &node.row.branches_at else { return Ok(None) };
        let prefixes = [branches_at.child(false), branches_at.child(true)];

        let mut rows = self.rows.nodes(&prefixes).await?.into_iter();
        let (Some(Some(first)), Some(Some(second))) = (rows.next(), rows.next()) else { return Err(missing_node()) };

        let [first_prefix, second_prefix] = prefixes;
        Ok(Some([Node { prefix: first_prefix, row: first }, Node { prefix: second_prefix, row: second }]))
    }

    /// The children of the smallest subtree that contains each of `spans`,
    /// the deepest node whose subtree does, or `None` where that node is a
    /// bucket, in one walk from the root: each node's children are read
    /// once, however many of the spans lie beneath it.
    pub(super) async fn children_of_smallest_containing(&mut self, spans: &[Span]) -> Result<Vec<Option<[Node<G>; 2]>>, Error> {
        let mut found = vec![None; spans.len()];
        if spans.is_empty() {
            return Ok(found);
        }

        let mut pending: Vec<(Node<G>, Vec<usize>)> = vec![(self.root().await?, (0..spans.len()).collect())];
        while let Some((node, placing)) = pending.pop() {
            let Some([first, second]) = self.children(&node).await? else { continue };
            let (first_subtree, second_subtree) = (first.prefix.span(), second.prefix.span());

            // A span within one child lies beneath it. One that reaches both,
            // or that holds no address and so lies within both, stays with
            // their parent.
            let (mut into_first, mut into_second) = (Vec::new(), Vec::new());
            for index in placing {
                match (spans[index].is_subset_of(&first_subtree), spans[index].is_subset_of(&second_subtree)) {
                    (true, false) => into_first.push(index),
                    (false, true) => into_second.push(index),
                    _ => found[index] = Some([first.clone(), second.clone()]),
                }
            }

            for (child, beneath) in [(first, into_first), (second, into_second)] {
                if !beneath.is_empty() {
                    pending.push((child, beneath));
                }
            }
        }

        Ok(found)
    }

    /// The digest of the members within each of `regions`, in one walk from
    /// the root: each node's children are read once, however many of the
    /// regions cut beneath it.
    pub(super) async fn digests_within(&mut self, regions: &[Span]) -> Result<Vec<Digest<G>>, Error> {
        let mut totals = vec![Digest::empty(); regions.len()];
        if regions.is_empty() {
            return Ok(totals);
        }

        let mut pending: Vec<(Node<G>, Vec<usize>)> = vec![(self.root().await?, (0..regions.len()).collect())];
        while let Some((node, reaching)) = pending.pop() {
            // A subtree of no members, as an empty tree's root is, holds
            // nothing for any region to take.
            if node.row.digest.count() == 0 {
                continue;
            }

            let subtree = node.prefix.span();

            // A region holding the whole subtree takes its digest; one that
            // cuts it is summed from beneath.
            let mut cutting: Vec<(usize, Span)> = Vec::new();
            for index in reaching {
                if subtree.is_subset_of(&regions[index]) {
                    totals[index].add(&node.row.digest)?;
                } else if let Some(cut) = subtree.intersect(&regions[index]) {
                    cutting.push((index, cut));
                }
            }

            if cutting.is_empty() {
                continue;
            }

            match self.children(&node).await? {
                Some(children) => {
                    let beneath: Vec<usize> = cutting.into_iter().map(|(index, _)| index).collect();
                    pending.extend(children.into_iter().map(|child| (child, beneath.clone())));
                }

                None => {
                    for (index, cut) in cutting {
                        for (_, leaf) in self.rows.leaves(&cut, ScanDirection::Forward, usize::MAX).await? {
                            totals[index].add_element(&leaf.element)?;
                        }
                    }
                }
            }
        }

        Ok(totals)
    }

    /// Every node strictly beneath `prefix`, in prefix order, found in one
    /// range read rather than by following records: for what must find every
    /// node of a subtree, a node that no record names among them. A subtree's
    /// nodes are one run in prefix order, which ends at the first prefix past
    /// them, or with the node rows themselves.
    pub(super) async fn beneath(&mut self, prefix: &NodePrefix) -> Result<Vec<Node<G>>, Error> {
        let start = Bound::Excluded(RowKey::Node(prefix.clone()));

        let end = match prefix.after_subtree() {
            Some(after) => Bound::Excluded(RowKey::Node(after)),
            None => Bound::Excluded(RowKey::Leaf(Vec::new())),
        };

        self.rows
            .range(start.as_ref(), end.as_ref(), ScanDirection::Forward, usize::MAX)
            .await?
            .into_iter()
            .map(|row| match row {
                (RowKey::Node(prefix), Row::Node(row)) => Ok(Node { prefix, row }),
                _ => Err(wrong_kind()),
            })
            .collect()
    }
}

/// A node the trie reaches, by a record of where its parent branches, has no
/// row: the store's rows are damaged.
pub(super) fn missing_node() -> Error { Error::Inconsistent("a node the trie reaches has no row") }

/// The nodes of the subtree at `prefix` laid out bottom-up over its members,
/// given in address order with their elements: the subtree's own node, and
/// beneath it, wherever more members lie than a bucket holds, a branch where
/// their addresses first differ, which the node records. Each bucket is
/// summed once, and each branch from its children.
pub(super) fn lay_out<G: DigestGroup>(prefix: &NodePrefix, leaves: &[(&[u8], &G::Element)]) -> Result<Vec<Node<G>>, Error> {
    enum Step {
        /// Lay out the node at the prefix over these leaves.
        LayOut { prefix: NodePrefix, from: usize, to: usize },

        /// Sum the node at the prefix from its children, laid out just before,
        /// which share the prefix it branches at.
        Join { prefix: NodePrefix, branches_at: NodePrefix },
    }

    let mut nodes = Vec::new();
    let mut steps = vec![Step::LayOut { prefix: prefix.clone(), from: 0, to: leaves.len() }];
    let mut completed = Vec::<Digest<G>>::new();

    while let Some(step) = steps.pop() {
        let (prefix, from, to) = match step {
            Step::LayOut { prefix, from, to } => (prefix, from, to),

            Step::Join { prefix, branches_at } => {
                let right = completed.pop().expect("the second child is laid out before the join");
                let mut digest = completed.pop().expect("the first child is laid out before the join");
                digest.add(&right)?;
                nodes.push(Node { prefix, row: NodeRow { digest, branches_at: Some(branches_at) } });
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

            nodes.push(Node { prefix, row: NodeRow { digest, branches_at: None } });
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

        steps.push(Step::Join { prefix, branches_at: NodePrefix::of(first, depth) });
        steps.push(Step::LayOut { prefix: right, from: middle, to });
        steps.push(Step::LayOut { prefix: left, from, to: middle });
    }

    Ok(nodes)
}

/// One member's address begins another's, so no bit tells them apart and no
/// branch can hold them apart. No address the tree forms begins another, so
/// the tree's addresses are damaged.
pub(super) fn address_begins_another() -> Error { Error::Inconsistent("one member's address begins another's") }

#[cfg(test)]
mod tests;
