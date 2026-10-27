//! The audit: every row of a tree checked against its members, for tests.

use std::collections::HashSet;
use std::ops::Bound;

use super::rows::StagedRows;
use super::trie::{Node, Trie, BUCKET_CAPACITY};
use super::DigestTree;
use crate::digest::leaf_encoding;
use crate::span::split_address;
use crate::{Digest, DigestGroup, Error, MemberId, Payload, Row, RowKey, ScanDirection, Span, Store};

/// What an audit counted in a consistent tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Audit {
    /// How many members the tree holds.
    pub members: u64,

    /// How many nodes its trie has, buckets included.
    pub nodes: u64,
}

impl<K, I, L, G, S> DigestTree<K, I, L, G, S>
where
    K: Clone + PartialEq + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    /// Check every row of the tree against its members, changing nothing:
    /// every node's digest is the sum of the elements beneath it, every
    /// member's element is its leaf's, no member is held under two keys, and
    /// every node keeps the trie's shape. Each node is summed once, bottom-up.
    /// [`Error::Inconsistent`] names the first fault found.
    pub async fn audit(&self) -> Result<Audit, Error> {
        let _read = self.inner.lock.read().await;
        let rows = StagedRows::new(&self.inner.store);
        let meta = rows.meta().await?.ok_or(Error::Inconsistent("the tree has no metadata row"))?;

        if meta.group != G::ROW_VERSION || meta.key_spec != self.inner.key_spec {
            return Err(Error::Inconsistent("the metadata row names another group or key spec"));
        }

        let trie = Trie::new(&rows);
        let root = trie.root().await?;
        let beneath = trie.beneath(&root.prefix, usize::MAX).await?;

        let mut nodes = vec![root];
        nodes.extend(beneath);
        let leaves = rows.leaves(&Span::all(), ScanDirection::Forward, usize::MAX).await?;

        let parents = parents(&nodes)?;
        check_digests(&nodes, &parents, leaves.iter().map(|(address, leaf)| (address.as_slice(), &leaf.element)))?;

        // A member held under two keys would be counted twice: no change can
        // tell, since a change finds a member by the key it names.
        let mut ids = HashSet::new();
        for (address, leaf) in &leaves {
            let (key, id) = split_address(address)?;
            if I::from_bytes(&id).is_none() {
                return Err(Error::Inconsistent("a member's address holds bytes that are no member id"));
            }

            if G::hash_leaf(&leaf_encoding(&key, &id, &leaf.payload.digest_bytes())) != leaf.element {
                return Err(Error::Inconsistent("a member's element is not its leaf's"));
            }

            if !ids.insert(id) {
                return Err(Error::Inconsistent("a member is held under two keys"));
            }
        }

        Ok(Audit { members: leaves.len() as u64, nodes: nodes.len() as u64 })
    }
}

/// Each node's parent, as an index into `nodes`, which are in prefix order
/// from the root: the nearest node that contains it. The root has none.
/// A node with children has two, siblings.
fn parents<G: DigestGroup>(nodes: &[Node<G>]) -> Result<Vec<Option<usize>>, Error> {
    let mut parents = vec![None; nodes.len()];
    let mut children = vec![Vec::new(); nodes.len()];

    // The node's ancestors, nearest last: prefix order meets a node after
    // every ancestor and before anything outside its subtree.
    let mut ancestors: Vec<usize> = Vec::new();
    for (index, node) in nodes.iter().enumerate() {
        while ancestors.last().is_some_and(|&ancestor| !nodes[ancestor].prefix.contains(&node.prefix)) {
            ancestors.pop();
        }

        if let Some(&parent) = ancestors.last() {
            parents[index] = Some(parent);
            children[parent].push(index);
        }

        ancestors.push(index);
    }

    for kids in &children {
        match kids[..] {
            [] => {}
            [first, second] if nodes[first].prefix.sibling().as_ref() == Some(&nodes[second].prefix) => {}
            _ => return Err(Error::Inconsistent("a node's children are not two siblings")),
        }
    }

    Ok(parents)
}

/// Sum the members beneath each node once, bottom-up, and check every node
/// against the sum and the trie's shape. `nodes` are in prefix order, and
/// `leaves` in address order.
fn check_digests<'a, G: DigestGroup>(
    nodes: &[Node<G>],
    parents: &[Option<usize>],
    leaves: impl Iterator<Item = (&'a [u8], &'a G::Element)>,
) -> Result<(), Error> {
    let mut has_children = vec![false; nodes.len()];
    parents.iter().flatten().for_each(|&parent| has_children[parent] = true);
    let mut sums = vec![Digest::<G>::empty(); nodes.len()];

    // Buckets in prefix order hold disjoint runs of addresses in address
    // order, so one pass places every member.
    let mut buckets = (0..nodes.len()).filter(|&index| !has_children[index]).peekable();
    for (address, element) in leaves {
        while let Some(&bucket) = buckets.peek() {
            let span = nodes[bucket].prefix.span();
            if span.contains(address) || matches!(&span.start, Bound::Included(start) if address < start.as_slice()) {
                break;
            }

            buckets.next();
        }

        match buckets.peek() {
            Some(&bucket) if nodes[bucket].prefix.contains_address(address) => sums[bucket].add_element(element)?,
            _ => return Err(Error::Inconsistent("a member lies beneath no bucket")),
        }
    }

    for index in (0..nodes.len()).rev() {
        let sum = sums[index];
        if sum != nodes[index].digest {
            return Err(Error::Inconsistent("a node's digest is not the sum of the members beneath it"));
        }

        let fits = match has_children[index] {
            true => sum.count() > BUCKET_CAPACITY,
            false => sum.count() <= BUCKET_CAPACITY,
        };

        if !fits {
            return Err(Error::Inconsistent("a node holds a number of members its shape doesn't allow"));
        }

        if parents[index].is_some() && sum.count() == 0 {
            return Err(Error::Inconsistent("a branch has an empty side"));
        }

        if let Some(parent) = parents[index] {
            sums[parent].add(&sum)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests;
