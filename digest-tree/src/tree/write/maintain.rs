//! Keeping a tree's nodes right as its members change: node by node for a
//! small batch, or laid out anew for a large one.
//!
//! Node by node, each edit walks from the root to the member's bucket,
//! moving every digest on the way, and inserts a branch where an inserted
//! member lies outside a node's children. Once every edit has walked, the
//! nodes whose counts moved settle their shape: a bucket grown past its
//! capacity splits, and a subtree with children shrunk to the merge threshold
//! merges into one bucket. Settling after the walks means a split reads the
//! batch's final members, which the digests it replaces already sum, and that
//! the walks change the shape only where they insert branches.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::tree::rows::StagedRows;
use crate::tree::trie::{address_begins_another, lay_out, Trie, BUCKET_CAPACITY, MERGE_AT};
use crate::{Digest, DigestGroup, Error, NodePrefix, Payload, Row, RowKey, ScanDirection, Span, Store};

/// One member's effect on the trie: where its leaf went, and its element.
pub(super) enum Edit<G: DigestGroup> {
    /// A leaf filed at `address`.
    Insert { address: Vec<u8>, element: G::Element },

    /// The leaf at `address` removed.
    Remove { address: Vec<u8>, element: G::Element },

    /// The leaf at `address` given another element: another payload under
    /// the same key.
    Replace { address: Vec<u8>, old: G::Element, new: G::Element },
}

/// Apply `edits` node by node, then settle the shape of every node whose
/// count they moved.
pub(super) async fn edit<K, L, G, S>(rows: &mut StagedRows<'_, K, L, G, S>, edits: Vec<Edit<G>>) -> Result<(), Error>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    let mut trie = TrieEdit { rows, children: HashMap::new(), moved: BTreeSet::new() };
    for edit in edits {
        match edit {
            Edit::Insert { address, element } => trie.insert(&address, &element).await?,
            Edit::Remove { address, element } => trie.remove(&address, &element).await?,
            Edit::Replace { address, old, new } => trie.replace(&address, &old, &new).await?,
        }
    }

    trie.settle().await
}

/// One batch's edits to the trie.
struct TrieEdit<'r, 's, K, L, G: DigestGroup, S> {
    rows: &'r mut StagedRows<'s, K, L, G, S>,

    /// Each node's children, read once per batch, since every walk passes the
    /// upper nodes, and kept as the walks insert branches.
    children: HashMap<NodePrefix, Option<[NodePrefix; 2]>>,

    /// The nodes whose counts moved, in prefix order.
    moved: BTreeSet<NodePrefix>,
}

/// Where a walk stopped short of a bucket: a node with children neither of
/// which holds its address, with the node's digest after the walk's update.
struct OutsideChildren<G: DigestGroup> {
    parent: NodePrefix,
    digest: Digest<G>,
    children: [NodePrefix; 2],
}

/// A member's address lies beneath a node with children but beneath neither.
fn beneath_neither() -> Error { Error::Inconsistent("a member lies beneath neither child of a node it lies beneath") }

impl<K, L, G, S> TrieEdit<'_, '_, K, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    /// Add a leaf's element to every node down to where it lies: a bucket, or
    /// a node with children neither of which holds it, where it gets a branch.
    async fn insert(&mut self, address: &[u8], element: &G::Element) -> Result<(), Error> {
        match self.walk(address, |digest| digest.add_element(element)).await? {
            None => Ok(()),
            Some(outside) => self.insert_branch(outside, address, element),
        }
    }

    /// Subtract a leaf's element from every node down to its bucket.
    async fn remove(&mut self, address: &[u8], element: &G::Element) -> Result<(), Error> {
        match self.walk(address, |digest| digest.sub_element(element)).await? {
            None => Ok(()),
            Some(_) => Err(beneath_neither()),
        }
    }

    /// Replace a leaf's element with another in every node down to its
    /// bucket.
    async fn replace(&mut self, address: &[u8], old: &G::Element, new: &G::Element) -> Result<(), Error> {
        let update = |digest: &mut Digest<G>| {
            digest.sub_element(old)?;
            digest.add_element(new)
        };

        match self.walk(address, update).await? {
            None => Ok(()),
            Some(_) => Err(beneath_neither()),
        }
    }

    /// Walk from the root toward `address`, updating every node's digest on
    /// the way, to the bucket that holds the address, or to a node with
    /// children neither of which does.
    async fn walk(
        &mut self,
        address: &[u8],
        update: impl Fn(&mut Digest<G>) -> Result<(), Error>,
    ) -> Result<Option<OutsideChildren<G>>, Error> {
        let mut prefix = NodePrefix::root();

        // The next node's digest, when finding its parent's children just
        // read it.
        let mut known = None;
        loop {
            let mut digest = match known.take() {
                Some(digest) => digest,
                None => self.rows.node(&prefix).await?.ok_or(Error::Inconsistent("a node the trie reaches has no row"))?,
            };

            let count = digest.count();
            update(&mut digest)?;
            self.rows.put(RowKey::Node(prefix.clone()), Row::Node(digest));

            if digest.count() != count {
                self.moved.insert(prefix.clone());
            }

            let Some((children, digests)) = self.children_of(&prefix).await? else { return Ok(None) };
            let Some(next) = children.iter().position(|child| child.contains_address(address)) else {
                return Ok(Some(OutsideChildren { parent: prefix, digest, children }));
            };

            known = digests.map(|digests| digests[next]);
            prefix = children[next].clone();
        }
    }

    /// A node's children, and their digests too when they were read from the
    /// rows just now rather than remembered.
    async fn children_of(&mut self, prefix: &NodePrefix) -> Result<Option<([NodePrefix; 2], Option<[Digest<G>; 2]>)>, Error> {
        if let Some(children) = self.children.get(prefix) {
            return Ok(children.clone().map(|children| (children, None)));
        }

        let read = Trie::new(self.rows).children(prefix).await?;
        self.children.insert(prefix.clone(), read.as_ref().map(|[first, second]| [first.prefix.clone(), second.prefix.clone()]));

        Ok(read.map(|[first, second]| ([first.prefix, second.prefix], Some([first.digest, second.digest]))))
    }

    /// Insert a branch beneath the node where the new leaf at `address` first
    /// differs from the bits its children share: one side holds the new leaf
    /// alone, the other the children, which keep their rows, since their
    /// prefixes already place them beneath it.
    fn insert_branch(&mut self, outside: OutsideChildren<G>, address: &[u8], element: &G::Element) -> Result<(), Error> {
        let OutsideChildren { parent, digest, children } = outside;
        let child = &children[0];
        let bit_of = |index: u32| address.get((index / 8) as usize).map(|byte| byte & (0x80 >> (index % 8)) != 0);

        // The children share every bit but their last, where they differ.
        let depth = match (parent.bit_len()..child.bit_len()).find(|&index| bit_of(index) != Some(child.bit(index))) {
            // Out of bits first: the address begins the children's bits.
            Some(depth) if bit_of(depth).is_none() => return Err(address_begins_another()),

            // Differing before the children's last bit, the leaf lies beside them.
            Some(depth) if depth + 1 < child.bit_len() => depth,

            // Agreeing with them up to their last bit, it would lie beneath one.
            _ => return Err(beneath_neither()),
        };

        let added = NodePrefix::of(address, depth + 1);
        let kept = NodePrefix::of(address, depth).child(child.bit(depth));

        let mut kept_digest = digest;
        kept_digest.sub_element(element)?;
        self.rows.put(RowKey::Node(kept.clone()), Row::Node(kept_digest));
        self.rows.put(RowKey::Node(added.clone()), Row::Node(Digest::of_element(*element)));

        let sides = if kept < added { [kept.clone(), added.clone()] } else { [added.clone(), kept.clone()] };
        self.children.insert(parent, Some(sides));
        self.children.insert(kept.clone(), Some(children));
        self.children.insert(added.clone(), None);
        self.moved.extend([kept, added]);

        Ok(())
    }

    /// Settle the shape of the nodes whose counts moved, ancestors first, so
    /// a merge or a collapse removes its descendants before they are reached.
    /// A node an ancestor removed is skipped, and a bucket has no node beneath
    /// it to be reached, so each node's children are as the walks left them.
    async fn settle(mut self) -> Result<(), Error> {
        for prefix in std::mem::take(&mut self.moved) {
            let Some(digest) = self.rows.node(&prefix).await? else { continue };
            self.settle_node(&prefix, &digest).await?;
        }

        Ok(())
    }

    /// Merge, collapse, split or keep the node at `prefix`, whose digest is
    /// `digest`. A collapse gives the node the shape of the side it keeps,
    /// which the same batch may have changed too, so the node settles again:
    /// a bucket grown past its capacity splits, and a subtree with a side the
    /// batch emptied collapses once more.
    async fn settle_node(&mut self, prefix: &NodePrefix, digest: &Digest<G>) -> Result<(), Error> {
        loop {
            match self.children_of(prefix).await? {
                Some(_) if digest.count() <= MERGE_AT => return self.delete_beneath(prefix).await,

                Some((children, _)) => {
                    if !self.collapse_empty_side(prefix, children).await? {
                        return Ok(());
                    }
                }

                None if digest.count() > BUCKET_CAPACITY => return split(self.rows, prefix, digest).await,
                None => return Ok(()),
            }
        }
    }

    /// Delete every node beneath `prefix`, leaving it one bucket.
    async fn delete_beneath(&mut self, prefix: &NodePrefix) -> Result<(), Error> {
        for node in Trie::new(self.rows).beneath(prefix, usize::MAX).await? {
            self.rows.delete(RowKey::Node(node.prefix));
        }

        Ok(())
    }

    /// A branch whose removals emptied one side no longer parts anything, so
    /// it gives way to the side that holds its members: the empty side's
    /// subtree goes, and so does the other side's node, whose children, or
    /// whose members if it was a bucket, become the branch's own. The branch
    /// already holds that side's digest. Whether a side was empty, and the
    /// branch gave way.
    async fn collapse_empty_side(&mut self, prefix: &NodePrefix, children: [NodePrefix; 2]) -> Result<bool, Error> {
        let mut counts = [0; 2];
        for (count, child) in counts.iter_mut().zip(&children) {
            *count = self.rows.node(child).await?.ok_or(Error::Inconsistent("a node the trie reaches has no row"))?.count();
        }

        let [first, second] = children;
        let (empty, held) = match counts {
            [0, _] => (first, second),
            [_, 0] => (second, first),
            _ => return Ok(false),
        };

        self.delete_beneath(&empty).await?;
        let promoted = self.children_of(&held).await?.map(|(children, _)| children);

        self.rows.delete(RowKey::Node(empty));
        self.rows.delete(RowKey::Node(held));
        self.children.insert(prefix.clone(), promoted);

        Ok(true)
    }
}

/// Lay out the subtree of a bucket grown past its capacity over the members
/// beneath it, whose sum is `digest`.
async fn split<K, L, G, S>(rows: &mut StagedRows<'_, K, L, G, S>, prefix: &NodePrefix, digest: &Digest<G>) -> Result<(), Error>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    let leaves = rows.leaves(&prefix.span(), ScanDirection::Forward, usize::MAX).await?;
    let placed: Vec<(&[u8], &G::Element)> = leaves.iter().map(|(address, leaf)| (address.as_slice(), &leaf.element)).collect();
    let mut nodes = lay_out::<G>(prefix, &placed)?;

    // The layout ends with the bucket's own node, summed from its members.
    if nodes.pop().map(|node| node.digest) != Some(*digest) {
        return Err(Error::Inconsistent("a bucket's digest differs from its members"));
    }

    for node in nodes {
        rows.put(RowKey::Node(node.prefix), Row::Node(node.digest));
    }

    Ok(())
}

/// Lay the whole tree out anew over the members `rows` hold, replacing every
/// node that differs and removing every node the new layout lacks.
pub(super) async fn lay_out_anew<K, L, G, S>(rows: &mut StagedRows<'_, K, L, G, S>) -> Result<(), Error>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    let leaves = rows.leaves(&Span::all(), ScanDirection::Forward, usize::MAX).await?;
    let placed: Vec<(&[u8], &G::Element)> = leaves.iter().map(|(address, leaf)| (address.as_slice(), &leaf.element)).collect();

    let mut nodes: BTreeMap<NodePrefix, Digest<G>> =
        lay_out::<G>(&NodePrefix::root(), &placed)?.into_iter().map(|node| (node.prefix, node.digest)).collect();

    let trie = Trie::new(rows);
    let root = trie.root().await?;
    let mut old = trie.beneath(&root.prefix, usize::MAX).await?;
    old.push(root);

    for node in old {
        match nodes.remove(&node.prefix) {
            Some(new) if new == node.digest => {}
            Some(new) => rows.put(RowKey::Node(node.prefix), Row::Node(new)),
            None => rows.delete(RowKey::Node(node.prefix)),
        }
    }

    for (prefix, digest) in nodes {
        rows.put(RowKey::Node(prefix), Row::Node(digest));
    }

    Ok(())
}

#[cfg(test)]
mod tests;
