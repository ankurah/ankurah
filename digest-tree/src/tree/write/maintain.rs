//! Keeping a tree's nodes right as its members change: node by node for a
//! small batch, or laid out anew for a large one.
//!
//! Node by node, each edit walks from the root to the member's bucket,
//! moving every digest on the way, and inserts a branch where an inserted
//! member lies outside a node's children. Once every edit has walked, the
//! nodes whose counts moved settle their shape: a bucket grown past its
//! capacity splits, and a subtree with children that one bucket could now
//! hold merges into one. Settling after the walks means a split reads the
//! batch's final members, which the digests it replaces already sum, and that
//! the walks change the shape only where they insert branches.

use std::collections::{BTreeMap, BTreeSet};

use crate::store::NodeRow;
use crate::tree::rows::{branches_outside, TreeRows};
use crate::tree::trie::{address_begins_another, lay_out, missing_node, Node, Trie, BUCKET_CAPACITY};
use crate::{Digest, DigestGroup, Error, NodePrefix, Payload, ReadTrx, Row, RowKey, ScanDirection, Span};

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
pub(super) async fn edit<K, L, G, R>(rows: &mut TreeRows<K, L, G, R>, edits: Vec<Edit<G>>) -> Result<(), Error>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    R: ReadTrx<RowKey, Row<K, L, G>>,
{
    let mut trie = TrieEdit { rows, moved: BTreeSet::new() };
    for edit in edits {
        match edit {
            Edit::Insert { address, element } => trie.insert(&address, &element).await?,
            Edit::Remove { address, element } => trie.remove(&address, &element).await?,
            Edit::Replace { address, old, new } => trie.replace(&address, &old, &new).await?,
        }
    }

    trie.settle().await
}

/// One batch's edits to the trie. Every node is read through `rows`, which
/// hold the batch's changes so far, so each record of where a node branches
/// is read as the batch has left it.
struct TrieEdit<'r, K, L, G: DigestGroup, R> {
    rows: &'r mut TreeRows<K, L, G, R>,

    /// The nodes whose counts moved, in prefix order: those whose shape may
    /// need settling, and the only ones the batch can have emptied.
    moved: BTreeSet<NodePrefix>,
}

/// Where a walk stopped short of a bucket: a node with children neither of
/// which holds its address, with the node's digest after the walk's update,
/// and the prefix its children share.
struct OutsideChildren<G: DigestGroup> {
    parent: NodePrefix,
    digest: Digest<G>,
    branches_at: NodePrefix,
}

/// A member's address lies beneath a node with children but beneath neither.
fn beneath_neither() -> Error { Error::Inconsistent("a member lies beneath neither child of a node it lies beneath") }

/// The bit of `address` at `index`, counting from the most significant, or
/// `None` past its end.
fn address_bit(address: &[u8], index: u32) -> Option<bool> {
    address.get((index / 8) as usize).map(|byte| byte & (0x80 >> (index % 8)) != 0)
}

impl<K, L, G, R> TrieEdit<'_, K, L, G, R>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    R: ReadTrx<RowKey, Row<K, L, G>>,
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
    /// children neither of which does. Each node's record of where it
    /// branches names the child the address lies beneath, so the walk reads
    /// one node row per level, by its key.
    async fn walk(
        &mut self,
        address: &[u8],
        update: impl Fn(&mut Digest<G>) -> Result<(), Error>,
    ) -> Result<Option<OutsideChildren<G>>, Error> {
        let mut prefix = NodePrefix::root();
        loop {
            let mut row = self.rows.node(&prefix).await?.ok_or_else(missing_node)?;

            let count = row.digest.count();
            update(&mut row.digest)?;
            self.rows.put(RowKey::Node(prefix.clone()), Row::Node(row.clone()));

            if row.digest.count() != count {
                self.moved.insert(prefix.clone());
            }

            let NodeRow { digest, branches_at: Some(branches_at) } = row else { return Ok(None) };
            if !branches_at.contains_address(address) {
                return Ok(Some(OutsideChildren { parent: prefix, digest, branches_at }));
            }

            // Every member beneath the branch holds a bit past the prefix its
            // children share; an address that ends there begins all of theirs.
            let next = address_bit(address, branches_at.bit_len()).ok_or_else(address_begins_another)?;
            prefix = branches_at.child(next);
        }
    }

    /// Insert a branch beneath the node where the new leaf at `address` first
    /// differs from the prefix its children share: one side holds the new leaf
    /// alone, the other the children, which keep their rows, since their
    /// prefixes already place them beneath it. The new side over the children
    /// records where they branch, and the node records the new branch.
    fn insert_branch(&mut self, outside: OutsideChildren<G>, address: &[u8], element: &G::Element) -> Result<(), Error> {
        let OutsideChildren { parent, digest, branches_at } = outside;

        // The address begins with the node's prefix, and the children's
        // shared prefix begins with it too, so they part beyond it.
        let parts_at = (parent.bit_len()..branches_at.bit_len()).find(|&index| address_bit(address, index) != Some(branches_at.bit(index)));
        let depth = match parts_at {
            // Out of bits first: the address begins the children's bits.
            Some(depth) if address_bit(address, depth).is_none() => return Err(address_begins_another()),

            Some(depth) => depth,

            // Agreeing with every bit past the node's own, the address would
            // lie beneath a child, unless the record lies outside the node,
            // which the read of its row refuses.
            None => return Err(branches_outside()),
        };

        let added = NodePrefix::of(address, depth + 1);
        let kept = NodePrefix::of(address, depth).child(branches_at.bit(depth));

        let mut kept_digest = digest;
        kept_digest.sub_element(element)?;

        self.rows.put(RowKey::Node(kept.clone()), Row::Node(NodeRow { digest: kept_digest, branches_at: Some(branches_at) }));
        self.rows.put(RowKey::Node(added.clone()), Row::Node(NodeRow { digest: Digest::of_element(*element), branches_at: None }));
        self.rows.put(RowKey::Node(parent), Row::Node(NodeRow { digest, branches_at: Some(NodePrefix::of(address, depth)) }));
        self.moved.extend([kept, added]);

        Ok(())
    }

    /// Settle the shape of the nodes whose counts moved, ancestors first, so
    /// a merge or a collapse removes its descendants before they are reached.
    /// A node an ancestor removed is skipped, and a bucket has no node beneath
    /// it to be reached, so each node's record names its children as the
    /// walks left them.
    async fn settle(mut self) -> Result<(), Error> {
        let moved = std::mem::take(&mut self.moved);
        for prefix in &moved {
            let Some(row) = self.rows.node(prefix).await? else { continue };
            self.settle_node(Node { prefix: prefix.clone(), row }, &moved).await?;
        }

        Ok(())
    }

    /// Merge, collapse, split or keep `node`, where the batch moved the counts
    /// of the nodes in `moved`. A collapse gives the node the shape of the
    /// side it keeps, which the same batch may have changed too, so the node
    /// settles again: a bucket grown past its capacity splits, and a subtree
    /// with a side the batch emptied collapses once more.
    async fn settle_node(&mut self, mut node: Node<G>, moved: &BTreeSet<NodePrefix>) -> Result<(), Error> {
        loop {
            let holds_more_than_a_bucket = node.row.digest.count() > BUCKET_CAPACITY;

            match (&node.row.branches_at, holds_more_than_a_bucket) {
                (Some(_), false) => return self.merge(node).await,

                (Some(_), true) => match self.collapse_empty_side(node, moved).await? {
                    Some(collapsed) => node = collapsed,
                    None => return Ok(()),
                },

                (None, true) => return split(self.rows, node).await,
                (None, false) => return Ok(()),
            }
        }
    }

    /// Merge the subtree of `node`, a node with children that one bucket
    /// could hold, into one bucket: every node beneath it goes, and the node,
    /// whose digest already sums the subtree, records no branch.
    async fn merge(&mut self, node: Node<G>) -> Result<(), Error> {
        self.delete_beneath(&node).await?;
        self.rows.put(RowKey::Node(node.prefix), Row::Node(NodeRow { digest: node.row.digest, branches_at: None }));

        Ok(())
    }

    /// Delete every node beneath `node`: none beneath a bucket, and beneath a
    /// branch its whole subtree, found by one range read.
    async fn delete_beneath(&mut self, node: &Node<G>) -> Result<(), Error> {
        if node.row.branches_at.is_none() {
            return Ok(());
        }

        for beneath in Trie::new(self.rows).beneath(&node.prefix).await? {
            self.rows.delete(RowKey::Node(beneath.prefix));
        }

        Ok(())
    }

    /// A branch whose removals emptied one side no longer parts anything, so
    /// it gives way to the side that holds its members: the empty side's
    /// subtree goes, and so does the other side's node, whose children, or
    /// whose members if it was a bucket, become the branch's own, as its
    /// record of where it branches does. The branch already holds that side's
    /// digest. The node as it collapsed, or `None` where neither side was
    /// empty.
    async fn collapse_empty_side(&mut self, node: Node<G>, moved: &BTreeSet<NodePrefix>) -> Result<Option<Node<G>>, Error> {
        let Some(branches_at) = &node.row.branches_at else { return Ok(None) };
        let [first, second] = [branches_at.child(false), branches_at.child(true)];

        let (empty, held) = match (self.emptied(&first, moved).await?, self.emptied(&second, moved).await?) {
            (Some(empty), _) => (empty, second),
            (_, Some(empty)) => (empty, first),
            _ => return Ok(None),
        };

        let held_row = self.rows.node(&held).await?.ok_or_else(missing_node)?;

        self.delete_beneath(&empty).await?;
        self.rows.delete(RowKey::Node(empty.prefix));
        self.rows.delete(RowKey::Node(held));

        let row = NodeRow { digest: node.row.digest, branches_at: held_row.branches_at };
        self.rows.put(RowKey::Node(node.prefix.clone()), Row::Node(row.clone()));

        Ok(Some(Node { prefix: node.prefix, row }))
    }

    /// The node at `prefix`, a side of a branch, if the batch emptied it.
    /// Only a node in `moved` can be empty, since every other holds the
    /// members it held, at least one, so no other is read: a commit that
    /// collapses nothing reads no node in settling beyond what its walks
    /// wrote.
    async fn emptied(&mut self, prefix: &NodePrefix, moved: &BTreeSet<NodePrefix>) -> Result<Option<Node<G>>, Error> {
        if !moved.contains(prefix) {
            return Ok(None);
        }

        let row = self.rows.node(prefix).await?.ok_or_else(missing_node)?;
        Ok((row.digest.count() == 0).then(|| Node { prefix: prefix.clone(), row }))
    }
}

/// Lay out the subtree of `node`, a bucket grown past its capacity, over the
/// members beneath it.
async fn split<K, L, G, R>(rows: &mut TreeRows<K, L, G, R>, node: Node<G>) -> Result<(), Error>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    R: ReadTrx<RowKey, Row<K, L, G>>,
{
    let leaves = rows.leaves(&node.prefix.span(), ScanDirection::Forward, usize::MAX).await?;
    let placed: Vec<(&[u8], &G::Element)> = leaves.iter().map(|(address, leaf)| (address.as_slice(), &leaf.element)).collect();
    let nodes = lay_out::<G>(&node.prefix, &placed)?;

    // The layout ends with the bucket's own node, summed from its members,
    // which now records where it branches.
    if nodes.last().map(|laid_out| laid_out.row.digest) != Some(node.row.digest) {
        return Err(Error::Inconsistent("a bucket's digest differs from its members"));
    }

    for laid_out in nodes {
        rows.put(RowKey::Node(laid_out.prefix), Row::Node(laid_out.row));
    }

    Ok(())
}

/// Lay the whole tree out anew over the members `rows` hold, replacing every
/// node whose row differs and removing every node the new layout lacks.
pub(super) async fn lay_out_anew<K, L, G, R>(rows: &mut TreeRows<K, L, G, R>) -> Result<(), Error>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    R: ReadTrx<RowKey, Row<K, L, G>>,
{
    let leaves = rows.leaves(&Span::all(), ScanDirection::Forward, usize::MAX).await?;
    let placed: Vec<(&[u8], &G::Element)> = leaves.iter().map(|(address, leaf)| (address.as_slice(), &leaf.element)).collect();

    let mut nodes: BTreeMap<NodePrefix, NodeRow<G>> =
        lay_out::<G>(&NodePrefix::root(), &placed)?.into_iter().map(|node| (node.prefix, node.row)).collect();

    let mut trie = Trie::new(rows);
    let root = trie.root().await?;
    let mut old = trie.beneath(&root.prefix).await?;
    old.push(root);

    for node in old {
        match nodes.remove(&node.prefix) {
            Some(new) if new == node.row => {}
            Some(new) => rows.put(RowKey::Node(node.prefix), Row::Node(new)),
            None => rows.delete(RowKey::Node(node.prefix)),
        }
    }

    for (prefix, row) in nodes {
        rows.put(RowKey::Node(prefix), Row::Node(row));
    }

    Ok(())
}

#[cfg(test)]
mod tests;
