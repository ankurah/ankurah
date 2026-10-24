//! Leaf changes: applying one or a batch to a tree, with the entity lookup and
//! every tree node above each leaf maintained, bucket splits and merges
//! included.
//!
//! A change decodes the leaf it expects, encodes the leaf it files, and walks
//! the topology above both without decoding a tree node's digest. Applied
//! alone, it appends one delta, encoded once, under every ancestor whose sum
//! it changes; in a batch, each tree node's differences are summed in memory
//! and its base written once. An insertion, removal or move may also split a
//! bucket or merge a subtree, which reads the bounded set of leaves beneath
//! it, or insert a branch, which reads its parent's summary.

use std::collections::BTreeMap;
use std::mem;
use std::ops::Bound;

use ankurah_core_types::EntityId;

use super::build::BUCKET_CAPACITY;
use super::TreeTransaction;
use crate::address::{leaf_address, CanonicalCodec, NodePrefix, TreeSpan};
use crate::digest::{Digest, DigestGroup, HeadHash, Leaf, LeafElement};
use crate::error::TreeError;
use crate::store::{EncodedLeaf, LeafRow, MapAccess, MapName};

/// The inputs of one leaf, before hashing it into the tree's digest group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafInput {
    /// Canonical key bytes, empty for an entity-id tree.
    pub key: Vec<u8>,
    /// The entity at this address.
    pub entity_id: EntityId,
    /// Its current head hash.
    pub head: HeadHash,
}

impl LeafInput {
    /// The leaf row these inputs make, its element hashed in the group `G`.
    pub(super) fn into_row<G: DigestGroup>(self) -> LeafRow<G> {
        let element = Leaf::new(&self.key, self.entity_id, self.head).element();
        LeafRow { key: self.key, entity_id: self.entity_id, head: self.head, element }
    }

    /// The leaf these inputs make, filed at its address and encoded.
    pub(super) fn encode<G: DigestGroup>(self) -> EncodedLeaf<G> {
        let address = leaf_address(&self.key, self.entity_id);
        EncodedLeaf::new(address, self.into_row())
    }
}

/// Insert, remove, move, or replace a leaf. `before` must match the current
/// leaf exactly; a stale change fails the entire transaction.
#[derive(Debug, Clone)]
pub struct LeafChange {
    /// The expected old leaf; absent for insertion.
    pub before: Option<LeafInput>,
    /// The replacement leaf; absent for removal.
    pub after: Option<LeafInput>,
}

/// A change checked against the stored leaves: the leaf it removes or
/// replaces, as stored, and the leaf it files in its place.
pub(super) struct ResolvedChange<G: DigestGroup> {
    pub(super) before: Option<EncodedLeaf<G>>,
    pub(super) after: Option<EncodedLeaf<G>>,
}

/// Where a change records the difference it makes to each tree node above
/// its leaf.
enum NodeDifferences<G: DigestGroup> {
    /// Appended as a delta row for each tree node, so that concurrent changes
    /// never conflict on a base row.
    Appended,
    /// Summed for each tree node in memory, and folded into its base row,
    /// with any delta rows there, when the batch ends or before the topology
    /// changes, since a topology change reads and writes base rows.
    Summed(BTreeMap<NodePrefix, Digest<G>>),
}

/// The tree nodes from the root down to the deepest one whose prefix an
/// address begins with, and each one's children, as one walk read them.
#[derive(Default)]
struct Path {
    /// The tree nodes' prefixes, the root's first.
    prefixes: Vec<NodePrefix>,
    /// The children of the tree node at the same place in `prefixes`.
    children: Vec<Vec<NodePrefix>>,
}

impl Path {
    /// Each tree node's prefix with its children.
    fn into_nodes(self) -> impl Iterator<Item = (NodePrefix, Vec<NodePrefix>)> { self.prefixes.into_iter().zip(self.children) }
}

impl<G: DigestGroup, T: MapAccess> TreeTransaction<G, T> {
    /// Apply a leaf change and maintain its lookup and every affected summary.
    /// A failed or cancelled operation poisons the transaction, so partial
    /// buffered writes cannot accidentally be committed.
    pub async fn apply_change(&mut self, change: LeafChange) -> Result<(), TreeError> {
        self.generation += 1;
        let mut guard = self.mutation_guard();
        let result = guard.change_leaf(change, &mut NodeDifferences::Appended).await;
        guard.finish(result)
    }

    /// Apply a batch of leaf changes, each as [`TreeTransaction::apply_change`]
    /// applies it, and leave no delta row on any tree node they touched: each
    /// tree node's differences are summed in memory and written to its base
    /// row once, with any delta rows there folded in. The base writes make
    /// two such batches over one tree node conflict, so this suits a tree's
    /// only writer; changes from writers that run at once go through
    /// [`TreeTransaction::apply_change`]. A failed or cancelled batch poisons
    /// the transaction.
    pub async fn apply_changes(&mut self, changes: impl IntoIterator<Item = LeafChange>) -> Result<(), TreeError> {
        self.generation += 1;
        let mut guard = self.mutation_guard();
        let result = guard.change_leaves(changes).await;
        guard.finish(result)
    }

    async fn change_leaves(&mut self, changes: impl IntoIterator<Item = LeafChange>) -> Result<(), TreeError> {
        // Collected before the first await, so that a caller's iterator, which
        // may borrow from it, is not held across awaits: a future that holds
        // one cannot always be proven `Send` (rust-lang/rust#100013).
        let changes: Vec<LeafChange> = changes.into_iter().collect();
        let mut differences = NodeDifferences::Summed(BTreeMap::new());
        for change in changes {
            self.change_leaf(change, &mut differences).await?;
        }
        self.settle(&mut differences).await
    }

    async fn change_leaf(&mut self, change: LeafChange, differences: &mut NodeDifferences<G>) -> Result<(), TreeError> {
        self.transaction.watch_structure();
        let Some(change) = self.resolve(change.before, change.after.map(LeafInput::encode)).await? else { return Ok(()) };
        self.ensure_root().await?;
        self.write_leaves(&change).await?;
        match (&change.before, &change.after) {
            (Some(before), Some(after)) if before.address() == after.address() => {
                // A change of head alone keeps the lookup and the topology.
                let path = self.path(after.address()).await?;
                let difference = Digest::from(after.row().element) - Digest::from(before.row().element);
                self.record(differences, &path.prefixes, difference).await
            }
            _ => self.refile_nodes(&change, differences).await,
        }
    }

    /// Check a change against the stored leaves: `before` must name the leaf
    /// stored at its address, with its head, and `after` must find its
    /// address free unless it replaces `before` there:
    /// [`TreeError::Conflict`] otherwise. `None` for a change that changes
    /// nothing.
    pub(super) async fn resolve(
        &mut self,
        before: Option<LeafInput>,
        after: Option<EncodedLeaf<G>>,
    ) -> Result<Option<ResolvedChange<G>>, TreeError> {
        let keys = before.iter().map(|input| &input.key).chain(after.iter().map(|leaf| &leaf.row().key));
        for key in keys {
            CanonicalCodec::check(self.spec.key_spec(), key)?;
        }
        let before = match before {
            Some(expected) => Some(self.expected_leaf(expected).await?),
            None => None,
        };
        if let Some(after) = &after {
            let replaces = before.as_ref().is_some_and(|before| before.address() == after.address());
            if !replaces && self.transaction.get(MapName::Leaves, after.address()).await?.is_some() {
                return Err(TreeError::Conflict);
            }
        }
        if before.as_ref().map(EncodedLeaf::row) == after.as_ref().map(EncodedLeaf::row) {
            return Ok(None);
        }
        Ok(Some(ResolvedChange { before, after }))
    }

    /// Write a change's leaves: the new leaf over the stored one where the
    /// change keeps its address, and otherwise the stored leaf removed and
    /// the new one inserted, with the entity lookup kept in step.
    pub(super) async fn write_leaves(&mut self, change: &ResolvedChange<G>) -> Result<(), TreeError> {
        match (&change.before, &change.after) {
            (Some(before), Some(after)) if before.address() == after.address() => self.replace_leaf(before, after).await,
            (before, after) => {
                if let Some(leaf) = before {
                    self.delete_leaf(leaf).await?;
                }
                if let Some(leaf) = after {
                    self.insert_leaf(leaf).await?;
                }
                Ok(())
            }
        }
    }

    /// Record that a change moved the digest of the leaves beneath each of
    /// `prefixes` by `difference`, as `differences` records it.
    async fn record(
        &mut self,
        differences: &mut NodeDifferences<G>,
        prefixes: &[NodePrefix],
        difference: Digest<G>,
    ) -> Result<(), TreeError> {
        match differences {
            NodeDifferences::Appended => self.append_node_deltas(prefixes, difference).await,
            NodeDifferences::Summed(sums) => {
                if difference != Digest::empty() {
                    for prefix in prefixes {
                        let sum = sums.entry(prefix.clone()).or_default();
                        *sum = sum.checked_add(difference)?;
                    }
                }
                Ok(())
            }
        }
    }

    /// Write every summed difference into its tree node's base row, which
    /// leaves no difference pending: before the topology changes, and when a
    /// batch ends.
    async fn settle(&mut self, differences: &mut NodeDifferences<G>) -> Result<(), TreeError> {
        if let NodeDifferences::Summed(sums) = differences {
            for (prefix, sum) in mem::take(sums) {
                self.fold_into_base(&prefix, sum).await?;
            }
        }
        Ok(())
    }

    /// The leaf `expected` names, as stored: [`TreeError::Conflict`] when no
    /// row is filed there, or one with another head.
    async fn expected_leaf(&mut self, expected: LeafInput) -> Result<EncodedLeaf<G>, TreeError> {
        let address = leaf_address(&expected.key, expected.entity_id);
        let value = self.transaction.get(MapName::Leaves, &address).await?.ok_or(TreeError::Conflict)?;
        let stored = EncodedLeaf::decode(address, value)?;
        if stored.row().head != expected.head {
            return Err(TreeError::Conflict);
        }
        Ok(stored)
    }

    /// Insert the empty root's base row where there is none, which only an
    /// empty tree may lack.
    async fn ensure_root(&mut self) -> Result<(), TreeError> {
        let root = NodePrefix::root();
        if self.transaction.get(MapName::Nodes, &root.to_bytes()).await?.is_some() {
            return Ok(());
        }
        self.transaction.change_structure();
        if !self.leaves(&TreeSpan::all(), 1).await?.is_empty()
            || !self.transaction.scan(MapName::Nodes, (Bound::Unbounded, Bound::Unbounded), 1).await?.is_empty()
        {
            return Err(TreeError::Inconsistent("tree rows exist without a root summary"));
        }
        self.insert_node_base(&root, Digest::empty()).await
    }

    /// Keep every tree node above a removed leaf and an inserted one right,
    /// once the leaves are written: its summary, and the topology, splitting
    /// a bucket grown past its capacity, merging a subtree shrunk to half of
    /// it, and inserting a branch where no child holds the new leaf.
    async fn refile_nodes(&mut self, change: &ResolvedChange<G>, differences: &mut NodeDifferences<G>) -> Result<(), TreeError> {
        let ResolvedChange { before, after } = change;
        // Ancestors of only the removed leaf lose its contribution and
        // ancestors of only the added leaf gain its contribution. The
        // ancestors a move shares, a run down from the root, receive the
        // difference. Each of these deltas is encoded once, however deep.
        let removed_path = match &before {
            Some(leaf) => self.path(leaf.address()).await?,
            None => Path::default(),
        };
        let added_path = match &after {
            Some(leaf) => self.path(leaf.address()).await?,
            None => Path::default(),
        };
        let shared = removed_path.prefixes.iter().zip(&added_path.prefixes).take_while(|(removed, added)| removed == added).count();
        let removal = before.as_ref().map_or(Digest::empty(), |leaf| -Digest::from(leaf.row().element));
        let addition = after.as_ref().map_or(Digest::empty(), |leaf| Digest::from(leaf.row().element));
        self.record(differences, &removed_path.prefixes[..shared], removal + addition).await?;
        self.record(differences, &removed_path.prefixes[shared..], removal).await?;
        self.record(differences, &added_path.prefixes[shared..], addition).await?;
        let removed_address = before.as_ref().map(EncodedLeaf::address);
        let added = after.as_ref().map(|leaf| (leaf.address(), leaf.row().element));
        // The children each path's walk read stay right until a rebuild,
        // which removes the rows beneath it. Ancestors precede descendants, so
        // a descendant a rebuilt ancestor removed is skipped when reached.
        let affected: BTreeMap<NodePrefix, Vec<NodePrefix>> = removed_path.into_nodes().chain(added_path.into_nodes()).collect();
        for (prefix, children) in affected {
            if self.transaction.get(MapName::Nodes, &prefix.to_bytes()).await?.is_none() {
                continue;
            }
            let removed_beneath = removed_address.is_some_and(|address| prefix.contains_address(address));
            let added_beneath = added.filter(|(address, _)| prefix.contains_address(address));
            match added_beneath {
                Some((address, element)) if !children.is_empty() && !children.iter().any(|child| child.contains_address(address)) => {
                    self.settle(differences).await?;
                    self.insert_branch(&prefix, &children, address, element).await?;
                }
                Some(_) if children.is_empty() && !removed_beneath => {
                    // The topology depends on the count of leaves alone, which
                    // decodes none; reading or rebuilding a leaf validates it.
                    let leaves =
                        self.transaction.count_up_to(MapName::Leaves, TreeSpan::under(&prefix).bounds(), BUCKET_CAPACITY + 1).await?;
                    if leaves > BUCKET_CAPACITY {
                        self.settle(differences).await?;
                        self.rebuild_subtree(&prefix).await?;
                    }
                }
                None if !children.is_empty() && removed_beneath => {
                    let leaves =
                        self.transaction.count_up_to(MapName::Leaves, TreeSpan::under(&prefix).bounds(), BUCKET_CAPACITY / 2 + 1).await?;
                    if leaves <= BUCKET_CAPACITY / 2 {
                        self.settle(differences).await?;
                        self.rebuild_subtree(&prefix).await?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Splice two branches above the existing children: one holding the new
    /// leaf at `address`, whose element is `element`, and one above the
    /// children. Prefix addresses already give the old descendants their new
    /// parent; their rows stay put.
    async fn insert_branch(
        &mut self,
        parent: &NodePrefix,
        children: &[NodePrefix],
        address: &[u8],
        element: LeafElement<G>,
    ) -> Result<(), TreeError> {
        self.transaction.change_structure();
        let child = &children[0];
        let depth = (parent.bit_len()..child.bit_len())
            .find(|&bit| {
                let value = address.get((bit / 8) as usize).map(|byte| byte & (0x80 >> (bit % 8)) != 0);
                value != Some(child.bit(bit))
            })
            .ok_or(TreeError::Inconsistent("new branch overlaps an existing child"))?;
        if depth as usize >= address.len() * 8 {
            return Err(TreeError::InvalidKey("leaf addresses must be prefix-free"));
        }
        let added = NodePrefix::of(address, depth + 1);
        let retained = NodePrefix::of(address, depth).child(child.bit(depth));
        if !children.iter().all(|child| retained.contains(child)) {
            return Err(TreeError::Inconsistent("compressed children do not share one branch"));
        }
        let contribution = Digest::from(element);
        // The parent's pending delta already includes the new leaf. Its
        // other contribution is exactly the retained branch's summary.
        let digest = self.node(parent).await?.ok_or_else(|| TreeError::IncompleteNode(parent.clone()))?;
        self.insert_node_base(&retained, digest.checked_sub(contribution)?).await?;
        self.insert_node_base(&added, contribution).await
    }

    /// The tree nodes from the root down to the deepest one whose prefix
    /// `address` begins with, each with its children.
    async fn path(&mut self, address: &[u8]) -> Result<Path, TreeError> {
        let mut prefix = NodePrefix::root();
        let mut path = Path::default();
        loop {
            let children = self.child_prefixes(&prefix).await?;
            let next = children.iter().find(|child| child.contains_address(address)).cloned();
            path.prefixes.push(prefix);
            path.children.push(children);
            match next {
                Some(child) => prefix = child,
                None => return Ok(path),
            }
        }
    }
}

#[cfg(test)]
mod tests;
