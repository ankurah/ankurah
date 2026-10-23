//! Leaf changes: applying one to a tree, with the entity lookup and every tree
//! node above the leaf maintained, bucket splits and merges included.

use std::collections::BTreeMap;
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
        let result = guard.change_leaf(change).await;
        guard.finish(result)
    }

    async fn change_leaf(&mut self, change: LeafChange) -> Result<(), TreeError> {
        self.transaction.watch_structure();
        for leaf in change.before.iter().chain(change.after.iter()) {
            CanonicalCodec::split(self.spec.key_spec(), &leaf.key)?;
        }
        let before = match change.before {
            Some(expected) => Some(self.expected_leaf(expected).await?),
            None => None,
        };
        let after = change.after.map(|input| {
            let address = leaf_address(&input.key, input.entity_id);
            (address, input.into_row::<G>())
        });
        if let Some((address, _)) = &after {
            let replaces = before.as_ref().is_some_and(|before| before.address() == address.as_slice());
            if !replaces && self.transaction.get(MapName::Leaves, address).await?.is_some() {
                return Err(TreeError::Conflict);
            }
        }
        if before.as_ref().map(EncodedLeaf::row) == after.as_ref().map(|(_, row)| row) {
            return Ok(());
        }
        self.ensure_root().await?;
        match (before, after) {
            (Some(before), Some((address, row))) if before.address() == address.as_slice() => {
                // Changing only the head preserves both the lookup and topology.
                let path = self.path(&address).await?;
                let difference = Digest::from(row.element) - Digest::from(before.row().element);
                self.replace_leaf(&before, &EncodedLeaf::new(address, row)).await?;
                self.append_node_deltas(&path.prefixes, difference).await
            }
            (before, after) => self.refile_leaf(before, after.map(|(address, row)| EncodedLeaf::new(address, row))).await,
        }
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

    /// Remove `before`, as the transaction read it, insert `after`, and keep
    /// every tree node above either right: its summary, and the topology,
    /// splitting a bucket grown past its capacity, merging a subtree shrunk
    /// to half of it, and inserting a branch where no child holds the new leaf.
    async fn refile_leaf(&mut self, before: Option<EncodedLeaf<G>>, after: Option<EncodedLeaf<G>>) -> Result<(), TreeError> {
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
        if let Some(leaf) = &before {
            self.delete_leaf(leaf).await?;
        }
        if let Some(leaf) = &after {
            self.insert_leaf(leaf).await?;
        }
        self.append_node_deltas(&removed_path.prefixes[..shared], removal + addition).await?;
        self.append_node_deltas(&removed_path.prefixes[shared..], removal).await?;
        self.append_node_deltas(&added_path.prefixes[shared..], addition).await?;
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
                    self.insert_branch(&prefix, &children, address, element).await?;
                }
                Some(_) if children.is_empty() && !removed_beneath => {
                    // Topology depends on row count; reading or rebuilding a leaf validates its contents.
                    let leaves =
                        self.transaction.count_up_to(MapName::Leaves, TreeSpan::under(&prefix).bounds(), BUCKET_CAPACITY + 1).await?;
                    if leaves > BUCKET_CAPACITY {
                        self.rebuild_subtree(&prefix).await?;
                    }
                }
                None if !children.is_empty() && removed_beneath => {
                    let leaves =
                        self.transaction.count_up_to(MapName::Leaves, TreeSpan::under(&prefix).bounds(), BUCKET_CAPACITY / 2 + 1).await?;
                    if leaves <= BUCKET_CAPACITY / 2 {
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
