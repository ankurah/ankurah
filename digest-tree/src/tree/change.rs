//! Leaf changes: applying one to a tree, with the entity lookup and every tree
//! node above the leaf maintained, bucket splits and merges included.

use std::collections::BTreeSet;
use std::ops::Bound;

use ankurah_core_types::EntityId;

use super::build::BUCKET_CAPACITY;
use super::TreeTransaction;
use crate::address::{CanonicalCodec, NodePrefix, TreeSpan};
use crate::digest::{Digest, DigestGroup, HeadHash, Leaf};
use crate::error::TreeError;
use crate::store::layout;
use crate::store::{LeafRow, MapAccess, MapName, NodeRow};

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
    pub(super) fn row<G: DigestGroup>(&self) -> LeafRow<G> {
        LeafRow {
            key: self.key.clone(),
            entity_id: self.entity_id,
            head: self.head,
            element: Leaf::new(&self.key, self.entity_id, self.head).element(),
        }
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
            CanonicalCodec.split(self.spec.key_spec(), &leaf.key)?;
        }
        let mut before_value = None;
        let before = match &change.before {
            Some(expected) => {
                let address = crate::leaf_address(&expected.key, expected.entity_id);
                let value = self.transaction.get(MapName::Leaves, &address).await?.ok_or(TreeError::Conflict)?;
                let row = layout::leaf_row::<G>(&address, &value)?;
                if row.head != expected.head {
                    return Err(TreeError::Conflict);
                }
                before_value = Some(value);
                Some(row)
            }
            None => None,
        };
        let after = change.after.as_ref().map(LeafInput::row::<G>);
        if let Some(row) = &after {
            let expected = before.as_ref().filter(|old| old.address() == row.address());
            if expected.is_none() && self.leaf(&row.key, row.entity_id).await?.is_some() {
                return Err(TreeError::Conflict);
            }
        }
        if before == after {
            return Ok(());
        }
        let root = NodePrefix::root();
        if self.transaction.get(MapName::Nodes, &root.to_bytes()).await?.is_none() {
            self.transaction.change_structure();
            if !self.leaves(&TreeSpan::all(), 1).await?.is_empty()
                || !self.transaction.scan(MapName::Nodes, (Bound::Unbounded, Bound::Unbounded), 1).await?.is_empty()
            {
                return Err(TreeError::Audit("tree rows exist without a root summary"));
            }
            self.put_node_base(&root, None, NodeRow { digest: Digest::empty() }).await?;
        }
        if let Some((old, new)) = before.as_ref().zip(after.as_ref()).filter(|(a, b)| a.address() == b.address()) {
            // Changing only the head preserves both the lookup and topology.
            let address = new.address();
            let path = self.path(&address).await?;
            self.transaction.put_if(MapName::Leaves, &address, before_value.as_deref(), &layout::leaf_value(new)).await?;
            return self.append_node_deltas(&path, Digest::from(new.element) - Digest::from(old.element)).await;
        }
        // Ancestors of only the removed leaf lose its contribution and
        // ancestors of only the added leaf gain its contribution. The
        // ancestors a move shares, a run down from the root, receive the
        // difference. Each of these deltas is encoded once, however deep.
        let removed_path = match &before {
            Some(row) => self.path(&row.address()).await?,
            None => Vec::new(),
        };
        let added_path = match &after {
            Some(row) => self.path(&row.address()).await?,
            None => Vec::new(),
        };
        let shared = removed_path.iter().zip(&added_path).take_while(|(removed, added)| removed == added).count();
        let removal = before.as_ref().map_or(Digest::empty(), |row| -Digest::from(row.element));
        let addition = after.as_ref().map_or(Digest::empty(), |row| Digest::from(row.element));
        if let Some(row) = &before {
            self.delete_leaf(row).await?;
        }
        if let Some(row) = &after {
            self.put_leaf(row.clone(), None).await?;
        }
        self.append_node_deltas(&removed_path[..shared], removal + addition).await?;
        self.append_node_deltas(&removed_path[shared..], removal).await?;
        self.append_node_deltas(&added_path[shared..], addition).await?;
        let affected: BTreeSet<NodePrefix> = removed_path.into_iter().chain(added_path).collect();
        // Ancestors precede descendants. A rebuilt ancestor may have removed
        // old descendants; skip those when reaching them.
        for prefix in affected {
            if self.transaction.get(MapName::Nodes, &prefix.to_bytes()).await?.is_none() {
                continue;
            }
            let children = self.child_prefixes(&prefix).await?;
            let removed = before.as_ref().is_some_and(|row| prefix.contains_address(&row.address()));
            let added = after.as_ref().is_some_and(|row| prefix.contains_address(&row.address()));
            let uncovered = after.as_ref().is_some_and(|row| {
                prefix.contains_address(&row.address())
                    && !children.is_empty()
                    && !children.iter().any(|p| p.contains_address(&row.address()))
            });
            if uncovered {
                self.insert_branch(&prefix, &children, after.as_ref().expect("the insertion is uncovered")).await?;
            } else if children.is_empty() && added && !removed {
                // Topology depends on row count; reading or rebuilding a leaf validates its contents.
                let leaves = self.transaction.count_up_to(MapName::Leaves, TreeSpan::under(&prefix).bounds(), BUCKET_CAPACITY + 1).await?;
                if leaves > BUCKET_CAPACITY {
                    self.rebuild_subtree(&prefix).await?;
                }
            } else if !children.is_empty() && removed && !added {
                let leaves =
                    self.transaction.count_up_to(MapName::Leaves, TreeSpan::under(&prefix).bounds(), BUCKET_CAPACITY / 2 + 1).await?;
                if leaves <= BUCKET_CAPACITY / 2 {
                    self.rebuild_subtree(&prefix).await?;
                }
            }
        }
        Ok(())
    }

    /// Splice two branches above the existing children. Prefix addresses
    /// already give the old descendants their new parent; their rows stay put.
    async fn insert_branch(&mut self, parent: &NodePrefix, children: &[NodePrefix], leaf: &LeafRow<G>) -> Result<(), TreeError> {
        self.transaction.change_structure();
        let address = leaf.address();
        let child = &children[0];
        let depth = (parent.bit_len()..child.bit_len())
            .find(|&bit| {
                let value = address.get((bit / 8) as usize).map(|byte| byte & (0x80 >> (bit % 8)) != 0);
                value != Some(child.bit(bit))
            })
            .ok_or(TreeError::Audit("new branch overlaps an existing child"))?;
        if depth as usize >= address.len() * 8 {
            return Err(TreeError::InvalidKey("leaf addresses must be prefix-free"));
        }
        let added = NodePrefix::of(&address, depth + 1);
        let retained = NodePrefix::of(&address, depth).child(child.bit(depth));
        if !children.iter().all(|child| retained.contains(child)) {
            return Err(TreeError::Audit("compressed children do not share one branch"));
        }
        let contribution = Digest::from(leaf.element);
        // The parent's pending delta already includes the new leaf. Its
        // other contribution is exactly the retained branch's summary.
        let digest = self.node(parent).await?.ok_or_else(|| TreeError::IncompleteNode(parent.clone()))?.digest;
        self.put_node_base(&retained, None, NodeRow { digest: digest.checked_sub(contribution)? }).await?;
        self.put_node_base(&added, None, NodeRow { digest: contribution }).await
    }

    async fn path(&mut self, address: &[u8]) -> Result<Vec<NodePrefix>, TreeError> {
        let mut prefix = NodePrefix::root();
        let mut path = Vec::new();
        loop {
            path.push(prefix.clone());
            let child = self.child_prefixes(&prefix).await?.into_iter().find(|p| p.contains_address(address));
            match child {
                Some(child) => prefix = child,
                None => return Ok(path),
            }
        }
    }
}

#[cfg(test)]
mod tests;
