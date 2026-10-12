//! Atomic leaf changes and maintenance of the summaries above them.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use ankurah_core_types::EntityId;

use crate::digest::{Digest, DigestGroup, HeadHash, Leaf};
use crate::layout;
use crate::map::{MapName, MapTransaction};
use crate::{AddressRange, CanonicalCodec, KeyCodec, LeafRow, NodePrefix, NodeRow, TreeError, TreeTransaction};

/// The target maximum number of leaves in a terminal bucket.
/// Merging at half this size avoids repeatedly splitting and merging near the threshold.
pub const BUCKET_CAPACITY: usize = 64;

/// The inputs of one leaf, before hashing it into the tree's digest group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafData {
    /// Canonical key bytes, empty for an entity-id tree.
    pub key: Vec<u8>,
    /// The entity at this address.
    pub entity_id: EntityId,
    /// Its current head hash.
    pub head: HeadHash,
}

impl LeafData {
    fn row<G: DigestGroup>(&self) -> LeafRow<G> {
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
    pub before: Option<LeafData>,
    /// The replacement leaf; absent for removal.
    pub after: Option<LeafData>,
}

/// Counts verified by a successful audit of one transaction's snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Audit {
    /// Number of leaves, which may exceed the number of entities.
    pub leaves: usize,
    /// Number of entities with at least one leaf.
    pub entities: usize,
    /// Number of cached summaries.
    pub nodes: usize,
}

impl<G: DigestGroup, T: MapTransaction> TreeTransaction<G, T> {
    /// Apply a leaf change and maintain its lookup and every affected summary.
    /// A failed operation poisons the transaction, so earlier buffered writes
    /// cannot accidentally be committed after an error.
    pub async fn apply_change(&mut self, change: LeafChange) -> Result<(), TreeError> {
        let result = self.change_leaf(change).await;
        self.failed |= result.is_err();
        self.generation += 1;
        result
    }

    async fn change_leaf(&mut self, change: LeafChange) -> Result<(), TreeError> {
        self.transaction.watch_structure();
        if let Some(spec) = &self.spec {
            for leaf in change.before.iter().chain(change.after.iter()) {
                CanonicalCodec.split(spec.key_spec(), &leaf.key)?;
            }
        }
        let before = match &change.before {
            Some(expected) => {
                let row = self.leaf(&expected.key, expected.entity_id).await?.ok_or(TreeError::Conflict)?;
                if row.head != expected.head {
                    return Err(TreeError::Conflict);
                }
                Some(row)
            }
            None => None,
        };
        let after = change.after.as_ref().map(LeafData::row::<G>);
        if let Some(row) = &after {
            let expected = before.as_ref().filter(|old| old.address() == row.address());
            if self.leaf(&row.key, row.entity_id).await?.as_ref() != expected {
                return Err(TreeError::Conflict);
            }
        }
        if before == after {
            return Ok(());
        }
        let root = NodePrefix::root();
        if self.transaction.get(MapName::Nodes, &root.to_bytes()).await?.is_none() {
            self.transaction.change_structure();
            if !self.leaves(&AddressRange::all(), 1).await?.is_empty()
                || !self.transaction.scan(MapName::Nodes, (Bound::Unbounded, Bound::Unbounded), 1).await?.is_empty()
            {
                return Err(TreeError::Audit("tree rows exist without a root summary"));
            }
            self.put_node_base(&root, None, NodeRow { digest: Digest::empty() }).await?;
        }
        // A move may share some ancestors. Combine contributions before
        // writing so each affected node receives one delta.
        let mut deltas: BTreeMap<NodePrefix, Digest<G>> = BTreeMap::new();
        let mut affected = BTreeSet::new();
        for (row, removing) in [(&before, true), (&after, false)] {
            if let Some(row) = row {
                let contribution = Digest::from(row.element);
                let contribution = if removing { -contribution } else { contribution };
                for prefix in self.path(&row.address()).await? {
                    *deltas.entry(prefix.clone()).or_default() += contribution;
                    affected.insert(prefix);
                }
            }
        }
        if let Some(row) = &before {
            self.delete_leaf(row).await?;
        }
        if let Some(row) = &after {
            self.put_leaf(row.clone(), None).await?;
        }
        for (prefix, delta) in deltas {
            if delta != Digest::empty() {
                self.append_node_delta(&prefix, delta).await?;
            }
        }
        if before.as_ref().zip(after.as_ref()).is_some_and(|(a, b)| a.address() == b.address()) {
            return Ok(());
        }
        // Ancestors precede descendants. A rebuilt ancestor may have removed
        // old descendants; skip those when reaching them.
        for prefix in affected {
            if self.transaction.get(MapName::Nodes, &prefix.to_bytes()).await?.is_none() {
                continue;
            }
            let children = self.child_prefixes(&prefix).await?;
            let sample = self.leaves(&AddressRange::under(&prefix), BUCKET_CAPACITY + 1).await?;
            let uncovered = after.as_ref().is_some_and(|row| {
                prefix.contains_address(&row.address())
                    && !children.is_empty()
                    && !children.iter().any(|p| p.contains_address(&row.address()))
            });
            let split = children.is_empty() && sample.len() > BUCKET_CAPACITY;
            let merge = !children.is_empty() && sample.len() <= BUCKET_CAPACITY / 2;
            if split || merge || uncovered {
                self.rebuild_subtree(&prefix).await?;
            }
        }
        Ok(())
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

    pub(crate) async fn all_leaves(&mut self, range: &AddressRange) -> Result<Vec<LeafRow<G>>, TreeError> {
        let mut rows = Vec::new();
        let mut remaining = range.clone();
        loop {
            let page = self.leaves(&remaining, 1024).await?;
            let done = page.len() < 1024;
            if let Some(last) = page.last() { remaining = remaining.after(&last.address()); }
            rows.extend(page);
            if done { return Ok(rows); }
        }
    }

    /// Replace the entire tree with these already-admitted leaves. Duplicate
    /// addresses are rejected. The replacement is atomic at commit.
    pub async fn build(&mut self, leaves: impl IntoIterator<Item = LeafData>) -> Result<(), TreeError> {
        let result = self.build_inner(leaves).await;
        self.failed |= result.is_err();
        self.generation += 1;
        result
    }

    async fn build_inner(&mut self, leaves: impl IntoIterator<Item = LeafData>) -> Result<(), TreeError> {
        self.transaction.change_structure();
        let mut rows = BTreeMap::new();
        for leaf in leaves {
            if let Some(spec) = &self.spec { CanonicalCodec.split(spec.key_spec(), &leaf.key)?; }
            let row = leaf.row::<G>();
            if rows.insert(row.address(), row).is_some() { return Err(TreeError::DuplicateLeaf); }
        }
        for map in [MapName::Leaves, MapName::Lookup, MapName::Nodes] {
            let entries = self.transaction.scan(map, (Bound::Unbounded, Bound::Unbounded), usize::MAX).await?;
            for (key, value) in entries { self.transaction.delete_if(map, &key, &value).await?; }
        }
        for row in rows.into_values() { self.put_leaf(row, None).await?; }
        self.rebuild_subtree(&NodePrefix::root()).await
    }

    async fn rebuild_subtree(&mut self, prefix: &NodePrefix) -> Result<(), TreeError> {
        // This fence is deliberately conservative: structural maintenance
        // retries after any intervening commit; ordinary leaf changes only
        // watch the topology and can append their deltas concurrently.
        self.transaction.change_structure();
        let leaves = self.all_leaves(&AddressRange::under(prefix)).await?;
        let start = prefix.to_bytes();
        let end = prefix.subtree_key_end();
        let entries = self.transaction.scan(MapName::Nodes,
            (Bound::Included(&start), end.as_deref().map_or(Bound::Unbounded, Bound::Excluded)), usize::MAX).await?;
        for (key, value) in entries { self.transaction.delete_if(MapName::Nodes, &key, &value).await?; }
        let mut pending = vec![(prefix.clone(), 0, leaves.len())];
        while let Some((prefix, from, to)) = pending.pop() {
            let rows = &leaves[from..to];
            let digest = rows.iter().map(|row| row.element).map(Digest::from).sum();
            self.put_node_base(&prefix, None, NodeRow { digest }).await?;
            if rows.len() <= BUCKET_CAPACITY { continue; }
            let first = rows.first().unwrap().address();
            let last = rows.last().unwrap().address();
            let common = first.iter().zip(&last).take_while(|(a, b)| a == b).count();
            if common == first.len().min(last.len()) {
                return Err(TreeError::InvalidKey("leaf addresses must be prefix-free"));
            }
            let depth = (common * 8) as u32 + (first[common] ^ last[common]).leading_zeros();
            let left = NodePrefix::of(&first, depth + 1);
            let right = NodePrefix::of(&last, depth + 1);
            let middle = from + rows.partition_point(|row| left.contains_address(&row.address()));
            pending.push((right, middle, to));
            pending.push((left, from, middle));
        }
        Ok(())
    }

    /// Rehash every leaf, verify both directions of the entity lookup, and
    /// compare every stored summary against the leaves under its prefix.
    /// This checks the current snapshot without repairing or changing it.
    pub async fn audit(&mut self) -> Result<Audit, TreeError> {
        let leaves = self.all_leaves(&AddressRange::all()).await?;
        let mut lookup: BTreeMap<EntityId, BTreeSet<Vec<u8>>> = BTreeMap::new();
        for row in &leaves {
            if let Some(spec) = &self.spec { CanonicalCodec.split(spec.key_spec(), &row.key)?; }
            if row.element != Leaf::new(&row.key, row.entity_id, row.head).element() {
                return Err(TreeError::Audit("leaf element differs from its inputs"));
            }
            lookup.entry(row.entity_id).or_default().insert(row.key.clone());
        }
        let entries = self.transaction.scan(MapName::Lookup, (Bound::Unbounded, Bound::Unbounded), usize::MAX).await?;
        if entries.iter().filter(|(key, _)| !key.is_empty()).count() != lookup.len() {
            return Err(TreeError::Audit("entity lookup cardinality differs"));
        }
        if self.multiple_key_entities().await? != lookup.values().filter(|keys| keys.len() > 1).count() as u64 {
            return Err(TreeError::Audit("multi-key entity count differs from leaves"));
        }
        for (id, keys) in &lookup {
            let stored = self.transaction.get(MapName::Lookup, &id.to_bytes()).await?;
            if stored.as_deref() != Some(layout::lookup_value(keys).as_slice()) {
                return Err(TreeError::Audit("entity lookup differs from leaves"));
            }
        }
        let entries = self.transaction.scan(MapName::Nodes, (Bound::Unbounded, Bound::Unbounded), usize::MAX).await?;
        let prefixes: BTreeSet<_> = entries.iter().map(|(key, _)| layout::node_key_prefix(key)).collect::<Result<_, _>>()?;
        if (!leaves.is_empty() || !prefixes.is_empty()) && !prefixes.contains(&NodePrefix::root()) {
            return Err(TreeError::Audit("leaves exist without a root summary"));
        }
        for prefix in &prefixes {
            let node = self.node(prefix).await?.ok_or(TreeError::Audit("missing node"))?;
            let beneath: Vec<_> = leaves.iter().filter(|row| prefix.contains_address(&row.address())).collect();
            let expected: Digest<G> = beneath.iter().map(|row| Digest::from(row.element)).sum();
            if node.digest != expected { return Err(TreeError::Audit("summary differs from leaves")); }
            let children = self.children(prefix).await?;
            if !children.is_empty() && beneath.iter().any(|row| !children.iter().any(|(p, _)| p.contains_address(&row.address()))) {
                return Err(TreeError::Audit("children do not cover their parent's leaves"));
            }
        }
        Ok(Audit { leaves: leaves.len(), entities: lookup.len(), nodes: prefixes.len() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::ast::Predicate;
    use ankurah_core_types::KeySpec;
    use crate::{DigestTree, Ristretto255, TreeSpec};

    #[tokio::test]
    async fn audit_detects_each_kind_of_derived_data_corruption() {
        let tree = DigestTree::<Ristretto255>::new(TreeSpec::new(KeySpec::new(vec![]), Predicate::True));
        let data = LeafData { key: vec![], entity_id: EntityId::from_bytes([1; 32]), head: HeadHash::of([[2; 32]]) };
        let mut setup = tree.begin();
        setup.build([data.clone()]).await.unwrap();
        setup.commit().await.unwrap();
        for kind in 0..4 {
            let mut tx = tree.begin();
            let (map, key, value) = match kind {
                0 => {
                    let mut row = data.row::<Ristretto255>();
                    row.head = HeadHash::of([[3; 32]]);
                    (MapName::Leaves, row.address(), layout::leaf_value(&row))
                }
                1 => (MapName::Lookup, data.entity_id.to_bytes().to_vec(), layout::lookup_value(&BTreeSet::new())),
                2 => (MapName::Nodes, vec![], layout::digest_value(&Digest::<Ristretto255>::empty())),
                _ => (MapName::Lookup, layout::MULTIPLE_KEYS.to_vec(), 1u64.to_be_bytes().to_vec()),
            };
            let old = tx.transaction.get(map, &key).await.unwrap();
            tx.transaction.put_if(map, &key, old.as_deref(), &value).await.unwrap();
            assert!(tx.audit().await.is_err(), "corruption {kind}");
        }
        tree.begin().audit().await.unwrap();
        let mut invalid = tree.begin();
        assert!(matches!(invalid.build([data.clone(), data]).await, Err(TreeError::DuplicateLeaf)));
        assert!(matches!(invalid.commit().await, Err(TreeError::Conflict)));
        assert_eq!(tree.begin().audit().await.unwrap().leaves, 1);
    }
}
