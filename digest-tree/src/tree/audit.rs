//! The audit: a check of every row a tree derives from its leaves against
//! the leaves themselves.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use ankurah_core_types::EntityId;

use super::TreeTransaction;
use crate::address::{CanonicalCodec, NodePrefix, TreeSpan};
use crate::digest::{Digest, DigestGroup, Leaf};
use crate::error::TreeError;
use crate::store::layout;
use crate::store::{MapAccess, MapName};

/// Counts verified by a successful audit of one transaction's snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditCounts {
    /// Number of leaves, which may exceed the number of entities.
    pub leaves: usize,
    /// Number of entities with at least one leaf.
    pub entities: usize,
    /// Number of cached summaries.
    pub nodes: usize,
}

impl<G: DigestGroup, T: MapAccess> TreeTransaction<G, T> {
    /// Rehash every leaf, verify both directions of the entity lookup, and
    /// compare every stored summary against the leaves under its prefix.
    /// This checks the current snapshot without repairing or changing it.
    pub async fn audit(&mut self) -> Result<AuditCounts, TreeError> {
        let leaves = self.all_leaves(&TreeSpan::all()).await?;
        let mut lookup: BTreeMap<EntityId, BTreeSet<Vec<u8>>> = BTreeMap::new();
        for row in &leaves {
            CanonicalCodec::split(self.spec.key_spec(), &row.key)?;
            if row.element != Leaf::new(&row.key, row.entity_id, row.head).element() {
                return Err(TreeError::Inconsistent("leaf element differs from its inputs"));
            }
            lookup.entry(row.entity_id).or_default().insert(row.key.clone());
        }
        let entries = self.transaction.scan(MapName::Lookup, (Bound::Unbounded, Bound::Unbounded), usize::MAX).await?;
        if entries.iter().filter(|(key, _)| !key.is_empty()).count() != lookup.len() {
            return Err(TreeError::Inconsistent("entity lookup cardinality differs"));
        }
        if self.multiple_key_entities().await? != lookup.values().filter(|keys| keys.len() > 1).count() as u64 {
            return Err(TreeError::Inconsistent("multi-key entity count differs from leaves"));
        }
        for (id, keys) in &lookup {
            let stored = self.transaction.get(MapName::Lookup, &id.to_bytes()).await?;
            if stored.as_deref() != Some(layout::lookup_value(keys).as_slice()) {
                return Err(TreeError::Inconsistent("entity lookup differs from leaves"));
            }
        }
        let entries = self.transaction.scan(MapName::Nodes, (Bound::Unbounded, Bound::Unbounded), usize::MAX).await?;
        let prefixes: BTreeSet<_> = entries.iter().map(|(key, _)| layout::node_key_prefix(key)).collect::<Result<_, _>>()?;
        if (!leaves.is_empty() || !prefixes.is_empty()) && !prefixes.contains(&NodePrefix::root()) {
            return Err(TreeError::Inconsistent("leaves exist without a root summary"));
        }
        for prefix in &prefixes {
            let digest = self.node(prefix).await?.ok_or(TreeError::Inconsistent("missing node"))?;
            let beneath: Vec<_> = leaves.iter().filter(|row| prefix.contains_address(&row.address())).collect();
            let expected: Digest<G> = beneath.iter().map(|row| Digest::from(row.element)).sum();
            if digest != expected {
                return Err(TreeError::Inconsistent("summary differs from leaves"));
            }
            let children = self.children(prefix).await?;
            if !children.is_empty() && beneath.iter().any(|row| !children.iter().any(|(p, _)| p.contains_address(&row.address()))) {
                return Err(TreeError::Inconsistent("children do not cover their parent's leaves"));
            }
        }
        Ok(AuditCounts { leaves: leaves.len(), entities: lookup.len(), nodes: prefixes.len() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest::HeadHash;
    use crate::{DigestTree, LeafInput, Ristretto255, TreeSpec};
    use ankql::ast::Predicate;
    use ankurah_core_types::KeySpec;

    #[tokio::test]
    async fn audit_detects_each_kind_of_derived_data_corruption() {
        let tree = DigestTree::<Ristretto255>::new(TreeSpec::new(KeySpec::new(vec![]), Predicate::True));
        let data = LeafInput { key: vec![], entity_id: EntityId::from_bytes([1; 32]), head: HeadHash::of([[2; 32]]) };
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
        assert!(matches!(invalid.commit().await, Err(TreeError::Poisoned)));
        assert_eq!(tree.begin().audit().await.unwrap().leaves, 1);
    }
}
