//! Atomic leaf changes and maintenance of the summaries above them.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use ankurah_core_types::EntityId;

use crate::digest::{Digest, DigestGroup, HeadHash, Leaf};
use crate::layout;
use crate::map::{MapAccess, MapName};
use crate::tree::PreparedLeaf;
use crate::{CanonicalCodec, LeafRow, NodePrefix, NodeRow, TreeError, TreeSpan, TreeTransaction};

#[cfg(not(target_arch = "wasm32"))]
const PARALLEL_PREPARATION_MIN_LEAVES: usize = 1024;
const PREPARATION_BATCH_LEAVES: usize = 1024;

#[cfg(not(target_arch = "wasm32"))]
fn preparation_pool() -> Option<&'static rayon::ThreadPool> {
    use std::sync::OnceLock;

    static POOL: OnceLock<Option<rayon::ThreadPool>> = OnceLock::new();
    POOL.get_or_init(|| {
        let workers = std::thread::available_parallelism().map_or(1, |count| count.get().min(4));
        if workers == 1 {
            return None;
        }
        rayon::ThreadPoolBuilder::new().num_threads(workers).build().ok()
    })
    .as_ref()
}

fn prepare_leaves<G: DigestGroup>(leaves: Vec<LeafData>) -> Vec<PreparedLeaf<G>> {
    #[cfg(not(target_arch = "wasm32"))]
    if leaves.len() >= PARALLEL_PREPARATION_MIN_LEAVES {
        use rayon::prelude::*;

        if let Some(pool) = preparation_pool() {
            return pool.install(|| leaves.into_par_iter().map(|leaf| PreparedLeaf::new(leaf.row::<G>())).collect());
        }
    }
    leaves.into_iter().map(|leaf| PreparedLeaf::new(leaf.row::<G>())).collect()
}

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

impl<G: DigestGroup, T: MapAccess> TreeTransaction<G, T> {
    /// Apply a leaf change and maintain its lookup and every affected summary.
    /// A failed or cancelled operation poisons the transaction, so partial
    /// buffered writes cannot accidentally be committed.
    pub async fn apply_change(&mut self, change: LeafChange) -> Result<(), TreeError> {
        self.generation += 1;
        let mut mutation = self.mutation();
        let result = mutation.change_leaf(change).await;
        mutation.finish(result)
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
        let after = change.after.as_ref().map(LeafData::row::<G>);
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
            // Encode the common difference once for all ancestors.
            let address = new.address();
            let path = self.path(&address).await?;
            self.transaction.put_if(MapName::Leaves, &address, before_value.as_deref(), &layout::leaf_value(new)).await?;
            let delta = layout::digest_value(&(Digest::from(new.element) - Digest::from(old.element)));
            for prefix in path {
                let (key_prefix, _) = layout::delta_key_bounds(&prefix);
                self.transaction.append(MapName::Nodes, &key_prefix, &delta).await?;
            }
            return Ok(());
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
                let sample = self.transaction.scan(MapName::Leaves, TreeSpan::under(&prefix).bounds(), BUCKET_CAPACITY + 1).await?;
                if sample.len() > BUCKET_CAPACITY {
                    self.rebuild_subtree(&prefix).await?;
                }
            } else if !children.is_empty() && removed && !added {
                let sample = self.transaction.scan(MapName::Leaves, TreeSpan::under(&prefix).bounds(), BUCKET_CAPACITY / 2 + 1).await?;
                if sample.len() <= BUCKET_CAPACITY / 2 {
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

    pub(crate) async fn all_leaves(&mut self, range: &TreeSpan) -> Result<Vec<LeafRow<G>>, TreeError> {
        let mut rows = Vec::new();
        let mut remaining = range.clone();
        loop {
            let page = self.leaves(&remaining, 1024).await?;
            let done = page.len() < 1024;
            if let Some(last) = page.last() {
                remaining = remaining.after(&last.address());
            }
            rows.extend(page);
            if done {
                return Ok(rows);
            }
        }
    }

    /// Replace the entire tree with these already-admitted leaves. Duplicate
    /// addresses are rejected. The replacement is atomic at commit; failure
    /// or cancellation prevents this transaction from committing.
    ///
    /// On native targets, large batches prepare leaf hashes and encodings on
    /// a shared pool of up to four workers; transaction writes stay serial.
    /// Builds fall back to serial preparation when workers are unavailable.
    pub async fn build(&mut self, leaves: impl IntoIterator<Item = LeafData>) -> Result<(), TreeError> {
        self.generation += 1;
        let mut mutation = self.mutation();
        let result = mutation.build_inner(leaves).await;
        mutation.finish(result)
    }

    async fn build_inner(&mut self, leaves: impl IntoIterator<Item = LeafData>) -> Result<(), TreeError> {
        self.transaction.change_structure();
        // Preserve the first invalid key or duplicate in input order before
        // workers perform the independent hashing and row encoding.
        let mut admitted = BTreeMap::new();
        for leaf in leaves {
            CanonicalCodec.split(self.spec.key_spec(), &leaf.key)?;
            let address = crate::leaf_address(&leaf.key, leaf.entity_id);
            if admitted.insert(address, leaf).is_some() {
                return Err(TreeError::DuplicateLeaf);
            }
        }
        for map in [MapName::Leaves, MapName::Lookup, MapName::Nodes] {
            let entries = self.transaction.scan(map, (Bound::Unbounded, Bound::Unbounded), usize::MAX).await?;
            for (key, value) in entries {
                self.transaction.delete_if(map, &key, &value).await?;
            }
        }
        let mut admitted = admitted.into_values();
        let mut rows = Vec::with_capacity(admitted.len());
        while admitted.len() != 0 {
            let batch = admitted.by_ref().take(PREPARATION_BATCH_LEAVES).collect();
            let prepared = prepare_leaves::<G>(batch);
            for leaf in &prepared {
                self.put_prepared_leaf(leaf, None).await?;
            }
            rows.extend(prepared.into_iter().map(PreparedLeaf::into_row));
        }
        // These rows already contain the admitted leaf elements. Building the
        // summaries from them avoids reading and decoding our own writes.
        self.write_subtree(&NodePrefix::root(), &rows).await
    }

    async fn rebuild_subtree(&mut self, prefix: &NodePrefix) -> Result<(), TreeError> {
        let leaves = self.all_leaves(&TreeSpan::under(prefix)).await?;
        self.write_subtree(prefix, &leaves).await
    }

    async fn write_subtree(&mut self, prefix: &NodePrefix, leaves: &[LeafRow<G>]) -> Result<(), TreeError> {
        // This fence is deliberately conservative: structural maintenance
        // retries after any intervening commit; ordinary leaf changes only
        // watch the topology and can append their deltas concurrently.
        self.transaction.change_structure();
        let start = prefix.to_bytes();
        let end = prefix.subtree_key_end();
        let entries = self
            .transaction
            .scan(MapName::Nodes, (Bound::Included(&start), end.as_deref().map_or(Bound::Unbounded, Bound::Excluded)), usize::MAX)
            .await?;
        for (key, value) in entries {
            self.transaction.delete_if(MapName::Nodes, &key, &value).await?;
        }
        enum Step {
            Build { prefix: NodePrefix, from: usize, to: usize },
            Join(NodePrefix),
        }
        let mut pending = vec![Step::Build { prefix: prefix.clone(), from: 0, to: leaves.len() }];
        let mut completed = Vec::<Digest<G>>::new();
        while let Some(step) = pending.pop() {
            let (prefix, from, to) = match step {
                Step::Build { prefix, from, to } => (prefix, from, to),
                Step::Join(prefix) => {
                    let right = completed.pop().expect("right subtree completed before join");
                    let left = completed.pop().expect("left subtree completed before join");
                    let digest = left + right;
                    self.put_node_base(&prefix, None, NodeRow { digest }).await?;
                    completed.push(digest);
                    continue;
                }
            };
            let rows = &leaves[from..to];
            if rows.len() <= BUCKET_CAPACITY {
                let digest = rows.iter().map(|row| row.element).map(Digest::from).sum();
                self.put_node_base(&prefix, None, NodeRow { digest }).await?;
                completed.push(digest);
                continue;
            }
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
            // Sum each terminal bucket once, then combine its completed
            // children instead of summing all leaves again at every ancestor.
            pending.push(Step::Join(prefix));
            pending.push(Step::Build { prefix: right, from: middle, to });
            pending.push(Step::Build { prefix: left, from, to: middle });
        }
        Ok(())
    }

    /// Rehash every leaf, verify both directions of the entity lookup, and
    /// compare every stored summary against the leaves under its prefix.
    /// This checks the current snapshot without repairing or changing it.
    pub async fn audit(&mut self) -> Result<Audit, TreeError> {
        let leaves = self.all_leaves(&TreeSpan::all()).await?;
        let mut lookup: BTreeMap<EntityId, BTreeSet<Vec<u8>>> = BTreeMap::new();
        for row in &leaves {
            CanonicalCodec.split(self.spec.key_spec(), &row.key)?;
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
            if node.digest != expected {
                return Err(TreeError::Audit("summary differs from leaves"));
            }
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
    use crate::{DigestTree, Ristretto255, TreeSpec};
    use ankql::ast::Predicate;
    use ankurah_core_types::KeySpec;
    use sha2::{Digest as _, Sha256};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static GROUP_ADDS: AtomicUsize = AtomicUsize::new(0);
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct CountedGroup;
    impl DigestGroup for CountedGroup {
        type Element = <Ristretto255 as DigestGroup>::Element;
        const ROW_VERSION: u8 = Ristretto255::ROW_VERSION;
        const ELEMENT_LEN: usize = Ristretto255::ELEMENT_LEN;
        fn zero() -> Self::Element { Ristretto255::zero() }
        fn add(sum: &mut Self::Element, other: &Self::Element) {
            GROUP_ADDS.fetch_add(1, Ordering::Relaxed);
            Ristretto255::add(sum, other);
        }
        fn sub(sum: &mut Self::Element, other: &Self::Element) { Ristretto255::sub(sum, other); }
        fn neg(element: &mut Self::Element) { Ristretto255::neg(element); }
        fn hash_leaf(bytes: &[u8]) -> Self::Element { Ristretto255::hash_leaf(bytes) }
        fn encode(element: &Self::Element, bytes: &mut [u8]) { Ristretto255::encode(element, bytes); }
        fn decode(bytes: &[u8]) -> Result<Self::Element, crate::digest::DecodeError> { Ristretto255::decode(bytes) }
    }

    #[tokio::test]
    async fn topology_counts_do_not_decode_neighboring_leaves() {
        static DECODES: AtomicUsize = AtomicUsize::new(0);
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        struct CountedDecode;
        impl DigestGroup for CountedDecode {
            type Element = <Ristretto255 as DigestGroup>::Element;
            const ROW_VERSION: u8 = Ristretto255::ROW_VERSION;
            const ELEMENT_LEN: usize = Ristretto255::ELEMENT_LEN;
            fn zero() -> Self::Element { Ristretto255::zero() }
            fn add(sum: &mut Self::Element, other: &Self::Element) { Ristretto255::add(sum, other); }
            fn sub(sum: &mut Self::Element, other: &Self::Element) { Ristretto255::sub(sum, other); }
            fn neg(element: &mut Self::Element) { Ristretto255::neg(element); }
            fn hash_leaf(bytes: &[u8]) -> Self::Element { Ristretto255::hash_leaf(bytes) }
            fn encode(element: &Self::Element, bytes: &mut [u8]) { Ristretto255::encode(element, bytes); }
            fn decode(bytes: &[u8]) -> Result<Self::Element, crate::digest::DecodeError> {
                DECODES.fetch_add(1, Ordering::Relaxed);
                Ristretto255::decode(bytes)
            }
        }
        let leaf = |n: u8| LeafData { key: vec![], entity_id: EntityId::from_bytes([n; 32]), head: HeadHash::of([[1; 32]]) };
        for removing in [false, true] {
            let maps = crate::MemoryMaps::new();
            let spec = TreeSpec::new(KeySpec::new(vec![]), Predicate::True);
            let size = if removing { 96 } else { 32 };
            let mut setup = TreeTransaction::<CountedDecode, _>::bind(&spec, maps.begin());
            setup.build((0..size).map(leaf)).await.unwrap();
            setup.commit().await.unwrap();
            let change = if removing {
                LeafChange { before: Some(leaf(size - 1)), after: None }
            } else {
                LeafChange { before: None, after: Some(leaf(size)) }
            };
            let mut tx = TreeTransaction::<CountedDecode, _>::bind(&spec, maps.begin());
            DECODES.store(0, Ordering::Relaxed);
            tx.apply_change(change).await.unwrap();
            assert_eq!(DECODES.load(Ordering::Relaxed), usize::from(removing), "only the removed leaf needs decoding");
            tx.commit().await.unwrap();
            let expected = if removing { size - 1 } else { size + 1 };
            assert_eq!(TreeTransaction::<Ristretto255, _>::bind(&spec, maps.begin()).audit().await.unwrap().leaves, expected as usize);
        }
    }

    #[tokio::test]
    async fn counting_defers_neighbor_validation_but_leaf_reads_and_rebuilds_reject_corruption() {
        let leaf = |n: u8| LeafData { key: vec![], entity_id: EntityId::from_bytes([n; 32]), head: HeadHash::of([[1; 32]]) };
        // A plain insertion, an insertion causing a split, and a removal causing a merge.
        for (initial, removing) in [(32, false), (64, false), (65, true)] {
            let tree = DigestTree::<Ristretto255>::new(TreeSpec::new(KeySpec::new(vec![]), Predicate::True));
            let mut setup = tree.begin();
            setup.build((0..initial).map(leaf)).await.unwrap();
            if removing {
                for n in 33..initial {
                    setup.apply_change(LeafChange { before: Some(leaf(n)), after: None }).await.unwrap();
                }
            }
            setup.commit().await.unwrap();
            tree.begin().audit().await.unwrap();
            let mut corrupt = tree.begin();
            let address = leaf(0).row::<Ristretto255>().address();
            let old = corrupt.transaction.get(MapName::Leaves, &address).await.unwrap().unwrap();
            let mut invalid = old.clone();
            let start = invalid.len() - Ristretto255::ELEMENT_LEN;
            invalid[start..].fill(0xff); // Valid row length/version, invalid compressed element.
            corrupt.transaction.put_if(MapName::Leaves, &address, Some(&old), &invalid).await.unwrap();
            corrupt.commit().await.unwrap();
            assert!(matches!(tree.begin().leaf(&[], leaf(0).entity_id).await, Err(TreeError::Unreadable { what: "leaf value", .. })));
            assert!(tree.begin().audit().await.is_err());
            let before = tree.begin().node(&NodePrefix::root()).await.unwrap();
            let change = if removing {
                LeafChange { before: Some(leaf(32)), after: None }
            } else {
                LeafChange { before: None, after: Some(leaf(initial)) }
            };
            let mut tx = tree.begin();
            if initial == 32 {
                tx.apply_change(change).await.unwrap();
                tx.commit().await.unwrap();
                assert!(matches!(tree.begin().leaf(&[], leaf(0).entity_id).await, Err(TreeError::Unreadable { .. })));
                assert!(tree.begin().audit().await.is_err());
            } else {
                assert!(matches!(tx.apply_change(change).await, Err(TreeError::Unreadable { what: "leaf value", .. })));
                assert!(matches!(tx.commit().await, Err(TreeError::Conflict)), "failed rebuild cannot publish partial changes");
                assert_eq!(tree.begin().node(&NodePrefix::root()).await.unwrap(), before);
            }
        }
    }

    #[tokio::test]
    async fn builder_preserves_encoded_nodes_with_linear_group_additions() {
        // These hashes freeze the original builder's canonical node rows,
        // including its empty root, first bucket split, and deeper topology.
        for (size, expected) in [
            (0, "9d1fb3c08105018833a285df8382c1b95ada371c03a625f76e70d64aa37e1c89"),
            (65, "3e9e30d9298023b788cf142a439538682f21ee3839345f640112058151c0f333"),
            (4096, "526125e2918ee4a7500ab57a8605165f5b2a6badefe266c779481fe9770fe21e"),
        ] {
            let maps = crate::MemoryMaps::new();
            let spec = TreeSpec::new(KeySpec::new(vec![]), Predicate::True);
            let mut tree = TreeTransaction::<CountedGroup, _>::bind(&spec, maps.begin());
            let leaves = (0..size).map(|n| {
                let mut id = [0; EntityId::BYTE_LEN];
                id[..8].copy_from_slice(&(n as u64).to_be_bytes());
                LeafData { key: vec![], entity_id: EntityId::from_bytes(id), head: HeadHash::of([[1; 32]]) }
            });
            GROUP_ADDS.store(0, Ordering::Relaxed);
            tree.build(leaves).await.unwrap();
            let additions = GROUP_ADDS.load(Ordering::Relaxed);
            tree.commit().await.unwrap();
            let rows = maps.begin().scan(MapName::Nodes, (Bound::Unbounded, Bound::Unbounded), usize::MAX).await.unwrap();
            assert!(additions <= size + rows.len(), "aggregate each leaf once and combine completed children: {additions} additions");
            let encoded = bincode::serialize(&rows).unwrap();
            let hash = Sha256::digest(encoded).iter().map(|byte| format!("{byte:02x}")).collect::<String>();
            assert_eq!(hash, expected, "node topology and every digest must retain the original encoding");
            assert_eq!(TreeTransaction::<CountedGroup, _>::bind(&spec, maps.begin()).audit().await.unwrap().leaves, size);
        }
    }

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

    fn without_runtime(future: impl std::future::Future<Output = ()>) {
        let mut future = std::pin::pin!(future);
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(future.as_mut().poll(&mut context).is_ready(), "memory builds require no async runtime");
    }

    async fn compare_prepared_build_to_serial<G: DigestGroup>() {
        use ankurah_core_types::{IndexKeyPart, PropertyId, Value, ValueType};

        let property = PropertyId::EntityId(EntityId::from_bytes([7; 32]));
        let spec = TreeSpec::new(KeySpec::new(vec![IndexKeyPart::asc(property, ValueType::I64)]), Predicate::True);
        let leaves = (0u64..2048)
            .rev()
            .map(|n| {
                let mut id = [0; EntityId::BYTE_LEN];
                id[..8].copy_from_slice(&(n / 2).to_be_bytes());
                LeafData {
                    key: CanonicalCodec.encode_prefix(spec.key_spec(), &[Value::I64(n as i64)]).unwrap(),
                    entity_id: EntityId::from_bytes(id),
                    head: HeadHash::of([id, [3; 32]]),
                }
            })
            .collect::<Vec<_>>();
        let prepared_maps = crate::MemoryMaps::new();
        let serial_maps = crate::MemoryMaps::new();
        let mut prepared = TreeTransaction::<G, _>::bind(&spec, prepared_maps.begin());
        prepared.build(leaves.clone()).await.unwrap();
        prepared.commit().await.unwrap();

        // Oracle follows the original serial builder, including serial hashing
        // and encoding inside put_leaf, rather than the prepared-value path.
        let mut serial = TreeTransaction::<G, _>::bind(&spec, serial_maps.begin());
        let rows = leaves
            .iter()
            .map(|leaf| {
                let row = leaf.row::<G>();
                (row.address(), row)
            })
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect::<Vec<_>>();
        for row in &rows {
            serial.put_leaf(row.clone(), None).await.unwrap();
        }
        serial.write_subtree(&NodePrefix::root(), &rows).await.unwrap();
        serial.commit().await.unwrap();
        for map in [MapName::Leaves, MapName::Lookup, MapName::Nodes] {
            let bounds = (Bound::Unbounded, Bound::Unbounded);
            assert_eq!(
                prepared_maps.begin().scan(map, bounds, usize::MAX).await.unwrap(),
                serial_maps.begin().scan(map, bounds, usize::MAX).await.unwrap(),
                "all canonical bytes and topology must match the serial oracle: {map:?}"
            );
        }
        let mut old = TreeTransaction::<G, _>::bind(&spec, prepared_maps.begin());
        let old_root = old.node(&NodePrefix::root()).await.unwrap();
        assert_eq!(old.audit().await.unwrap().entities, 1024, "two keys per entity retain lookup semantics");
        let mut changed = leaves;
        changed[0].head = HeadHash::of([[9; 32]]);
        let mut replacement = TreeTransaction::<G, _>::bind(&spec, prepared_maps.begin());
        replacement.build(changed).await.unwrap();
        replacement.commit().await.unwrap();
        assert_eq!(old.node(&NodePrefix::root()).await.unwrap(), old_root, "replacement must not mutate a captured snapshot");
        assert_ne!(TreeTransaction::<G, _>::bind(&spec, prepared_maps.begin()).node(&NodePrefix::root()).await.unwrap(), old_root);
    }

    #[test]
    fn large_prepared_builds_match_serial_bytes_for_both_groups_without_a_runtime() {
        without_runtime(async {
            compare_prepared_build_to_serial::<Ristretto255>().await;
            compare_prepared_build_to_serial::<crate::LtHash16>().await;
        });
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(pool) = preparation_pool() {
            assert!(pool.current_num_threads() <= 4);
        }
    }

    #[test]
    fn large_build_admission_preserves_first_error_and_poisons_the_transaction() {
        without_runtime(async {
            let maps = crate::MemoryMaps::new();
            let spec = TreeSpec::new(KeySpec::new(vec![]), Predicate::True);
            let leaves = (0u64..2048)
                .map(|n| {
                    let mut id = [0; EntityId::BYTE_LEN];
                    id[..8].copy_from_slice(&n.to_be_bytes());
                    LeafData { key: vec![], entity_id: EntityId::from_bytes(id), head: HeadHash::of([[1; 32]]) }
                })
                .collect::<Vec<_>>();
            let mut setup = TreeTransaction::<Ristretto255, _>::bind(&spec, maps.begin());
            setup.build([leaves[0].clone()]).await.unwrap();
            setup.commit().await.unwrap();
            for duplicate_first in [true, false] {
                let mut input = leaves.clone();
                let duplicate = leaves[0].clone();
                let mut invalid = leaves[1].clone();
                invalid.key = vec![1];
                input.extend(if duplicate_first { [duplicate, invalid] } else { [invalid, duplicate] });
                let mut tx = TreeTransaction::<Ristretto255, _>::bind(&spec, maps.begin());
                let error = tx.build(input).await.unwrap_err();
                assert!(if duplicate_first {
                    matches!(error, TreeError::DuplicateLeaf)
                } else {
                    matches!(error, TreeError::InvalidKey(_))
                });
                tx.apply_change(LeafChange { before: None, after: None }).await.unwrap();
                assert!(matches!(tx.commit().await, Err(TreeError::Conflict)));
                assert_eq!(TreeTransaction::<Ristretto255, _>::bind(&spec, maps.begin()).audit().await.unwrap().leaves, 1);
            }
        });
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct LaterBatchPanic;

    impl DigestGroup for LaterBatchPanic {
        type Element = <Ristretto255 as DigestGroup>::Element;
        const ROW_VERSION: u8 = Ristretto255::ROW_VERSION;
        const ELEMENT_LEN: usize = Ristretto255::ELEMENT_LEN;
        fn zero() -> Self::Element { Ristretto255::zero() }
        fn add(sum: &mut Self::Element, other: &Self::Element) { Ristretto255::add(sum, other); }
        fn sub(sum: &mut Self::Element, other: &Self::Element) { Ristretto255::sub(sum, other); }
        fn neg(element: &mut Self::Element) { Ristretto255::neg(element); }
        fn hash_leaf(bytes: &[u8]) -> Self::Element {
            // Empty key: two length fields precede the entity's bytes.
            assert_ne!(bytes.get(16..24), Some(2047u64.to_be_bytes().as_slice()), "injected later-batch hash panic");
            Ristretto255::hash_leaf(bytes)
        }
        fn encode(element: &Self::Element, bytes: &mut [u8]) { Ristretto255::encode(element, bytes); }
        fn decode(bytes: &[u8]) -> Result<Self::Element, crate::digest::DecodeError> { Ristretto255::decode(bytes) }
    }

    #[test]
    fn later_preparation_panic_poisons_prior_buffered_batches() {
        let maps = crate::MemoryMaps::new();
        let spec = TreeSpec::new(KeySpec::new(vec![]), Predicate::True);
        let leaves = (0u64..2048)
            .map(|n| {
                let mut id = [0; EntityId::BYTE_LEN];
                id[..8].copy_from_slice(&n.to_be_bytes());
                LeafData { key: vec![], entity_id: EntityId::from_bytes(id), head: HeadHash::of([[1; 32]]) }
            })
            .collect::<Vec<_>>();
        without_runtime(async {
            let mut setup = TreeTransaction::<LaterBatchPanic, _>::bind(&spec, maps.begin());
            setup.build([leaves[0].clone()]).await.unwrap();
            setup.commit().await.unwrap();
        });
        let mut tx = TreeTransaction::<LaterBatchPanic, _>::bind(&spec, maps.begin());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            without_runtime(async {
                tx.build(leaves).await.unwrap();
            });
        }));
        assert!(result.is_err(), "the worker panic must propagate to the mutation owner");
        without_runtime(async {
            // The first batch was written before the later hash panicked.
            assert_eq!(tx.leaves(&TreeSpan::all(), usize::MAX).await.unwrap().len(), 1024);
            tx.apply_change(LeafChange { before: None, after: None }).await.unwrap();
            assert!(matches!(tx.commit().await, Err(TreeError::Conflict)));
            assert_eq!(TreeTransaction::<LaterBatchPanic, _>::bind(&spec, maps.begin()).audit().await.unwrap().leaves, 1);
        });
    }
}
