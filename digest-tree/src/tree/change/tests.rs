use super::*;
use crate::store::layout;
use crate::{DigestTree, Ristretto255, TreeSpec};
use ankql::ast::Predicate;
use ankurah_core_types::KeySpec;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn insert_and_remove_encode_one_delta_for_all_ancestors() {
    static ENCODES: AtomicUsize = AtomicUsize::new(0);
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct CountedEncode;
    impl DigestGroup for CountedEncode {
        type Element = <Ristretto255 as DigestGroup>::Element;
        const ROW_VERSION: u8 = Ristretto255::ROW_VERSION;
        const ELEMENT_LEN: usize = Ristretto255::ELEMENT_LEN;
        fn zero() -> Self::Element { Ristretto255::zero() }
        fn add(sum: &mut Self::Element, other: &Self::Element) { Ristretto255::add(sum, other); }
        fn sub(sum: &mut Self::Element, other: &Self::Element) { Ristretto255::sub(sum, other); }
        fn neg(element: &mut Self::Element) { Ristretto255::neg(element); }
        fn hash_leaf(bytes: &[u8]) -> Self::Element { Ristretto255::hash_leaf(bytes) }
        fn encode(element: &Self::Element, bytes: &mut [u8]) {
            ENCODES.fetch_add(1, Ordering::Relaxed);
            Ristretto255::encode(element, bytes);
        }
        fn decode(bytes: &[u8]) -> Result<Self::Element, crate::digest::DecodeError> { Ristretto255::decode(bytes) }
    }
    let leaf = |n: u64| {
        let mut id = [0; EntityId::BYTE_LEN];
        id[..8].copy_from_slice(&n.to_be_bytes());
        LeafInput { key: vec![], entity_id: EntityId::from_bytes(id), head: HeadHash::of([[1; 32]]) }
    };
    let maps = crate::MemoryMaps::new();
    let spec = TreeSpec::new(KeySpec::new(vec![]), Predicate::True);
    let missing = leaf(200);
    let mut setup = TreeTransaction::<Ristretto255, _>::bind(&spec, maps.begin());
    setup.build((0..512).filter(|&n| n != 200).map(leaf)).await.unwrap();
    setup.commit().await.unwrap();
    let mut initial = TreeTransaction::<Ristretto255, _>::bind(&spec, maps.begin());
    let initial_root = initial.node(&NodePrefix::root()).await.unwrap();
    let topology = |rows: Vec<(Vec<u8>, Vec<u8>)>| {
        rows.into_iter().map(|(key, _)| layout::node_key(&key).unwrap().into_prefix()).collect::<BTreeSet<_>>()
    };
    let initial_topology = topology(maps.begin().scan(MapName::Nodes, (Bound::Unbounded, Bound::Unbounded), usize::MAX).await.unwrap());
    let mut encode_counts = Vec::new();
    for removing in [false, true] {
        let mut tx = TreeTransaction::<CountedEncode, _>::bind(&spec, maps.begin());
        assert!(tx.path(&missing.clone().into_row::<Ristretto255>().address()).await.unwrap().prefixes.len() > 2);
        let change = if removing {
            LeafChange { before: Some(missing.clone()), after: None }
        } else {
            LeafChange { before: None, after: Some(missing.clone()) }
        };
        ENCODES.store(0, Ordering::Relaxed);
        tx.apply_change(change).await.unwrap();
        encode_counts.push(ENCODES.load(Ordering::Relaxed));
        tx.commit().await.unwrap();
        let mut check = TreeTransaction::<Ristretto255, _>::bind(&spec, maps.begin());
        assert_eq!(check.audit().await.unwrap().leaves, if removing { 511 } else { 512 });
        assert_eq!(
            topology(maps.begin().scan(MapName::Nodes, (Bound::Unbounded, Bound::Unbounded), usize::MAX).await.unwrap()),
            initial_topology
        );
        assert_eq!(check.leaf(&[], missing.entity_id).await.unwrap(), (!removing).then(|| missing.clone().into_row::<Ristretto255>()));
        if removing {
            assert_eq!(check.node(&NodePrefix::root()).await.unwrap(), initial_root);
        }
    }
    assert_eq!(encode_counts, [2, 1], "an insertion encodes its leaf value and one delta, a removal one delta, however deep");
}

#[tokio::test]
async fn moves_give_shared_and_separate_ancestors_their_own_deltas() {
    use ankurah_core_types::{IndexKeyPart, PropertyId, Value, ValueType};

    let property = PropertyId::EntityId(EntityId::from_bytes([7; 32]));
    let spec = TreeSpec::new(KeySpec::new(vec![IndexKeyPart::asc(property, ValueType::I64)]), Predicate::True);
    let leaf = |n: u64, key: i64| {
        let mut id = [0; EntityId::BYTE_LEN];
        id[..8].copy_from_slice(&n.to_be_bytes());
        let key = CanonicalCodec::encode_prefix(spec.key_spec(), &[Value::I64(key)]).unwrap();
        LeafInput { key, entity_id: EntityId::from_bytes(id), head: HeadHash::of([[1; 32]]) }
    };
    let tree = DigestTree::<Ristretto255>::new(spec.clone());
    let mut setup = tree.begin();
    setup.build((0..512).map(|n| leaf(n, n as i64 * 2))).await.unwrap();
    setup.commit().await.unwrap();
    let initial_root = tree.begin().node(&NodePrefix::root()).await.unwrap();
    // To a neighboring key, then to one far across the tree.
    for (to, far) in [(201, false), (1001, true)] {
        let (from, to) = (leaf(100, 200), leaf(100, to));
        let mut probe = tree.begin();
        let from_path = probe.path(&from.clone().into_row::<Ristretto255>().address()).await.unwrap();
        let to_path = probe.path(&to.clone().into_row::<Ristretto255>().address()).await.unwrap();
        let shared = from_path.prefixes.iter().zip(&to_path.prefixes).take_while(|(a, b)| a == b).count();
        if far {
            assert!(shared < from_path.prefixes.len().min(to_path.prefixes.len()), "the far move leaves its own branch");
        }
        for (before, after) in [(from.clone(), to.clone()), (to, from)] {
            let mut tx = tree.begin();
            tx.apply_change(LeafChange { before: Some(before), after: Some(after) }).await.unwrap();
            tx.commit().await.unwrap();
            assert_eq!(tree.begin().audit().await.unwrap().leaves, 512, "every summary matches the leaves beneath it");
        }
        assert_eq!(tree.begin().node(&NodePrefix::root()).await.unwrap(), initial_root);
    }
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
    let leaf = |n: u8| LeafInput { key: vec![], entity_id: EntityId::from_bytes([n; 32]), head: HeadHash::of([[1; 32]]) };
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
    let leaf = |n: u8| LeafInput { key: vec![], entity_id: EntityId::from_bytes([n; 32]), head: HeadHash::of([[1; 32]]) };
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
        let address = leaf(0).into_row::<Ristretto255>().address();
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
            assert!(matches!(tx.commit().await, Err(TreeError::Poisoned)), "failed rebuild cannot publish partial changes");
            assert_eq!(tree.begin().node(&NodePrefix::root()).await.unwrap(), before);
        }
    }
}

/// The leaf of the entity numbered `n` filed under `key`, at `version`.
fn numbered(n: u32, key: Vec<u8>, version: u8) -> LeafInput {
    let mut id = [0; EntityId::BYTE_LEN];
    id[..4].copy_from_slice(&n.to_be_bytes());
    LeafInput { key, entity_id: EntityId::from_bytes(id), head: HeadHash::of([[version; 32]]) }
}

/// Every key of the nodes map is a base row's: no tree node carries a delta
/// row.
async fn only_base_rows<G: DigestGroup, T: MapAccess>(tree: &mut TreeTransaction<G, T>) -> bool {
    let rows = tree.transaction.scan(MapName::Nodes, (Bound::Unbounded, Bound::Unbounded), usize::MAX).await.unwrap();
    rows.iter().all(|(key, _)| matches!(layout::node_key(key).unwrap(), layout::NodeKey::Base(_)))
}

/// A batch of changes folds every difference it makes, and every delta row
/// an earlier change appended to a tree node it touches, into base rows, and
/// leaves the tree a build from the same leaves gives: what a tree's only
/// writer publishes after each batch. A transaction begun before still reads
/// the tree it began with.
#[tokio::test]
async fn a_batch_folds_its_differences_and_earlier_deltas_into_base_rows() {
    let spec = TreeSpec::new(KeySpec::new(vec![]), Predicate::True);
    let tree = DigestTree::<Ristretto255>::new(spec.clone());
    let mut versions: BTreeMap<u32, u8> = (0..1024).map(|n| (n, 1)).collect();
    let leaves = |versions: &BTreeMap<u32, u8>| versions.iter().map(|(&n, &version)| numbered(n, vec![], version)).collect::<Vec<_>>();
    let mut setup = tree.begin();
    setup.build(leaves(&versions)).await.unwrap();
    setup.commit().await.unwrap();
    let mut original = tree.begin();
    let original_root = original.node(&NodePrefix::root()).await.unwrap();

    for version in 2..80 {
        let mut tx = tree.begin();
        tx.apply_change(LeafChange { before: Some(numbered(50, vec![], version - 1)), after: Some(numbered(50, vec![], version)) })
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    versions.insert(50, 79);
    assert!(!only_base_rows(&mut tree.begin()).await, "single changes append delta rows");
    let changes: Vec<LeafChange> =
        (0..160).map(|n| LeafChange { before: Some(numbered(n, vec![], versions[&n])), after: Some(numbered(n, vec![], 99)) }).collect();
    versions.extend((0..160).map(|n| (n, 99)));
    let mut tx = tree.begin();
    tx.apply_changes(changes).await.unwrap();
    tx.commit().await.unwrap();

    let mut current = tree.begin();
    assert!(only_base_rows(&mut current).await, "the batch leaves no delta row");
    current.audit().await.unwrap();
    let rebuilt = DigestTree::<Ristretto255>::new(spec);
    let mut build = rebuilt.begin();
    build.build(leaves(&versions)).await.unwrap();
    build.commit().await.unwrap();
    assert_eq!(current.node(&NodePrefix::root()).await.unwrap(), rebuilt.begin().node(&NodePrefix::root()).await.unwrap());
    assert_eq!(original.node(&NodePrefix::root()).await.unwrap(), original_root, "a transaction begun before reads its own tree");
}

/// A batch whose changes split buckets, merge subtrees, insert branches and
/// move leaves between them leaves every tree node holding what the same
/// changes applied one at a time leave there, and no delta row.
#[tokio::test]
async fn a_batch_that_changes_the_topology_matches_its_changes_one_at_a_time() {
    use ankurah_core_types::{IndexKeyPart, PropertyId, Value, ValueType};

    let property = PropertyId::EntityId(EntityId::from_bytes([7; 32]));
    let spec = TreeSpec::new(KeySpec::new(vec![IndexKeyPart::asc(property, ValueType::I64)]), Predicate::True);
    let key = |value: i64| CanonicalCodec::encode_prefix(spec.key_spec(), &[Value::I64(value)]).unwrap();
    let initial: Vec<LeafInput> = (0..100).map(|n| numbered(n, key(i64::from(n)), 1)).collect();
    let mut changes = Vec::new();
    // Insertions past every key, which insert branches and then split buckets.
    changes.extend((100..250).map(|n| LeafChange { before: None, after: Some(numbered(n, key(1_000 + i64::from(n)), 1)) }));
    // Removals of most of the first keys, which merge their subtrees.
    changes.extend((10..90).map(|n| LeafChange { before: Some(numbered(n, key(i64::from(n)), 1)), after: None }));
    // Moves to keys far away, and head changes in place.
    changes.extend(
        (0..5).map(|n| LeafChange {
            before: Some(numbered(n, key(i64::from(n)), 1)),
            after: Some(numbered(n, key(5_000 + i64::from(n)), 1)),
        }),
    );
    changes.extend(
        (90..95).map(|n| LeafChange { before: Some(numbered(n, key(i64::from(n)), 1)), after: Some(numbered(n, key(i64::from(n)), 2)) }),
    );

    let (batched, one_at_a_time) = (DigestTree::<Ristretto255>::new(spec.clone()), DigestTree::<Ristretto255>::new(spec.clone()));
    for tree in [&batched, &one_at_a_time] {
        let mut setup = tree.begin();
        setup.build(initial.clone()).await.unwrap();
        setup.commit().await.unwrap();
    }
    let mut tx = batched.begin();
    tx.apply_changes(changes.clone()).await.unwrap();
    tx.commit().await.unwrap();
    for change in changes {
        let mut tx = one_at_a_time.begin();
        tx.apply_change(change).await.unwrap();
        tx.commit().await.unwrap();
    }

    let (mut batched, mut one_at_a_time) = (batched.begin(), one_at_a_time.begin());
    assert!(only_base_rows(&mut batched).await);
    assert_eq!(batched.audit().await.unwrap(), one_at_a_time.audit().await.unwrap());
    let rows = batched.transaction.scan(MapName::Nodes, (Bound::Unbounded, Bound::Unbounded), usize::MAX).await.unwrap();
    for (key, _) in rows {
        let prefix = layout::node_key(&key).unwrap().into_prefix();
        assert_eq!(batched.node(&prefix).await.unwrap(), one_at_a_time.node(&prefix).await.unwrap(), "{prefix:?}");
    }
}

/// What a change costs in operations on the maps, counted by an adapter around
/// the memory maps, independently of an engine's timing or caches.
mod map_costs {
    use super::*;
    use crate::store::{ByteBounds, Failure, MapTransaction};
    use crate::tree::fixtures::node_base;
    use crate::{MemoryMaps, MemoryTransaction};
    use ankurah_core_types::{IndexKeyPart, PropertyId, Value, ValueType};
    use async_trait::async_trait;
    use std::ops::Bound::{Excluded, Included};
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    struct Cost {
        gets: usize,
        scans: usize,
        read_rows: usize,
        read_bytes: usize,
        puts: usize,
        deletes: usize,
        appends: usize,
        write_bytes: usize,
    }
    type Counts = Arc<Mutex<[Cost; 3]>>;
    struct Counted {
        inner: MemoryTransaction,
        counts: Counts,
    }
    impl Counted {
        fn scanned(&self, map: MapName, rows: &[(Vec<u8>, Vec<u8>)]) {
            let mut counts = self.counts.lock().unwrap();
            let cost = &mut counts[map as usize];
            cost.scans += 1;
            cost.read_rows += rows.len();
            cost.read_bytes += rows.iter().map(|(key, value)| key.len() + value.len()).sum::<usize>();
        }
    }
    #[async_trait]
    impl MapAccess for Counted {
        fn poison(&mut self, failure: Failure) { self.inner.poison(failure); }

        fn watch_structure(&mut self) { self.inner.watch_structure(); }
        fn change_structure(&mut self) { self.inner.change_structure(); }
        async fn get(&mut self, map: MapName, key: &[u8]) -> Result<Option<Vec<u8>>, TreeError> {
            let value = self.inner.get(map, key).await?;
            let mut counts = self.counts.lock().unwrap();
            let cost = &mut counts[map as usize];
            cost.gets += 1;
            cost.read_rows += usize::from(value.is_some());
            cost.read_bytes += value.as_ref().map_or(0, |value| key.len() + value.len());
            Ok(value)
        }
        async fn scan(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError> {
            let rows = self.inner.scan(map, bounds, limit).await?;
            self.scanned(map, &rows);
            Ok(rows)
        }
        async fn scan_reverse(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError> {
            let rows = self.inner.scan_reverse(map, bounds, limit).await?;
            self.scanned(map, &rows);
            Ok(rows)
        }
        async fn insert(&mut self, map: MapName, key: &[u8], value: &[u8]) -> Result<(), TreeError> {
            {
                let mut counts = self.counts.lock().unwrap();
                counts[map as usize].puts += 1;
                counts[map as usize].write_bytes += key.len() + value.len();
            }
            self.inner.insert(map, key, value).await
        }
        async fn put_if(&mut self, map: MapName, key: &[u8], expected: Option<&[u8]>, value: &[u8]) -> Result<(), TreeError> {
            {
                let mut counts = self.counts.lock().unwrap();
                counts[map as usize].puts += 1;
                counts[map as usize].write_bytes += key.len() + value.len();
            }
            self.inner.put_if(map, key, expected, value).await
        }
        async fn delete_if(&mut self, map: MapName, key: &[u8], expected: &[u8]) -> Result<(), TreeError> {
            {
                let mut counts = self.counts.lock().unwrap();
                counts[map as usize].deletes += 1;
                counts[map as usize].write_bytes += key.len();
            }
            self.inner.delete_if(map, key, expected).await
        }
        async fn append(&mut self, map: MapName, key: &[u8], value: &[u8]) -> Result<(), TreeError> {
            {
                let mut counts = self.counts.lock().unwrap();
                counts[map as usize].appends += 1;
                counts[map as usize].write_bytes += key.len() + 8 + value.len();
            }
            self.inner.append(map, key, value).await
        }
    }

    #[async_trait]
    impl MapTransaction for Counted {
        async fn commit(self) -> Result<(), TreeError> { self.inner.commit().await }
    }

    fn spec() -> TreeSpec {
        TreeSpec::new(
            KeySpec::new(vec![IndexKeyPart::asc(PropertyId::EntityId(EntityId::from_bytes([1; 32])), ValueType::I64)]),
            Predicate::True,
        )
    }
    fn leaf(value: i64, version: u8) -> LeafInput {
        let mut id = [0; 32];
        id[..8].copy_from_slice(&value.to_be_bytes());
        LeafInput {
            key: CanonicalCodec::encode_prefix(spec().key_spec(), &[Value::I64(value)]).unwrap(),
            entity_id: EntityId::from_bytes(id),
            head: HeadHash::of([[version; 32]]),
        }
    }
    fn begin(maps: &MemoryMaps, counts: &Counts) -> TreeTransaction<Ristretto255, Counted> {
        TreeTransaction::bind(&spec(), Counted { inner: maps.begin(), counts: counts.clone() })
    }
    async fn populated(size: i64) -> (MemoryMaps, Counts) {
        let maps = MemoryMaps::new();
        let counts = Counts::default();
        let mut tx = begin(&maps, &counts);
        tx.build((0..size).map(|n| leaf(n, 0))).await.unwrap();
        tx.commit().await.unwrap();
        reset(&counts);
        (maps, counts)
    }
    fn reset(counts: &Counts) { *counts.lock().unwrap() = Default::default(); }

    #[tokio::test]
    async fn extending_a_compressed_branch_preserves_existing_node_rows() {
        for size in [256, 4096] {
            let (maps, counts) = populated(size).await;
            let mut tx = begin(&maps, &counts);
            let root = node_base(&mut tx, &NodePrefix::root()).await;
            reset(&counts);
            tx.apply_change(LeafChange { before: None, after: Some(leaf(1_000_000, 0)) }).await.unwrap();
            let measured = counts.lock().unwrap().clone();
            assert_eq!(measured[MapName::Leaves as usize].read_rows, 0, "a new branch does not read existing leaves");
            assert_eq!(measured[MapName::Nodes as usize].deletes, 0, "existing summaries stay in place");
            assert!(measured[MapName::Nodes as usize].puts <= 2, "only the new branches need baselines");
            assert_eq!(node_base(&mut tx, &NodePrefix::root()).await, root);
            tx.commit().await.unwrap();
            assert_eq!(begin(&maps, &counts).audit().await.unwrap().leaves, size as usize + 1);
        }
    }

    #[tokio::test]
    async fn changing_only_a_head_leaves_lookup_and_multiplicity_untouched() {
        let (maps, counts) = populated(4096).await;
        let mut extra = leaf(5000, 0);
        extra.entity_id = leaf(100, 0).entity_id;
        let mut setup = begin(&maps, &counts);
        setup.apply_change(LeafChange { before: None, after: Some(extra) }).await.unwrap();
        setup.commit().await.unwrap();
        let mut tx = begin(&maps, &counts);
        reset(&counts);
        tx.apply_change(LeafChange { before: Some(leaf(100, 0)), after: Some(leaf(100, 1)) }).await.unwrap();
        let measured = counts.lock().unwrap().clone();
        assert_eq!(measured[MapName::Lookup as usize], Cost::default());
        assert_eq!(measured[MapName::Leaves as usize].gets, 1);
        assert_eq!(measured[MapName::Leaves as usize].puts, 1);
        assert_eq!(measured[MapName::Leaves as usize].deletes, 0);
        assert_eq!(measured[MapName::Nodes as usize].puts, 0);
        assert_eq!(measured[MapName::Nodes as usize].deletes, 0);
        tx.commit().await.unwrap();
        begin(&maps, &counts).audit().await.unwrap();
    }

    /// A batch writes each tree node it touches once, and appends no delta
    /// row.
    #[tokio::test]
    async fn a_batch_writes_each_tree_node_it_touches_once() {
        let (maps, counts) = populated(4096).await;
        let mut tx = begin(&maps, &counts);
        let mut touched = BTreeSet::new();
        for n in 100..110 {
            touched.extend(tx.path(&leaf(n, 0).into_row::<Ristretto255>().address()).await.unwrap().prefixes);
        }
        reset(&counts);
        tx.apply_changes((100..110).map(|n| LeafChange { before: Some(leaf(n, 0)), after: Some(leaf(n, 1)) })).await.unwrap();
        let measured = counts.lock().unwrap().clone();
        assert_eq!(measured[MapName::Nodes as usize].appends, 0, "no delta rows");
        assert_eq!(measured[MapName::Nodes as usize].puts, touched.len(), "one base write per tree node touched");
        tx.commit().await.unwrap();
        begin(&maps, &counts).audit().await.unwrap();
    }

    #[tokio::test]
    async fn bounded_reads_do_not_load_delta_history_from_an_excluded_branch() {
        let (maps, counts) = populated(128).await;
        let range = TreeSpan::new(Included(leaf(0, 0).key), Excluded(leaf(2, 0).key));
        let mut tx = begin(&maps, &counts);
        let scope = tx.scope_spans([range.clone()]);
        assert_eq!(tx.count(&scope).await.unwrap(), 2);
        let baseline = counts.lock().unwrap().clone();
        drop(tx);
        let mut tx = begin(&maps, &counts);
        for version in 1..=200 {
            tx.apply_change(LeafChange { before: Some(leaf(100, version - 1)), after: Some(leaf(100, version)) }).await.unwrap();
        }
        tx.commit().await.unwrap();
        let mut tx = begin(&maps, &counts);
        let scope = tx.scope_spans([range]);
        reset(&counts);
        assert_eq!(tx.count(&scope).await.unwrap(), 2);
        let measured = counts.lock().unwrap().clone();
        assert_eq!(measured[MapName::Leaves as usize], baseline[MapName::Leaves as usize]);
        // Topology discovery may encounter a delta key to skip its entire run,
        // but must not read a number of rows proportional to the delta history.
        assert!(measured[MapName::Nodes as usize].read_rows <= baseline[MapName::Nodes as usize].read_rows + 2);
    }
}
