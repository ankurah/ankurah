use super::*;
use crate::digest::HeadHash;
use crate::tree::fixtures::{Hook, Hooked};
use crate::{LeafChange, Ristretto255, TreeSpec};
use ankql::ast::Predicate;
use ankurah_core_types::{EntityId, KeySpec};
use sha2::{Digest as _, Sha256};
use std::sync::atomic::{AtomicUsize, Ordering};

static GROUP_ADDS: AtomicUsize = AtomicUsize::new(0);

/// Counts the group's additions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CountAdds;

impl Hook for CountAdds {
    fn add() { GROUP_ADDS.fetch_add(1, Ordering::Relaxed); }
}

type CountedGroup = Hooked<CountAdds>;

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
            LeafInput { key: vec![], entity_id: EntityId::from_bytes(id), head: HeadHash::of([[1; 32]]) }
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
            LeafInput {
                key: CanonicalCodec::encode_prefix(spec.key_spec(), &[Value::I64(n as i64)]).unwrap(),
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

    // The oracle hashes, encodes and inserts each leaf by itself, then builds
    // the tree nodes with `write_subtree`, apart from the build's batched
    // preparation.
    let mut serial = TreeTransaction::<G, _>::bind(&spec, serial_maps.begin());
    let encoded = leaves
        .iter()
        .map(|leaf| {
            let row = leaf.clone().into_row::<G>();
            (row.address(), row)
        })
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .map(|(address, row)| EncodedLeaf::new(address, row))
        .collect::<Vec<_>>();
    for leaf in &encoded {
        serial.insert_leaf(leaf).await.unwrap();
    }
    serial.write_subtree(&NodePrefix::root(), &encoded).await.unwrap();
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
                LeafInput { key: vec![], entity_id: EntityId::from_bytes(id), head: HeadHash::of([[1; 32]]) }
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
            assert!(if duplicate_first { matches!(error, TreeError::DuplicateLeaf) } else { matches!(error, TreeError::InvalidKey(_)) });
            tx.apply_change(LeafChange { before: None, after: None }).await.unwrap();
            assert!(matches!(tx.commit().await, Err(TreeError::Poisoned)));
            assert_eq!(TreeTransaction::<Ristretto255, _>::bind(&spec, maps.begin()).audit().await.unwrap().leaves, 1);
        }
    });
}

/// Panics hashing the leaf of the entity numbered 2047, in a build's second
/// batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PanicOnLeaf2047;

impl Hook for PanicOnLeaf2047 {
    fn hash_leaf(encoded_leaf: &[u8]) {
        // Empty key: two length fields precede the entity's bytes.
        assert_ne!(encoded_leaf.get(16..24), Some(2047u64.to_be_bytes().as_slice()), "injected later-batch hash panic");
    }
}

type LaterBatchPanic = Hooked<PanicOnLeaf2047>;

#[test]
fn later_preparation_panic_poisons_prior_buffered_batches() {
    let maps = crate::MemoryMaps::new();
    let spec = TreeSpec::new(KeySpec::new(vec![]), Predicate::True);
    let leaves = (0u64..2048)
        .map(|n| {
            let mut id = [0; EntityId::BYTE_LEN];
            id[..8].copy_from_slice(&n.to_be_bytes());
            LeafInput { key: vec![], entity_id: EntityId::from_bytes(id), head: HeadHash::of([[1; 32]]) }
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
        assert!(matches!(tx.commit().await, Err(TreeError::Poisoned)));
        assert_eq!(TreeTransaction::<LaterBatchPanic, _>::bind(&spec, maps.begin()).audit().await.unwrap().leaves, 1);
    });
}

/// A rebuild after a batch of changes leaves the maps a build of the
/// resulting leaves leaves, and hashes only the leaves the batch files: every
/// other leaf's element is decoded from its row. A change it finds stale fails
/// the transaction, as one applied alone does.
#[tokio::test]
async fn a_rebuild_with_changes_hashes_only_the_leaves_the_batch_files() {
    static HASHES: AtomicUsize = AtomicUsize::new(0);
    static DECODES: AtomicUsize = AtomicUsize::new(0);
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct CountHashesAndDecodes;
    impl Hook for CountHashesAndDecodes {
        fn hash_leaf(_encoded_leaf: &[u8]) { HASHES.fetch_add(1, Ordering::Relaxed); }
        fn decode() { DECODES.fetch_add(1, Ordering::Relaxed); }
    }
    type Counted = Hooked<CountHashesAndDecodes>;

    let leaf = |n: u64, version: u8| {
        let mut id = [0; EntityId::BYTE_LEN];
        id[..8].copy_from_slice(&n.to_be_bytes());
        LeafInput { key: vec![], entity_id: EntityId::from_bytes(id), head: HeadHash::of([[version; 32]]) }
    };
    let spec = TreeSpec::new(KeySpec::new(vec![]), Predicate::True);
    let maps = crate::MemoryMaps::new();
    let mut setup = TreeTransaction::<Counted, _>::bind(&spec, maps.begin());
    setup.build((0..512).map(|n| leaf(n, 1))).await.unwrap();
    setup.commit().await.unwrap();
    // Fifty head changes, twenty-five removals and twenty-five insertions.
    let changes: Vec<LeafChange> = (0..50)
        .map(|n| LeafChange { before: Some(leaf(n, 1)), after: Some(leaf(n, 2)) })
        .chain((50..75).map(|n| LeafChange { before: Some(leaf(n, 1)), after: None }))
        .chain((512..537).map(|n| LeafChange { before: None, after: Some(leaf(n, 1)) }))
        .collect();

    let mut tx = TreeTransaction::<Counted, _>::bind(&spec, maps.begin());
    HASHES.store(0, Ordering::Relaxed);
    DECODES.store(0, Ordering::Relaxed);
    tx.rebuild_with_changes(changes).await.unwrap();
    assert_eq!(HASHES.load(Ordering::Relaxed), 75, "only the leaves the batch files are hashed");
    assert_eq!(DECODES.load(Ordering::Relaxed), 75 + 437, "the leaves the changes expect, then every leaf the batch did not write");
    tx.commit().await.unwrap();

    let built = crate::MemoryMaps::new();
    let mut build = TreeTransaction::<Counted, _>::bind(&spec, built.begin());
    build.build((0..50).map(|n| leaf(n, 2)).chain((75..537).map(|n| leaf(n, 1)))).await.unwrap();
    build.commit().await.unwrap();
    for map in [MapName::Leaves, MapName::Lookup, MapName::Nodes] {
        let bounds = (Bound::Unbounded, Bound::Unbounded);
        assert_eq!(
            maps.begin().scan(map, bounds, usize::MAX).await.unwrap(),
            built.begin().scan(map, bounds, usize::MAX).await.unwrap(),
            "{map:?}"
        );
    }

    let mut stale = TreeTransaction::<Counted, _>::bind(&spec, maps.begin());
    let missing = LeafChange { before: Some(leaf(9_999, 1)), after: None };
    assert!(matches!(stale.rebuild_with_changes([missing]).await, Err(TreeError::Conflict)));
    assert!(matches!(stale.commit().await, Err(TreeError::Conflict)));
}
