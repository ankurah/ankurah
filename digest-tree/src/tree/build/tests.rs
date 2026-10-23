use super::*;
use crate::digest::HeadHash;
use crate::{LeafChange, Ristretto255, TreeSpec};
use ankql::ast::Predicate;
use ankurah_core_types::{EntityId, KeySpec};
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
        assert!(matches!(tx.commit().await, Err(TreeError::Conflict)));
        assert_eq!(TreeTransaction::<LaterBatchPanic, _>::bind(&spec, maps.begin()).audit().await.unwrap().leaves, 1);
    });
}
