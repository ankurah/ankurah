use ankql::ast::Predicate;
use ankurah_core_types::{EntityId, KeySpec};
use ankurah_digest_tree::{Digest, DigestGroup, DigestTree, HeadHash, Leaf, LeafChange, LeafData, NodePrefix, TreeError, TreeSpec};
use std::collections::BTreeMap;

fn empty_tree<G: DigestGroup>() -> DigestTree<G> { DigestTree::new(TreeSpec::new(KeySpec::new(vec![]), Predicate::True)) }
fn leaf(n: u16, version: u8) -> LeafData {
    let mut id = [0; 32];
    id[..2].copy_from_slice(&n.to_be_bytes());
    LeafData { key: vec![], entity_id: EntityId::from_bytes(id), head: HeadHash::of([[version; 32]]) }
}
async fn check<G: DigestGroup>(tree: &DigestTree<G>, expected: &BTreeMap<u16, LeafData>) {
    let mut tx = tree.begin();
    assert_eq!(tx.audit().await.unwrap().leaves, expected.len());
    let digest: Digest<G> = expected.values().map(|v| Digest::from(Leaf::new(&v.key, v.entity_id, v.head).element())).sum();
    assert_eq!(tx.node(&NodePrefix::root()).await.unwrap().unwrap().digest, digest);
}
async fn maintenance<G: DigestGroup>() {
    let tree = empty_tree::<G>();
    let mut model = BTreeMap::new();
    let mut tx = tree.begin();
    tx.build([]).await.unwrap();
    tx.commit().await.unwrap();
    for n in 0..150 {
        let new = leaf(n, 1);
        let mut tx = tree.begin();
        tx.apply_change(LeafChange { before: None, after: Some(new.clone()) }).await.unwrap();
        tx.commit().await.unwrap();
        model.insert(n, new);
    }
    check(&tree, &model).await;
    assert!(tree.begin().audit().await.unwrap().nodes > 1);
    for n in 0..130 {
        let old = model.remove(&n).unwrap();
        let mut tx = tree.begin();
        tx.apply_change(LeafChange { before: Some(old), after: None }).await.unwrap();
        tx.commit().await.unwrap();
    }
    check(&tree, &model).await;
    assert_eq!(tree.begin().audit().await.unwrap().nodes, 1);
    for n in 130..150 {
        let new = leaf(n, 2);
        let old = model.insert(n, new.clone()).unwrap();
        let mut tx = tree.begin();
        tx.apply_change(LeafChange { before: Some(old), after: Some(new) }).await.unwrap();
        tx.commit().await.unwrap();
    }
    check(&tree, &model).await;
    let rebuilt = empty_tree::<G>();
    let mut tx = rebuilt.begin();
    tx.build(model.values().cloned()).await.unwrap();
    tx.commit().await.unwrap();
    check(&rebuilt, &model).await;
}
#[tokio::test]
async fn split_merge_replace_and_build_ristretto() { maintenance::<ankurah_digest_tree::Ristretto255>().await; }
#[tokio::test]
async fn split_merge_replace_and_build_lthash() { maintenance::<ankurah_digest_tree::LtHash16>().await; }

#[tokio::test]
async fn concurrent_updates_commute_but_rebuilds_and_stale_changes_retry() {
    let tree = empty_tree::<ankurah_digest_tree::Ristretto255>();
    let mut setup = tree.begin();
    setup.build((0..80).map(|n| leaf(n, 1))).await.unwrap();
    setup.commit().await.unwrap();
    let (mut a, mut b) = (tree.begin(), tree.begin());
    a.apply_change(LeafChange { before: Some(leaf(1, 1)), after: Some(leaf(1, 2)) }).await.unwrap();
    b.apply_change(LeafChange { before: Some(leaf(2, 1)), after: Some(leaf(2, 2)) }).await.unwrap();
    a.commit().await.unwrap();
    b.commit().await.unwrap();
    tree.begin().audit().await.unwrap();
    for rebuild_first in [false, true] {
        let (mut rebuild, mut update) = (tree.begin(), tree.begin());
        rebuild.build((0..80).map(|n| leaf(n, 1))).await.unwrap();
        update.apply_change(LeafChange { before: Some(leaf(3, 1)), after: Some(leaf(3, 2)) }).await.unwrap();
        if rebuild_first {
            rebuild.commit().await.unwrap();
            assert!(matches!(update.commit().await, Err(TreeError::Conflict)));
        } else {
            update.commit().await.unwrap();
            assert!(matches!(rebuild.commit().await, Err(TreeError::Conflict)));
            let mut restore = tree.begin();
            restore.apply_change(LeafChange { before: Some(leaf(3, 2)), after: Some(leaf(3, 1)) }).await.unwrap();
            restore.commit().await.unwrap();
        }
        tree.begin().audit().await.unwrap();
    }
    let mut stale = tree.begin();
    stale.apply_change(LeafChange { before: None, after: Some(leaf(100, 1)) }).await.unwrap();
    assert!(matches!(stale.apply_change(LeafChange { before: Some(leaf(3, 99)), after: None }).await, Err(TreeError::Conflict)));
    assert!(matches!(stale.commit().await, Err(TreeError::Conflict)));
    assert!(tree.begin().entity_leaves(leaf(100, 1).entity_id).await.unwrap().is_empty());
}

#[tokio::test]
async fn split_and_merge_conflict_with_updates_in_both_commit_orders() {
    for split in [true, false] {
        for structural_first in [true, false] {
            let tree = empty_tree::<ankurah_digest_tree::Ristretto255>();
            let initial = if split { 64 } else { 65 };
            let mut setup = tree.begin();
            setup.build((0..initial).map(|n| leaf(n, 1))).await.unwrap();
            setup.commit().await.unwrap();
            let (mut structure, mut update) = (tree.begin(), tree.begin());
            if split {
                structure.apply_change(LeafChange { before: None, after: Some(leaf(64, 1)) }).await.unwrap();
            } else {
                for n in 32..65 {
                    structure.apply_change(LeafChange { before: Some(leaf(n, 1)), after: None }).await.unwrap();
                }
            }
            update.apply_change(LeafChange { before: Some(leaf(1, 1)), after: Some(leaf(1, 2)) }).await.unwrap();
            if structural_first {
                structure.commit().await.unwrap();
                assert!(matches!(update.commit().await, Err(TreeError::Conflict)));
            } else {
                update.commit().await.unwrap();
                assert!(matches!(structure.commit().await, Err(TreeError::Conflict)));
            }
            tree.begin().audit().await.unwrap();
        }
    }
}

#[tokio::test]
async fn seeded_updates_moves_and_deletes_match_an_independent_model() {
    use ankurah_core_types::{IndexKeyPart, PropertyId, Value, ValueType};
    use ankurah_digest_tree::{CanonicalCodec, KeyCodec};
    use rand::{rngs::SmallRng, Rng, SeedableRng};
    let key = KeySpec::new(vec![IndexKeyPart::asc(PropertyId::EntityId(EntityId::from_bytes([8; 32])), ValueType::I64)]);
    let tree = DigestTree::<ankurah_digest_tree::Ristretto255>::new(TreeSpec::new(key, Predicate::True));
    let mut model = BTreeMap::new();
    let mut setup = tree.begin();
    setup.build([]).await.unwrap();
    setup.commit().await.unwrap();
    let mut rng = SmallRng::seed_from_u64(314159);
    for step in 0..400 {
        let n = rng.gen_range(0..180u16);
        let before = model.get(&n).cloned();
        let after = rng.gen_bool(0.8).then(|| {
            let mut row = leaf(n, rng.gen());
            row.key = CanonicalCodec.encode_prefix(tree.spec().key_spec(), &[Value::I64(rng.gen_range(-50..50))]).unwrap();
            row
        });
        let mut tx = tree.begin();
        tx.apply_change(LeafChange { before, after: after.clone() }).await.unwrap();
        tx.commit().await.unwrap();
        match after {
            Some(leaf) => {
                model.insert(n, leaf);
            }
            None => {
                model.remove(&n);
            }
        }
        if step % 31 == 0 {
            check(&tree, &model).await;
        }
    }
    check(&tree, &model).await;
}
