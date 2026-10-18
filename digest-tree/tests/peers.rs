//! A source-authoritative reconciliation fixture, using only the standalone
//! API. This tests comparison and repair regions, not event conflict resolution.
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Excluded, Included};

use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate, Selection};
use ankurah_core_types::{EntityId, IndexKeyPart, KeySpec, PropertyId, PropertyPath, Value, ValueType};
use ankurah_digest_tree::{
    leaf_address, CanonicalCodec, DigestGroup, DigestTree, HeadHash, LeafChange, LeafData, LtHash16, Ristretto255, TreeMatch, TreeRequest,
    TreeSpan, TreeSpanDigest, TreeSpec, TreeTransaction,
};

fn property() -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([1; 32])) }
fn spec() -> TreeSpec { TreeSpec::new(KeySpec::new(vec![IndexKeyPart::asc(property(), ValueType::I64)]), Predicate::True) }
fn key(value: i64) -> Vec<u8> { CanonicalCodec.encode_prefix(spec().key_spec(), &[Value::I64(value)]).unwrap() }
fn leaf(value: i64, version: u8) -> LeafData {
    let mut id = [0; 32];
    id[..8].copy_from_slice(&value.to_be_bytes());
    LeafData { key: key(value), entity_id: EntityId::from_bytes(id), head: HeadHash::of([[version; 32]]) }
}
fn address(leaf: &LeafData) -> Vec<u8> { leaf_address(&leaf.key, leaf.entity_id) }
fn model(leaves: impl IntoIterator<Item = LeafData>) -> BTreeMap<Vec<u8>, LeafData> {
    leaves.into_iter().map(|leaf| (address(&leaf), leaf)).collect()
}
async fn build<G: DigestGroup>(leaves: impl IntoIterator<Item = LeafData>) -> DigestTree<G> {
    let tree = DigestTree::new(spec());
    let mut tx = tree.begin();
    tx.build(leaves).await.unwrap();
    tx.commit().await.unwrap();
    tree
}
async fn rows<G: DigestGroup>(tx: &mut TreeTransaction<G>, range: &TreeSpan) -> BTreeMap<Vec<u8>, LeafData> {
    let scope = tx.scope_ranges([range.clone()]);
    let mut result = BTreeMap::new();
    let mut cursor = None;
    loop {
        let page = tx.page(&scope, cursor.as_ref(), 17).await.unwrap();
        for row in page.leaves {
            let leaf = LeafData { key: row.key, entity_id: row.entity_id, head: row.head };
            result.insert(address(&leaf), leaf);
        }
        cursor = page.next;
        if cursor.is_none() {
            return result;
        }
    }
}

fn same_region<G: DigestGroup>(heads: &[TreeSpanDigest<G>], parent: &TreeSpanDigest<G>) -> bool {
    heads.len() == 1 && heads[0].prefix == parent.prefix && heads[0].bounds == parent.bounds
}

/// Compare the same byte regions on independent snapshots. Either side may
/// refine a region; a terminal mismatch falls back to paginated leaf inputs.
async fn differences<G: DigestGroup>(
    opener: &DigestTree<G>,
    responder: &DigestTree<G>,
    ranges: &[TreeSpan],
) -> BTreeMap<Vec<u8>, LeafChange> {
    let mut a = opener.begin();
    let mut b = responder.begin();
    let scope = a.scope_ranges(ranges.iter().cloned());
    let mut pending = a.heads(&scope).await.unwrap();
    let mut changes = BTreeMap::new();
    let mut comparisons = 0;
    while let Some(head) = pending.pop() {
        comparisons += 1;
        assert!(comparisons < 10_000, "descent must make progress");
        let head = TreeSpanDigest::<G>::from_bytes(&head.to_bytes()).unwrap();
        let sa = a.scope_ranges([head.bounds.clone()]);
        let sb = b.scope_ranges([head.bounds.clone()]);
        if a.digest(&sa).await.unwrap() == b.digest(&sb).await.unwrap() {
            continue;
        }
        let mut next = a.refine(&head).await.unwrap();
        if same_region(&next, &head) {
            next = b.refine(&head).await.unwrap();
        }
        if !same_region(&next, &head) {
            assert!(!next.is_empty(), "a mismatching region cannot disappear");
            pending.extend(next);
            continue;
        }
        let local = rows(&mut a, &head.bounds).await;
        let remote = rows(&mut b, &head.bounds).await;
        for key in local.keys().chain(remote.keys()) {
            let before = local.get(key).cloned();
            let after = remote.get(key).cloned();
            if before != after {
                changes.insert(key.clone(), LeafChange { before, after });
            }
        }
    }
    changes
}

async fn reconcile<G: DigestGroup>(opener: &DigestTree<G>, responder: &DigestTree<G>, ranges: &[TreeSpan]) -> usize {
    let changes = differences(opener, responder, ranges).await;
    let count = changes.len();
    let mut tx = opener.begin();
    for change in changes.into_values() {
        tx.apply_change(change).await.unwrap();
    }
    tx.commit().await.unwrap();
    opener.begin().audit().await.unwrap();
    assert!(differences(opener, responder, ranges).await.is_empty());
    count
}

async fn asymmetric_peers<G: DigestGroup>() {
    let initial: Vec<_> = (0..128).map(|n| leaf(n, 0)).collect();
    let opener = build::<G>(initial.clone()).await;
    let mut authoritative = model(initial.clone());
    authoritative.remove(&address(&leaf(81, 0)));
    authoritative.insert(address(&leaf(80, 1)), leaf(80, 1));
    let mut moved = authoritative.remove(&address(&leaf(82, 0))).unwrap();
    moved.key = key(230);
    authoritative.insert(address(&moved), moved);
    for value in [-20, 150, 255, 500] {
        authoritative.insert(address(&leaf(value, 0)), leaf(value, 0));
    }
    let responder = build::<G>(authoritative.values().cloned()).await;
    let range = TreeSpan::new(Included(key(-50)), Excluded(key(300)));
    let expected: BTreeMap<_, _> = authoritative.into_iter().filter(|(address, _)| range.contains(address)).collect();
    assert_eq!(reconcile(&opener, &responder, &[range.clone()]).await, 7);
    assert_eq!(rows(&mut opener.begin(), &range).await, expected);
    assert_eq!(rows(&mut opener.begin(), &TreeSpan::all()).await.len(), expected.len(), "out-of-scope key 500 stays absent");

    let empty = build::<G>([]).await;
    reconcile(&empty, &responder, &[range.clone()]).await;
    assert_eq!(rows(&mut empty.begin(), &range).await, expected);
}

#[tokio::test]
async fn asymmetric_regions_reconcile_in_both_groups() {
    asymmetric_peers::<Ristretto255>().await;
    asymmetric_peers::<LtHash16>().await;
}

#[tokio::test]
async fn head_and_refinement_bounds_cover_empty_gaps_exactly_once() {
    let tree = build::<Ristretto255>((64..192).map(|n| leaf(n, 0))).await;
    let range = TreeSpan::new(Excluded(key(10)), Included(key(250)));
    let mut tx = tree.begin();
    let scope = tx.scope_ranges([range.clone()]);
    let heads = tx.heads(&scope).await.unwrap();
    let samples: Vec<_> = (-20..300).flat_map(|n| [key(n), address(&leaf(n, 0))]).collect();
    for sample in &samples {
        assert_eq!(heads.iter().filter(|head| head.bounds.contains(sample)).count(), usize::from(range.contains(sample)));
    }
    for head in heads {
        let children = tx.refine(&head).await.unwrap();
        for sample in &samples {
            assert_eq!(children.iter().filter(|child| child.bounds.contains(sample)).count(), usize::from(head.bounds.contains(sample)));
        }
    }
}

fn limited_request(direction: OrderDirection, limit: u64) -> TreeRequest {
    TreeRequest::for_selection(
        &Selection {
            predicate: Predicate::True,
            order_by: Some(vec![OrderByItem { path: PropertyPath::from(property()), direction }]),
            limit: Some(limit),
        },
        &HashMap::from([(property(), ValueType::I64)]),
    )
    .unwrap()
}

#[tokio::test]
async fn underfilled_limits_keep_the_range_needed_to_acquire_members() {
    for direction in [OrderDirection::Asc, OrderDirection::Desc] {
        for initial in [0, 3] {
            let opener = build::<Ristretto255>((0..initial).map(|n| leaf(n, 0))).await;
            let responder = build::<Ristretto255>((-10..15).map(|n| leaf(n, 0))).await;
            let request = limited_request(direction.clone(), 10);
            let ranges = opener.begin().scope(&request).await.unwrap().ranges().to_vec();
            assert!(!ranges.is_empty());
            reconcile(&opener, &responder, &ranges).await;
            let mut a = opener.begin();
            let mut b = responder.begin();
            let sa = a.scope(&request).await.unwrap();
            let sb = b.scope(&request).await.unwrap();
            assert_eq!(a.count(&sa).await.unwrap(), 10);
            assert_eq!(a.digest(&sa).await.unwrap(), b.digest(&sb).await.unwrap());
            assert!(a.scope(&limited_request(direction.clone(), 0)).await.unwrap().ranges().is_empty());
        }
    }
}

#[tokio::test]
async fn equal_membership_with_different_local_shapes_compares_equal() {
    let compact = build::<Ristretto255>((0..64).map(|n| leaf(n, 0))).await;
    let expanded = build::<Ristretto255>((0..100).map(|n| leaf(n, 0))).await;
    let mut tx = expanded.begin();
    for n in 64..100 {
        tx.apply_change(LeafChange { before: Some(leaf(n, 0)), after: None }).await.unwrap();
    }
    tx.commit().await.unwrap();
    assert_ne!(compact.begin().audit().await.unwrap().nodes, expanded.begin().audit().await.unwrap().nodes);
    for range in [TreeSpan::all(), TreeSpan::new(Included(key(-20)), Excluded(key(80)))] {
        assert!(differences(&compact, &expanded, &[range.clone()]).await.is_empty());
        assert!(differences(&expanded, &compact, &[range]).await.is_empty());
    }
}

#[tokio::test]
async fn peers_with_different_admissions_compare_a_proven_common_selection() {
    let selection = |threshold| {
        Selection::from(Predicate::Comparison {
            left: Box::new(Expr::Path(PropertyPath::from(property()))),
            operator: ComparisonOperator::GreaterThan,
            right: Box::new(Expr::Literal(Value::I64(threshold))),
        })
    };
    let types = HashMap::from([(property(), ValueType::I64)]);
    let broad: DigestTree = DigestTree::new(TreeSpec::for_selection(&selection(7), &types).unwrap());
    let narrow: DigestTree = DigestTree::new(TreeSpec::for_selection(&selection(10), &types).unwrap());
    assert_eq!(broad.spec().key_spec(), narrow.spec().key_spec());
    let mut tx = broad.begin();
    tx.build((8..128).map(|n| leaf(n, 0))).await.unwrap();
    tx.commit().await.unwrap();
    let mut tx = narrow.begin();
    tx.build((11..128).chain([150]).map(|n| leaf(n, u8::from(n == 80)))).await.unwrap();
    tx.commit().await.unwrap();

    let request = TreeRequest::for_selection(&selection(10), &types).unwrap();
    assert_eq!(broad.spec().serves(&request), Some(TreeMatch::Span));
    assert_eq!(narrow.spec().serves(&request), Some(TreeMatch::Whole));
    let ranges = broad.begin().scope(&request).await.unwrap().ranges().to_vec();
    let narrow_ranges = narrow.begin().scope(&request).await.unwrap().ranges().to_vec();
    // A whole-tree scope omits constraints already enforced by admission.
    // Sending its raw bounds to the broader peer would admit keys 8..=10.
    assert!(narrow_ranges.iter().any(|range| range.contains(&address(&leaf(8, 0)))));
    assert!(!ranges.iter().any(|range| range.contains(&address(&leaf(8, 0)))));
    // These bounds came from the broader tree's proof of the common request.
    // Both peers have the same key layout and can compare this region exactly.
    assert_eq!(reconcile(&broad, &narrow, &ranges).await, 2);
    let mut tx = broad.begin();
    for n in 8..=10 {
        assert!(tx.leaf(&key(n), leaf(n, 0).entity_id).await.unwrap().is_some());
    }
    let sa = tx.scope(&request).await.unwrap();
    let mut other = narrow.begin();
    let sb = other.scope(&request).await.unwrap();
    assert_eq!(tx.digest(&sa).await.unwrap(), other.digest(&sb).await.unwrap());
}
