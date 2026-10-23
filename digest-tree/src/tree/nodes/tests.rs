use super::*;
use crate::store::{ByteBounds, Failure, MapTransaction};
use crate::tree::fixtures::{empty_tree, encoded, entity, in_each_group, node_base, prefix, row};
use crate::TreeSpan;
use async_trait::async_trait;
use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// The digest of the one leaf of the entity `n` under the key `[n]`.
fn leaf<G: DigestGroup>(n: u8) -> Digest<G> { Digest::from(row::<G>(&[n], entity(n)).element) }

/// The digest of a tree node summing the leaves 1 to `leaves`.
fn node<G: DigestGroup>(leaves: u8) -> Digest<G> { (1..=leaves).map(leaf).sum() }

async fn node_keys<G: DigestGroup, T: MapAccess>(maps: &mut TreeTransaction<G, T>) -> usize {
    maps.transaction.scan(MapName::Nodes, (Bound::Unbounded, Bound::Unbounded), usize::MAX).await.unwrap().len()
}

async fn delta_count<G: DigestGroup, T: MapAccess>(maps: &mut TreeTransaction<G, T>, prefix: &NodePrefix) -> usize {
    let (first, past) = layout::delta_key_bounds(&prefix.to_bytes());
    let bounds = (Bound::Included(first.as_slice()), past.as_deref().map_or(Bound::Unbounded, Bound::Excluded));
    maps.transaction.scan(MapName::Nodes, bounds, usize::MAX).await.unwrap().len()
}

/// Append one delta to the tree node at `prefix`, as a change beneath it
/// does.
async fn append_delta<G: DigestGroup, T: MapAccess>(
    maps: &mut TreeTransaction<G, T>,
    prefix: &NodePrefix,
    delta: Digest<G>,
) -> Result<(), TreeError> {
    maps.append_node_deltas(std::slice::from_ref(prefix), delta).await
}

/// Remove the tree node at `prefix`, its base row and every delta, each only
/// if still as read: [`TreeError::Conflict`] otherwise. Its descendants stay,
/// and so does a delta appended after the deletion read: the damage a
/// concurrent writer can leave.
async fn delete_node<G: DigestGroup, T: MapAccess>(maps: &mut TreeTransaction<G, T>, prefix: &NodePrefix) -> Result<(), TreeError> {
    if let Some(base) = maps.base(prefix).await? {
        maps.transaction.delete_if(MapName::Nodes, &prefix.to_bytes(), &base.value).await?;
    }
    let mut deltas = DeltaPages::of(prefix);
    loop {
        let page = deltas.next(&mut maps.transaction).await?;
        if page.is_empty() {
            return Ok(());
        }
        for (key, value) in page {
            maps.transaction.delete_if(MapName::Nodes, &key, &value).await?;
        }
    }
}

/// The prefixes of some tree nodes.
fn prefixes<G: DigestGroup>(nodes: Vec<(NodePrefix, Digest<G>)>) -> Vec<NodePrefix> {
    nodes.into_iter().map(|(prefix, _)| prefix).collect()
}

/// A transaction that lets another commit land between two of its reads,
/// as a concurrent transaction would: once, before its next scan of the
/// nodes map, it awaits the meddle.
struct Interleaved<T> {
    inner: T,
    before_next_nodes_scan: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

#[async_trait]
impl<T: MapAccess> MapAccess for Interleaved<T> {
    fn poison(&mut self, failure: Failure) { self.inner.poison(failure); }
    fn watch_structure(&mut self) { self.inner.watch_structure(); }
    fn change_structure(&mut self) { self.inner.change_structure(); }
    async fn scan_reverse(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError> {
        self.inner.scan_reverse(map, bounds, limit).await
    }
    async fn get(&mut self, map: MapName, key: &[u8]) -> Result<Option<Vec<u8>>, TreeError> { self.inner.get(map, key).await }

    async fn scan(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError> {
        if map == MapName::Nodes {
            if let Some(meddle) = self.before_next_nodes_scan.take() {
                meddle.await;
            }
        }
        self.inner.scan(map, bounds, limit).await
    }

    async fn insert(&mut self, map: MapName, key: &[u8], value: &[u8]) -> Result<(), TreeError> { self.inner.insert(map, key, value).await }

    async fn put_if(&mut self, map: MapName, key: &[u8], expected: Option<&[u8]>, value: &[u8]) -> Result<(), TreeError> {
        self.inner.put_if(map, key, expected, value).await
    }

    async fn delete_if(&mut self, map: MapName, key: &[u8], expected: &[u8]) -> Result<(), TreeError> {
        self.inner.delete_if(map, key, expected).await
    }

    async fn append(&mut self, map: MapName, key_prefix: &[u8], value: &[u8]) -> Result<(), TreeError> {
        self.inner.append(map, key_prefix, value).await
    }
}

#[async_trait]
impl<T: MapTransaction> MapTransaction for Interleaved<T> {
    async fn commit(self) -> Result<(), TreeError> { self.inner.commit().await }
}

in_each_group!(
    a_node_is_its_base_plus_its_deltas,
    compaction_folds_the_deltas_it_read_into_the_base,
    compaction_leaves_a_late_delta_and_fails_whole_on_a_removed_one,
    a_delta_without_its_base_is_not_a_subtree_summary,
    children_skip_compressed_levels_and_delta_rows,
    children_reject_a_missing_base_at_seven_bits,
    children_reject_missing_bases_at_fourteen_bits,
    more_deltas_than_a_page_in_any_suffix_order_read_and_compact_whole,
    competing_compactors_fold_each_delta_once,
    a_reader_crossing_a_compaction_sees_the_node_whole,
);

async fn a_node_is_its_base_plus_its_deltas<G: DigestGroup>() {
    let tree = empty_tree::<G>();
    let bucket = prefix("0110");
    let mut maps = tree.begin();
    maps.insert_node_base(&bucket, node(2)).await.unwrap();
    assert_eq!(maps.node(&bucket).await.unwrap(), Some(node(2)));
    maps.commit().await.unwrap();
    assert!(matches!(tree.begin().insert_node_base(&bucket, node(2)).await, Err(TreeError::Conflict)), "a base is there now");

    // Leaf 3 arrives, then leaf 1 leaves: a delta with a negative count.
    // Each append takes a key of its own, so both are summed.
    let mut maps = tree.begin();
    append_delta(&mut maps, &bucket, leaf(3)).await.unwrap();
    append_delta(&mut maps, &bucket, -leaf(1)).await.unwrap();
    maps.commit().await.unwrap();
    let mut maps = tree.begin();
    let summed = maps.node(&bucket).await.unwrap().unwrap();
    assert_eq!(summed, leaf(2) + leaf(3));
    assert_eq!(summed.count(), 2);
    assert_eq!(node_base(&mut maps, &bucket).await, Some(node(2)), "the base alone");
    assert_eq!(node_keys(&mut maps).await, 3, "the base and two deltas");

    // An empty baseline is explicit.
    let fresh = prefix("1");
    maps.insert_node_base(&fresh, node(0)).await.unwrap();
    append_delta(&mut maps, &fresh, leaf(5)).await.unwrap();
    assert_eq!(maps.node(&fresh).await.unwrap(), Some(leaf(5)));
    assert_eq!(node_base(&mut maps, &fresh).await, Some(node(0)));
    maps.commit().await.unwrap();
}

async fn compaction_folds_the_deltas_it_read_into_the_base<G: DigestGroup>() {
    let tree = empty_tree::<G>();
    let bucket = prefix("0110");
    let mut maps = tree.begin();
    maps.insert_node_base(&bucket, node(2)).await.unwrap();
    for n in [3, 4, 5] {
        append_delta(&mut maps, &bucket, leaf(n)).await.unwrap();
    }
    let before = maps.node(&bucket).await.unwrap();
    maps.commit().await.unwrap();

    let mut maps = tree.begin();
    assert!(!maps.compact_node(&bucket).await.unwrap(), "the deltas fit one batch");
    assert_eq!(maps.node(&bucket).await.unwrap(), before, "the sum is unchanged");
    maps.commit().await.unwrap();
    let mut maps = tree.begin();
    assert_eq!(maps.node(&bucket).await.unwrap(), before);
    assert_eq!(node_base(&mut maps, &bucket).await, Some(node(5)));
    assert_eq!(node_keys(&mut maps).await, 1, "the base alone");
    assert!(!maps.compact_node(&bucket).await.unwrap());
    assert_eq!(node_keys(&mut maps).await, 1, "nothing to fold");

    // An explicit empty base can be compacted like any other.
    let fresh = prefix("1");
    maps.insert_node_base(&fresh, node(0)).await.unwrap();
    append_delta(&mut maps, &fresh, leaf(6)).await.unwrap();
    maps.compact_node(&fresh).await.unwrap();
    maps.commit().await.unwrap();
    let mut maps = tree.begin();
    assert_eq!(node_base(&mut maps, &fresh).await, Some(leaf(6)));
    assert_eq!(node_keys(&mut maps).await, 2);
}

/// A delta appended after a compaction read is left in place, and a delta
/// it read that another writer removed meanwhile fails the compaction
/// whole: nothing it wrote lands.
async fn compaction_leaves_a_late_delta_and_fails_whole_on_a_removed_one<G: DigestGroup>() {
    let tree = empty_tree::<G>();
    let bucket = prefix("0110");
    let mut maps = tree.begin();
    maps.insert_node_base(&bucket, node(2)).await.unwrap();
    append_delta(&mut maps, &bucket, leaf(3)).await.unwrap();
    maps.commit().await.unwrap();

    let mut compactor = tree.begin();
    compactor.compact_node(&bucket).await.unwrap();
    let mut late = tree.begin();
    append_delta(&mut late, &bucket, leaf(4)).await.unwrap();
    late.commit().await.unwrap();
    compactor.commit().await.unwrap();
    let mut maps = tree.begin();
    assert_eq!(node_base(&mut maps, &bucket).await, Some(node(3)), "what compaction read");
    assert_eq!(delta_count(&mut maps, &bucket).await, 1, "the late delta stays");
    assert_eq!(maps.node(&bucket).await.unwrap(), Some(node(4)));

    let mut compactor = tree.begin();
    compactor.compact_node(&bucket).await.unwrap();
    let mut remover = tree.begin();
    let (first, past) = layout::delta_key_bounds(&bucket.to_bytes());
    let bounds = (Bound::Included(first.as_slice()), past.as_deref().map_or(Bound::Unbounded, Bound::Excluded));
    let (key, value) = remover.transaction.scan(MapName::Nodes, bounds, 1).await.unwrap().remove(0);
    remover.transaction.delete_if(MapName::Nodes, &key, &value).await.unwrap();
    remover.commit().await.unwrap();
    assert!(matches!(compactor.commit().await, Err(TreeError::Conflict)));
    let mut maps = tree.begin();
    assert_eq!(node_base(&mut maps, &bucket).await, Some(node(3)), "the base as it was");
    assert_eq!(delta_count(&mut maps, &bucket).await, 0, "as the remover left it");
    assert_eq!(maps.node(&bucket).await.unwrap(), Some(node(3)));
}

/// Removing a parent's summary leaves its leaves and children intact. A
/// concurrent insertion can leave a delta behind, but that delta alone
/// must never stand for the parent or be compacted into a base.
async fn a_delta_without_its_base_is_not_a_subtree_summary<G: DigestGroup>() {
    let tree = empty_tree::<G>();
    let (root, parent, left, right) = (prefix(""), prefix("0"), prefix("00"), prefix("01"));
    let (a, b, c) = (row::<G>(b"", entity(0x01)), row::<G>(b"", entity(0x41)), row::<G>(b"", entity(0x02)));
    let (da, db, dc) = (Digest::from(a.element), Digest::from(b.element), Digest::from(c.element));
    let mut build = tree.begin();
    build.insert_leaf(&encoded(a)).await.unwrap();
    build.insert_leaf(&encoded(b)).await.unwrap();
    for (p, digest) in [(&root, da + db), (&parent, da + db), (&left, da), (&right, db)] {
        build.insert_node_base(p, digest).await.unwrap();
    }
    build.commit().await.unwrap();

    let (mut deletion, mut append) = (tree.begin(), tree.begin());
    delete_node(&mut deletion, &parent).await.unwrap();
    append.insert_leaf(&encoded(c)).await.unwrap();
    for p in [&root, &parent, &left] {
        append_delta(&mut append, p, dc).await.unwrap();
    }
    append.commit().await.unwrap();
    deletion.commit().await.unwrap();

    let mut maps = tree.begin();
    let leaves: Digest<G> = maps.leaves(&TreeSpan::under(&parent), 10).await.unwrap().iter().map(|row| Digest::from(row.element)).sum();
    assert_eq!(leaves.count(), 3);
    let children: Digest<G> = maps.children(&parent).await.unwrap().iter().map(|(_, digest)| *digest).sum();
    assert_eq!(children, leaves);
    assert_eq!(maps.node(&root).await.unwrap().unwrap(), leaves);
    assert!(matches!(maps.node(&parent).await, Err(TreeError::IncompleteNode(p)) if p == parent));
    assert!(matches!(maps.children(&root).await, Err(TreeError::IncompleteNode(p)) if p == parent));
    assert!(matches!(maps.compact_node(&parent).await, Err(TreeError::IncompleteNode(p)) if p == parent));
    assert!(matches!(maps.commit().await, Err(TreeError::Poisoned)));
    let mut maps = tree.begin();
    assert_eq!(node_base(&mut maps, &parent).await, None, "compaction wrote no base");
    assert_eq!(delta_count(&mut maps, &parent).await, 1, "compaction consumed no delta");
}

/// Children are the nearest existing tree nodes beneath a prefix, with deltas
/// on the parent and the children in the way, across both placements of a
/// tree node's deltas: after its own base (a prefix of one bit) and after its
/// descendants (the root, and a prefix of seven bits, whose descendants'
/// encodings extend its bytes).
async fn children_skip_compressed_levels_and_delta_rows<G: DigestGroup>() {
    let tree = empty_tree::<G>();
    let mut maps = tree.begin();
    // The root, its children 0 and 1; beneath 0 the seven-bit 0000000 and
    // 01; beneath 0000000 its child 00000000; 01 starts with an empty base.
    for (text, leaves) in [("", 6), ("0", 3), ("0000000", 2), ("00000000", 2), ("01", 0), ("1", 3)] {
        maps.insert_node_base(&prefix(text), node(leaves)).await.unwrap();
    }
    for text in ["", "0", "0000000", "1", "01"] {
        append_delta(&mut maps, &prefix(text), leaf(7)).await.unwrap();
    }
    assert_eq!(maps.child_prefixes(&prefix("0")).await.unwrap(), [prefix("0000000"), prefix("01")], "pending insertions");
    maps.commit().await.unwrap();

    let mut maps = tree.begin();
    assert_eq!(prefixes(maps.children(&NodePrefix::root()).await.unwrap()), [prefix("0"), prefix("1")]);
    assert_eq!(prefixes(maps.children(&prefix("0")).await.unwrap()), [prefix("0000000"), prefix("01")]);
    assert_eq!(prefixes(maps.children(&prefix("00")).await.unwrap()), [prefix("0000000")], "beneath an unstored prefix too");
    assert_eq!(prefixes(maps.children(&prefix("0000000")).await.unwrap()), [prefix("00000000")]);
    assert_eq!(prefixes(maps.children(&prefix("00000000")).await.unwrap()), []);
    assert_eq!(prefixes(maps.children(&prefix("01")).await.unwrap()), []);
    assert_eq!(prefixes(maps.children(&prefix("1")).await.unwrap()), [], "the last prefix, with nothing after it");

    let (child, summary) = &maps.children(&prefix("0")).await.unwrap()[1];
    assert_eq!((child, summary), (&prefix("01"), &leaf(7)), "a child with an explicit empty baseline");
    assert_eq!(maps.node(&prefix("0")).await.unwrap(), Some(node(3) + leaf(7)));
    delete_node(&mut maps, &prefix("01")).await.unwrap();
    assert_eq!(maps.child_prefixes(&prefix("0")).await.unwrap(), [prefix("0000000")], "pending removal hides a committed child");
    assert_eq!(maps.child_prefixes(&prefix("0")).await.unwrap(), prefixes(maps.children(&prefix("0")).await.unwrap()));
}

/// A tree node whose prefix ends on a whole byte of its encoding, as one of
/// seven bits does, keeps its deltas after everything beneath it. A
/// missing base must still be detected after scanning its descendants.
async fn children_reject_a_missing_base_at_seven_bits<G: DigestGroup>() {
    let tree = empty_tree::<G>();
    let deltas_only = prefix("0110100");
    let (leftmost, rightmost) = (prefix("01101000000000"), prefix("01101001111111"));
    let mut maps = tree.begin();
    for based in [prefix("00"), leftmost.clone(), rightmost.clone(), prefix("1")] {
        maps.insert_node_base(&based, node(2)).await.unwrap();
    }
    append_delta(&mut maps, &deltas_only, leaf(5)).await.unwrap();
    maps.commit().await.unwrap();

    let mut maps = tree.begin();
    assert!(matches!(maps.children(&NodePrefix::root()).await, Err(TreeError::IncompleteNode(p)) if p == deltas_only));
    assert!(matches!(maps.children(&prefix("0")).await, Err(TreeError::IncompleteNode(p)) if p == deltas_only));
    assert_eq!(prefixes(maps.children(&deltas_only).await.unwrap()), [leftmost, rightmost]);
    assert!(matches!(maps.node(&deltas_only).await, Err(TreeError::IncompleteNode(p)) if p == deltas_only));
}

/// The same at fourteen bits, the second whole byte, beneath a tree node
/// with a base, and for one tree node of deltas beneath another.
async fn children_reject_missing_bases_at_fourteen_bits<G: DigestGroup>() {
    let tree = empty_tree::<G>();
    let (parent, deltas_only) = (prefix("1010101"), prefix("10101010011001"));
    let (leftmost, rightmost) = (prefix("101010100110010"), prefix("101010100110011111111"));
    let (outer, inner) = (prefix("0011110"), prefix("00111101100110"));
    let (inner_leftmost, inner_rightmost) = (prefix("001111011001100000000"), prefix("001111011001101"));
    let mut maps = tree.begin();
    for based in [&parent, &leftmost, &rightmost, &inner_leftmost, &inner_rightmost] {
        maps.insert_node_base(based, node(2)).await.unwrap();
    }
    for (deltas_only, n) in [(&deltas_only, 5), (&outer, 6), (&inner, 7)] {
        append_delta(&mut maps, deltas_only, leaf(n)).await.unwrap();
    }
    maps.commit().await.unwrap();

    let mut maps = tree.begin();
    assert!(matches!(maps.children(&NodePrefix::root()).await, Err(TreeError::IncompleteNode(p)) if p == inner));
    assert!(matches!(maps.children(&parent).await, Err(TreeError::IncompleteNode(p)) if p == deltas_only));
    assert_eq!(prefixes(maps.children(&deltas_only).await.unwrap()), [leftmost, rightmost]);
    assert!(matches!(maps.children(&outer).await, Err(TreeError::IncompleteNode(p)) if p == inner));
    assert_eq!(prefixes(maps.children(&inner).await.unwrap()), [inner_leftmost, inner_rightmost]);
    assert!(matches!(maps.node(&outer).await, Err(TreeError::IncompleteNode(p)) if p == outer));
    assert!(matches!(maps.node(&inner).await, Err(TreeError::IncompleteNode(p)) if p == inner));
}

/// More deltas than a page and a compaction batch, filed under suffixes in
/// no particular order: a read sums them all, and batched compaction folds
/// them all, a batch per transaction, leaving a delta appended after a
/// batch's read, whose key sorts among the batch's, for the next batch.
async fn more_deltas_than_a_page_in_any_suffix_order_read_and_compact_whole<G: DigestGroup>() {
    let tree = empty_tree::<G>();
    let bucket = prefix("0110");
    let (first, _) = layout::delta_key_bounds(&bucket.to_bytes());
    let delta_key = |suffix: &[u8]| [first.as_slice(), suffix].concat();
    let leaves: Vec<Digest<G>> = (1..=5).map(leaf).collect();
    let mut suffixes: Vec<u32> = (0..1100).collect();
    suffixes.shuffle(&mut SmallRng::seed_from_u64(23));

    let mut maps = tree.begin();
    maps.insert_node_base(&bucket, node(2)).await.unwrap();
    let mut expected = node::<G>(2);
    for suffix in suffixes {
        let delta = leaves[suffix as usize % leaves.len()];
        expected += delta;
        maps.transaction.insert(MapName::Nodes, &delta_key(&suffix.to_be_bytes()), &layout::digest_value(&delta)).await.unwrap();
    }
    maps.commit().await.unwrap();
    let mut maps = tree.begin();
    assert_eq!(maps.node(&bucket).await.unwrap(), Some(expected));
    assert_eq!(expected.count(), 2 + 1100);
    assert_eq!(prefixes(maps.children(&NodePrefix::root()).await.unwrap()), std::slice::from_ref(&bucket));

    let mut compactor = tree.begin();
    assert!(compactor.compact_node(&bucket).await.unwrap(), "more deltas than a batch");
    // Suffix 5 and then a zero byte sorts between the suffixes 5 and 6,
    // among the batch's keys.
    let late_delta = leaf::<G>(6);
    let mut late = tree.begin();
    late.transaction.insert(MapName::Nodes, &delta_key(&[0, 0, 0, 5, 0]), &layout::digest_value(&late_delta)).await.unwrap();
    late.commit().await.unwrap();
    compactor.commit().await.unwrap();

    let mut maps = tree.begin();
    assert_eq!(maps.node(&bucket).await.unwrap(), Some(expected + late_delta), "nothing lost or folded twice");
    assert_eq!(delta_count(&mut maps, &bucket).await, 1100 - COMPACTION_BATCH + 1, "the late delta among them");
    assert!(!maps.compact_node(&bucket).await.unwrap(), "the rest fit one batch");
    maps.commit().await.unwrap();
    let mut maps = tree.begin();
    assert_eq!(delta_count(&mut maps, &bucket).await, 0);
    assert_eq!(node_base(&mut maps, &bucket).await, Some(expected + late_delta));
}

/// Two compactions of one tree node read the same deltas: the first folds
/// them, and the second fails whole, so no delta is folded twice.
async fn competing_compactors_fold_each_delta_once<G: DigestGroup>() {
    let tree = empty_tree::<G>();
    let bucket = prefix("0110");
    let mut maps = tree.begin();
    maps.insert_node_base(&bucket, node(2)).await.unwrap();
    for n in [3, 4, 5] {
        append_delta(&mut maps, &bucket, leaf(n)).await.unwrap();
    }
    maps.commit().await.unwrap();

    let (mut first, mut second) = (tree.begin(), tree.begin());
    first.compact_node(&bucket).await.unwrap();
    second.compact_node(&bucket).await.unwrap();
    first.commit().await.unwrap();
    assert!(matches!(second.commit().await, Err(TreeError::Conflict)));

    let mut maps = tree.begin();
    assert_eq!(maps.node(&bucket).await.unwrap(), Some(node(5)));
    assert_eq!(node_base(&mut maps, &bucket).await, Some(node(5)));
    assert_eq!(delta_count(&mut maps, &bucket).await, 0);
}

/// A reader that reads a tree node's base, then its deltas, with a
/// compaction landing in between, still sums the tree node it began with:
/// the base and the delta, not the base alone.
async fn a_reader_crossing_a_compaction_sees_the_node_whole<G: DigestGroup + 'static>() {
    let tree = Arc::new(empty_tree::<G>());
    let bucket = prefix("0110");
    let mut maps = tree.begin();
    maps.insert_node_base(&bucket, node(2)).await.unwrap();
    append_delta(&mut maps, &bucket, leaf(3)).await.unwrap();
    maps.commit().await.unwrap();

    let (compacting, compacted) = (tree.clone(), bucket.clone());
    let compaction = async move {
        let mut compactor = compacting.begin();
        compactor.compact_node(&compacted).await.unwrap();
        compactor.commit().await.unwrap();
    };
    let interleaved = Interleaved { inner: tree.maps.begin(), before_next_nodes_scan: Some(Box::pin(compaction)) };
    let mut reader = TreeTransaction::<G, _>::bind(tree.spec(), interleaved);
    assert_eq!(reader.node(&bucket).await.unwrap(), Some(node(3)), "the base and the delta it began with");

    let mut maps = tree.begin();
    assert_eq!(node_base(&mut maps, &bucket).await, Some(node(3)), "the compaction landed");
    assert_eq!(maps.node(&bucket).await.unwrap(), Some(node(3)));
}
