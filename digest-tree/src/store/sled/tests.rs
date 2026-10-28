//! What the sled store keeps in memory. The tests change rows on disk
//! underneath the store: a read that returns the old row came from memory,
//! and one that returns the new row came from disk.

use super::*;
use crate::common::Tag;
use crate::store::RowKey;
use crate::NodePrefix;

fn temporary_tree() -> sled::Tree { sled::Config::new().temporary(true).open().unwrap().open_tree("rows").unwrap() }

/// Replace a row on disk without the store's knowledge.
fn overwrite_on_disk(tree: &sled::Tree, key: &RowKey, row: &[u8]) { tree.insert(key.encode(), Tag(row.to_vec()).encode()).unwrap(); }

fn tag(row: &[u8]) -> Option<Tag> { Some(Tag(row.to_vec())) }

#[tokio::test]
async fn rows_that_stay_in_memory_outlast_any_number_of_recent_rows() {
    let tree = temporary_tree();
    let store = SledStore::<Tag>::with_recent_capacity(tree.clone(), 2);
    let (meta, node) = (RowKey::Meta, RowKey::Node(NodePrefix::root()));

    store
        .write(vec![WriteItem::Put(meta.clone(), Tag(b"meta".to_vec())), WriteItem::Put(node.clone(), Tag(b"node".to_vec()))])
        .await
        .unwrap();

    overwrite_on_disk(&tree, &meta, b"changed");
    overwrite_on_disk(&tree, &node, b"changed");

    let leaves: Vec<RowKey> = (0..8).map(|n| RowKey::Leaf(vec![n])).collect();
    store.write(leaves.iter().map(|leaf| WriteItem::Put(leaf.clone(), Tag(b"leaf".to_vec()))).collect()).await.unwrap();

    for leaf in &leaves {
        store.get(leaf).await.unwrap();
    }

    assert_eq!(store.get(&meta).await.unwrap(), tag(b"meta"));
    assert_eq!(store.get(&node).await.unwrap(), tag(b"node"));

    // A store with nothing in memory reads the disk.
    let fresh = SledStore::<Tag>::new(tree);
    assert_eq!(fresh.get(&node).await.unwrap(), tag(b"changed"));
}

#[tokio::test]
async fn recent_rows_past_capacity_leave_memory_and_read_from_disk() {
    let tree = temporary_tree();
    let store = SledStore::<Tag>::with_recent_capacity(tree.clone(), 2);
    let (one, two, three) = (RowKey::Leaf(vec![1]), RowKey::Leaf(vec![2]), RowKey::Leaf(vec![3]));

    for (key, row) in [(&one, b"1"), (&two, b"2"), (&three, b"3")] {
        store.write(vec![WriteItem::Put(key.clone(), Tag(row.to_vec()))]).await.unwrap();
        store.get(key).await.unwrap();
        overwrite_on_disk(&tree, key, b"disk");
    }

    assert_eq!(store.get(&three).await.unwrap(), tag(b"3"), "kept while recent");
    assert_eq!(store.get(&two).await.unwrap(), tag(b"2"), "kept while recent");
    assert_eq!(store.get(&one).await.unwrap(), tag(b"disk"), "the oldest left memory, and reads come from disk");

    // Reading `one` made `three` the least recently used.
    assert_eq!(store.get(&three).await.unwrap(), tag(b"disk"));
}

/// A write updates a recently used row the cache holds, so a read never finds
/// the cache older than the disk, and leaves the others to reads, so a large
/// write doesn't push the recently used rows out.
#[tokio::test]
async fn a_write_updates_the_recent_rows_it_finds_and_keeps_no_others() {
    let tree = temporary_tree();
    let store = SledStore::<Tag>::new(tree.clone());

    let (held, unheld) = (RowKey::Leaf(vec![1]), RowKey::Leaf(vec![2]));
    store.write(vec![WriteItem::Put(held.clone(), Tag(b"old".to_vec()))]).await.unwrap();
    store.get(&held).await.unwrap();

    store
        .write(vec![WriteItem::Put(held.clone(), Tag(b"new".to_vec())), WriteItem::Put(unheld.clone(), Tag(b"new".to_vec()))])
        .await
        .unwrap();

    overwrite_on_disk(&tree, &held, b"disk");
    overwrite_on_disk(&tree, &unheld, b"disk");

    assert_eq!(store.get(&held).await.unwrap(), tag(b"new"), "updated in memory by the write");
    assert_eq!(store.get(&unheld).await.unwrap(), tag(b"disk"), "never kept, so read from disk");
}

/// A range read keeps the rows it found that stay in memory always, and
/// leaves the others to reads that ask for them one by one, so that a scan
/// doesn't push recently used rows out.
#[tokio::test]
async fn a_range_read_keeps_only_the_rows_that_stay_in_memory_always() {
    let tree = temporary_tree();
    let (node, leaf) = (RowKey::Node(NodePrefix::root()), RowKey::Leaf(vec![1]));

    for key in [&node, &leaf] {
        tree.insert(key.encode(), Tag(b"stored".to_vec()).encode()).unwrap();
    }

    let store = SledStore::<Tag>::new(tree.clone());
    assert_eq!(store.range(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, 10).await.unwrap().len(), 2);

    overwrite_on_disk(&tree, &node, b"changed");
    overwrite_on_disk(&tree, &leaf, b"changed");

    assert_eq!(store.get(&node).await.unwrap(), tag(b"stored"), "kept from the range");
    assert_eq!(store.get(&leaf).await.unwrap(), tag(b"changed"), "read from disk");
}

/// A row that counts how many times rows of its kind were decoded.
#[derive(Clone, Debug, PartialEq)]
struct Counted(Vec<u8>);

static DECODED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

impl Encode for Counted {
    fn encode(&self) -> Vec<u8> { self.0.clone() }

    fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        DECODED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Counted(bytes.to_vec()))
    }
}

/// A range read takes the rows the cache holds from memory instead of
/// decoding them again: nodes, which a tree's every read ranges over, cost no
/// decoding once the store holds them, while leaves the cache never kept are
/// decoded from disk.
#[tokio::test]
async fn a_range_read_decodes_only_the_rows_the_cache_lacks() {
    let store = SledStore::<Counted>::new(temporary_tree());
    let (node, leaf) = (RowKey::Node(NodePrefix::root()), RowKey::Leaf(vec![1]));

    store
        .write(vec![WriteItem::Put(node.clone(), Counted(b"node".to_vec())), WriteItem::Put(leaf, Counted(b"leaf".to_vec()))])
        .await
        .unwrap();

    let before = DECODED.load(std::sync::atomic::Ordering::Relaxed);

    for _ in 0..3 {
        let nodes =
            store.range(Bound::Included(&node), Bound::Excluded(&RowKey::Leaf(Vec::new())), ScanDirection::Forward, 10).await.unwrap();

        assert_eq!(nodes, [(node.clone(), Counted(b"node".to_vec()))]);
    }

    assert_eq!(DECODED.load(std::sync::atomic::Ordering::Relaxed), before, "the node came from memory every time");
    assert_eq!(store.range(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, 10).await.unwrap().len(), 2);
    assert_eq!(DECODED.load(std::sync::atomic::Ordering::Relaxed), before + 1, "only the leaf was decoded");
}

#[tokio::test]
async fn bytes_that_do_not_decode_are_a_permanent_failure() {
    let tree = temporary_tree();
    tree.insert(vec![0x09u8], vec![0u8]).unwrap();

    let store = SledStore::<Tag>::new(tree);
    let failure = store.range(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, 10).await.unwrap_err();

    assert!(!failure.is_retryable());
}

#[test]
fn sled_failures_of_io_are_retryable_and_the_rest_permanent() {
    assert!(failure(sled::Error::Io(std::io::Error::other("a disk that went away"))).is_retryable());

    for error in
        [sled::Error::Unsupported("a use".into()), sled::Error::ReportableBug("a bug".into()), sled::Error::Corruption { at: None, bt: () }]
    {
        assert!(!failure(error).is_retryable());
    }
}
