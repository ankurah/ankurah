use super::*;
use crate::address::NodePrefix;
use crate::digest::{HeadHash, Leaf};
use crate::tree::fixtures::{empty_tree, entity, in_each_group, prefix, row};
use crate::{DigestTree, TreeSpec};
use ankql::ast::Predicate;
use ankurah_core_types::KeySpec;
use std::ops::Bound;

/// The same row after a change to the entity: a new head and element.
fn changed<G: DigestGroup>(row: &LeafRow<G>) -> LeafRow<G> {
    let head = HeadHash::of([row.head.as_bytes().map(|byte| byte ^ 0xff)]);
    LeafRow { head, element: Leaf::new(&row.key, row.entity_id, head).element(), ..row.clone() }
}

in_each_group!(
    a_new_tree_is_empty,
    a_leaf_is_written_against_the_row_the_writer_read,
    the_leaves_of_one_entity_follow_its_keys,
    a_missing_leaf_in_the_lookup_is_an_error,
    leaves_are_read_by_span_a_page_at_a_time,
    the_span_under_a_prefix_reads_its_leaves,
    competing_lookup_updates_for_one_entity_leave_no_orphaned_row,
);

async fn a_new_tree_is_empty<G: DigestGroup>() {
    let spec = TreeSpec::new(KeySpec::new(vec![]), Predicate::True);
    let tree = DigestTree::<G>::new(spec.clone());
    assert_eq!(*tree.spec(), spec);
    let mut maps = tree.begin();
    assert_eq!(maps.node(&NodePrefix::root()).await.unwrap(), None, "no root row until a leaf arrives");
    assert_eq!(maps.leaves(&TreeSpan::all(), usize::MAX).await.unwrap(), []);
    assert_eq!(maps.entity_leaves(entity(1)).await.unwrap(), []);
    assert_eq!(maps.children(&NodePrefix::root()).await.unwrap(), []);
}

async fn a_leaf_is_written_against_the_row_the_writer_read<G: DigestGroup>() {
    let tree = empty_tree::<G>();
    let first = row::<G>(b"blue\0", entity(1));
    let mut maps = tree.begin();
    maps.put_leaf(first.clone(), None).await.unwrap();
    assert_eq!(maps.leaf(b"blue\0", entity(1)).await.unwrap(), Some(first.clone()));
    assert_eq!(maps.leaf(b"blue\0", entity(2)).await.unwrap(), None);
    assert_eq!(maps.leaf(b"red\0", entity(1)).await.unwrap(), None);
    maps.commit().await.unwrap();
    assert!(matches!(tree.begin().put_leaf(first.clone(), None).await, Err(TreeError::Conflict)), "a row is there now");

    // The next change to the entity replaces the row it read; a change
    // that read an older row conflicts.
    let later = changed(&first);
    let mut maps = tree.begin();
    maps.put_leaf(later.clone(), Some(&first)).await.unwrap();
    maps.commit().await.unwrap();
    let mut maps = tree.begin();
    assert_eq!(maps.leaf(b"blue\0", entity(1)).await.unwrap(), Some(later.clone()));
    assert_eq!(maps.leaves(&TreeSpan::all(), usize::MAX).await.unwrap(), std::slice::from_ref(&later));
    assert!(matches!(maps.put_leaf(changed(&later), Some(&first)).await, Err(TreeError::Conflict)), "stale");

    assert!(matches!(tree.begin().delete_leaf(&first).await, Err(TreeError::Conflict)), "stale");
    let mut maps = tree.begin();
    maps.delete_leaf(&later).await.unwrap();
    maps.commit().await.unwrap();
    assert_eq!(tree.begin().leaf(b"blue\0", entity(1)).await.unwrap(), None);
    assert!(matches!(tree.begin().delete_leaf(&later).await, Err(TreeError::Conflict)), "gone already");
}

/// The lookup from an entity to its rows follows every put and delete,
/// and lists the rows in address order even where one key is a prefix
/// of another.
async fn the_leaves_of_one_entity_follow_its_keys<G: DigestGroup>() {
    let tree = empty_tree::<G>();
    // An id of 0xff bytes: against a longer key's next byte, the id's
    // first byte sorts after it, so address order is not key order.
    let moved = entity(0xff);
    let mut maps = tree.begin();
    assert_eq!(maps.entity_leaves(moved).await.unwrap(), []);
    for key in [b"ab".as_slice(), b"abc", b"a"] {
        maps.put_leaf(row(key, moved), None).await.unwrap();
    }
    maps.put_leaf(row(b"ab", entity(2)), None).await.unwrap();
    maps.commit().await.unwrap();

    let mut maps = tree.begin();
    let rows = maps.entity_leaves(moved).await.unwrap();
    let addresses: Vec<Vec<u8>> = rows.iter().map(LeafRow::address).collect();
    assert!(addresses.windows(2).all(|pair| pair[0] < pair[1]), "address order");
    let keys: Vec<&[u8]> = rows.iter().map(|row| row.key.as_slice()).collect();
    assert_eq!(keys, [b"abc".as_slice(), b"ab", b"a"]);

    // A change under a key the entity already has leaves the lookup as it is.
    let under_a = row(b"a", moved);
    maps.put_leaf(changed(&under_a), Some(&under_a)).await.unwrap();
    assert_eq!(maps.entity_leaves(moved).await.unwrap().len(), 3);

    // Moving the entity from one key to another.
    maps.delete_leaf(&row(b"ab", moved)).await.unwrap();
    maps.put_leaf(row(b"zz", moved), None).await.unwrap();
    let keys: Vec<Vec<u8>> = maps.entity_leaves(moved).await.unwrap().into_iter().map(|row| row.key).collect();
    assert_eq!(keys, [b"abc".to_vec(), b"a".to_vec(), b"zz".to_vec()]);
    assert_eq!(maps.entity_leaves(entity(2)).await.unwrap(), [row(b"ab", entity(2))], "another entity's rows are its own");
    maps.commit().await.unwrap();

    let mut maps = tree.begin();
    maps.delete_leaf(&changed(&under_a)).await.unwrap();
    for key in [b"abc".as_slice(), b"zz"] {
        maps.delete_leaf(&row(key, moved)).await.unwrap();
    }
    assert_eq!(maps.entity_leaves(moved).await.unwrap(), []);
    maps.commit().await.unwrap();
    let mut maps = tree.begin();
    assert_eq!(maps.transaction.get(MapName::Lookup, &moved.to_bytes()).await.unwrap(), None, "an entity with no rows leaves the lookup");
    assert!(maps.transaction.get(MapName::Lookup, &entity(2).to_bytes()).await.unwrap().is_some());
}

async fn a_missing_leaf_in_the_lookup_is_an_error<G: DigestGroup>() {
    let tree = empty_tree::<G>();
    let leaf = row::<G>(b"blue", entity(1));
    let mut maps = tree.begin();
    maps.put_leaf(leaf.clone(), None).await.unwrap();
    maps.commit().await.unwrap();
    let mut damaged = tree.begin();
    damaged.transaction.delete_if(MapName::Leaves, &leaf.address(), &layout::leaf_value(&leaf)).await.unwrap();
    damaged.commit().await.unwrap();
    assert!(matches!(tree.begin().entity_leaves(leaf.entity_id).await,
        Err(TreeError::MissingLeaf { entity_id, key }) if entity_id == leaf.entity_id && key == leaf.key));
}

async fn leaves_are_read_by_span_a_page_at_a_time<G: DigestGroup>() {
    let tree = empty_tree::<G>();
    let rows: Vec<LeafRow<G>> = (1..=6).map(|n| row(&[n], entity(n))).collect();
    let mut maps = tree.begin();
    for row in rows.iter().rev() {
        maps.put_leaf(row.clone(), None).await.unwrap();
    }
    maps.commit().await.unwrap();

    let mut maps = tree.begin();
    assert_eq!(maps.leaves(&TreeSpan::all(), usize::MAX).await.unwrap(), rows, "address order, not arrival order");
    let span = TreeSpan::new(Bound::Included(vec![2]), Bound::Excluded(vec![6]));
    let first_page = maps.leaves(&span, 2).await.unwrap();
    assert_eq!(first_page, rows[1..3]);
    let second_page = maps.leaves(&span.after(&first_page[1].address()), 2).await.unwrap();
    assert_eq!(second_page, rows[3..5]);
    assert_eq!(maps.leaves(&span.after(&second_page[1].address()), 2).await.unwrap(), []);
    assert_eq!(maps.leaves(&TreeSpan::all(), 0).await.unwrap(), []);
}

/// The span under a prefix reads the leaves beneath that tree node.
async fn the_span_under_a_prefix_reads_its_leaves<G: DigestGroup>() {
    let tree = empty_tree::<G>();
    let mut maps = tree.begin();
    for n in [0x00, 0x3f, 0x40, 0x7f, 0x80, 0xff] {
        maps.put_leaf(row(&[n], entity(n)), None).await.unwrap();
    }
    maps.commit().await.unwrap();

    let mut maps = tree.begin();
    let keys = |rows: Vec<LeafRow<G>>| rows.into_iter().map(|row| row.key[0]).collect::<Vec<u8>>();
    assert_eq!(keys(maps.leaves(&TreeSpan::under(&prefix("0")), usize::MAX).await.unwrap()), [0x00, 0x3f, 0x40, 0x7f]);
    assert_eq!(keys(maps.leaves(&TreeSpan::under(&prefix("01")), usize::MAX).await.unwrap()), [0x40, 0x7f]);
    assert_eq!(keys(maps.leaves(&TreeSpan::under(&prefix("1")), usize::MAX).await.unwrap()), [0x80, 0xff]);
    assert_eq!(keys(maps.leaves(&TreeSpan::under(&NodePrefix::root()), usize::MAX).await.unwrap()).len(), 6);
}

/// Two transactions file one entity, with no lookup yet, under two keys:
/// the second fails whole, leaving no row its lookup does not list, and
/// its retry from fresh reads lands.
async fn competing_lookup_updates_for_one_entity_leave_no_orphaned_row<G: DigestGroup>() {
    let tree = empty_tree::<G>();
    let filed = entity(7);
    let (blue, red) = (row::<G>(b"blue\0", filed), row::<G>(b"red\0", filed));
    let (mut first, mut second) = (tree.begin(), tree.begin());
    first.put_leaf(blue.clone(), None).await.unwrap();
    second.put_leaf(red.clone(), None).await.unwrap();
    first.commit().await.unwrap();
    assert!(matches!(second.commit().await, Err(TreeError::Conflict)), "it expected no lookup");

    let mut maps = tree.begin();
    assert_eq!(maps.entity_leaves(filed).await.unwrap(), std::slice::from_ref(&blue));
    assert_eq!(maps.leaf(b"red\0", filed).await.unwrap(), None, "no orphaned row");

    let mut retry = tree.begin();
    retry.put_leaf(red.clone(), None).await.unwrap();
    retry.commit().await.unwrap();
    assert_eq!(tree.begin().entity_leaves(filed).await.unwrap(), [blue, red]);
}
