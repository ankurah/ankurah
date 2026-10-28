//! The stores a tree keeps its rows in: one that pages its reads, as a store
//! may, and sled on disk.

mod common;

use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;
use ankurah_digest_tree::{DigestTree, DigestTreeView, Leaf, MemoryStore, Ristretto255, Row, RowKey, ScanRange, Span, Store};
use common::{asc, item, tree_of, Id, Item, ItemTree, Property, ShortPages, Tag};
use futures::TryStreamExt;

/// Every member a range read of `view` yields, in its order.
async fn range_read<S: Store<RowKey, Row<Property, Tag, Ristretto255>>>(
    view: &DigestTreeView<Property, Id, Tag, Ristretto255, S>,
    range: ScanRange,
) -> Vec<Leaf<Id, Tag>> {
    view.range(range).try_collect().await.unwrap()
}

/// Commit `changes` to `tree` in one transaction, each the version the tree
/// holds, if any, and the version to replace it, if any.
async fn commit<S: Store<RowKey, Row<Property, Tag, Ristretto255>>>(
    tree: &DigestTree<Property, Id, Tag, Ristretto255, S>,
    changes: &[(Option<Item>, Option<Item>)],
) {
    let mut trx = tree.transaction();
    for change in changes.iter().cloned() {
        match change {
            (Some(old), Some(new)) => trx.update(old, new),
            (Some(old), None) => trx.remove(old),
            (None, new) => trx.insert(new.expect("a change has a version on one side")),
        }
        .unwrap();
    }

    trx.commit().await.unwrap();
}

/// A store may return fewer rows than asked while more remain: a tree over
/// one that returns three at a time builds, changes node by node and by
/// laying out anew, and reads both ways, as a tree over a store that returns
/// them all.
#[tokio::test]
async fn a_tree_over_a_store_with_short_pages_works_as_any_other() {
    type ShortTree = DigestTree<Property, Id, Tag, Ristretto255, ShortPages<Row<Property, Tag, Ristretto255>>>;

    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let member = |n: u64| item!(n, (n % 50) as i64, b"");
    let members: Vec<Item> = (0..300).map(member).collect();
    let short = ShortTree::build(ShortPages(MemoryStore::new()), spec.clone(), members.clone()).await.unwrap();
    let full: ItemTree = tree_of!(spec, members);

    // Forty new members and thirty removed, node by node; then the rest of
    // the first 200 moved, enough to lay the tree out anew.
    let small: Vec<(Option<Item>, Option<Item>)> =
        (300..340).map(|n| (None, Some(item!(n, 7, b"new")))).chain((0..30).map(|n| (Some(member(n)), None))).collect();

    let large: Vec<(Option<Item>, Option<Item>)> = (30..200).map(|n| (Some(member(n)), Some(item!(n, -1, b"moved")))).collect();
    for batch in [small, large] {
        commit(&short, &batch).await;
        commit(&full, &batch).await;
        short.audit().await.unwrap();

        let (short_view, full_view) = (short.view(), full.view());
        assert_eq!(short_view.span_digest(&Span::all()).await.unwrap().digest, full_view.span_digest(&Span::all()).await.unwrap().digest);

        for range in [ScanRange::forward(..), ScanRange::reverse(..)] {
            assert_eq!(range_read(&short_view, range.clone()).await, range_read(&full_view, range).await);
        }
    }
}

/// A tree written to sled, its database closed and opened again from its
/// files, is the same tree: every row survives on disk and decodes from its
/// bytes.
#[cfg(feature = "sled")]
#[tokio::test]
async fn a_tree_reopened_from_its_sled_files_is_the_same_tree() {
    use ankurah_digest_tree::SledStore;
    use common::desc;

    type SledTree = DigestTree<Property, Id, Tag, Ristretto255, SledStore<Row<Property, Tag, Ristretto255>>>;

    let directory = tempfile::tempdir().unwrap();

    // No background flusher: the test flushes the database itself before
    // closing it, so what opens again is exactly what it flushed.
    let config = || sled::Config::new().path(directory.path()).flush_every_ms(None);
    let key_spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64), desc!(Property::B, ValueType::I64)]);

    let members: Vec<Item> = (0..300).map(|n| item!(n, [(n % 7) as i64, n as i64], &[n as u8])).collect();

    let (digest, len) = {
        let db = config().open().unwrap();
        let tree = SledTree::build(SledStore::new(db.open_tree("tree").unwrap()), key_spec.clone(), members.clone()).await.unwrap();

        commit(&tree, &[(Some(members[5].clone()), None), (Some(members[6].clone()), Some(item!(6, [6, 6], b"retitled")))])
            .await;

        let view = tree.view();
        let written = (view.span_digest(&Span::all()).await.unwrap(), view.len().await.unwrap());

        db.flush().unwrap();
        written
    };

    let db = config().open().unwrap();
    let reopened = SledTree::open(SledStore::new(db.open_tree("tree").unwrap())).await.unwrap();

    assert_eq!(reopened.key_spec(), &key_spec);
    reopened.audit().await.unwrap();

    let view = reopened.view();
    assert_eq!((view.span_digest(&Span::all()).await.unwrap(), view.len().await.unwrap()), (digest, len));
    assert_eq!(len, 299);
}
