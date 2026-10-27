//! The stores a tree keeps its rows in: one that pages its reads, as a store
//! may, and sled on disk.

mod common;

use ankurah_digest_tree::{DigestTree, DigestTreeView, Leaf, MemoryStore, Ristretto255, Row, RowKey, ScanDirection, Span, Store, ViewPlan};
use common::{built, integer_spec, item, Id, Item, ShortPages, Spec, Tag};
use futures::TryStreamExt;

/// Every member `view` covers, in its direction.
async fn every_member<S: Store<RowKey, Row<Spec, Tag, Ristretto255>>>(
    view: &DigestTreeView<Spec, Id, Tag, Ristretto255, S>,
) -> Vec<Leaf<Id, Tag>> {
    view.members(&Span::all()).try_collect().await.unwrap()
}

/// Commit `changes` to `tree` in one transaction, each the version the tree
/// holds, if any, and the version to replace it, if any.
async fn commit<S: Store<RowKey, Row<Spec, Tag, Ristretto255>>>(
    tree: &DigestTree<Spec, Id, Tag, Ristretto255, S>,
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
/// laying out anew, and reads, as a tree over a store that returns them all.
#[tokio::test]
async fn a_tree_over_a_store_with_short_pages_works_as_any_other() {
    type ShortTree = DigestTree<Spec, Id, Tag, Ristretto255, ShortPages<Row<Spec, Tag, Ristretto255>>>;

    let member = |n: u64| item(n, Some((n % 50) as i64), b"");
    let members: Vec<Item> = (0..300).map(member).collect();
    let short = ShortTree::build(ShortPages(MemoryStore::new()), integer_spec(), members.clone()).await.unwrap();
    let full = built::<Ristretto255>(integer_spec(), members).await;

    // Forty new members and thirty removed, node by node; then the rest of
    // the first 200 moved, enough to lay the tree out anew.
    let small: Vec<(Option<Item>, Option<Item>)> =
        (300..340).map(|n| (None, Some(item(n, Some(7), b"new")))).chain((0..30).map(|n| (Some(member(n)), None))).collect();

    let large: Vec<(Option<Item>, Option<Item>)> = (30..200).map(|n| (Some(member(n)), Some(item(n, Some(-1), b"moved")))).collect();
    for batch in [small, large] {
        commit(&short, &batch).await;
        commit(&full, &batch).await;
        short.audit().await.unwrap();

        let (short_view, full_view) = (short.view(), full.view());
        assert_eq!(short_view.span_digest(&Span::all()).await.unwrap().digest, full_view.span_digest(&Span::all()).await.unwrap().digest);
        assert_eq!(every_member(&short_view).await, every_member(&full_view).await);

        let plan = ViewPlan { spans: vec![Span::all()], direction: ScanDirection::Reverse, limit: Some(25) };

        let (short_window, full_window) = (short.select(&plan).await.unwrap(), full.select(&plan).await.unwrap());
        assert_eq!(every_member(&short_window).await, every_member(&full_window).await);
    }
}

/// A tree written to sled, its database closed and opened again from its
/// files, is the same tree: every row survives on disk and decodes from its
/// bytes.
#[cfg(feature = "sled")]
#[tokio::test]
async fn a_tree_reopened_from_its_sled_files_is_the_same_tree() {
    use ankurah_digest_tree::SledStore;
    use common::Order;

    type SledTree = DigestTree<Spec, Id, Tag, Ristretto255, SledStore<Row<Spec, Tag, Ristretto255>>>;

    let directory = tempfile::tempdir().unwrap();

    // No background flusher: the test flushes the database itself before
    // closing it, so what opens again is exactly what it flushed.
    let config = || sled::Config::new().path(directory.path()).flush_every_ms(None);
    let key_spec = common::spec(&[Order::Ascending, Order::Descending]);

    let members: Vec<Item> = (0..300).map(|n| Item::new(n, [Some((n % 7) as i64), Some(n as i64)], &[n as u8])).collect();

    let (digest, len) = {
        let db = config().open().unwrap();
        let tree = SledTree::build(SledStore::new(db.open_tree("tree").unwrap()), key_spec.clone(), members.clone()).await.unwrap();

        commit(&tree, &[(Some(members[5].clone()), None), (Some(members[6].clone()), Some(Item::new(6, [Some(6), Some(6)], b"retitled")))])
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
