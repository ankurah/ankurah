//! The happy path, as a primer on the API: a tree of albums keyed by the
//! year each came out, changed, read back, opened again from its store, and
//! compared with another tree, whole and span by span.

mod common;

use std::ops::Bound;

use ankurah_digest_tree::{DigestTree, MemoryStore, Ristretto255, Span};
use common::{album, key_spec, year, Id, Title, Tree};

/// Members go in through a transaction, and a view reads them back in key
/// order, here the order of the years the albums came out, up to a limit per
/// read, or one at a time by key and id.
#[tokio::test]
async fn a_tree_holds_its_members_in_key_order() {
    let tree: Tree<Ristretto255> = DigestTree::create(MemoryStore::new(), key_spec()).await.unwrap();

    let mut trx = tree.transaction();
    trx.insert(album(1, 2016, "Two Vines")).unwrap();
    trx.insert(album(2, 2008, "Walking on a Dream")).unwrap();
    trx.insert(album(3, 2013, "Ice on the Dune")).unwrap();
    trx.commit().await.unwrap();

    let view = tree.view();
    let members = view.members(&Span::all(), 10).await.unwrap();

    assert_eq!(members.iter().map(|leaf| leaf.id).collect::<Vec<_>>(), [Id(2), Id(3), Id(1)]);
    assert_eq!(view.member(&year(2013), &Id(3)).await.unwrap().map(|leaf| leaf.payload), Some(Title("Ice on the Dune".into())));
}

/// A tree keeps everything in its store: opening the store again gives the
/// same tree, keyed by the same spec.
#[tokio::test]
async fn a_tree_opens_again_from_its_store() {
    let store = MemoryStore::new();
    let albums = [album(1, 2016, "Two Vines"), album(2, 2008, "Walking on a Dream")];
    let tree: Tree<Ristretto255> = DigestTree::build(store.clone(), key_spec(), albums).await.unwrap();
    let digest = tree.view().span_digest(&Span::all()).await.unwrap().digest;
    drop(tree);

    let reopened: Tree<Ristretto255> = DigestTree::open(store).await.unwrap();

    assert_eq!(reopened.key_spec(), &key_spec());
    assert_eq!(reopened.view().span_digest(&Span::all()).await.unwrap().digest, digest);
}

/// A tree's digest stands for its members, so it changes whenever they do.
/// An update and a removal name the version the tree holds.
#[tokio::test]
async fn every_change_to_the_members_changes_the_digest() {
    let albums = [album(1, 2016, "Two Vines"), album(2, 2008, "Walking on a Dream")];
    let tree: Tree<Ristretto255> = DigestTree::build(MemoryStore::new(), key_spec(), albums).await.unwrap();
    let first = tree.view().span_digest(&Span::all()).await.unwrap().digest;

    let mut trx = tree.transaction();
    trx.update(album(1, 2016, "Two Vines"), album(1, 2016, "Two Vines (Live)")).unwrap();
    trx.commit().await.unwrap();
    let second = tree.view().span_digest(&Span::all()).await.unwrap().digest;

    let mut trx = tree.transaction();
    trx.remove(album(2, 2008, "Walking on a Dream")).unwrap();
    trx.insert(album(3, 2013, "Ice on the Dune")).unwrap();
    trx.commit().await.unwrap();
    let third = tree.view().span_digest(&Span::all()).await.unwrap().digest;

    assert_ne!(first, second, "album 1 retitled");
    assert_ne!(second, third, "album 2 removed and album 3 added");
    assert_eq!(third.count(), 2);
}

/// Trees that hold the same members hold the same digest, however each came
/// to hold them: here one is built whole and the other changed into them.
#[tokio::test]
async fn trees_of_the_same_members_have_the_same_digest() {
    let albums = [album(1, 2016, "Two Vines (Live)"), album(3, 2013, "Ice on the Dune")];
    let built: Tree<Ristretto255> = DigestTree::build(MemoryStore::new(), key_spec(), albums).await.unwrap();

    let changed: Tree<Ristretto255> = DigestTree::create(MemoryStore::new(), key_spec()).await.unwrap();

    let mut trx = changed.transaction();
    trx.insert(album(1, 2016, "Two Vines")).unwrap();
    trx.insert(album(2, 2008, "Walking on a Dream")).unwrap();
    trx.commit().await.unwrap();

    let mut trx = changed.transaction();
    trx.update(album(1, 2016, "Two Vines"), album(1, 2016, "Two Vines (Live)")).unwrap();
    trx.remove(album(2, 2008, "Walking on a Dream")).unwrap();
    trx.insert(album(3, 2013, "Ice on the Dune")).unwrap();
    trx.commit().await.unwrap();

    let built_digest = built.view().span_digest(&Span::all()).await.unwrap().digest;
    let changed_digest = changed.view().span_digest(&Span::all()).await.unwrap().digest;

    assert_eq!(built_digest, changed_digest);
}

/// Every span of the key space has a digest of its own, so two trees can
/// tell where they differ: these two differ only in the album from 2013.
#[tokio::test]
async fn trees_compare_span_by_span() {
    let our_albums = [album(1, 2008, "Walking on a Dream"), album(2, 2013, "Ice on the Dune")];
    let their_albums = [album(1, 2008, "Walking on a Dream"), album(2, 2013, "Ice on the Dune (Live)")];

    let ours: Tree<Ristretto255> = DigestTree::build(MemoryStore::new(), key_spec(), our_albums).await.unwrap();
    let theirs: Tree<Ristretto255> = DigestTree::build(MemoryStore::new(), key_spec(), their_albums).await.unwrap();

    let before_2013 = Span::for_key_range(Bound::Unbounded, Bound::Excluded(&year(2013)));
    let from_2013 = Span::for_key_range(Bound::Included(&year(2013)), Bound::Unbounded);
    let (our_view, their_view) = (ours.view(), theirs.view());

    assert_eq!(our_view.span_digest(&before_2013).await.unwrap().digest, their_view.span_digest(&before_2013).await.unwrap().digest);
    assert_ne!(our_view.span_digest(&from_2013).await.unwrap().digest, their_view.span_digest(&from_2013).await.unwrap().digest);
}
