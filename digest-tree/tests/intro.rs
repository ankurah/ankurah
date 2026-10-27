//! An introduction to the API: albums, keyed by the year each came out.

mod common;

use ankurah_digest_tree::{DigestTree, MemoryStore, Ristretto255, ScanRange, Span};
use common::{album, asc, Id, KeySpec, Title, Tree};
use futures::{StreamExt, TryStreamExt};

#[tokio::test]
async fn a_tree_holds_its_members_in_key_order() {
    let years = KeySpec::new(vec![asc!("year")]);
    let tree: Tree<Ristretto255> = DigestTree::create(MemoryStore::new(), years.clone()).await.unwrap();

    let mut trx = tree.transaction();
    trx.insert(album!(1, 2016, "Two Vines")).unwrap();
    trx.insert(album!(2, 2008, "Walking on a Dream")).unwrap();
    trx.insert(album!(3, 2013, "Ice on the Dune")).unwrap();
    trx.commit().await.unwrap();

    let view = tree.view();
    let members = view.range(..);
    let titles: Vec<Title> = members.map_ok(|leaf| leaf.payload).try_collect().await.unwrap();

    assert_eq!(titles, ["Walking on a Dream", "Ice on the Dune", "Two Vines"]);
    assert_eq!(view.member(&years.collate([2013]), &Id(3)).await.unwrap().unwrap().payload, "Ice on the Dune");
}

#[tokio::test]
async fn the_two_newest_albums_read_in_reverse() {
    let albums = [album!(1, 2016, "Two Vines"), album!(2, 2008, "Walking on a Dream"), album!(3, 2013, "Ice on the Dune")];
    let tree: Tree<Ristretto255> = DigestTree::build(MemoryStore::new(), KeySpec::new(vec![asc!("year")]), albums).await.unwrap();

    let newest = tree.view().range(ScanRange::reverse(..)).take(2);
    let titles: Vec<Title> = newest.map_ok(|leaf| leaf.payload).try_collect().await.unwrap();

    assert_eq!(titles, ["Two Vines", "Ice on the Dune"]);
}

#[tokio::test]
async fn narrower_views_of_one_tree_serve_different_selections() {
    let years = KeySpec::new(vec![asc!("year")]);
    let albums = [album!(1, 2016, "Two Vines"), album!(2, 2008, "Walking on a Dream"), album!(3, 2013, "Ice on the Dune")];
    let tree: Tree<Ristretto255> = DigestTree::build(MemoryStore::new(), years.clone(), albums).await.unwrap();

    let since_2010 = tree.select([Span::for_key_range(years.collate([2010])..)]);
    let of_2013 = tree.select([Span::for_key_range(years.collate([2013])..=years.collate([2013]))]);

    let titles_since_2010: Vec<Title> = since_2010.range(..).map_ok(|leaf| leaf.payload).try_collect().await.unwrap();
    let titles_of_2013: Vec<Title> = of_2013.range(..).map_ok(|leaf| leaf.payload).try_collect().await.unwrap();

    assert_eq!(titles_since_2010, ["Ice on the Dune", "Two Vines"]);
    assert_eq!(titles_of_2013, ["Ice on the Dune"]);
    assert_eq!(of_2013.span_digest(&Span::all()).await.unwrap().digest.count(), 1);
}

#[tokio::test]
async fn a_tree_opens_again_from_its_store() {
    let store = MemoryStore::new();
    let albums = [album!(1, 2016, "Two Vines"), album!(2, 2008, "Walking on a Dream")];
    let tree: Tree<Ristretto255> = DigestTree::build(store.clone(), KeySpec::new(vec![asc!("year")]), albums).await.unwrap();
    let digest = tree.view().span_digest(&Span::all()).await.unwrap().digest;
    drop(tree);

    let reopened: Tree<Ristretto255> = DigestTree::open(store).await.unwrap();

    assert_eq!(reopened.key_spec(), &KeySpec::new(vec![asc!("year")]));
    assert_eq!(reopened.view().span_digest(&Span::all()).await.unwrap().digest, digest);
}

#[tokio::test]
async fn every_change_to_the_members_changes_the_digest() {
    let albums = [album!(1, 2016, "Two Vines"), album!(2, 2008, "Walking on a Dream")];
    let tree: Tree<Ristretto255> = DigestTree::build(MemoryStore::new(), KeySpec::new(vec![asc!("year")]), albums).await.unwrap();
    let first = tree.view().span_digest(&Span::all()).await.unwrap().digest;

    let mut trx = tree.transaction();
    trx.update(album!(1, 2016, "Two Vines"), album!(1, 2016, "Two Vines (Live)")).unwrap();
    trx.commit().await.unwrap();
    let second = tree.view().span_digest(&Span::all()).await.unwrap().digest;

    let mut trx = tree.transaction();
    trx.remove(album!(2, 2008, "Walking on a Dream")).unwrap();
    trx.insert(album!(3, 2013, "Ice on the Dune")).unwrap();
    trx.commit().await.unwrap();
    let third = tree.view().span_digest(&Span::all()).await.unwrap().digest;

    assert_ne!(first, second, "album 1 retitled");
    assert_ne!(second, third, "album 2 removed and album 3 added");
    assert_eq!(third.count(), 2);
}

#[tokio::test]
async fn trees_of_the_same_members_have_the_same_digest() {
    let albums = [album!(1, 2016, "Two Vines (Live)"), album!(3, 2013, "Ice on the Dune")];
    let built: Tree<Ristretto255> = DigestTree::build(MemoryStore::new(), KeySpec::new(vec![asc!("year")]), albums).await.unwrap();

    let changed: Tree<Ristretto255> = DigestTree::create(MemoryStore::new(), KeySpec::new(vec![asc!("year")])).await.unwrap();

    let mut trx = changed.transaction();
    trx.insert(album!(1, 2016, "Two Vines")).unwrap();
    trx.insert(album!(2, 2008, "Walking on a Dream")).unwrap();
    trx.commit().await.unwrap();

    let mut trx = changed.transaction();
    trx.update(album!(1, 2016, "Two Vines"), album!(1, 2016, "Two Vines (Live)")).unwrap();
    trx.remove(album!(2, 2008, "Walking on a Dream")).unwrap();
    trx.insert(album!(3, 2013, "Ice on the Dune")).unwrap();
    trx.commit().await.unwrap();

    let built_digest = built.view().span_digest(&Span::all()).await.unwrap().digest;
    let changed_digest = changed.view().span_digest(&Span::all()).await.unwrap().digest;

    assert_eq!(built_digest, changed_digest);
}

#[tokio::test]
async fn comparing_trees_narrows_to_the_members_that_differ() {
    let years = KeySpec::new(vec![asc!("year")]);
    let our_albums = [album!(1, 2008, "Walking on a Dream"), album!(2, 2013, "Ice on the Dune")];
    let their_albums = [album!(1, 2008, "Walking on a Dream"), album!(2, 2013, "Ice on the Dune (Live)")];

    let ours: Tree<Ristretto255> = DigestTree::build(MemoryStore::new(), years.clone(), our_albums).await.unwrap();
    let theirs: Tree<Ristretto255> = DigestTree::build(MemoryStore::new(), years.clone(), their_albums).await.unwrap();
    let (our_view, their_view) = (ours.view(), theirs.view());

    let before_2013 = Span::for_key_range(..years.collate([2013]));
    let from_2013 = Span::for_key_range(years.collate([2013])..);

    assert_eq!(our_view.span_digest(&before_2013).await.unwrap().digest, their_view.span_digest(&before_2013).await.unwrap().digest);
    assert_ne!(our_view.span_digest(&from_2013).await.unwrap().digest, their_view.span_digest(&from_2013).await.unwrap().digest);

    let ours_from_2013 = our_view.range(from_2013.clone());
    let theirs_from_2013 = their_view.range(from_2013);

    let our_titles: Vec<Title> = ours_from_2013.map_ok(|leaf| leaf.payload).try_collect().await.unwrap();
    let their_titles: Vec<Title> = theirs_from_2013.map_ok(|leaf| leaf.payload).try_collect().await.unwrap();

    assert_eq!(our_titles, ["Ice on the Dune"]);
    assert_eq!(their_titles, ["Ice on the Dune (Live)"]);
}
