//! An introduction to the API: albums, keyed by the year each came out.

mod common;

use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;
use ankurah_digest_tree::{DigestTree, MemoryStore, Ristretto255, ScanRange, Span};
use common::{album, asc, Id, Property, Title, Tree};
use futures::{StreamExt, TryStreamExt};

#[tokio::test]
async fn a_tree_holds_its_members_in_key_order() -> anyhow::Result<()> {
    let years = KeySpec::new(vec![asc!(Property::Year, ValueType::I32)]);
    let tree: Tree<Ristretto255> = DigestTree::create(MemoryStore::new(), years).await?;

    let mut trx = tree.transaction();
    trx.insert(album!(1, 2016, "Two Vines"))?;
    trx.insert(album!(2, 2008, "Walking on a Dream"))?;
    trx.insert(album!(3, 2013, "Ice on the Dune"))?;
    trx.commit().await?;

    let view = tree.view();
    let members = view.range(..);
    let titles: Vec<Title> = members.map_ok(|leaf| leaf.payload).try_collect().await?;

    assert_eq!(titles, ["Walking on a Dream", "Ice on the Dune", "Two Vines"]);
    assert_eq!(view.member([2013], &Id(3)).await?.unwrap().payload, "Ice on the Dune");

    Ok(())
}

#[tokio::test]
async fn the_two_newest_albums_read_in_reverse() -> anyhow::Result<()> {
    let years = KeySpec::new(vec![asc!(Property::Year, ValueType::I32)]);
    let albums = [album!(1, 2016, "Two Vines"), album!(2, 2008, "Walking on a Dream"), album!(3, 2013, "Ice on the Dune")];
    let tree: Tree<Ristretto255> = DigestTree::build(MemoryStore::new(), years, albums).await?;

    let newest = tree.view().range(ScanRange::reverse(..)).take(2);
    let titles: Vec<Title> = newest.map_ok(|leaf| leaf.payload).try_collect().await?;

    assert_eq!(titles, ["Two Vines", "Ice on the Dune"]);

    Ok(())
}

#[tokio::test]
async fn narrower_views_of_one_tree_serve_different_selections() -> anyhow::Result<()> {
    let years = KeySpec::new(vec![asc!(Property::Year, ValueType::I32)]);
    let albums = [album!(1, 2016, "Two Vines"), album!(2, 2008, "Walking on a Dream"), album!(3, 2013, "Ice on the Dune")];
    let tree: Tree<Ristretto255> = DigestTree::build(MemoryStore::new(), years, albums).await?;

    let since_2010 = tree.select([tree.span_for(2010..)?]);
    let of_2013 = tree.select([tree.span_for(2013..=2013)?]);

    let titles_since_2010: Vec<Title> = since_2010.range(..).map_ok(|leaf| leaf.payload).try_collect().await?;
    let titles_of_2013: Vec<Title> = of_2013.range(..).map_ok(|leaf| leaf.payload).try_collect().await?;

    assert_eq!(titles_since_2010, ["Ice on the Dune", "Two Vines"]);
    assert_eq!(titles_of_2013, ["Ice on the Dune"]);
    assert_eq!(of_2013.span_digest(&Span::all()).await?.digest.count(), 1);

    Ok(())
}

#[tokio::test]
async fn a_tree_opens_again_from_its_store() -> anyhow::Result<()> {
    let years = KeySpec::new(vec![asc!(Property::Year, ValueType::I32)]);
    let store = MemoryStore::new();
    let albums = [album!(1, 2016, "Two Vines"), album!(2, 2008, "Walking on a Dream")];
    let tree: Tree<Ristretto255> = DigestTree::build(store.clone(), years.clone(), albums).await?;
    let digest = tree.view().span_digest(&Span::all()).await?.digest;
    drop(tree);

    let reopened: Tree<Ristretto255> = DigestTree::open(store).await?;

    assert_eq!(reopened.key_spec(), &years);
    assert_eq!(reopened.view().span_digest(&Span::all()).await?.digest, digest);

    Ok(())
}

#[tokio::test]
async fn every_change_to_the_members_changes_the_digest() -> anyhow::Result<()> {
    let years = KeySpec::new(vec![asc!(Property::Year, ValueType::I32)]);
    let albums = [album!(1, 2016, "Two Vines"), album!(2, 2008, "Walking on a Dream")];
    let tree: Tree<Ristretto255> = DigestTree::build(MemoryStore::new(), years, albums).await?;
    let first = tree.view().span_digest(&Span::all()).await?.digest;

    let mut trx = tree.transaction();
    trx.update(album!(1, 2016, "Two Vines"), album!(1, 2016, "Two Vines (Live)"))?;
    trx.commit().await?;
    let second = tree.view().span_digest(&Span::all()).await?.digest;

    let mut trx = tree.transaction();
    trx.remove(album!(2, 2008, "Walking on a Dream"))?;
    trx.insert(album!(3, 2013, "Ice on the Dune"))?;
    trx.commit().await?;
    let third = tree.view().span_digest(&Span::all()).await?.digest;

    assert_ne!(first, second, "album 1 retitled");
    assert_ne!(second, third, "album 2 removed and album 3 added");
    assert_eq!(third.count(), 2);

    Ok(())
}

#[tokio::test]
async fn trees_of_the_same_members_have_the_same_digest() -> anyhow::Result<()> {
    let years = KeySpec::new(vec![asc!(Property::Year, ValueType::I32)]);
    let albums = [album!(1, 2016, "Two Vines (Live)"), album!(3, 2013, "Ice on the Dune")];
    let built: Tree<Ristretto255> = DigestTree::build(MemoryStore::new(), years.clone(), albums).await?;

    let changed: Tree<Ristretto255> = DigestTree::create(MemoryStore::new(), years).await?;

    let mut trx = changed.transaction();
    trx.insert(album!(1, 2016, "Two Vines"))?;
    trx.insert(album!(2, 2008, "Walking on a Dream"))?;
    trx.commit().await?;

    let mut trx = changed.transaction();
    trx.update(album!(1, 2016, "Two Vines"), album!(1, 2016, "Two Vines (Live)"))?;
    trx.remove(album!(2, 2008, "Walking on a Dream"))?;
    trx.insert(album!(3, 2013, "Ice on the Dune"))?;
    trx.commit().await?;

    let built_digest = built.view().span_digest(&Span::all()).await?.digest;
    let changed_digest = changed.view().span_digest(&Span::all()).await?.digest;

    assert_eq!(built_digest, changed_digest);

    Ok(())
}

#[tokio::test]
async fn comparing_trees_narrows_to_the_members_that_differ() -> anyhow::Result<()> {
    let years = KeySpec::new(vec![asc!(Property::Year, ValueType::I32)]);
    let our_albums = [album!(1, 2008, "Walking on a Dream"), album!(2, 2013, "Ice on the Dune")];
    let their_albums = [album!(1, 2008, "Walking on a Dream"), album!(2, 2013, "Ice on the Dune (Live)")];

    let ours: Tree<Ristretto255> = DigestTree::build(MemoryStore::new(), years.clone(), our_albums).await?;
    let theirs: Tree<Ristretto255> = DigestTree::build(MemoryStore::new(), years, their_albums).await?;
    let (our_view, their_view) = (ours.view(), theirs.view());

    let before_2013 = ours.span_for(..2013)?;
    let from_2013 = ours.span_for(2013..)?;

    assert_eq!(our_view.span_digest(&before_2013).await?.digest, their_view.span_digest(&before_2013).await?.digest);
    assert_ne!(our_view.span_digest(&from_2013).await?.digest, their_view.span_digest(&from_2013).await?.digest);

    let ours_from_2013 = our_view.range(from_2013.clone());
    let theirs_from_2013 = their_view.range(from_2013);

    let our_titles: Vec<Title> = ours_from_2013.map_ok(|leaf| leaf.payload).try_collect().await?;
    let their_titles: Vec<Title> = theirs_from_2013.map_ok(|leaf| leaf.payload).try_collect().await?;

    assert_eq!(our_titles, ["Ice on the Dune"]);
    assert_eq!(their_titles, ["Ice on the Dune (Live)"]);

    Ok(())
}
