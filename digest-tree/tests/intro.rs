//! An introduction to the API, in three questions. What is a tree for: it
//! describes its members in a few bytes, so two nodes compare their trees by
//! description and, when they differ, find where in a few round trips. What
//! keeps a description right: the owner tells the tree every change, lands
//! the tree's rows with its own records, and gets the same tree back after a
//! restart. What the key does: it orders the members, so a query's window is
//! a key range, a second part breaks ties, and a missing value still has a
//! place.

mod common;

use std::time::Duration;

use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;
use ankurah_digest_tree::{DigestTree, Error, KeyRange, MemoryStore};
use common::{album, asc, Connection, Property, Title, Tree};
use futures::TryStreamExt;

// What is a tree for?

/// A tree concisely describes the members added to it, allowing recursive comparison of its contents.
#[tokio::test]
async fn trees_concisely_describe_their_contents() -> anyhow::Result<()> {
    // The albums a query selects.
    let albums = [album!(1, 2016, "Two Vines"), album!(2, 2008, "Walking on a Dream"), album!(3, 2013, "Ice on the Dune")];
    let keyspec = KeySpec::new(vec![asc!(Property::Year, ValueType::I32)]);

    let tree1: Tree = DigestTree::create(MemoryStore::new(), keyspec.clone(), albums.clone()).await?;
    let tree2: Tree = DigestTree::create(MemoryStore::new(), keyspec, albums[..2].to_vec()).await?;

    // The description is the same size for three albums or three million.
    let digest1 = tree1.view().span_digest(..).await?;
    assert_eq!(bincode::serialize(&digest1)?.len(), 52);

    assert_ne!(digest1, tree2.view().span_digest(..).await?);

    let mut trx = tree2.transaction();
    trx.insert(albums[2].clone())?;
    trx.commit().await?;

    assert_eq!(tree1.view().span_digest(..).await?, tree2.view().span_digest(..).await?);

    Ok(())
}

/// Two nodes a wire apart hold a query's tree, and one has fallen behind.
/// It fetches again, sending the description of what it holds; the other
/// node's differs, and the two find what the first is missing by exchanging
/// digests of narrower and narrower spans, then the members of the spans
/// that still differ: a few round trips, not one per member. Taking the
/// current versions makes the descriptions agree.
#[tokio::test]
async fn a_node_that_fell_behind_finds_what_it_is_missing() -> anyhow::Result<()> {
    let keyspec = KeySpec::new(vec![asc!(Property::Year, ValueType::I32)]);
    let albums: Vec<_> = (1..=300).map(|n| album!(n, 1700 + n as i32, format!("Album {n}"))).collect();
    let ours: Tree = DigestTree::create(MemoryStore::new(), keyspec.clone(), albums.clone()).await?;
    let theirs: Tree = DigestTree::create(MemoryStore::new(), keyspec, albums).await?;

    // Since we last fetched, their owner has retitled one album and added one.
    let mut trx = theirs.transaction();
    trx.update(album!(150, 1850, "Album 150"), album!(150, 1850, "Album 150 (Live)"))?;
    trx.insert(album!(301, 2001, "Album 301"))?;
    trx.commit().await?;

    // Our fetch carries the description of what we hold. Their node compares
    // it with its own: we are out of date.
    let known_state = ours.view().span_digest(..).await?;
    assert_ne!(theirs.view().span_digest(..).await?, known_state);

    // The connection carries each read our tree makes of theirs across the
    // wire as a request, waits the wire's latency, and brings back what their
    // node reads from its own tree in answer. Their node never compares.
    let mut their_node = Connection::to(theirs.view(), Duration::from_millis(1));
    let differences = ours.view().differences(&mut their_node).await?;

    assert_eq!(differences.len(), 2);
    let (retitled, added) = (&differences[0], &differences[1]);
    assert_eq!(retitled.ours.as_ref().unwrap().payload, "Album 150");
    assert_eq!(retitled.theirs.as_ref().unwrap().payload, "Album 150 (Live)");
    assert_eq!(added.ours, None);
    assert_eq!(added.theirs.as_ref().unwrap().payload, "Album 301");

    // Four round trips, not three hundred: one per level of their tree, the
    // last bringing the members of the two spans that differ.
    assert_eq!(their_node.round_trips(), 4);

    // Our owner takes their versions, as their owner sends them.
    let mut trx = ours.transaction();
    trx.update(album!(150, 1850, "Album 150"), album!(150, 1850, "Album 150 (Live)"))?;
    trx.insert(album!(301, 2001, "Album 301"))?;
    trx.commit().await?;

    assert_eq!(ours.view().span_digest(..).await?, theirs.view().span_digest(..).await?);

    Ok(())
}

// What keeps a description right?

/// A description is right only while the owner tells the tree every change,
/// as the version it holds and the one that replaces it. A change naming a
/// version the tree doesn't hold is refused, and the tree is left as it was.
#[tokio::test]
async fn a_change_names_the_version_the_tree_holds() -> anyhow::Result<()> {
    let keyspec = KeySpec::new(vec![asc!(Property::Year, ValueType::I32)]);
    let albums = [album!(1, 2016, "Two Vines"), album!(2, 2008, "Walking on a Dream")];
    let tree: Tree = DigestTree::create(MemoryStore::new(), keyspec, albums).await?;
    let before = tree.view().span_digest(..).await?.digest;

    let mut trx = tree.transaction();
    trx.update(album!(1, 2016, "Two Vines"), album!(1, 2016, "Two Vines (Live)"))?;
    trx.remove(album!(2, 2008, "Walking on a Dream"))?;
    trx.commit().await?;
    let after = tree.view().span_digest(..).await?.digest;

    assert_ne!(after, before);
    assert_eq!(after.count(), 1);

    // The tree holds "Two Vines (Live)" now, so a change naming "Two Vines"
    // is stale.
    let mut trx = tree.transaction();
    trx.update(album!(1, 2016, "Two Vines"), album!(1, 2016, "Two Vines (Remastered)"))?;

    assert!(matches!(trx.commit().await, Err(Error::NotHeld { .. })));
    assert_eq!(tree.view().span_digest(..).await?.digest, after);

    Ok(())
}

/// A stored tree's changes go into its owner's transaction on the store,
/// beside the owner's own records, and land only when the owner commits it:
/// the description and the records never drift apart.
#[tokio::test]
async fn an_owner_lands_the_trees_rows_with_its_own_records() -> anyhow::Result<()> {
    let keyspec = KeySpec::new(vec![asc!(Property::Year, ValueType::I32)]);
    let store = MemoryStore::new();
    let tree: Tree = DigestTree::create(store.clone(), keyspec, [album!(1, 2016, "Two Vines")]).await?;

    let mut trx = tree.transaction();
    trx.insert(album!(2, 2008, "Walking on a Dream"))?;

    // The owner's transaction, where its own records go too.
    let mut owner_trx = store.begin().await;
    let applied = trx.prepare().await.apply(&mut owner_trx).await?;
    assert_eq!(tree.view().count(..).await?, 1);

    owner_trx.commit().await?;
    assert_eq!(tree.view().count(..).await?, 2);

    // The rows have landed, so a store that keeps rows in memory may now take them in.
    applied.landed();

    Ok(())
}

/// A tree reopens from its store with its key spec and digest intact.
#[tokio::test]
async fn a_tree_opens_again_from_its_store() -> anyhow::Result<()> {
    let keyspec = KeySpec::new(vec![asc!(Property::Year, ValueType::I32)]);
    let store = MemoryStore::new();
    let albums = [album!(1, 2016, "Two Vines"), album!(2, 2008, "Walking on a Dream")];
    let tree: Tree = DigestTree::create(store.clone(), keyspec.clone(), albums).await?;
    let digest = tree.view().span_digest(..).await?.digest;
    drop(tree);

    let reopened: Tree = DigestTree::open(store).await?;

    assert_eq!(reopened.key_spec(), &keyspec);
    assert_eq!(reopened.view().span_digest(..).await?.digest, digest);

    Ok(())
}

// What does the key do?

/// A limited query holds a window of its tree: a view up to the last key it
/// shows, where reads and digests stop.
#[tokio::test]
async fn a_limited_query_holds_a_window_of_its_tree() -> anyhow::Result<()> {
    let keyspec = KeySpec::new(vec![asc!(Property::Year, ValueType::I32)]);
    let albums = [album!(1, 2016, "Two Vines"), album!(2, 2008, "Walking on a Dream"), album!(3, 2013, "Ice on the Dune")];
    let tree: Tree = DigestTree::create(MemoryStore::new(), keyspec, albums).await?;

    let window = tree.select(..=2013)?;
    let titles: Vec<Title> = window.range(..)?.map_ok(|leaf| leaf.payload).try_collect().await?;

    assert_eq!(titles, ["Walking on a Dream", "Ice on the Dune"]);
    assert_eq!(window.span_digest(..).await?.digest.count(), 2);

    Ok(())
}

/// A second key part orders members that tie on the first; a key range names
/// the leading parts' values.
#[tokio::test]
async fn a_key_of_two_parts_sorts_by_the_first_and_then_the_second() -> anyhow::Result<()> {
    let keyspec = KeySpec::new(vec![asc!(Property::Year, ValueType::I32), asc!(Property::Title, ValueType::String)]);
    let albums = [
        album!(1, 2016, "Two Vines"),
        album!(2, 2008, "Walking on a Dream"),
        album!(3, 2013, "Ice on the Dune"),
        album!(4, 2013, "Ice on the Dune (Live)"),
    ];
    let tree: Tree = DigestTree::create(MemoryStore::new(), keyspec, albums).await?;
    let view = tree.view();

    let of_2013: Vec<Title> = view.range(KeyRange::prefix([2013]))?.map_ok(|leaf| leaf.payload).try_collect().await?;

    assert_eq!(of_2013, ["Ice on the Dune", "Ice on the Dune (Live)"]);

    Ok(())
}

/// A member without a value for a key part is still filed, where the part's
/// nulls order puts it.
#[tokio::test]
async fn an_album_without_a_year_is_a_member_too() -> anyhow::Result<()> {
    let keyspec = KeySpec::new(vec![asc!(Property::Year, ValueType::I32)]);
    let albums = [album!(1, 2016, "Two Vines"), album!(2, 2008, "Walking on a Dream"), album!(3, None, "Demos")];
    let tree: Tree = DigestTree::create(MemoryStore::new(), keyspec, albums).await?;
    let view = tree.view();

    // A range of years leaves it out; `..` keeps it; the albums with no year
    // are a key range of their own.
    let all: Vec<Title> = view.range(..)?.map_ok(|leaf| leaf.payload).try_collect().await?;
    let since_2010: Vec<Title> = view.range(2010..)?.map_ok(|leaf| leaf.payload).try_collect().await?;
    let undated: Vec<Title> = view.range(KeyRange::new(vec![None], None))?.map_ok(|leaf| leaf.payload).try_collect().await?;

    assert_eq!(all, ["Walking on a Dream", "Two Vines", "Demos"]);
    assert_eq!(since_2010, ["Two Vines"]);
    assert_eq!(undated, ["Demos"]);

    Ok(())
}
