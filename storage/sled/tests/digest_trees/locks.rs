//! A tree's locks, which sled keeps in its process: a commit to a tree waits
//! for the reads open on it and for none on another tree.

use std::ops::Bound;
use std::pin::pin;

use ankurah::core::storage::{StorageEngine, StorageTransaction};
use ankurah::core::sync::conformance::fixtures::{assert_holds, commit, created, item, stores};
use ankurah::core::sync::TreeChange;
use ankurah::core::test_helpers::map_conformance::fixtures::{poll_once, HANG};
use ankurah_digest_tree::{Encode, ReadTrx, ScanDirection, Store};
use tokio::time::timeout;

use crate::support::{Database, Tree};

/// A commit to a tree waits while a read of the tree is open, and a commit
/// to another tree doesn't wait for it: that one lands, and the read still
/// sees its tree as it was. Once the read ends, the first commit lands.
#[tokio::test]
async fn a_commit_to_a_tree_waits_for_a_read_open_on_it_and_for_none_on_another_tree() -> anyhow::Result<()> {
    let database = Database::start()?;
    let engine = database.engine();
    let trees = stores(engine);
    let (albums, songs): (Tree, Tree) =
        (created(&trees, engine, "albums", [item!(1, 10)]).await?, created(&trees, engine, "songs", [item!(1, 10)]).await?);

    let mut read = albums.store().begin_read().await?;
    let before = read.range(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, usize::MAX).await?;

    let mut changes = albums.transaction();
    changes.insert(item!(2, 20))?;
    let mut trx = engine.transaction();
    let applied = trees.apply(&mut trx, vec![TreeChange::Changes(changes)]).await?;

    let mut committing = pin!(trx.commit());
    assert!(poll_once(committing.as_mut()).is_pending(), "the commit waits for the read open on its tree");

    // Commits land one at a time: had the first gone ahead, it would have landed before this one.
    timeout(HANG, commit(&trees, engine, &songs, |changes| changes.insert(item!(2, 20)))).await??;
    let during = read.range(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, usize::MAX).await?;
    assert_eq!(encoded(during), encoded(before), "the open read sees the tree as it was");

    drop(read);
    let committed = timeout(HANG, committing).await??.committed()?;
    applied.landed(&committed);

    assert_holds(&albums, &[item!(1, 10), item!(2, 20)]).await?;
    assert_holds(&songs, &[item!(1, 10), item!(2, 20)]).await
}

/// Rows a read gave, as the bytes they're stored as.
fn encoded(rows: Vec<(impl Encode, impl Encode)>) -> Vec<(Vec<u8>, Vec<u8>)> {
    rows.into_iter().map(|(key, row)| (key.encode(), row.encode())).collect()
}
