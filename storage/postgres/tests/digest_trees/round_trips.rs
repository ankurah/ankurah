//! A commit's round trips to the database, counted at a proxy between a node
//! and it: an apply's locks and reads go in one, and every tree's write goes
//! with the commit's `COMMIT` in the other, while the rows of the trees' trie
//! nodes are cached. Where a test says how many statements each round trip
//! holds, the proxy withholds its answers until all have arrived, so that the
//! count proves the node sent them together.

use ankurah::core::sync::conformance::fixtures::TestStores;

use ankurah::core::storage::{StorageEngine, StorageTransaction};
use ankurah::core::sync::conformance::fixtures::{assert_holds, commit, created, item, stores, Changes, Item};
use ankurah::core::sync::{TreeChange, TreeId};
use ankurah::core::test_helpers::map_conformance::fixtures::entity;
use ankurah::proto::{Attested, Clock, EntityState};
use ankurah_digest_tree::Ristretto255;
use ankurah_storage_postgres::Postgres;

use crate::database::proxy::Proxy;
use crate::database::Database;
use crate::Tree;

/// Members numbered 0 to 299, enough for a trie of several levels, item 7
/// labelled `$label`.
macro_rules! several_levels {
    ($label:expr) => {
        (0..300u64).map(|n| if n == 7 { item!(n, n as i64, $label) } else { item!(n, n as i64) }).collect::<Vec<Item>>()
    };
}

/// The statements of a storage transaction's first request when it applies
/// changes to `$trees` trees whose trie nodes' rows are cached: the
/// transaction's beginning, each tree's lock, the trees' entries, and each
/// tree's leaves, change log and history metadata.
macro_rules! locks_and_reads {
    ($trees:expr) => {
        1 + $trees + 1 + 3 * $trees
    };
}

/// The statements of the request that commits changes to `$trees` trees:
/// each tree's rows, change log and history, the trees' entries, and the `COMMIT`.
macro_rules! writes_and_commit {
    ($trees:expr) => {
        3 * $trees + 1 + 1
    };
}

/// An apply to three trees whose trie nodes' rows the cache keeps, as their
/// creations' landings leave them, relabelling a member of each, and its
/// commit make two round trips.
#[tokio::test]
async fn an_apply_to_several_trees_whose_trie_nodes_are_cached_and_its_commit_make_two_round_trips() -> anyhow::Result<()> {
    let database = Database::start().await?;
    let proxy = database.proxy().await?;
    let engine = database.node_through(&proxy).await?;
    let trees = stores(&engine);

    let mut opened: Vec<Tree> = Vec::new();
    for id in ["first", "second", "third"] {
        opened.push(created(&trees, &engine, id, several_levels!("")).await?);
    }

    let mut changes = Vec::new();
    for tree in &opened {
        let mut relabelling = tree.transaction();
        relabelling.update(item!(7, 7), item!(7, 7, "relabelled"))?;
        changes.push(TreeChange::Changes(relabelling));
    }

    proxy.expect_round_trips(&[locks_and_reads!(3), writes_and_commit!(3)]);
    let before = proxy.round_trips();
    let mut trx = engine.transaction();
    let applied = trees.apply(&mut trx, changes).await?;
    let committed = trx.commit().await?.committed()?;
    let made = proxy.round_trips() - before;
    applied.landed(&committed);

    assert_eq!(made, 2, "the locks and reads, then the writes and the COMMIT");
    for tree in &opened {
        assert_holds(tree, &several_levels!("relabelled")).await?;
    }

    Ok(())
}

/// After two commits by another node to a tree, a node whose cache keeps the
/// rows of the tree's trie nodes from before them catches its cache up from
/// the change log in its next commit's first request, and the commit still
/// makes two round trips.
#[tokio::test]
async fn after_another_nodes_commits_a_nodes_next_commit_still_makes_two_round_trips() -> anyhow::Result<()> {
    let database = Database::start().await?;
    let proxy = database.proxy().await?;
    let (here, there) = (database.node_through(&proxy).await?, database.node().await?);
    let (here_trees, there_trees) = (stores(&here), stores(&there));
    let tree: Tree = created(&here_trees, &here, "tree", several_levels!("")).await?;
    let elsewhere: Tree = there_trees.open(&TreeId::new("tree")).await?;

    commit(&there_trees, &there, &elsewhere, |changes| changes.update(item!(150, 150), item!(150, 150, "there"))).await?;
    commit(&there_trees, &there, &elsewhere, |changes| changes.update(item!(290, 290), item!(290, 290, "there"))).await?;

    proxy.expect_round_trips(&[locks_and_reads!(1), writes_and_commit!(1)]);
    let before = proxy.round_trips();
    commit(&here_trees, &here, &tree, |changes| changes.update(item!(7, 7), item!(7, 7, "relabelled"))).await?;
    let made = proxy.round_trips() - before;

    let mut expected = several_levels!("relabelled");
    expected[150] = item!(150, 150, "there");
    expected[290] = item!(290, 290, "there");

    assert_eq!(made, 2, "the locks, the reads and the change log, then the writes and the COMMIT");
    assert_holds(&tree, &expected).await?;
    assert_holds(&elsewhere, &expected).await
}

/// A commit with an entity record makes as many round trips whether the
/// tree it applies to changes or not: the tree's writes go with the
/// `COMMIT`, after the record's own statements.
#[tokio::test]
async fn a_commit_with_an_entity_record_makes_no_round_trip_for_its_trees_writes() -> anyhow::Result<()> {
    let database = Database::start().await?;
    let proxy = database.proxy().await?;
    let engine = database.node_through(&proxy).await?;
    let trees = stores(&engine);
    let tree: Tree = created(&trees, &engine, "tree", several_levels!("")).await?;

    // The engine's first commit of a record creates the tables records live in.
    let mut first = engine.transaction();
    first.set_state(&Clock::default(), &entity!(1).0).await?;
    first.commit().await?.committed()?;

    let mut relabelling = tree.transaction();
    relabelling.update(item!(7, 7), item!(7, 7, "relabelled"))?;
    let changing = round_trips_of_a_commit(&proxy, &trees, &engine, relabelling, entity!(2).0, writes_and_commit!(1)).await?;
    let unchanging = round_trips_of_a_commit(&proxy, &trees, &engine, tree.transaction(), entity!(3).0, 1).await?;

    assert_eq!(changing, unchanging, "the round trips of a commit whose tree changes, and of one whose tree doesn't");
    assert_holds(&tree, &several_levels!("relabelled")).await
}

/// The round trips, counted at `proxy`, of one storage transaction on
/// `engine` that applies `changes` to a tree whose trie nodes' rows are
/// cached, sets `record`, in no model, and commits, its request that commits
/// holding `commit_statements` statements: the apply's locks and reads; the
/// record's lock, its row as stored and its row as written, each a statement
/// prepared in one round trip and run in the next; and the commit. The
/// client closes each statement it prepared as it sends its next.
async fn round_trips_of_a_commit(
    proxy: &Proxy,
    trees: &TestStores<Postgres>,
    engine: &Postgres,
    changes: Changes<Postgres, Ristretto255>,
    record: Attested<EntityState>,
    commit_statements: u64,
) -> anyhow::Result<u64> {
    proxy.expect_round_trips(&[locks_and_reads!(1), 1, 1, 1 + 1, 1, 1 + 1, 1, 1 + commit_statements]);
    let before = proxy.round_trips();
    let mut trx = engine.transaction();
    trx.set_state(&Clock::default(), &record).await?;
    let applied = trees.apply(&mut trx, vec![TreeChange::Changes(changes)]).await?;
    let committed = trx.commit().await?.committed()?;
    let made = proxy.round_trips() - before;

    applied.landed(&committed);
    Ok(made)
}
