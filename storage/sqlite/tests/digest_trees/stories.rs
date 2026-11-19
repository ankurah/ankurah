//! Digest-tree maintenance on SQLite: consistent readers, serialized writers,
//! and SQL query membership that agrees with later Rust maintenance.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use ankql::ast::{ComparisonOperator, Expr, Predicate, Selection};
use ankurah::core::property::backend::{LWWBackend, PropertyBackend};
use ankurah::core::storage::{StorageEngine, StorageTransaction};
use ankurah::core::sync::conformance::fixtures::{assert_holds, created, item, stores};
use ankurah::core::sync::conformance::{several_writers_on_one_node_committing_to_two_trees_at_once_land_every_commit, TestDatabase};
use ankurah::core::sync::{CacheSettings, TreeChange, TreeId, TreeManager};
use ankurah::core::test_helpers::map_conformance::fixtures::HANG;
use ankurah::core::value::Value;
use ankurah_digest_tree::Ristretto255;
use ankurah_proto::{Attested, Clock, EntityId, EntityState, ModelId, PropertyId, State, StateBuffers};
use ankurah_storage_sqlite::SqliteStorageEngine;
use futures_util::TryStreamExt;
use tokio::sync::Notify;
use tokio::time::timeout;

use crate::support::{Busy, Database, Tree};

/// A reader on another node, its snapshot a read transaction of its own,
/// reads the tree as the last commit left it while a node's storage
/// transaction holds the write lock, and sees that transaction's commit once
/// it lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reader_on_another_node_sees_the_last_commit_while_a_node_holds_the_write_lock() -> anyhow::Result<()> {
    let database = Database::start()?;
    let (writer, reader) = (database.node().await?, database.node().await?);
    let (writer_trees, reader_trees) = (stores(&writer), stores(&reader));
    let tree: Tree = created(&writer_trees, &writer, "albums", [item!(1, 10)]).await?;
    let elsewhere: Tree = reader_trees.open(&TreeId::new("albums")).await?;

    let mut changes = tree.transaction();
    changes.insert(item!(2, 20))?;
    let mut holding = writer.transaction();
    let applied = writer_trees.apply(&mut holding, vec![TreeChange::Changes(changes)]).await?;

    let during = timeout(HANG, elsewhere.view().count(..)).await??;
    let committed = holding.commit().await?.committed()?;
    applied.landed(&committed);

    assert_eq!(during, 1, "the reader read the last commit");
    assert_holds(&elsewhere, &[item!(1, 10), item!(2, 20)]).await
}

/// A node whose apply finds another node's storage transaction holding the
/// write lock waits at the database until that transaction ends, and then
/// applies its changes to the tree as the other's commit left it: here it
/// removes the member the other inserted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_node_waits_for_the_write_lock_and_applies_on_top_of_the_first_ones_commit() -> anyhow::Result<()> {
    static WAITING: Notify = Notify::const_new();

    /// The second node's busy handler: it tells the test the node waits, and
    /// tries again a moment later, for as long as it takes. The moment paces
    /// the tries, and decides nothing.
    fn waiting(_tries: i32) -> bool {
        WAITING.notify_one();
        std::thread::sleep(Duration::from_millis(1));
        true
    }

    let database = Database::start()?;
    let (first, second) = (database.node().await?, database.node_with(Busy::Handler(waiting)).await?);
    let (first_trees, second_trees) = (stores(&first), stores(&second));
    let tree: Tree = created(&first_trees, &first, "albums", [item!(1, 10)]).await?;
    let elsewhere: Tree = second_trees.open(&TreeId::new("albums")).await?;

    let mut changes = tree.transaction();
    changes.insert(item!(2, 20))?;
    let mut holding = first.transaction();
    let applied = first_trees.apply(&mut holding, vec![TreeChange::Changes(changes)]).await?;

    let second_applying = tokio::spawn({
        let elsewhere = elsewhere.clone();
        async move {
            let mut changes = elsewhere.transaction();
            changes.remove(item!(2, 20))?;
            changes.insert(item!(3, 30))?;

            let mut trx = second.transaction();
            let applied = second_trees.apply(&mut trx, vec![TreeChange::Changes(changes)]).await?;
            let committed = trx.commit().await?.committed()?;
            anyhow::Ok((applied, committed))
        }
    });

    timeout(HANG, WAITING.notified()).await?;
    let committed = holding.commit().await?.committed()?;
    applied.landed(&committed);

    let (second_applied, second_committed) = timeout(HANG, second_applying).await???;
    assert!(second_applied.refused().is_empty(), "the second found the member the first inserted");
    second_applied.landed(&second_committed);

    assert_holds(&tree, &[item!(1, 10), item!(3, 30)]).await?;
    assert_holds(&elsewhere, &[item!(1, 10), item!(3, 30)]).await
}

/// Several writers on one node, committing to two trees at once, wait their
/// turn at the engine, never at the database's write lock: on a node whose
/// busy timeout is SQLite's smallest, a millisecond, every commit lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn several_writers_on_one_node_wait_their_turn_and_none_meets_a_held_write_lock() -> anyhow::Result<()> {
    /// The test database, whose nodes each give up on the write lock after
    /// SQLite's smallest busy timeout.
    struct Impatient(Database);

    impl TestDatabase for Impatient {
        type Engine = SqliteStorageEngine;

        async fn engine(&self) -> anyhow::Result<Arc<SqliteStorageEngine>> {
            self.0.node_with(Busy::Timeout(Duration::from_millis(1))).await
        }

        // WAL snapshots retain their state while another connection commits.
        fn keeps_a_reads_state_across_commits(&self) -> bool { true }
    }

    several_writers_on_one_node_committing_to_two_trees_at_once_land_every_commit::<Ristretto255, _>(&Impatient(Database::start()?)).await
}

/// SQL initial membership and Rust maintenance must agree on a nullable
/// conjunction beneath NOT, or a stored member silently misses later changes.
#[tokio::test]
async fn manager_maintenance_agrees_with_sql_for_a_nullable_conjunction_under_not() -> anyhow::Result<()> {
    let database = Database::start()?;
    let storage = database.node().await?;
    let model = ModelId::EntityId(EntityId::from_bytes([80; 32]));
    let optional = PropertyId::EntityId(EntityId::from_bytes([90; 32]));
    let flag = PropertyId::EntityId(EntityId::from_bytes([91; 32]));
    let payload = PropertyId::EntityId(EntityId::from_bytes([92; 32]));
    let state = |id, optional_value: Option<i64>, enabled, value, seed| -> anyhow::Result<_> {
        let backend = LWWBackend::new();
        if let Some(value) = optional_value {
            backend.set(optional, Some(Value::I64(value)));
        }
        backend.set(flag, Some(Value::Bool(enabled)));
        backend.set(payload, Some(Value::I64(value)));
        let event = ankurah::core::test_helpers::commit_backend(&backend, seed)?;
        Ok(Attested::opt(
            EntityState {
                entity_id: EntityId::from_bytes([id; 32]),
                state: State {
                    state_buffers: StateBuffers(BTreeMap::from([("lww".into(), backend.to_state_buffer()?)])),
                    memberships: [model].into_iter().collect(),
                    head: Clock::singleton(&event),
                },
            },
            None,
        ))
    };
    let before = state(1, None, false, 1, 1)?;
    let after = state(1, None, false, 2, 3)?;
    // Materialize the optional-property column; the target's value is SQL NULL.
    let sentinel = state(2, Some(1), true, 0, 2)?;
    let mut trx = storage.transaction();
    for state in [&before, &sentinel] {
        trx.set_state(&Clock::default(), state).await?;
    }
    trx.commit().await?.committed()?;

    let equals = |property: PropertyId, value| Predicate::Comparison {
        left: Box::new(Expr::Path(property.into())),
        operator: ComparisonOperator::Equal,
        right: Box::new(Expr::Literal(value)),
    };
    let selection = Selection {
        predicate: Predicate::Not(Box::new(Predicate::And(
            Box::new(equals(optional, Value::I64(1))),
            Box::new(equals(flag, Value::Bool(true))),
        ))),
        order_by: None,
        limit: None,
    }
    .and_member_of(model);
    let fetched = storage.fetch_states(&selection).await?;
    assert_eq!(fetched.iter().map(|state| state.payload.entity_id).collect::<Vec<_>>(), vec![before.payload.entity_id]);

    let publication = Arc::new(tokio::sync::Mutex::new(()));
    let manager = TreeManager::<_, Ristretto255>::new(Arc::clone(&storage), Arc::clone(&publication), CacheSettings { bytes: 4096 }, 1);
    let tree = manager.open(selection).await?;
    let initial: Vec<_> = tree.view().range(..)?.map_ok(|leaf| (leaf.id.0, leaf.payload.0)).try_collect().await?;
    assert_eq!(initial, vec![(before.payload.entity_id, before.payload.state.head.clone())]);

    {
        let _publication = publication.lock().await;
        let mut trx = storage.transaction();
        trx.set_state(&before.payload.state.head, &after).await?;
        manager.invalidate();
        trx.commit().await?.committed()?;
    }
    manager.maintain().await?;
    manager.flush().await?;

    assert_eq!(storage.get_state(after.payload.entity_id).await?.payload.state.head, after.payload.state.head);
    let maintained: Vec<_> = tree.view().range(..)?.map_ok(|leaf| (leaf.id.0, leaf.payload.0)).try_collect().await?;
    assert_eq!(maintained, vec![(after.payload.entity_id, after.payload.state.head)], "the tree must receive the canonical head change");
    Ok(())
}
