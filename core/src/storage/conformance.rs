//! The conformance suite every engine with a commit log and tree storage runs:
//! executable statements of the contracts in [`super::log`] and
//! [`super::tree`].
//!
//! Each case is a public function taking a fresh engine and panicking on a
//! violation. An engine adopts the whole suite with
//! [`tree_storage_conformance!`](crate::tree_storage_conformance), which
//! expands to one `#[tokio::test]` per case:
//!
//! ```ignore
//! mod tree_storage {
//!     ankurah_core::tree_storage_conformance!(MyEngine::open_temporary().await.unwrap());
//! }
//! ```
//!
//! The cases assume nothing an engine may choose: positions may have gaps,
//! batches may lock or detect conflicts optimistically, and tree ids may be
//! any numbers.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
    sync::Arc,
    time::Duration,
};

use ankql::ast::PropertyId;
use ankurah_proto::{Attested, AuthorId, Clock, EntityId, EntityState, Event, EventId, ModelId, OperationSet, State, StateBuffers};

use super::{
    log::{CommitLog, LogError, LogIncarnation, LogPosition, LogRow},
    tree::{
        AddressRange, BuildStatus, FoldedRow, HashedIndex, NodePrefix, NodeRow, Tombstone, TreeBatch, TreeBatchOutcome, TreeCell, TreeId,
        TreeRead, TreeStorage, TreeStorageError,
    },
    StorageCommitOutcome, StorageEngine, StorageTransaction,
};
use crate::{
    error::RetrievalError,
    indexing::{encode_tuple_values_with_key_spec, IndexKeyPart, KeySpec},
    property::backend::{LWWBackend, PropertyBackend},
    value::{Value, ValueType},
};

/// How long a case waits for an operation that must finish before it calls
/// the operation stuck.
const PROGRESS: Duration = Duration::from_secs(10);

/// Expand to one `#[tokio::test]` per conformance case, each running against
/// a fresh engine that `$make` builds; `$make` may `.await`. The calling crate
/// needs tokio with its `macros` and `rt` features.
#[macro_export]
macro_rules! tree_storage_conformance {
    ($make:expr) => {
        $crate::tree_storage_conformance!(@cases ($make)
            fresh_store_keeps_an_entity_id_tree
            commits_take_increasing_positions
            multi_entity_commit_shares_one_position
            state_only_commits_are_logged
            log_rows_carry_keys_under_every_tree
            log_reads_end_at_whole_positions
            durable_position_trails_the_stable_position
            retention_floor_bounds_reads
            reset_mints_a_new_incarnation
            conditional_position_check
            positions_stay_in_the_tree_incarnation
            entity_to_keys_lookup
            tombstones_through_a_split
            snapshot_reads_inside_a_batch
            concurrent_batches_commit_one
            children_and_leaf_ranges_at_ragged_depths
            build_state_and_generation
            prune_horizon_rises_with_every_lost_removal
        );
    };
    (@cases ($make:expr) $($case:ident)*) => {
        $(
            #[tokio::test]
            async fn $case() { $crate::storage::conformance::$case(::std::sync::Arc::new($make)).await; }
        )*
    };
}

/// A new store keeps a ready entity-id tree folded from the start of its log,
/// registered under that index and permanent, and no removal predates it.
pub async fn fresh_store_keeps_an_entity_id_tree<E: TreeStorage + 'static>(engine: Arc<E>) {
    let trees = engine.trees().await.unwrap();
    assert_eq!(trees.len(), 1, "a new store has exactly the entity-id tree");
    assert_eq!(trees[0].index, HashedIndex::EntityId);
    let start = LogPosition::start(engine.stable_position().await.unwrap().incarnation());
    assert_eq!(cell_of(&*engine, trees[0].id).await, TreeCell { generation: 0, status: BuildStatus::Ready, folded: start });
    assert_eq!(engine.register_tree(HashedIndex::EntityId).await.unwrap(), trees[0], "registration is keyed by the index");
    assert!(matches!(engine.unregister_tree(trees[0].id).await, Err(TreeStorageError::PermanentTree)));
    assert_eq!(engine.prune_horizon().await.unwrap(), start);
}

/// Each commit that sets states takes a position above every earlier one; an
/// aborted commit writes no rows, and neither does a commit that only stores
/// events.
pub async fn commits_take_increasing_positions<E: TreeStorage + 'static>(engine: Arc<E>) {
    let start = engine.stable_position().await.unwrap();
    let first = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let stale = commit_states(&*engine, vec![(head(99), state(entity(1), 2, &[], &[]))]).await;
    assert!(matches!(stale, StorageCommitOutcome::Conflict { .. }), "the expected head is wrong");
    let second = create(&*engine, vec![state(entity(2), 3, &[], &[])]).await;
    assert!(second > first, "positions grow in commit order");

    let mut events_only = engine.transaction();
    let event = Event::update(entity(1), head(1), AuthorId::Unknown, OperationSet(Vec::new()));
    events_only.add_events(&[Attested::opt(event, None)]).await.unwrap();
    assert_eq!(events_only.commit().await.unwrap().committed().unwrap().position, None, "a commit that sets no state writes no row");

    let page = engine.read_log(start, usize::MAX).await.unwrap();
    let logged: Vec<_> = page.rows.iter().map(|row| (row.position, row.entity_id)).collect();
    assert_eq!(logged, [(first, entity(1)), (second, entity(2))], "the aborted commit left no row");
    assert_eq!(page.next, engine.stable_position().await.unwrap());
    assert!(page.next > second);
}

/// One transaction that sets several entities writes one row per entity, all
/// at the commit's position, in entity id order.
pub async fn multi_entity_commit_shares_one_position<E: TreeStorage + 'static>(engine: Arc<E>) {
    let tree = entity_id_tree(&*engine).await;
    let written = [(entity(3), 1), (entity(1), 2), (entity(2), 3)];
    let position = create(&*engine, written.iter().map(|&(id, n)| state(id, n, &[], &[])).collect()).await;
    let rows = read_all(&*engine, position).await;
    let mut expected = written.to_vec();
    expected.sort_by_key(|&(id, _)| id);
    assert_eq!(rows.len(), expected.len());
    for (row, (id, n)) in rows.iter().zip(expected) {
        assert_eq!((row.position, row.entity_id, &row.head), (position, id, &head(n)));
        assert_eq!(row.keys[&tree], BTreeSet::from([id.to_bytes().to_vec()]), "the entity-id index files an entity under its id");
    }
}

/// A transaction that sets a state without storing an event is logged, and an
/// entity set twice in one transaction has one row, with its last head.
pub async fn state_only_commits_are_logged<E: TreeStorage + 'static>(engine: Arc<E>) {
    let mut transaction = engine.transaction();
    transaction.set_state(&Clock::default(), &state(entity(1), 1, &[], &[])).await.unwrap();
    transaction.set_state(&head(1), &state(entity(1), 2, &[], &[])).await.unwrap();
    let position = transaction.commit().await.unwrap().committed().unwrap().position.expect("a state-only commit is logged");
    let rows = read_all(&*engine, position).await;
    assert_eq!(rows.iter().map(|row| (row.entity_id, row.head.clone())).collect::<Vec<_>>(), [(entity(1), head(2))]);
    assert_eq!(engine.get_state(entity(1)).await.unwrap().payload.state.head, head(2));
}

/// A log row carries the entity's keys under every tree registered before its
/// commit, as a set that may be empty, and nothing for a tree registered after.
pub async fn log_rows_carry_keys_under_every_tree<E: TreeStorage + 'static>(engine: Arc<E>) {
    let before = create(&*engine, vec![state(entity(9), 9, &[component()], &[(title(), text("early"))])]).await;
    let tree = engine.register_tree(title_index()).await.unwrap().id;
    let registered = cell_of(&*engine, tree).await.folded;
    assert!(registered > before, "a tree starts after the commits that preceded it");
    let position = create(
        &*engine,
        vec![
            state(entity(1), 1, &[component()], &[(title(), text("a"))]),
            state(entity(2), 2, &[component()], &[]),
            state(entity(3), 3, &[], &[(title(), text("c"))]),
        ],
    )
    .await;
    assert!(position >= registered);
    let rows = read_all(&*engine, before).await;
    let keys = |id: EntityId| rows.iter().find(|row| row.entity_id == id).expect("a row per entity").keys.get(&tree).cloned();
    assert_eq!(keys(entity(9)), None, "a row written before registration carries nothing for the tree");
    let encoded = encode_tuple_values_with_key_spec(&[text("a")], &title_key_spec()).unwrap();
    assert_eq!(keys(entity(1)), Some(BTreeSet::from([encoded])));
    assert_eq!(keys(entity(2)), Some(BTreeSet::new()), "a member without the title is filed under no key");
    assert_eq!(keys(entity(3)), Some(BTreeSet::new()), "an entity outside the component is filed under no key");
}

/// A log read returns whole positions: it stops after the first position at
/// which it holds the limit, and the next read continues there.
pub async fn log_reads_end_at_whole_positions<E: TreeStorage + 'static>(engine: Arc<E>) {
    let start = engine.stable_position().await.unwrap();
    let mut positions = Vec::new();
    for n in [1u8, 3, 5] {
        positions.push(create(&*engine, vec![state(entity(n), n, &[], &[]), state(entity(n + 1), n + 1, &[], &[])]).await);
    }
    let page = engine.read_log(start, 3).await.unwrap();
    let read: Vec<_> = page.rows.iter().map(|row| row.position).collect();
    assert_eq!(read, [positions[0], positions[0], positions[1], positions[1]], "the limit falls inside the second commit, read whole");
    assert!(page.next > positions[1] && page.next <= positions[2]);
    let rest = engine.read_log(page.next, 3).await.unwrap();
    assert_eq!(rest.rows.iter().map(|row| row.position).collect::<Vec<_>>(), [positions[2], positions[2]]);
    assert_eq!(rest.next, engine.stable_position().await.unwrap());
    assert_eq!(engine.read_log(start, 1).await.unwrap().rows.len(), 2, "a commit larger than the limit is still read whole");
}

/// The durable position never lies above the stable position and never moves
/// back, through commits, aborts, event-only commits and trimming, and it
/// reaches every commit.
pub async fn durable_position_trails_the_stable_position<E: TreeStorage + 'static>(engine: Arc<E>) {
    let start = LogPosition::start(engine.stable_position().await.unwrap().incarnation());
    let durable = durable_after(&*engine, start).await;
    create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let durable = durable_after(&*engine, durable).await;
    let stale = commit_states(&*engine, vec![(head(99), state(entity(1), 2, &[], &[]))]).await;
    assert!(matches!(stale, StorageCommitOutcome::Conflict { .. }));
    let durable = durable_after(&*engine, durable).await;
    let mut events_only = engine.transaction();
    events_only
        .add_events(&[Attested::opt(Event::update(entity(1), head(1), AuthorId::Unknown, OperationSet(Vec::new())), None)])
        .await
        .unwrap();
    events_only.commit().await.unwrap();
    let durable = durable_after(&*engine, durable).await;
    let last = create(&*engine, vec![state(entity(2), 2, &[], &[]), state(entity(3), 3, &[], &[])]).await;
    let durable = durable_after(&*engine, durable).await;
    engine.discard_log_below(last).await.unwrap();
    durable_after(&*engine, durable).await;

    let deadline = tokio::time::Instant::now() + PROGRESS;
    while engine.durable_position().await.unwrap() <= last {
        assert!(tokio::time::Instant::now() < deadline, "the durable position never passed the last commit");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Read the durable position, checking it against the stable position and
/// the durable position read before.
async fn durable_after<E: CommitLog>(engine: &E, before: LogPosition) -> LogPosition {
    // Both only rise, so reading the durable position first keeps the check
    // sound while commits go on.
    let durable = engine.durable_position().await.unwrap();
    let stable = engine.stable_position().await.unwrap();
    assert!(durable <= stable, "the durable position {durable:?} lies above the stable position {stable:?}");
    assert!(durable >= before, "the durable position moved back from {before:?} to {durable:?}");
    durable
}

/// Reading below the retention floor fails; the floor rises when rows are
/// discarded, never falls, and never passes the stable position.
pub async fn retention_floor_bounds_reads<E: TreeStorage + 'static>(engine: Arc<E>) {
    let start = engine.retention_floor().await.unwrap();
    let first = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let second = create(&*engine, vec![state(entity(2), 2, &[], &[])]).await;
    engine.discard_log_below(second).await.unwrap();
    assert_eq!(engine.retention_floor().await.unwrap(), second);
    assert!(matches!(engine.read_log(first, 10).await, Err(LogError::BelowRetentionFloor { floor }) if floor == second));
    assert!(matches!(engine.read_log(start, 10).await, Err(LogError::BelowRetentionFloor { .. })));
    assert_eq!(read_all(&*engine, second).await.iter().map(|row| row.entity_id).collect::<Vec<_>>(), [entity(2)]);
    engine.discard_log_below(first).await.unwrap();
    assert_eq!(engine.retention_floor().await.unwrap(), second, "the floor never falls");
    let stable = engine.stable_position().await.unwrap();
    engine.discard_log_below(LogPosition::new(stable.incarnation(), stable.offset() + 1000)).await.unwrap();
    assert_eq!(engine.retention_floor().await.unwrap(), stable, "the floor stops at the stable position");
}

/// A reset mints a new log incarnation: earlier positions neither compare nor
/// read, and only a fresh entity-id tree remains.
pub async fn reset_mints_a_new_incarnation<E: TreeStorage + 'static>(engine: Arc<E>) {
    let old = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    engine.register_tree(title_index()).await.unwrap();
    engine.delete_all().await.unwrap();
    let stable = engine.stable_position().await.unwrap();
    assert_ne!(stable.incarnation(), old.incarnation());
    assert_eq!(stable.partial_cmp(&old), None, "positions of different incarnations do not compare");
    assert!(engine.durable_position().await.unwrap() <= stable, "the durable position belongs to the new incarnation");
    assert!(matches!(engine.read_log(old, 10).await, Err(LogError::IncarnationMismatch { .. })));
    let trees = engine.trees().await.unwrap();
    assert_eq!(trees.iter().map(|tree| &tree.index).collect::<Vec<_>>(), [&HashedIndex::EntityId]);
    let start = LogPosition::start(stable.incarnation());
    assert_eq!(cell_of(&*engine, trees[0].id).await, TreeCell { generation: 0, status: BuildStatus::Ready, folded: start });
    assert_eq!(engine.prune_horizon().await.unwrap(), start);
    assert!(matches!(engine.get_state(entity(1)).await, Err(RetrievalError::EntityNotFound(_))));
}

/// A batch commits only while the tree's cell equals the one the caller
/// expected; otherwise none of its writes applies and the caller learns the
/// cell.
pub async fn conditional_position_check<E: TreeStorage + 'static>(engine: Arc<E>) {
    let tree = entity_id_tree(&*engine).await;
    let position = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let stable = engine.stable_position().await.unwrap();
    let cell = cell_of(&*engine, tree).await;
    let key = entity(1).to_bytes();
    let reader = engine.reader(tree).await.unwrap();

    let mut batch = engine.batch(tree).await.unwrap();
    batch.put_row(folded_row(&key, entity(1), position)).await.unwrap();
    batch.set_folded(stable).await.unwrap();
    let stale = TreeCell { generation: cell.generation + 1, ..cell };
    assert_eq!(batch.commit(&stale).await.unwrap(), TreeBatchOutcome::Conflict { observed: cell });
    assert_eq!(reader.row(&key, entity(1)).await.unwrap(), None, "a failed batch writes nothing");
    assert_eq!(reader.cell().await.unwrap(), cell);

    let mut batch = engine.batch(tree).await.unwrap();
    batch.put_row(folded_row(&key, entity(1), position)).await.unwrap();
    batch.set_folded(stable).await.unwrap();
    let advanced = TreeCell { folded: stable, ..cell };
    assert_eq!(batch.commit(&cell).await.unwrap(), TreeBatchOutcome::Committed(advanced));
    assert_eq!(reader.cell().await.unwrap(), advanced);
    assert!(reader.row(&key, entity(1)).await.unwrap().is_some());

    let mut late = engine.batch(tree).await.unwrap();
    late.delete_row(&key, entity(1)).await.unwrap();
    assert_eq!(late.commit(&cell).await.unwrap(), TreeBatchOutcome::Conflict { observed: advanced }, "the cell moved since");
    assert!(reader.row(&key, entity(1)).await.unwrap().is_some());
}

/// Every position written into a tree belongs to its cell's incarnation, and
/// the fold position never moves back.
pub async fn positions_stay_in_the_tree_incarnation<E: TreeStorage + 'static>(engine: Arc<E>) {
    let tree = entity_id_tree(&*engine).await;
    let position = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let foreign = LogPosition::start(LogIncarnation::mint());
    let key = entity(1).to_bytes();
    let mismatch = |result: Result<(), TreeStorageError>| matches!(result, Err(TreeStorageError::IncarnationMismatch { .. }));
    let mut batch = engine.batch(tree).await.unwrap();
    assert!(mismatch(batch.put_row(folded_row(&key, entity(1), foreign)).await));
    assert!(mismatch(batch.put_tombstone(Tombstone { key: key.to_vec(), entity_id: entity(1), position: foreign }).await));
    assert!(mismatch(batch.put_node(NodePrefix::root(), node_row(1, foreign)).await));
    assert!(mismatch(batch.prune_tombstones(foreign).await));
    assert!(mismatch(batch.set_folded(foreign).await));
    assert!(mismatch(batch.restart(foreign).await));
    batch.set_folded(position.next()).await.unwrap();
    assert!(matches!(batch.set_folded(position).await, Err(TreeStorageError::FoldBackwards { .. })));
}

/// The lookup from an entity to its keys finds every folded row of the entity,
/// follows a move, and forgets removed rows.
pub async fn entity_to_keys_lookup<E: TreeStorage + 'static>(engine: Arc<E>) {
    let tree = engine.register_tree(title_index()).await.unwrap().id;
    let position = create(&*engine, vec![state(entity(9), 9, &[], &[])]).await;
    let row = |key: &[u8], id: EntityId| folded_row(key, id, position);
    let (moved, other) = (entity(1), entity(2));
    let reader = engine.reader(tree).await.unwrap();

    let cell = reader.cell().await.unwrap();
    let mut batch = engine.batch(tree).await.unwrap();
    for (key, id) in [(b"k1", moved), (b"k2", moved), (b"k1", other)] {
        batch.put_row(row(key, id)).await.unwrap();
    }
    let cell = committed(batch.commit(&cell).await.unwrap());
    assert_eq!(reader.entity_rows(moved).await.unwrap(), [row(b"k1", moved), row(b"k2", moved)], "two keys, two rows");
    assert_eq!(reader.entity_rows(other).await.unwrap(), [row(b"k1", other)]);

    // Rewriting a row keeps one entry for its key; a move replaces the key.
    let rewritten = FoldedRow { head: head(7), ..row(b"k1", moved) };
    let mut batch = engine.batch(tree).await.unwrap();
    batch.put_row(rewritten.clone()).await.unwrap();
    batch.delete_row(b"k2", moved).await.unwrap();
    batch.put_row(row(b"k3", moved)).await.unwrap();
    let cell = committed(batch.commit(&cell).await.unwrap());
    assert_eq!(reader.entity_rows(moved).await.unwrap(), [rewritten, row(b"k3", moved)]);
    assert_eq!(reader.row(b"k2", moved).await.unwrap(), None);

    let mut batch = engine.batch(tree).await.unwrap();
    batch.delete_row(b"k1", moved).await.unwrap();
    batch.delete_row(b"k3", moved).await.unwrap();
    committed(batch.commit(&cell).await.unwrap());
    assert!(reader.entity_rows(moved).await.unwrap().is_empty());
    assert_eq!(reader.entity_rows(other).await.unwrap(), [row(b"k1", other)], "another entity under the same key stays");
}

/// Tombstones keep their addresses through a split and a merge, so each
/// child's range holds exactly the tombstones beneath it; pruning removes
/// those below the horizon it raises.
pub async fn tombstones_through_a_split<E: TreeStorage + 'static>(engine: Arc<E>) {
    let tree = entity_id_tree(&*engine).await;
    let mut positions = Vec::new();
    for n in 1..=3 {
        positions.push(create(&*engine, vec![state(entity(100 + n), n, &[], &[])]).await);
    }
    let latest = positions[2];
    // Removals spread over the sixteen children of the bucket 0101.
    let bucket = NodePrefix::of(&[0x50], 4);
    let tombstones: Vec<Tombstone> = (0..32u8)
        .map(|n| Tombstone { key: vec![0x50 | (n % 16), n], entity_id: entity(n), position: positions[usize::from(n % 3)] })
        .collect();
    let reader = engine.reader(tree).await.unwrap();

    let cell = reader.cell().await.unwrap();
    let mut batch = engine.batch(tree).await.unwrap();
    batch.put_node(bucket.clone(), node_row(0, latest)).await.unwrap();
    for tombstone in &tombstones {
        batch.put_tombstone(tombstone.clone()).await.unwrap();
    }
    let cell = committed(batch.commit(&cell).await.unwrap());

    let children: Vec<NodePrefix> = (0..16u8).map(|digit| NodePrefix::of(&[0x50 | digit], 8)).collect();
    let mut batch = engine.batch(tree).await.unwrap();
    for child in &children {
        batch.put_node(child.clone(), node_row(0, latest)).await.unwrap();
    }
    let cell = committed(batch.commit(&cell).await.unwrap());
    assert_eq!(prefixes(reader.children(&bucket).await.unwrap()), children);
    let mut seen = 0;
    for child in &children {
        let beneath = reader.tombstones(&AddressRange::under(child), usize::MAX).await.unwrap();
        assert_eq!(beneath, by_address(tombstones.iter().filter(|tombstone| child.contains_address(&tombstone.address())).cloned()));
        seen += beneath.len();
    }
    assert_eq!(seen, tombstones.len(), "the children's ranges partition the bucket's tombstones");

    let mut batch = engine.batch(tree).await.unwrap();
    for child in &children {
        batch.delete_node(child).await.unwrap();
    }
    let cell = committed(batch.commit(&cell).await.unwrap());
    assert!(reader.children(&bucket).await.unwrap().is_empty());
    assert_eq!(reader.tombstones(&AddressRange::under(&bucket), usize::MAX).await.unwrap(), by_address(tombstones.iter().cloned()));

    let mut batch = engine.batch(tree).await.unwrap();
    batch.prune_tombstones(latest).await.unwrap();
    committed(batch.commit(&cell).await.unwrap());
    let kept = reader.tombstones(&AddressRange::all(), usize::MAX).await.unwrap();
    assert_eq!(kept, by_address(tombstones.iter().filter(|tombstone| tombstone.position == latest).cloned()));
    assert!(engine.prune_horizon().await.unwrap() >= latest);
}

/// Reads through a batch see the batch's own writes, a restart included, and a
/// batch dropped without committing leaves the tree as it was.
pub async fn snapshot_reads_inside_a_batch<E: TreeStorage + 'static>(engine: Arc<E>) {
    let tree = engine.register_tree(title_index()).await.unwrap().id;
    let position = create(&*engine, vec![state(entity(9), 9, &[], &[])]).await;
    let stable = engine.stable_position().await.unwrap();
    let kept = (folded_row(b"a", entity(3), position), Tombstone { key: b"b".to_vec(), entity_id: entity(4), position });
    let kept_node = (NodePrefix::of(b"a", 8), node_row(1, position));
    let cell = cell_of(&*engine, tree).await;
    let mut setup = engine.batch(tree).await.unwrap();
    setup.put_row(kept.0.clone()).await.unwrap();
    setup.put_tombstone(kept.1.clone()).await.unwrap();
    setup.put_node(kept_node.0.clone(), kept_node.1.clone()).await.unwrap();
    let cell = committed(setup.commit(&cell).await.unwrap());
    let horizon = engine.prune_horizon().await.unwrap();

    let row = folded_row(b"k", entity(1), position);
    let tombstone = Tombstone { key: b"j".to_vec(), entity_id: entity(2), position };
    let node = (NodePrefix::of(b"k", 8), node_row(2, position));
    let mut batch = engine.batch(tree).await.unwrap();
    batch.put_row(row.clone()).await.unwrap();
    batch.put_tombstone(tombstone.clone()).await.unwrap();
    batch.put_node(node.0.clone(), node.1.clone()).await.unwrap();
    batch.set_folded(stable).await.unwrap();
    assert_eq!(batch.row(b"k", entity(1)).await.unwrap(), Some(row.clone()));
    assert_eq!(batch.entity_rows(entity(1)).await.unwrap(), std::slice::from_ref(&row));
    assert_eq!(batch.rows(&AddressRange::all(), 10).await.unwrap(), [kept.0.clone(), row]);
    assert_eq!(batch.tombstones(&AddressRange::all(), 10).await.unwrap(), [kept.1.clone(), tombstone]);
    assert_eq!(batch.node(&node.0).await.unwrap(), Some(node.1.clone()));
    assert_eq!(batch.children(&NodePrefix::root()).await.unwrap(), [kept_node.clone(), node.clone()]);
    assert_eq!(batch.cell().await.unwrap(), TreeCell { folded: stable, ..cell });
    batch.delete_row(b"k", entity(1)).await.unwrap();
    assert_eq!(batch.row(b"k", entity(1)).await.unwrap(), None);
    assert!(batch.entity_rows(entity(1)).await.unwrap().is_empty());
    batch.restart(stable).await.unwrap();
    assert!(batch.rows(&AddressRange::all(), 10).await.unwrap().is_empty());
    assert!(batch.tombstones(&AddressRange::all(), 10).await.unwrap().is_empty());
    assert!(batch.children(&NodePrefix::root()).await.unwrap().is_empty());
    assert_eq!(batch.cell().await.unwrap(), TreeCell { generation: cell.generation + 1, status: BuildStatus::Building, folded: stable });
    drop(batch);

    let reader = engine.reader(tree).await.unwrap();
    assert_eq!(reader.cell().await.unwrap(), cell, "a dropped batch changes nothing");
    assert_eq!(reader.rows(&AddressRange::all(), 10).await.unwrap(), [kept.0]);
    assert_eq!(reader.tombstones(&AddressRange::all(), 10).await.unwrap(), [kept.1]);
    assert_eq!(reader.children(&NodePrefix::root()).await.unwrap(), [kept_node]);
    assert_eq!(engine.prune_horizon().await.unwrap(), horizon);
}

/// Of two batches prepared against the same cell exactly one commits, and a
/// batch's reads stay as they were while the other tries to commit.
pub async fn concurrent_batches_commit_one<E: TreeStorage + 'static>(engine: Arc<E>) {
    let tree = entity_id_tree(&*engine).await;
    let position = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let key = entity(1).to_bytes().to_vec();
    let version = move |n: u8| FoldedRow { head: head(n), ..folded_row(&entity(1).to_bytes(), entity(1), position) };
    let cell = cell_of(&*engine, tree).await;
    let mut setup = engine.batch(tree).await.unwrap();
    setup.put_row(version(1)).await.unwrap();
    setup.put_node(NodePrefix::root(), node_row(1, position)).await.unwrap();
    let cell = committed(setup.commit(&cell).await.unwrap());
    let stable = engine.stable_position().await.unwrap();

    let mut first = engine.batch(tree).await.unwrap();
    let before = (first.row(&key, entity(1)).await.unwrap(), first.node(&NodePrefix::root()).await.unwrap());
    let rival = tokio::spawn({
        let engine = engine.clone();
        async move {
            let mut second = engine.batch(tree).await.unwrap();
            second.put_row(version(2)).await.unwrap();
            second.put_node(NodePrefix::root(), node_row(2, position)).await.unwrap();
            second.set_folded(stable).await.unwrap();
            second.commit(&cell).await.unwrap()
        }
    });
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    let after = (first.row(&key, entity(1)).await.unwrap(), first.node(&NodePrefix::root()).await.unwrap());
    assert_eq!(before, after, "a batch's reads do not move while another batch commits");
    first.put_row(version(3)).await.unwrap();
    first.set_folded(stable).await.unwrap();
    let first = first.commit(&cell).await.unwrap();
    let second = rival.await.unwrap();

    let winners = [&first, &second].into_iter().filter(|outcome| matches!(outcome, TreeBatchOutcome::Committed(_))).count();
    assert_eq!(winners, 1, "exactly one of two batches against the same cell commits");
    let winner = if matches!(first, TreeBatchOutcome::Committed(_)) { version(3) } else { version(2) };
    assert_eq!(engine.reader(tree).await.unwrap().row(&key, entity(1)).await.unwrap(), Some(winner));
}

/// A prefix's children are the nearest stored nodes beneath it at any depth,
/// and its address range holds exactly the leaves beneath it.
pub async fn children_and_leaf_ranges_at_ragged_depths<E: TreeStorage + 'static>(engine: Arc<E>) {
    let tree = engine.register_tree(title_index()).await.unwrap().id;
    let position = create(&*engine, vec![state(entity(200), 9, &[], &[])]).await;
    let firsts = [0x00, 0x01, 0x0F, 0x10, 0x80, 0xA0, 0xAA, 0xAB, 0xFF];
    let keys = firsts.into_iter().flat_map(|first| [[first, 0x00], [first, 0xAA], [first, 0xFF]]);
    let mut rows: Vec<FoldedRow> = keys.zip(0u8..).map(|(key, n)| folded_row(&key, entity(n), position)).collect();
    // Beneath 264 zero bits then a one lies the 34-byte address of the key
    // 00 00 and an id ending in 80, but not the 33-byte address of the key 00
    // and an id ending in 01, which a carry past whole bytes once admitted.
    let id_ending_in = |last: u8| EntityId::from_bytes(std::array::from_fn(|index| if index == 31 { last } else { 0 }));
    let beneath = folded_row(&[0x00, 0x00], id_ending_in(0x80), position);
    let deep = NodePrefix::of(&beneath.address(), 265);
    rows.extend([beneath, folded_row(&[0x00], id_ending_in(0x01), position)]);
    let at = |address: [u8; 2], len: u32| NodePrefix::of(&address, len);
    // Ragged depths with path compression: no rows at 0000 0000 or 1010 1010.
    let nodes = [
        NodePrefix::root(),
        at([0x00, 0x00], 4),
        at([0x00, 0x00], 12),
        at([0x08, 0x00], 5),
        at([0xA0, 0x00], 4),
        at([0xAA, 0xA0], 13),
        at([0xFF, 0x00], 8),
    ];
    let reader = engine.reader(tree).await.unwrap();
    let cell = reader.cell().await.unwrap();
    let mut batch = engine.batch(tree).await.unwrap();
    for row in &rows {
        batch.put_row(row.clone()).await.unwrap();
    }
    for prefix in &nodes {
        batch.put_node(prefix.clone(), node_row(1, position)).await.unwrap();
    }
    committed(batch.commit(&cell).await.unwrap());
    let children = |prefix: NodePrefix| child_prefixes(&reader, prefix);
    assert_eq!(children(NodePrefix::root()).await, [at([0x00, 0x00], 4), at([0xA0, 0x00], 4), at([0xFF, 0x00], 8)]);
    assert_eq!(children(at([0x00, 0x00], 4)).await, [at([0x00, 0x00], 12), at([0x08, 0x00], 5)]);
    assert_eq!(children(at([0xA0, 0x00], 4)).await, [at([0xAA, 0xA0], 13)], "a child several levels down");
    assert_eq!(children(at([0xAA, 0x00], 8)).await, [at([0xAA, 0xA0], 13)], "a prefix with no row of its own");
    assert!(children(at([0xAA, 0xA0], 13)).await.is_empty());

    let mut probes = nodes.to_vec();
    probes.extend([at([0x01, 0x00], 8), at([0x80, 0x00], 1), at([0xAA, 0xA0], 11), at([0xAA, 0xFF], 16), at([0xAB, 0x00], 8), deep]);
    for prefix in &probes {
        let beneath = by_address(rows.iter().filter(|row| prefix.contains_address(&row.address())).cloned());
        assert_eq!(reader.rows(&AddressRange::under(prefix), usize::MAX).await.unwrap(), beneath, "{prefix:?}");
    }
    let keys = AddressRange::new(Bound::Included(vec![0x0F]), Bound::Excluded(vec![0xA0]));
    assert_eq!(reader.rows(&keys, usize::MAX).await.unwrap(), by_address(rows.iter().filter(|row| keys.contains(&row.address())).cloned()));

    // The forward scan, three rows a page.
    let (mut scanned, mut range) = (Vec::<FoldedRow>::new(), AddressRange::all());
    loop {
        let page = reader.rows(&range, 3).await.unwrap();
        let Some(last) = page.last() else { break };
        assert!(page.len() <= 3, "a page holds at most its limit");
        assert!(scanned.last().is_none_or(|previous| previous.address() < page[0].address()), "each page continues after the last");
        range = range.after(&last.address());
        scanned.extend(page);
    }
    assert_eq!(scanned, by_address(rows.iter().cloned()));
}

/// A new registration builds at generation 0 from the stable position;
/// publishing makes it ready; a restart starts the next generation with no
/// rows, and fails a batch from the old generation even at the same fold
/// position; tree ids are not reused.
pub async fn build_state_and_generation<E: TreeStorage + 'static>(engine: Arc<E>) {
    create(&*engine, vec![state(entity(9), 9, &[], &[])]).await;
    let registration = engine.register_tree(title_index()).await.unwrap();
    let registered = engine.stable_position().await.unwrap();
    assert_eq!(engine.register_tree(title_index()).await.unwrap(), registration, "registration is keyed by the index");
    let building = cell_of(&*engine, registration.id).await;
    assert_eq!(building, TreeCell { generation: 0, status: BuildStatus::Building, folded: registered });

    let position = create(&*engine, vec![state(entity(1), 1, &[component()], &[(title(), text("a"))])]).await;
    let stable = engine.stable_position().await.unwrap();
    let mut batch = engine.batch(registration.id).await.unwrap();
    batch.put_row(folded_row(b"old", entity(1), position)).await.unwrap();
    batch.set_folded(stable).await.unwrap();
    batch.publish().await.unwrap();
    let ready = committed(batch.commit(&building).await.unwrap());
    assert_eq!(ready, TreeCell { generation: 0, status: BuildStatus::Ready, folded: stable });

    let mut batch = engine.batch(registration.id).await.unwrap();
    batch.restart(stable).await.unwrap();
    batch.put_row(folded_row(b"new", entity(1), position)).await.unwrap();
    let rebuilt = committed(batch.commit(&ready).await.unwrap());
    assert_eq!(rebuilt, TreeCell { generation: 1, status: BuildStatus::Building, folded: stable });
    let reader = engine.reader(registration.id).await.unwrap();
    assert_eq!(
        reader.entity_rows(entity(1)).await.unwrap(),
        [folded_row(b"new", entity(1), position)],
        "the old generation's rows are gone"
    );

    let mut stale = engine.batch(registration.id).await.unwrap();
    stale.put_row(folded_row(b"stale", entity(2), position)).await.unwrap();
    assert_eq!(stale.commit(&ready).await.unwrap(), TreeBatchOutcome::Conflict { observed: rebuilt }, "same position, older generation");

    engine.unregister_tree(registration.id).await.unwrap();
    assert!(matches!(engine.reader(registration.id).await, Err(TreeStorageError::UnknownTree(_))));
    assert!(matches!(engine.batch(registration.id).await, Err(TreeStorageError::UnknownTree(_))));
    let again = engine.register_tree(title_index()).await.unwrap();
    assert_ne!(again.id, registration.id, "a tree id is not reused within an incarnation");
    let stable = engine.stable_position().await.unwrap();
    assert_eq!(cell_of(&*engine, again.id).await, TreeCell { generation: 0, status: BuildStatus::Building, folded: stable });
}

/// The prune horizon rises wherever a tree may stop holding a removal: at a
/// registration, a prune and a restart. It never falls.
pub async fn prune_horizon_rises_with_every_lost_removal<E: TreeStorage + 'static>(engine: Arc<E>) {
    create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let tree = engine.register_tree(title_index()).await.unwrap().id;
    let registered = cell_of(&*engine, tree).await;
    assert_eq!(engine.prune_horizon().await.unwrap(), registered.folded, "a new tree saw no removal before it");

    create(&*engine, vec![state(entity(2), 2, &[], &[])]).await;
    let pruned = engine.stable_position().await.unwrap();
    let mut batch = engine.batch(tree).await.unwrap();
    batch.prune_tombstones(pruned).await.unwrap();
    let cell = committed(batch.commit(&registered).await.unwrap());
    assert_eq!(engine.prune_horizon().await.unwrap(), pruned);

    let mut batch = engine.batch(tree).await.unwrap();
    batch.prune_tombstones(registered.folded).await.unwrap();
    let cell = committed(batch.commit(&cell).await.unwrap());
    assert_eq!(engine.prune_horizon().await.unwrap(), pruned, "the horizon never falls");

    create(&*engine, vec![state(entity(3), 3, &[], &[])]).await;
    let restarted = engine.stable_position().await.unwrap();
    let mut batch = engine.batch(tree).await.unwrap();
    batch.restart(restarted).await.unwrap();
    committed(batch.commit(&cell).await.unwrap());
    assert_eq!(engine.prune_horizon().await.unwrap(), restarted, "a rebuilt tree saw no removal before its start");
}

fn entity(n: u8) -> EntityId { EntityId::from_bytes([n; 32]) }

fn head(n: u8) -> Clock { Clock::from(EventId::from_bytes([n; 32])) }

fn text(value: &str) -> Value { Value::String(value.to_owned()) }

fn component() -> ModelId { ModelId::EntityId(entity(240)) }

fn title() -> PropertyId { PropertyId::EntityId(entity(241)) }

fn title_key_spec() -> KeySpec<PropertyId> { KeySpec::new(vec![IndexKeyPart::asc(title(), ValueType::String)]) }

/// The component's members, filed by title.
fn title_index() -> HashedIndex { HashedIndex::Component { component: component(), key_spec: title_key_spec() } }

/// An entity's state at the head `head(n)`, with these memberships and these
/// property values written by that head's event.
fn state(entity_id: EntityId, n: u8, memberships: &[ModelId], values: &[(PropertyId, Value)]) -> Attested<EntityState> {
    let mut state_buffers = BTreeMap::new();
    if !values.is_empty() {
        let backend = LWWBackend::new();
        for (property, value) in values {
            backend.set(*property, Some(value.clone()));
        }
        let operations = backend.to_operations().unwrap().expect("values were set");
        backend.apply_operations_with_event(&operations, EventId::from_bytes([n; 32])).unwrap();
        state_buffers.insert(LWWBackend::property_backend_name().to_owned(), backend.to_state_buffer().unwrap());
    }
    let state = State { state_buffers: StateBuffers(state_buffers), memberships: memberships.iter().copied().collect(), head: head(n) };
    Attested::opt(EntityState { entity_id, state }, None)
}

/// Commit these states as one transaction, each expected to follow its head.
async fn commit_states<E: StorageEngine>(engine: &E, writes: Vec<(Clock, Attested<EntityState>)>) -> StorageCommitOutcome {
    let mut transaction = engine.transaction();
    for (expected, state) in &writes {
        transaction.set_state(expected, state).await.unwrap();
    }
    transaction.commit().await.unwrap()
}

/// Commit new entities as one transaction and return its position.
async fn create<E: StorageEngine>(engine: &E, states: Vec<Attested<EntityState>>) -> LogPosition {
    let outcome = commit_states(engine, states.into_iter().map(|state| (Clock::default(), state)).collect()).await;
    outcome.committed().unwrap().position.expect("a commit that sets states is logged")
}

async fn read_all<E: CommitLog>(engine: &E, from: LogPosition) -> Vec<LogRow> { engine.read_log(from, usize::MAX).await.unwrap().rows }

async fn entity_id_tree<E: TreeStorage>(engine: &E) -> TreeId {
    let trees = engine.trees().await.unwrap();
    trees.into_iter().find(|tree| tree.index == HashedIndex::EntityId).expect("every store keeps an entity-id tree").id
}

async fn cell_of<E: TreeStorage>(engine: &E, tree: TreeId) -> TreeCell { engine.reader(tree).await.unwrap().cell().await.unwrap() }

fn committed(outcome: TreeBatchOutcome) -> TreeCell {
    match outcome {
        TreeBatchOutcome::Committed(cell) => cell,
        TreeBatchOutcome::Conflict { observed } => panic!("the batch conflicted with {observed:?}"),
    }
}

fn folded_row(key: &[u8], entity_id: EntityId, position: LogPosition) -> FoldedRow {
    FoldedRow { key: key.to_vec(), entity_id, head: head(1), point: vec![0xA5; 33], position }
}

fn node_row(count: u64, watermark: LogPosition) -> NodeRow { NodeRow { digest: vec![count as u8; 41], count, watermark } }

fn prefixes(children: Vec<(NodePrefix, NodeRow)>) -> Vec<NodePrefix> { children.into_iter().map(|(prefix, _)| prefix).collect() }

async fn child_prefixes<R: TreeRead>(reader: &R, prefix: NodePrefix) -> Vec<NodePrefix> {
    prefixes(reader.children(&prefix).await.unwrap())
}

/// Folded rows or tombstones in address order.
fn by_address<T: Addressed>(items: impl Iterator<Item = T>) -> Vec<T> {
    let mut items: Vec<T> = items.collect();
    items.sort_by_cached_key(Addressed::address);
    items
}

trait Addressed {
    fn address(&self) -> Vec<u8>;
}

impl Addressed for FoldedRow {
    fn address(&self) -> Vec<u8> { FoldedRow::address(self) }
}

impl Addressed for Tombstone {
    fn address(&self) -> Vec<u8> { Tombstone::address(self) }
}
