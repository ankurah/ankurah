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
//! An engine whose store outlives its process also implements [`Reopen`] and
//! adds the cases that crash and reopen the store:
//!
//! ```ignore
//! mod tree_storage {
//!     ankurah_core::tree_storage_conformance!(MyEngine::open_temporary().await.unwrap(), reopen: MyReopen::temporary());
//! }
//! ```
//!
//! An engine fixture may also state how its batches on one tree meet, as a
//! [`BatchConcurrency`] after `batches:` (and after `reopen:`, when both are
//! given), so that the cases racing batches order their contenders on purpose
//! rather than by timing.
//!
//! The cases assume nothing an engine may choose: positions may have gaps,
//! batches may lock or detect conflicts optimistically, and tree ids may be
//! any numbers.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    ops::Bound,
    sync::Arc,
    time::Duration,
};

use ankql::ast::PropertyId;
use ankurah_proto::{Attested, AuthorId, Clock, EntityId, EntityState, Event, EventId, ModelId, OperationSet, State, StateBuffers};
use async_trait::async_trait;
use futures::StreamExt;
use tokio::sync::{
    oneshot::{self, error::TryRecvError},
    Barrier,
};

use super::{
    log::{CommitLog, LogError, LogIncarnation, LogPage, LogPosition, LogRow},
    tree::{
        AddressRange, BuildStatus, FoldedRow, HashedIndex, NodePrefix, NodeRow, SnapshotEntity, Tombstone, TreeBatch, TreeBatchOutcome,
        TreeCell, TreeId, TreeOptions, TreeRead, TreeRegistration, TreeStorage, TreeStorageError,
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

/// How long a case lets an operation run before taking it to be waiting for
/// another, as an engine that locks may make it.
const BLOCKED: Duration = Duration::from_millis(100);

/// Expand to one `#[tokio::test]` per conformance case, each running against
/// a fresh engine that `$make` builds; `$make` may `.await`. Given `reopen:`,
/// also one per case that crashes and reopens a store, each with a fresh
/// [`Reopen`](crate::storage::conformance::Reopen) that `$reopen` builds.
/// Given `batches:`, the cases racing batches order their contenders by the
/// [`BatchConcurrency`](crate::storage::conformance::BatchConcurrency) it
/// states. The calling crate needs tokio with its `macros` and `rt` features.
#[macro_export]
macro_rules! tree_storage_conformance {
    ($make:expr $(, reopen: $reopen:expr)? $(, batches: $batches:expr)? $(,)?) => {
        $crate::tree_storage_conformance!(@racing ($make)
            (::core::option::Option::<$crate::storage::conformance::BatchConcurrency>::None
                $(.or(::core::option::Option::Some($batches)))?)
            concurrent_batches_commit_one
            one_of_several_contenders_commits
        );
        $(
            $crate::tree_storage_conformance!(@reopen ($reopen)
                reopen_keeps_what_was_durable
                reopen_keeps_tree_batches_whole
                reopen_restarts_trees_past_the_durable_position
                reopen_after_a_reset_keeps_the_new_incarnation
            );
        )?
        $crate::tree_storage_conformance!(@cases ($make)
            fresh_store_keeps_an_entity_id_tree
            commits_take_increasing_positions
            multi_entity_commit_shares_one_position
            state_only_commits_are_logged
            log_rows_carry_keys_under_every_tree
            key_changes_are_logged_with_or_without_a_new_head
            a_failed_derivation_writes_nothing
            log_reads_end_at_whole_positions
            log_pages_at_their_limits
            durable_position_trails_the_stable_position
            retention_floor_bounds_reads
            reset_mints_a_new_incarnation
            conditional_position_check
            positions_stay_in_the_tree_incarnation
            entity_to_keys_lookup
            tombstones_through_a_split
            snapshot_reads_inside_a_batch
            trees_do_not_wait_for_each_other
            readers_never_see_an_open_batch
            a_cancelled_wait_leaves_no_trace
            handles_do_not_outlive_their_tree
            children_and_leaf_ranges_at_ragged_depths
            large_scans_continue_exactly
            empty_trees_read_empty
            build_state_and_generation
            an_index_that_opts_out_keeps_no_tree
            a_status_only_cell_change_conflicts
            removal_is_whole_when_cancelled
            snapshot_and_replay_give_the_current_index
            a_build_overtaken_by_retention_starts_over
            a_tree_is_ready_only_below_the_durable_position
            prune_horizon_rises_with_every_lost_removal
        );
    };
    (@cases ($make:expr) $($case:ident)*) => {
        $(
            #[tokio::test]
            async fn $case() { $crate::storage::conformance::$case(::std::sync::Arc::new($make)).await; }
        )*
    };
    (@racing ($make:expr) ($batches:expr) $($case:ident)*) => {
        $(
            #[tokio::test]
            async fn $case() { $crate::storage::conformance::$case(::std::sync::Arc::new($make), $batches).await; }
        )*
    };
    (@reopen ($reopen:expr) $($case:ident)*) => {
        $(
            #[tokio::test]
            async fn $case() { $crate::storage::conformance::$case($reopen).await; }
        )*
    };
}

/// How a persistent engine's store is opened, and opened again after a crash,
/// for the cases that check what a crash keeps. The in-memory engine keeps
/// nothing past its process and implements none of this.
#[async_trait]
pub trait Reopen: Send + Sync {
    type Engine: TreeStorage + 'static;

    /// Open a new, empty store.
    async fn open(&self) -> Arc<Self::Engine>;

    /// Abandon `engine` as a crash would, closing nothing cleanly, and open
    /// the same store again.
    async fn crash_and_reopen(&self, engine: Arc<Self::Engine>) -> Arc<Self::Engine>;
}

/// How an engine's batches on one tree meet, as an engine fixture may state
/// it. The cases racing batches then order their contenders' phases (begun,
/// about to commit) on purpose; without it they assume nothing about overlap
/// and check exclusion, the final tree and progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchConcurrency {
    /// A batch waits to begin while another batch on its tree is open.
    Waits,
    /// Batches on one tree run side by side, and of those committed against
    /// one cell all but the first conflict.
    SideBySide,
}

/// A new store keeps a ready entity-id tree folded from the start of its log,
/// registered under that index and permanent, and no removal predates it.
pub async fn fresh_store_keeps_an_entity_id_tree<E: TreeStorage + 'static>(engine: Arc<E>) {
    let trees = engine.trees().await.unwrap();
    assert_eq!(trees.len(), 1, "a new store has exactly the entity-id tree");
    assert_eq!(trees[0].index, HashedIndex::EntityId);
    let start = LogPosition::start(engine.stable_position().await.unwrap().incarnation());
    assert_eq!(cell_of(&*engine, trees[0].id).await, TreeCell { generation: 0, status: BuildStatus::Ready, folded: start });
    assert_eq!(register(&*engine, HashedIndex::EntityId).await, trees[0], "registration is keyed by the index");
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
    let tree = register(&*engine, title_index()).await.id;
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

/// A commit that changes an entity's keys is logged with the new keys whether
/// or not it moves the entity's head, and one that takes the entity out of the
/// component or back into it logs the empty set or the key it regains.
pub async fn key_changes_are_logged_with_or_without_a_new_head<E: TreeStorage + 'static>(engine: Arc<E>) {
    let tree = register(&*engine, title_index()).await.id;
    let titled = |n: u8, memberships: &[ModelId], value: &str| state(entity(1), n, memberships, &[(title(), text(value))]);
    create(&*engine, vec![titled(1, &[component()], "a")]).await;
    let logged = |outcome: StorageCommitOutcome| outcome.committed().unwrap().position.expect("a commit that sets a state is logged");
    let rows_at = |position: LogPosition| {
        let engine = engine.clone();
        async move {
            let rows = read_all(&*engine, position).await;
            rows.into_iter().filter(|row| row.position == position).map(|row| (row.head, row.keys[&tree].clone())).collect::<Vec<_>>()
        }
    };

    let same_head = logged(commit_states(&*engine, vec![(head(1), titled(1, &[component()], "b"))]).await);
    assert_eq!(rows_at(same_head).await, [(head(1), BTreeSet::from([title_key("b")]))], "a new key under the same head is logged");
    let left = logged(commit_states(&*engine, vec![(head(1), titled(2, &[], "b"))]).await);
    assert_eq!(rows_at(left).await, [(head(2), BTreeSet::new())], "leaving the component files the entity under no key");
    let rejoined = logged(commit_states(&*engine, vec![(head(2), titled(3, &[component()], "c"))]).await);
    assert_eq!(rows_at(rejoined).await, [(head(3), BTreeSet::from([title_key("c")]))], "rejoining files it under its title");
}

/// A transaction in which one entity's keys do not derive fails whole, though
/// another entity's state was prepared before it: no state, event or log row
/// is written, and the store takes the next commit.
pub async fn a_failed_derivation_writes_nothing<E: TreeStorage + 'static>(engine: Arc<E>) {
    register(&*engine, rank_index()).await;
    let start = engine.stable_position().await.unwrap();
    let event = Event::update(entity(1), head(1), AuthorId::Unknown, OperationSet(Vec::new()));
    let mut transaction = engine.transaction();
    transaction.add_events(&[Attested::opt(event.clone(), None)]).await.unwrap();
    transaction.set_state(&Clock::default(), &state(entity(1), 1, &[component()], &[(rank(), Value::I64(1))])).await.unwrap();
    // A rank that is no number cannot take the index's integer key part. An
    // engine may refuse it as it is prepared or as the transaction commits.
    let failed = match transaction.set_state(&Clock::default(), &state(entity(2), 2, &[component()], &[(rank(), text("high"))])).await {
        Ok(()) => transaction.commit().await.map(drop),
        Err(error) => Err(error),
    };
    assert!(failed.is_err(), "the second entity's key does not derive");
    for id in [entity(1), entity(2)] {
        assert!(matches!(engine.get_state(id).await, Err(RetrievalError::EntityNotFound(_))), "no state of {id:?} was written");
    }
    assert!(engine.get_events(vec![event.id()]).await.unwrap().is_empty(), "no event was written");
    assert!(read_all(&*engine, start).await.is_empty(), "no log row was written");
    let position = create(&*engine, vec![state(entity(1), 1, &[component()], &[(rank(), Value::I64(1))])]).await;
    assert_eq!(
        read_all(&*engine, start).await.iter().map(|row| (row.position, row.entity_id)).collect::<Vec<_>>(),
        [(position, entity(1))]
    );
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

/// Log pages at their limits: an empty log reads nothing; a limit of zero or
/// one reads one whole position; a page ends after the position that reaches
/// its limit; a commit larger than the limit is read whole; a read from inside
/// a long gap of aborted commits crosses it; reads at and beyond the stable
/// position return nothing without moving back; and paging one row at a time
/// reads every row once.
pub async fn log_pages_at_their_limits<E: TreeStorage + 'static>(engine: Arc<E>) {
    let start = engine.stable_position().await.unwrap();
    assert_eq!(engine.read_log(start, 10).await.unwrap(), LogPage { rows: vec![], next: start }, "an empty log reads nothing");
    let created = |range: std::ops::RangeInclusive<u8>| range.map(|n| state(entity(n), n, &[], &[])).collect::<Vec<_>>();
    let first = create(&*engine, created(1..=2)).await;
    let second = create(&*engine, created(3..=5)).await;
    for _ in 0..40 {
        let aborted = commit_states(&*engine, vec![(head(99), state(entity(1), 50, &[], &[]))]).await;
        assert!(matches!(aborted, StorageCommitOutcome::Conflict { .. }));
    }
    let third = create(&*engine, created(6..=6)).await;
    let stable = engine.stable_position().await.unwrap();
    let positions = |page: &LogPage| page.rows.iter().map(|row| row.position).collect::<Vec<_>>();

    for limit in [0, 1, 2] {
        let page = engine.read_log(start, limit).await.unwrap();
        assert_eq!(positions(&page), [first, first], "a limit of {limit} reads the first position whole");
        assert!(page.next > first && page.next <= second);
    }
    for limit in [3, 5] {
        let page = engine.read_log(start, limit).await.unwrap();
        assert_eq!(positions(&page), [first, first, second, second, second], "a limit of {limit} ends after the second position");
        assert!(page.next > second && page.next <= third);
    }
    for limit in [6, 7, usize::MAX] {
        let page = engine.read_log(start, limit).await.unwrap();
        assert_eq!(positions(&page), [first, first, second, second, second, third], "a limit of {limit} reads everything");
        assert_eq!(page.next, stable);
    }
    let crossing = engine.read_log(second.next(), 1).await.unwrap();
    assert_eq!(positions(&crossing), [third], "a read from inside the gap crosses it");
    assert!(crossing.next > third);
    assert_eq!(engine.read_log(stable, 10).await.unwrap(), LogPage { rows: vec![], next: stable });
    let beyond = LogPosition::new(stable.incarnation(), stable.offset() + 5);
    assert_eq!(engine.read_log(beyond, 10).await.unwrap(), LogPage { rows: vec![], next: beyond }, "a read never moves back");

    let large = create(&*engine, (0..300u16).map(|n| state(numbered(n), 7, &[], &[])).collect()).await;
    let page = engine.read_log(stable, 1).await.unwrap();
    assert_eq!(page.rows.len(), 300, "a commit larger than the limit is read whole");
    assert!(page.rows.iter().all(|row| row.position == large) && page.next > large);
    let (mut paged, mut from) = (Vec::new(), start);
    loop {
        let page = engine.read_log(from, 1).await.unwrap();
        if page.rows.is_empty() {
            assert_eq!(page.next, engine.stable_position().await.unwrap(), "a page with no rows ends at the stable position");
            break;
        }
        assert!(page.next > from, "a page with rows moves forward");
        paged.extend(page.rows);
        from = page.next;
    }
    assert_eq!(paged, read_all(&*engine, start).await, "paging reads every row once, in order");
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
    wait_until_durable(&*engine, last.next()).await;
}

/// Wait until the durable position reaches `position`, and return it.
async fn wait_until_durable<E: CommitLog>(engine: &E, position: LogPosition) -> LogPosition {
    let deadline = tokio::time::Instant::now() + PROGRESS;
    loop {
        let durable = engine.durable_position().await.unwrap();
        if durable >= position {
            return durable;
        }
        assert!(tokio::time::Instant::now() < deadline, "the durable position never reached {position:?}");
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
    register(&*engine, title_index()).await;
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
    assert_eq!(register(&*engine, HashedIndex::EntityId).await, trees[0], "registration is keyed by the index");
    assert!(matches!(engine.unregister_tree(trees[0].id).await, Err(TreeStorageError::PermanentTree)), "the fresh tree is permanent");
    assert_eq!(engine.prune_horizon().await.unwrap(), start);
    assert!(matches!(engine.get_state(entity(1)).await, Err(RetrievalError::EntityNotFound(_))));
}

/// A batch commits only while the tree's cell equals the one the caller
/// expected; otherwise none of its writes applies, the prune horizon
/// included, and the caller learns the cell.
pub async fn conditional_position_check<E: TreeStorage + 'static>(engine: Arc<E>) {
    let tree = entity_id_tree(&*engine).await;
    let position = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let stable = engine.stable_position().await.unwrap();
    // A tombstone the batches below prune.
    let mut setup = engine.batch(tree).await.unwrap();
    let cell = setup.cell().await.unwrap();
    setup.put_tombstone(Tombstone { key: b"older".to_vec(), entity_id: entity(3), position }).await.unwrap();
    assert_eq!(setup.commit(&cell).await.unwrap(), TreeBatchOutcome::Committed(cell));
    let probes = [entity(1), entity(2), entity(3)];
    let before = view(&*engine, tree, &probes).await;
    let row = folded_row(&entity(1).to_bytes(), entity(1), position);
    let tombstone = Tombstone { key: b"left".to_vec(), entity_id: entity(2), position };
    let node = node_row(1, position);
    async fn write<B: TreeBatch>(batch: &mut B, stable: LogPosition, row: &FoldedRow, tombstone: &Tombstone, node: &NodeRow) {
        batch.prune_tombstones(stable).await.unwrap();
        batch.put_row(row.clone()).await.unwrap();
        batch.put_tombstone(tombstone.clone()).await.unwrap();
        batch.put_node(NodePrefix::root(), node.clone()).await.unwrap();
        batch.set_folded(stable).await.unwrap();
    }

    let mut batch = engine.batch(tree).await.unwrap();
    write(&mut batch, stable, &row, &tombstone, &node).await;
    let stale = TreeCell { generation: cell.generation + 1, ..cell };
    assert_eq!(batch.commit(&stale).await.unwrap(), TreeBatchOutcome::Conflict { observed: cell });
    assert_eq!(view(&*engine, tree, &probes).await, before, "a failed batch writes nothing");

    let mut batch = engine.batch(tree).await.unwrap();
    write(&mut batch, stable, &row, &tombstone, &node).await;
    let advanced = TreeCell { folded: stable, ..cell };
    assert_eq!(batch.commit(&cell).await.unwrap(), TreeBatchOutcome::Committed(advanced));
    let after = TreeView {
        cell: advanced,
        rows: vec![row.clone()],
        lookup: BTreeMap::from([(entity(1), vec![row]), (entity(2), vec![]), (entity(3), vec![])]),
        tombstones: vec![tombstone],
        nodes: vec![(NodePrefix::root(), node)],
        horizon: stable,
    };
    assert_eq!(view(&*engine, tree, &probes).await, after, "a batch that commits writes everything");

    let mut late = engine.batch(tree).await.unwrap();
    late.delete_row(&entity(1).to_bytes(), entity(1)).await.unwrap();
    late.delete_node(&NodePrefix::root()).await.unwrap();
    assert_eq!(late.commit(&cell).await.unwrap(), TreeBatchOutcome::Conflict { observed: advanced }, "the cell moved since");
    assert_eq!(view(&*engine, tree, &probes).await, after);
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
    let tree = register(&*engine, title_index()).await.id;
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
    let tree = register(&*engine, title_index()).await.id;
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
    let probes = [entity(1), entity(3)];
    let before = view(&*engine, tree, &probes).await;

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
    assert_eq!(view(&*engine, tree, &probes).await, before, "a dropped batch changes nothing");
}

/// Of two batches prepared against the same cell exactly one commits, whole,
/// and a batch's reads stay as they were while its rival tries to commit.
/// Where batches run side by side, the rival begins beside the open batch and
/// commits first, and the open batch then conflicts; where a batch waits, the
/// rival begins only once the open batch has ended, and conflicts.
pub async fn concurrent_batches_commit_one<E: TreeStorage + 'static>(engine: Arc<E>, batches: Option<BatchConcurrency>) {
    let tree = entity_id_tree(&*engine).await;
    let position = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let contest = Contest::set_up(&*engine, tree, position).await;
    let before = view(&*engine, tree, &contest.probes()).await;

    let mut first = engine.batch(tree).await.unwrap();
    let seen = batch_reads(&first).await;
    let (begun, mut rival_begun) = oneshot::channel();
    let mut rival = tokio::spawn(contest.contend(engine.clone(), 2, Phases { begun: Some(begun), ..Phases::default() }));
    let finished = match batches {
        Some(BatchConcurrency::SideBySide) => {
            tokio::time::timeout(PROGRESS, &mut rival_begun).await.expect("a batch begins beside an open one").unwrap();
            Some(tokio::time::timeout(PROGRESS, &mut rival).await.expect("the rival commits beside the open batch").unwrap())
        }
        Some(BatchConcurrency::Waits) => {
            // Let the rival run until it waits; whenever it runs, it must not
            // begin while this batch is open.
            tokio::task::yield_now().await;
            assert!(matches!(rival_begun.try_recv(), Err(TryRecvError::Empty)), "a batch does not begin while another on its tree is open");
            None
        }
        None => None,
    };
    assert_eq!(batch_reads(&first).await, seen, "a batch's reads do not move while its rival tries to commit");
    contest.write(&mut first, 3).await;
    let first = first.commit(&contest.cell).await.unwrap();
    let second = match finished {
        Some(outcome) => outcome,
        None => tokio::time::timeout(PROGRESS, rival).await.expect("the rival finishes once the batch ends").unwrap(),
    };
    let (winner, cell) = one_winner([(3, first), (2, second)]);
    match batches {
        Some(BatchConcurrency::SideBySide) => assert_eq!(winner, 2, "the rival that committed beside the open batch wins"),
        Some(BatchConcurrency::Waits) => assert_eq!(winner, 3, "the open batch commits before its rival begins"),
        None => {}
    }
    assert_eq!(view(&*engine, tree, &contest.probes()).await, contest.won_by(&before, winner, cell));
}

/// Of several batches prepared against one cell exactly one commits, whole;
/// every other conflicts with the cell it left, and all of them finish. Where
/// batches run side by side, every contender has begun before any writes, and
/// every one has written before any commits.
pub async fn one_of_several_contenders_commits<E: TreeStorage + 'static>(engine: Arc<E>, batches: Option<BatchConcurrency>) {
    let tree = entity_id_tree(&*engine).await;
    let position = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let contest = Contest::set_up(&*engine, tree, position).await;
    let before = view(&*engine, tree, &contest.probes()).await;
    let numbers = 2..=5u8;
    let phases = match batches {
        Some(BatchConcurrency::SideBySide) => {
            let together = || Some(Arc::new(Barrier::new(numbers.len())));
            Phases { all_begun: together(), all_committing: together(), ..Phases::default() }
        }
        Some(BatchConcurrency::Waits) | None => Phases::default(),
    };
    let contenders: Vec<_> = numbers.map(|n| (n, tokio::spawn(contest.contend(engine.clone(), n, phases.clone())))).collect();
    let mut outcomes = Vec::new();
    for (n, contender) in contenders {
        outcomes.push((n, tokio::time::timeout(PROGRESS, contender).await.expect("every contender finishes").unwrap()));
    }
    let (winner, cell) = one_winner(outcomes);
    assert_eq!(view(&*engine, tree, &contest.probes()).await, contest.won_by(&before, winner, cell));
}

/// Nothing waits for a batch on another tree: a task holds batches on two
/// trees at once, begun in ascending id order, and commits both; a read of
/// one tree goes on while a batch on the other is open; and contenders on two
/// trees at once settle each tree on its own.
pub async fn trees_do_not_wait_for_each_other<E: TreeStorage + 'static>(engine: Arc<E>) {
    let position = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let mut trees = [entity_id_tree(&*engine).await, register(&*engine, title_index()).await.id];
    trees.sort();
    let mut contests = [Contest::set_up(&*engine, trees[0], position).await, Contest::set_up(&*engine, trees[1], position).await];
    let mut views = [view(&*engine, trees[0], &contests[0].probes()).await, view(&*engine, trees[1], &contests[1].probes()).await];

    let mut low = tokio::time::timeout(PROGRESS, engine.batch(trees[0])).await.expect("no batch is open").unwrap();
    let read = async { engine.reader(trees[1]).await.unwrap().cell().await.unwrap() };
    assert_eq!(tokio::time::timeout(PROGRESS, read).await.expect("a read waits for no batch on another tree"), contests[1].cell);
    let mut high =
        tokio::time::timeout(PROGRESS, engine.batch(trees[1])).await.expect("a batch waits for no batch on another tree").unwrap();
    contests[0].write(&mut low, 2).await;
    contests[1].write(&mut high, 2).await;
    let cells = [committed(low.commit(&contests[0].cell).await.unwrap()), committed(high.commit(&contests[1].cell).await.unwrap())];
    // Each round's batches advance the fold position, without which batches
    // against one cell would not exclude each other.
    create(&*engine, vec![state(entity(2), 2, &[], &[])]).await;
    let stable = engine.stable_position().await.unwrap();
    for side in 0..2 {
        views[side] = contests[side].won_by(&views[side], 2, cells[side]);
        assert_eq!(view(&*engine, trees[side], &contests[side].probes()).await, views[side]);
        contests[side] = Contest { cell: cells[side], stable, ..contests[side] };
    }

    let contenders: Vec<_> = (0..2)
        .flat_map(|side| (3..=5).map(move |n| (side, n)))
        .map(|(side, n)| (side, n, tokio::spawn(contests[side].contend(engine.clone(), n, Phases::default()))))
        .collect();
    let mut outcomes = [Vec::new(), Vec::new()];
    for (side, n, contender) in contenders {
        outcomes[side].push((n, tokio::time::timeout(PROGRESS, contender).await.expect("every contender finishes").unwrap()));
    }
    for (side, outcomes) in outcomes.into_iter().enumerate() {
        let (winner, cell) = one_winner(outcomes);
        assert_eq!(view(&*engine, trees[side], &contests[side].probes()).await, contests[side].won_by(&views[side], winner, cell));
    }
}

/// A read never sees an open batch's writes: made while the batch is open, it
/// shows the tree as it was, or waits and shows the tree as the batch left it.
pub async fn readers_never_see_an_open_batch<E: TreeStorage + 'static>(engine: Arc<E>) {
    let tree = entity_id_tree(&*engine).await;
    let position = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let contest = Contest::set_up(&*engine, tree, position).await;
    let before = view(&*engine, tree, &contest.probes()).await;
    let held = engine.reader(tree).await.unwrap();

    let mut batch = engine.batch(tree).await.unwrap();
    contest.write(&mut batch, 2).await;
    let mut reading = tokio::spawn({
        let engine = engine.clone();
        async move { engine.reader(tree).await.unwrap().rows(&AddressRange::all(), usize::MAX).await.unwrap() }
    });
    let early = tokio::time::timeout(BLOCKED, &mut reading).await.ok().map(|joined| joined.unwrap());
    if let Some(rows) = &early {
        assert_eq!(*rows, before.rows, "a read made while a batch is open shows the tree as it was");
        assert_eq!(held.rows(&AddressRange::all(), usize::MAX).await.unwrap(), before.rows);
    }
    let after = contest.won_by(&before, 2, committed(batch.commit(&contest.cell).await.unwrap()));
    let rows = match early {
        Some(rows) => rows,
        None => tokio::time::timeout(PROGRESS, reading).await.expect("a waiting read proceeds once the batch ends").unwrap(),
    };
    assert!(rows == before.rows || rows == after.rows, "a read shows a batch whole or not at all");
    assert_eq!(held.rows(&AddressRange::all(), usize::MAX).await.unwrap(), after.rows, "a reader held across the batch sees its commit");
    assert_eq!(view(&*engine, tree, &contest.probes()).await, after);
}

/// A batch cancelled while it waits for another batch on its tree leaves no
/// trace: the open batch commits whole, and the next batch begins.
pub async fn a_cancelled_wait_leaves_no_trace<E: TreeStorage + 'static>(engine: Arc<E>) {
    let tree = entity_id_tree(&*engine).await;
    let position = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let contest = Contest::set_up(&*engine, tree, position).await;
    let before = view(&*engine, tree, &contest.probes()).await;
    let mut open = engine.batch(tree).await.unwrap();
    contest.write(&mut open, 2).await;
    // An engine that makes the second batch wait is cancelled there; one that
    // does not hands out a batch, dropped unused.
    drop(tokio::time::timeout(BLOCKED, engine.batch(tree)).await);
    let cell = committed(open.commit(&contest.cell).await.unwrap());
    let next = tokio::time::timeout(PROGRESS, engine.batch(tree)).await.expect("a cancelled wait leaves the tree free").unwrap();
    assert_eq!(next.cell().await.unwrap(), cell);
    drop(next);
    assert_eq!(view(&*engine, tree, &contest.probes()).await, contest.won_by(&before, 2, cell));
}

/// Readers never outlive their tree: one taken before an unregistration fails
/// and never reaches the tree registered again for the same index, and after a
/// reset those of every tree fail, the entity-id tree's included.
pub async fn handles_do_not_outlive_their_tree<E: TreeStorage + 'static>(engine: Arc<E>) {
    let position = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let old = register(&*engine, title_index()).await.id;
    let old_reader = engine.reader(old).await.unwrap();
    engine.unregister_tree(old).await.unwrap();
    let again = register(&*engine, title_index()).await.id;
    assert_ne!(again, old, "a tree id is not reused within an incarnation");
    let mut batch = engine.batch(again).await.unwrap();
    let cell = batch.cell().await.unwrap();
    batch.put_row(folded_row(b"k", entity(1), position)).await.unwrap();
    committed(batch.commit(&cell).await.unwrap());
    assert_gone(&*engine, old, &old_reader).await;

    let entity_ids = entity_id_tree(&*engine).await;
    let (reader, entity_id_reader) = (engine.reader(again).await.unwrap(), engine.reader(entity_ids).await.unwrap());
    engine.delete_all().await.unwrap();
    assert_gone(&*engine, again, &reader).await;
    assert_gone(&*engine, entity_ids, &entity_id_reader).await;
    let fresh = entity_id_tree(&*engine).await;
    assert!(fresh != entity_ids && fresh != again, "a reset's fresh tree takes a new id");
    assert_eq!(view(&*engine, fresh, &[entity(1)]).await.rows, [], "nothing of the old store reaches the fresh tree");
}

/// A prefix's children are the nearest stored nodes beneath it at any depth,
/// and its address range holds exactly the leaves beneath it.
pub async fn children_and_leaf_ranges_at_ragged_depths<E: TreeStorage + 'static>(engine: Arc<E>) {
    let tree = register(&*engine, title_index()).await.id;
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
    let reader = &reader;
    let scanned = scan(&AddressRange::all(), 3, |range, limit| async move { reader.rows(&range, limit).await.unwrap() }).await;
    assert_eq!(scanned, by_address(rows.iter().cloned()));
}

/// Scans of many folded rows and tombstones continue exactly where the last
/// page ended, at every page size and over a part of the address space; a
/// limit of zero reads nothing.
pub async fn large_scans_continue_exactly<E: TreeStorage + 'static>(engine: Arc<E>) {
    let tree = register(&*engine, title_index()).await.id;
    let position = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    // Many entities under each key, so that pages end inside a key's rows.
    let rows: Vec<FoldedRow> = (0..1000u16).map(|n| folded_row(&(n % 250).to_be_bytes(), numbered(n), position)).collect();
    let tombstones: Vec<Tombstone> =
        (0..600u16).map(|n| Tombstone { key: (n % 150).to_be_bytes().to_vec(), entity_id: numbered(n), position }).collect();
    let mut cell = cell_of(&*engine, tree).await;
    for (rows, tombstones) in rows.chunks(250).zip(tombstones.chunks(150)) {
        let mut batch = engine.batch(tree).await.unwrap();
        for row in rows {
            batch.put_row(row.clone()).await.unwrap();
        }
        for tombstone in tombstones {
            batch.put_tombstone(tombstone.clone()).await.unwrap();
        }
        cell = committed(batch.commit(&cell).await.unwrap());
    }
    let (rows, tombstones) = (by_address(rows.into_iter()), by_address(tombstones.into_iter()));
    let reader = &engine.reader(tree).await.unwrap();
    let all = AddressRange::all();
    assert!(reader.rows(&all, 0).await.unwrap().is_empty(), "a limit of zero reads no row");
    assert!(reader.tombstones(&all, 0).await.unwrap().is_empty(), "a limit of zero reads no tombstone");
    for limit in [1, 7, 150, 250, 599, 600, 601, 999, 1000, 1001] {
        let scanned = scan(&all, limit, |range, limit| async move { reader.rows(&range, limit).await.unwrap() }).await;
        assert!(scanned == rows, "scanning rows {limit} a page reads each once, in order");
        let scanned = scan(&all, limit, |range, limit| async move { reader.tombstones(&range, limit).await.unwrap() }).await;
        assert!(scanned == tombstones, "scanning tombstones {limit} a page reads each once, in order");
    }
    // The key 00 07 holds four rows and four tombstones.
    let key = AddressRange::under(&NodePrefix::of(&[0x00, 0x07], 16));
    let beneath = scan(&key, 1, |range, limit| async move { reader.rows(&range, limit).await.unwrap() }).await;
    assert_eq!(beneath, rows.iter().filter(|row| row.key == [0x00, 0x07]).cloned().collect::<Vec<_>>());
    assert_eq!(beneath.len(), 4);
    let beneath = scan(&key, 3, |range, limit| async move { reader.tombstones(&range, limit).await.unwrap() }).await;
    assert_eq!(beneath, tombstones.iter().filter(|tombstone| tombstone.key == [0x00, 0x07]).cloned().collect::<Vec<_>>());
    assert_eq!(beneath.len(), 4);
}

/// Every read of an empty tree reads nothing, whether the tree is new,
/// emptied of its last leaf or published empty, and a tree of tombstones alone
/// holds no rows; an empty tree may store no root row, as a new one does.
pub async fn empty_trees_read_empty<E: TreeStorage + 'static>(engine: Arc<E>) {
    let entity_ids = entity_id_tree(&*engine).await;
    assert_empty(&engine.reader(entity_ids).await.unwrap()).await;
    assert_eq!(cell_of(&*engine, entity_ids).await.status, BuildStatus::Ready, "a new store's entity-id tree is ready and empty");
    let tree = register(&*engine, title_index()).await.id;
    assert_empty(&engine.reader(tree).await.unwrap()).await;
    assert_empty(&engine.batch(tree).await.unwrap()).await;

    let position = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let mut batch = engine.batch(tree).await.unwrap();
    let cell = batch.cell().await.unwrap();
    batch.put_row(folded_row(b"k", entity(1), position)).await.unwrap();
    batch.put_node(NodePrefix::root(), node_row(1, position)).await.unwrap();
    let cell = committed(batch.commit(&cell).await.unwrap());
    let mut batch = engine.batch(tree).await.unwrap();
    batch.delete_row(b"k", entity(1)).await.unwrap();
    batch.delete_node(&NodePrefix::root()).await.unwrap();
    assert_empty(&batch).await;
    let cell = committed(batch.commit(&cell).await.unwrap());
    assert_empty(&engine.reader(tree).await.unwrap()).await;

    let mut batch = engine.batch(tree).await.unwrap();
    batch.publish().await.unwrap();
    let cell = committed(batch.commit(&cell).await.unwrap());
    assert_eq!(cell.status, BuildStatus::Ready);
    assert_empty(&engine.reader(tree).await.unwrap()).await;

    // Tombstones alone: no rows, no lookup, and a root that counts no leaves.
    let tombstone = Tombstone { key: b"k".to_vec(), entity_id: entity(1), position };
    let mut batch = engine.batch(tree).await.unwrap();
    batch.put_tombstone(tombstone.clone()).await.unwrap();
    batch.put_node(NodePrefix::root(), node_row(0, position)).await.unwrap();
    committed(batch.commit(&cell).await.unwrap());
    let reader = engine.reader(tree).await.unwrap();
    let all = AddressRange::all();
    assert_eq!(reader.row(b"k", entity(1)).await.unwrap(), None);
    assert!(reader.entity_rows(entity(1)).await.unwrap().is_empty(), "a tombstone is not in the lookup");
    assert!(reader.rows(&all, 10).await.unwrap().is_empty());
    assert_eq!(reader.tombstones(&all, 10).await.unwrap(), [tombstone]);
    assert_eq!(reader.node(&NodePrefix::root()).await.unwrap(), Some(node_row(0, position)));
    assert!(reader.children(&NodePrefix::root()).await.unwrap().is_empty());
}

/// Every read of an empty tree, through a reader or a batch, reads nothing.
async fn assert_empty<R: TreeRead>(reader: &R) {
    let all = AddressRange::all();
    assert_eq!(reader.row(b"k", entity(1)).await.unwrap(), None, "an empty tree has no row");
    assert!(reader.entity_rows(entity(1)).await.unwrap().is_empty(), "an empty tree's lookup is empty");
    assert!(reader.rows(&all, 10).await.unwrap().is_empty(), "an empty tree scans no rows");
    assert!(reader.rows(&AddressRange::under(&NodePrefix::of(b"k", 8)), 10).await.unwrap().is_empty());
    assert!(reader.tombstones(&all, 10).await.unwrap().is_empty(), "an empty tree has no tombstones");
    assert_eq!(reader.node(&NodePrefix::root()).await.unwrap(), None, "an empty tree stores no root row unless given one");
    assert!(reader.children(&NodePrefix::root()).await.unwrap().is_empty(), "an empty tree has no nodes");
}

/// A new registration builds at generation 0 from the stable position;
/// publishing makes it ready; a restart starts the next generation with no
/// rows, and fails a batch from the old generation even at the same fold
/// position; tree ids are not reused.
pub async fn build_state_and_generation<E: TreeStorage + 'static>(engine: Arc<E>) {
    create(&*engine, vec![state(entity(9), 9, &[], &[])]).await;
    let registration = register(&*engine, title_index()).await;
    let registered = engine.stable_position().await.unwrap();
    assert_eq!(register(&*engine, title_index()).await, registration, "registration is keyed by the index");
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
    let again = register(&*engine, title_index()).await;
    assert_ne!(again.id, registration.id, "a tree id is not reused within an incarnation");
    let stable = engine.stable_position().await.unwrap();
    assert_eq!(cell_of(&*engine, again.id).await, TreeCell { generation: 0, status: BuildStatus::Building, folded: stable });
}

/// An index registered as opted out keeps no tree: registration returns and
/// lists none, and registering it so again removes the tree it had, whose
/// handles then fail. The entity-id index cannot opt out.
pub async fn an_index_that_opts_out_keeps_no_tree<E: TreeStorage + 'static>(engine: Arc<E>) {
    let opted_out = TreeOptions { opted_out: true };
    assert_eq!(engine.register_tree(title_index(), opted_out).await.unwrap(), None);
    assert!(engine.trees().await.unwrap().iter().all(|tree| tree.index != title_index()), "an index that opted out has no tree");
    let tree = register(&*engine, title_index()).await;
    let reader = engine.reader(tree.id).await.unwrap();
    assert_eq!(engine.register_tree(title_index(), opted_out).await.unwrap(), None);
    assert!(!engine.trees().await.unwrap().contains(&tree), "opting out removes the index's tree");
    assert_gone(&*engine, tree.id, &reader).await;
    assert!(matches!(engine.register_tree(HashedIndex::EntityId, opted_out).await, Err(TreeStorageError::PermanentTree)));
    assert_eq!(engine.trees().await.unwrap().len(), 1, "the entity-id tree stays");
}

/// A batch prepared against a cell that differs from the tree's only in its
/// build status conflicts and writes nothing.
pub async fn a_status_only_cell_change_conflicts<E: TreeStorage + 'static>(engine: Arc<E>) {
    let tree = register(&*engine, title_index()).await.id;
    let position = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let building = cell_of(&*engine, tree).await;
    let mut batch = engine.batch(tree).await.unwrap();
    batch.publish().await.unwrap();
    let ready = committed(batch.commit(&building).await.unwrap());
    assert_eq!(ready, TreeCell { status: BuildStatus::Ready, ..building });
    let before = view(&*engine, tree, &[entity(1)]).await;
    let mut stale = engine.batch(tree).await.unwrap();
    stale.put_row(folded_row(b"k", entity(1), position)).await.unwrap();
    stale.put_node(NodePrefix::root(), node_row(1, position)).await.unwrap();
    assert_eq!(stale.commit(&building).await.unwrap(), TreeBatchOutcome::Conflict { observed: ready }, "same generation and position");
    assert_eq!(view(&*engine, tree, &[entity(1)]).await, before);
}

/// Unregistering a tree or resetting the store is whole even when cancelled
/// while it waits for an open batch: afterwards every handle agrees whether
/// the tree exists, and once it is gone every handle fails.
pub async fn removal_is_whole_when_cancelled<E: TreeStorage + 'static>(engine: Arc<E>) {
    let position = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let tree = register(&*engine, title_index()).await.id;
    let reader = engine.reader(tree).await.unwrap();
    let mut batch = engine.batch(tree).await.unwrap();
    let cell = batch.cell().await.unwrap();
    batch.put_row(folded_row(b"k", entity(1), position)).await.unwrap();
    // An engine whose removal waits for the open batch is cancelled there.
    let finished = match tokio::time::timeout(BLOCKED, engine.unregister_tree(tree)).await {
        Ok(result) => {
            result.unwrap();
            true
        }
        Err(_cancelled) => false,
    };
    let committed = batch.commit(&cell).await;
    if engine.trees().await.unwrap().iter().any(|registration| registration.id == tree) {
        assert!(!finished, "a finished removal leaves the tree listed");
        assert!(matches!(committed, Ok(TreeBatchOutcome::Committed(_))), "the tree outlived the cancelled removal");
        assert!(reader.row(b"k", entity(1)).await.unwrap().is_some(), "a reader from before still reads the tree");
        engine.unregister_tree(tree).await.unwrap();
    } else {
        assert!(matches!(committed, Err(TreeStorageError::UnknownTree(_))), "a batch commits nothing to a removed tree");
    }
    assert_gone(&*engine, tree, &reader).await;

    let tree = register(&*engine, title_index()).await.id;
    let entity_ids = entity_id_tree(&*engine).await;
    let (reader, entity_id_reader) = (engine.reader(tree).await.unwrap(), engine.reader(entity_ids).await.unwrap());
    let mut batch = engine.batch(tree).await.unwrap();
    batch.put_row(folded_row(b"k", entity(1), position)).await.unwrap();
    let finished = match tokio::time::timeout(BLOCKED, engine.delete_all()).await {
        Ok(result) => {
            result.unwrap();
            true
        }
        Err(_cancelled) => false,
    };
    drop(batch);
    if engine.stable_position().await.unwrap().incarnation() == position.incarnation() {
        assert!(!finished, "a finished reset mints a new incarnation");
        assert_eq!(reader.row(b"k", entity(1)).await.unwrap(), None, "the store outlived the cancelled reset, without the dropped batch");
        engine.delete_all().await.unwrap();
    }
    assert_gone(&*engine, tree, &reader).await;
    assert_gone(&*engine, entity_ids, &entity_id_reader).await;
    let trees = engine.trees().await.unwrap();
    assert_eq!(trees.len(), 1, "only a fresh entity-id tree remains");
    assert!(trees[0].id != entity_ids && trees[0].index == HashedIndex::EntityId);
}

/// Every handle to a removed tree fails: each read through a reader taken
/// before the removal, and a new reader or batch.
async fn assert_gone<E: TreeStorage, R: TreeRead>(engine: &E, tree: TreeId, reader: &R) {
    let gone = |result: Result<(), TreeStorageError>| matches!(result, Err(TreeStorageError::UnknownTree(id)) if id == tree);
    let all = AddressRange::all();
    assert!(gone(reader.cell().await.map(drop)), "a reader of a removed tree reads no cell");
    assert!(gone(reader.row(b"k", entity(1)).await.map(drop)), "a reader of a removed tree reads no row");
    assert!(gone(reader.entity_rows(entity(1)).await.map(drop)), "a reader of a removed tree reads no lookup");
    assert!(gone(reader.rows(&all, 10).await.map(drop)), "a reader of a removed tree scans no rows");
    assert!(gone(reader.tombstones(&all, 10).await.map(drop)), "a reader of a removed tree scans no tombstones");
    assert!(gone(reader.node(&NodePrefix::root()).await.map(drop)), "a reader of a removed tree reads no node");
    assert!(gone(reader.children(&NodePrefix::root()).await.map(drop)), "a reader of a removed tree reads no children");
    assert!(gone(engine.reader(tree).await.map(drop)), "a removed tree has no new reader");
    assert!(gone(engine.batch(tree).await.map(drop)), "a removed tree has no new batch");
}

/// A snapshot shows the tree's index as of its boundary while commits go on,
/// and with the log replayed from the boundary it gives the current index:
/// nothing missing, nothing twice.
pub async fn snapshot_and_replay_give_the_current_index<E: TreeStorage + 'static>(engine: Arc<E>) {
    create(
        &*engine,
        vec![
            state(entity(1), 1, &[component()], &[(title(), text("a"))]),
            state(entity(2), 2, &[component()], &[(title(), text("b"))]),
            state(entity(3), 3, &[component()], &[(title(), text("c"))]),
            state(entity(4), 4, &[component()], &[]),
            state(entity(5), 5, &[], &[(title(), text("e"))]),
        ],
    )
    .await;
    let tree = register(&*engine, title_index()).await.id;
    let registered = cell_of(&*engine, tree).await.folded;
    let before = create(&*engine, vec![state(entity(6), 6, &[component()], &[(title(), text("f"))])]).await;

    let snapshot = engine.snapshot(tree).await.unwrap();
    let boundary = snapshot.boundary;
    assert!(boundary >= registered, "the boundary lies at or above the tree's registration");
    assert!(boundary > before && boundary <= engine.stable_position().await.unwrap(), "the boundary is where the snapshot was taken");
    let mut entities = Box::pin(snapshot.entities);
    let mut seen: Vec<SnapshotEntity> = vec![entities.next().await.expect("the index files entities").unwrap()];
    // Commits while the snapshot is read: a move, a departure from the
    // component, a member gaining its title, and a new member.
    let during = commit_states(
        &*engine,
        vec![
            (head(1), state(entity(1), 11, &[component()], &[(title(), text("z"))])),
            (head(3), state(entity(3), 13, &[], &[(title(), text("c"))])),
            (head(4), state(entity(4), 14, &[component()], &[(title(), text("d"))])),
            (Clock::default(), state(entity(7), 17, &[component()], &[(title(), text("g"))])),
        ],
    );
    tokio::time::timeout(PROGRESS, during).await.expect("a commit does not wait for an open snapshot").committed().unwrap();
    while let Some(entity) = entities.next().await {
        seen.push(entity.unwrap());
    }

    let keyed = |n: u8, value: &str| (head(n), BTreeSet::from([title_key(value)]));
    let mut index = BTreeMap::new();
    for entity in seen {
        let id = entity.entity_id;
        assert!(index.insert(id, (entity.head, entity.keys)).is_none(), "the snapshot holds {id:?} twice");
    }
    let as_of_boundary = [(1, keyed(1, "a")), (2, keyed(2, "b")), (3, keyed(3, "c")), (6, keyed(6, "f"))];
    assert_eq!(index, as_of_boundary.map(|(n, keyed)| (entity(n), keyed)).into(), "the snapshot is the index as of its boundary");
    for row in read_all(&*engine, boundary).await {
        match row.keys[&tree].clone() {
            keys if keys.is_empty() => index.remove(&row.entity_id),
            keys => index.insert(row.entity_id, (row.head, keys)),
        };
    }
    let current = [(1, keyed(11, "z")), (2, keyed(2, "b")), (4, keyed(14, "d")), (6, keyed(6, "f")), (7, keyed(17, "g"))];
    assert_eq!(index, current.map(|(n, keyed)| (entity(n), keyed)).into(), "with the log from the boundary, the current index");
}

/// A build whose boundary falls below the retention floor before it catches
/// up starts over from a fresh snapshot; the abandoned build's batches fail and
/// write nothing, and the new build replays the log and publishes.
pub async fn a_build_overtaken_by_retention_starts_over<E: TreeStorage + 'static>(engine: Arc<E>) {
    create(&*engine, vec![state(entity(1), 1, &[component()], &[(title(), text("a"))])]).await;
    let tree = register(&*engine, title_index()).await.id;
    let registered = cell_of(&*engine, tree).await;
    let first = engine.snapshot(tree).await.unwrap();
    let mut batch = engine.batch(tree).await.unwrap();
    batch.restart(first.boundary).await.unwrap();
    fill(&mut batch, first.boundary, first.entities).await;
    let abandoned = committed(batch.commit(&registered).await.unwrap());
    assert_eq!(abandoned, TreeCell { generation: registered.generation + 1, status: BuildStatus::Building, folded: first.boundary });

    // Commits pass, and the log is trimmed past the boundary before the build
    // replays it.
    create(&*engine, vec![state(entity(2), 2, &[component()], &[(title(), text("b"))])]).await;
    engine.discard_log_below(engine.stable_position().await.unwrap()).await.unwrap();
    assert!(matches!(engine.read_log(first.boundary, 10).await, Err(LogError::BelowRetentionFloor { .. })));

    let second = engine.snapshot(tree).await.unwrap();
    assert!(second.boundary >= engine.retention_floor().await.unwrap(), "a fresh snapshot's boundary is still in the log");
    let mut batch = engine.batch(tree).await.unwrap();
    batch.restart(second.boundary).await.unwrap();
    fill(&mut batch, second.boundary, second.entities).await;
    let rebuilt = committed(batch.commit(&abandoned).await.unwrap());
    assert_eq!(rebuilt, TreeCell { generation: abandoned.generation + 1, status: BuildStatus::Building, folded: second.boundary });
    assert!(engine.prune_horizon().await.unwrap() >= second.boundary, "the new build saw no removal before its boundary");
    let probes = [entity(1), entity(2), entity(3)];
    let before = view(&*engine, tree, &probes).await;
    assert_eq!(before.rows.iter().map(|row| row.entity_id).collect::<Vec<_>>(), [entity(1), entity(2)]);

    let mut stale = engine.batch(tree).await.unwrap();
    stale.put_row(folded_row(&title_key("c"), entity(3), second.boundary)).await.unwrap();
    stale.publish().await.unwrap();
    assert_eq!(stale.commit(&abandoned).await.unwrap(), TreeBatchOutcome::Conflict { observed: rebuilt }, "the abandoned build");
    assert_eq!(view(&*engine, tree, &probes).await, before);

    let caught_up = engine.read_log(second.boundary, usize::MAX).await.unwrap();
    assert!(caught_up.rows.is_empty(), "nothing committed since the fresh snapshot");
    let mut batch = engine.batch(tree).await.unwrap();
    batch.set_folded(caught_up.next).await.unwrap();
    batch.publish().await.unwrap();
    assert_eq!(
        committed(batch.commit(&rebuilt).await.unwrap()),
        TreeCell { status: BuildStatus::Ready, folded: caught_up.next, ..rebuilt }
    );
}

/// Write a folded row for every key of every entity of a snapshot.
async fn fill<B, S>(batch: &mut B, boundary: LogPosition, entities: S)
where
    B: TreeBatch,
    S: futures::Stream<Item = Result<SnapshotEntity, TreeStorageError>>,
{
    let mut entities = Box::pin(entities);
    while let Some(entity) = entities.next().await {
        let SnapshotEntity { entity_id, head, keys } = entity.unwrap();
        for key in keys {
            batch.put_row(FoldedRow { head: head.clone(), ..folded_row(&key, entity_id, boundary) }).await.unwrap();
        }
    }
}

/// A tree is ready only while its fold position lies at or below the durable
/// position: a batch that would publish a build, or fold a ready tree, past
/// the durable position fails and writes nothing, and the build publishes once
/// durability has reached its snapshot's boundary.
pub async fn a_tree_is_ready_only_below_the_durable_position<E: TreeStorage + 'static>(engine: Arc<E>) {
    create(&*engine, vec![state(entity(1), 1, &[component()], &[(title(), text("a"))])]).await;
    let tree = register(&*engine, title_index()).await.id;
    let registered = cell_of(&*engine, tree).await;
    let snapshot = engine.snapshot(tree).await.unwrap();
    let boundary = snapshot.boundary;
    let mut batch = engine.batch(tree).await.unwrap();
    batch.restart(boundary).await.unwrap();
    fill(&mut batch, boundary, snapshot.entities).await;
    let building = committed(batch.commit(&registered).await.unwrap());
    let before = view(&*engine, tree, &[entity(1)]).await;

    // Nothing past the stable position is durable, on any engine.
    let stable = engine.stable_position().await.unwrap();
    let past = LogPosition::new(stable.incarnation(), stable.offset() + 1);
    let mut batch = engine.batch(tree).await.unwrap();
    batch.set_folded(past).await.unwrap();
    batch.publish().await.unwrap();
    let refused = batch.commit(&building).await;
    assert!(matches!(refused, Err(TreeStorageError::NotYetDurable { .. })), "a build folded past the durable position is not published");
    assert_eq!(view(&*engine, tree, &[entity(1)]).await, before, "the refused batch writes nothing");

    wait_until_durable(&*engine, boundary).await;
    let mut batch = engine.batch(tree).await.unwrap();
    batch.publish().await.unwrap();
    let ready = committed(batch.commit(&building).await.unwrap());
    assert_eq!(ready, TreeCell { status: BuildStatus::Ready, ..building }, "a build publishes once its boundary is durable");

    let mut batch = engine.batch(tree).await.unwrap();
    batch.set_folded(past).await.unwrap();
    let refused = batch.commit(&ready).await;
    assert!(matches!(refused, Err(TreeStorageError::NotYetDurable { .. })), "a ready tree does not fold past the durable position");
    assert_eq!(cell_of(&*engine, tree).await, ready);
}

/// The prune horizon rises wherever a tree may stop holding a removal: at a
/// registration, a prune and a restart. It never falls.
pub async fn prune_horizon_rises_with_every_lost_removal<E: TreeStorage + 'static>(engine: Arc<E>) {
    create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let tree = register(&*engine, title_index()).await.id;
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

/// A crash keeps the log incarnation and everything below the durable
/// position: its commits' states and log rows, and the trees registered
/// before them.
pub async fn reopen_keeps_what_was_durable<H: Reopen>(reopen: H) {
    let engine = reopen.open().await;
    let first = create(&*engine, vec![state(entity(1), 1, &[component()], &[(title(), text("a"))])]).await;
    let tree = register(&*engine, title_index()).await;
    let last = create(&*engine, vec![state(entity(2), 2, &[component()], &[(title(), text("b"))])]).await;
    let durable = wait_until_durable(&*engine, last.next()).await;
    let rows = read_all(&*engine, first).await;
    let engine = reopen.crash_and_reopen(engine).await;

    let stable = engine.stable_position().await.unwrap();
    assert_eq!(stable.incarnation(), first.incarnation(), "a reopened store keeps its log incarnation");
    assert!(stable >= durable && engine.durable_position().await.unwrap() >= durable, "what was durable stays settled and durable");
    assert!(engine.trees().await.unwrap().contains(&tree), "a tree registered before a durable commit survives");
    assert_eq!(read_all(&*engine, first).await.into_iter().filter(|row| row.position < durable).collect::<Vec<_>>(), rows);
    for (id, n) in [(entity(1), 1), (entity(2), 2)] {
        assert_eq!(engine.get_state(id).await.unwrap().payload.state.head, head(n), "a durable commit's state survives");
    }
}

/// A crash keeps a tree's batches whole and in order: the reopened tree is as
/// some prefix of its committed batches left it, prune horizon included.
pub async fn reopen_keeps_tree_batches_whole<H: Reopen>(reopen: H) {
    let engine = reopen.open().await;
    let tree = entity_id_tree(&*engine).await;
    let position = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    let probes: Vec<EntityId> = (1..=9).map(entity).collect();
    let mut states = vec![view(&*engine, tree, &probes).await];
    let contest = Contest::set_up(&*engine, tree, position).await;
    states.push(view(&*engine, tree, &probes).await);
    // The batch folds no further than the durable position, so recovery has
    // no reason to restart the tree.
    wait_until_durable(&*engine, contest.stable).await;
    let mut batch = engine.batch(tree).await.unwrap();
    batch.prune_tombstones(contest.stable).await.unwrap();
    contest.write(&mut batch, 2).await;
    committed(batch.commit(&contest.cell).await.unwrap());
    states.push(view(&*engine, tree, &probes).await);
    let engine = reopen.crash_and_reopen(engine).await;

    let reopened = view(&*engine, tree, &probes).await;
    assert!(states.contains(&reopened), "a crash keeps each batch whole and in order, not {reopened:?}");
}

/// Recovery never leaves a tree describing commits it discarded: after a
/// crash no tree's fold position lies above the durable position, and a build
/// restarted at a snapshot boundary that the crash left undurable is either
/// lost with its batch or restarted again: a later generation, building, and
/// empty.
pub async fn reopen_restarts_trees_past_the_durable_position<H: Reopen>(reopen: H) {
    let engine = reopen.open().await;
    create(&*engine, vec![state(entity(1), 1, &[component()], &[(title(), text("a"))])]).await;
    let tree = register(&*engine, title_index()).await.id;
    let registered = cell_of(&*engine, tree).await;
    // A durable commit after the registration keeps the registration too.
    let kept = create(&*engine, vec![state(entity(2), 2, &[component()], &[(title(), text("b"))])]).await;
    wait_until_durable(&*engine, kept.next()).await;
    // A commit the crash may lose, and a build restarted at a boundary past it.
    create(&*engine, vec![state(entity(3), 3, &[component()], &[(title(), text("c"))])]).await;
    let building = {
        let snapshot = engine.snapshot(tree).await.unwrap();
        let mut batch = engine.batch(tree).await.unwrap();
        batch.restart(snapshot.boundary).await.unwrap();
        fill(&mut batch, snapshot.boundary, snapshot.entities).await;
        committed(batch.commit(&registered).await.unwrap())
    };
    let engine = reopen.crash_and_reopen(engine).await;

    let durable = engine.durable_position().await.unwrap();
    for registration in engine.trees().await.unwrap() {
        let cell = cell_of(&*engine, registration.id).await;
        assert!(cell.folded <= durable, "after recovery {registration:?} folds past the durable position: {cell:?}");
    }
    let (cell, boundary_kept) = (cell_of(&*engine, tree).await, building.folded <= durable);
    if !boundary_kept && cell != registered {
        assert!(cell.generation > building.generation && cell.status == BuildStatus::Building, "recovery restarts the build: {cell:?}");
        let view = view(&*engine, tree, &[entity(1), entity(2), entity(3)]).await;
        assert!(view.rows.is_empty() && view.tombstones.is_empty() && view.nodes.is_empty(), "a restarted tree holds nothing");
        assert!(view.horizon >= cell.folded, "a restarted tree saw no removal before its start");
    }
}

/// A reset's new incarnation survives a crash once a commit in it is durable,
/// and nothing of the old store returns.
pub async fn reopen_after_a_reset_keeps_the_new_incarnation<H: Reopen>(reopen: H) {
    let engine = reopen.open().await;
    let old = create(&*engine, vec![state(entity(1), 1, &[], &[])]).await;
    register(&*engine, title_index()).await;
    engine.delete_all().await.unwrap();
    let new = create(&*engine, vec![state(entity(2), 2, &[], &[])]).await;
    wait_until_durable(&*engine, new.next()).await;
    let engine = reopen.crash_and_reopen(engine).await;

    assert_eq!(engine.stable_position().await.unwrap().incarnation(), new.incarnation(), "the reset's incarnation survives");
    assert!(matches!(engine.read_log(old, 10).await, Err(LogError::IncarnationMismatch { .. })));
    assert!(matches!(engine.get_state(entity(1)).await, Err(RetrievalError::EntityNotFound(_))), "the old store does not return");
    assert_eq!(engine.get_state(entity(2)).await.unwrap().payload.state.head, head(2));
    let trees = engine.trees().await.unwrap();
    assert_eq!(trees.iter().map(|tree| &tree.index).collect::<Vec<_>>(), [&HashedIndex::EntityId], "only the fresh entity-id tree");
}

/// Everything a reader sees of one tree, with the store's prune horizon.
#[derive(Debug, Clone, PartialEq)]
struct TreeView {
    cell: TreeCell,
    rows: Vec<FoldedRow>,
    /// The entity-to-keys lookup of every entity with rows and every probe.
    lookup: BTreeMap<EntityId, Vec<FoldedRow>>,
    tombstones: Vec<Tombstone>,
    /// Every stored node, in prefix order.
    nodes: Vec<(NodePrefix, NodeRow)>,
    horizon: LogPosition,
}

/// Read the whole of one tree. The lookup is read for every entity with rows
/// and for each probe, so a lookup entry that outlived its row shows.
async fn view<E: TreeStorage>(engine: &E, tree: TreeId, probes: &[EntityId]) -> TreeView {
    let reader = engine.reader(tree).await.unwrap();
    let rows = reader.rows(&AddressRange::all(), usize::MAX).await.unwrap();
    let mut lookup = BTreeMap::new();
    for entity_id in rows.iter().map(|row| row.entity_id).chain(probes.iter().copied()) {
        lookup.insert(entity_id, reader.entity_rows(entity_id).await.unwrap());
    }
    TreeView {
        cell: reader.cell().await.unwrap(),
        rows,
        lookup,
        tombstones: reader.tombstones(&AddressRange::all(), usize::MAX).await.unwrap(),
        nodes: all_nodes(&reader).await,
        horizon: engine.prune_horizon().await.unwrap(),
    }
}

/// Every stored node: the root's row, if any, and each stored node's
/// nearest stored descendants, in prefix order.
async fn all_nodes<R: TreeRead>(reader: &R) -> Vec<(NodePrefix, NodeRow)> {
    let mut nodes: Vec<_> = reader.node(&NodePrefix::root()).await.unwrap().map(|row| (NodePrefix::root(), row)).into_iter().collect();
    let mut pending = vec![NodePrefix::root()];
    while let Some(prefix) = pending.pop() {
        for (child, row) in reader.children(&prefix).await.unwrap() {
            pending.push(child.clone());
            nodes.push((child, row));
        }
    }
    nodes.sort_by(|(a, _), (b, _)| a.cmp(b));
    nodes
}

/// What a batch reads of the contested tree, to compare over its life.
async fn batch_reads<B: TreeBatch>(batch: &B) -> (TreeCell, Vec<FoldedRow>, Vec<Tombstone>, Vec<(NodePrefix, NodeRow)>) {
    let all = AddressRange::all();
    (
        batch.cell().await.unwrap(),
        batch.rows(&all, usize::MAX).await.unwrap(),
        batch.tombstones(&all, usize::MAX).await.unwrap(),
        all_nodes(batch).await,
    )
}

/// One tree that several batches contend for: entity 1's row and the root
/// node, set up at `cell`, with rows positioned at an earlier commit. Contender
/// `n` writes its own version of both and a tombstone of its own, then commits
/// against `cell`.
#[derive(Clone, Copy)]
struct Contest {
    tree: TreeId,
    position: LogPosition,
    stable: LogPosition,
    cell: TreeCell,
}

impl Contest {
    async fn set_up<E: TreeStorage>(engine: &E, tree: TreeId, position: LogPosition) -> Self {
        let mut contest = Self { tree, position, stable: position, cell: cell_of(engine, tree).await };
        let mut setup = engine.batch(tree).await.unwrap();
        setup.put_row(contest.row(1)).await.unwrap();
        setup.put_node(NodePrefix::root(), contest.node(1)).await.unwrap();
        contest.cell = committed(setup.commit(&contest.cell).await.unwrap());
        contest.stable = engine.stable_position().await.unwrap();
        contest
    }

    fn row(&self, n: u8) -> FoldedRow { FoldedRow { head: head(n), ..folded_row(&entity(1).to_bytes(), entity(1), self.position) } }

    fn node(&self, n: u8) -> NodeRow { node_row(u64::from(n), self.position) }

    fn tombstone(&self, n: u8) -> Tombstone { Tombstone { key: vec![n], entity_id: entity(n), position: self.position } }

    /// Entity 1 and every contender's tombstone entity, whose lookups must
    /// stay empty.
    fn probes(&self) -> Vec<EntityId> { (1..=9).map(entity).collect() }

    async fn write<B: TreeBatch>(&self, batch: &mut B, n: u8) {
        batch.put_row(self.row(n)).await.unwrap();
        batch.put_node(NodePrefix::root(), self.node(n)).await.unwrap();
        batch.put_tombstone(self.tombstone(n)).await.unwrap();
        batch.set_folded(self.stable).await.unwrap();
    }

    /// Contender `n`'s whole attempt, from beginning its batch to committing
    /// it, passing each of `phases` on the way.
    async fn contend<E: TreeStorage>(self, engine: Arc<E>, n: u8, phases: Phases) -> TreeBatchOutcome {
        let mut batch = engine.batch(self.tree).await.unwrap();
        if let Some(begun) = phases.begun {
            let _ = begun.send(());
        }
        if let Some(all_begun) = &phases.all_begun {
            all_begun.wait().await;
        }
        self.write(&mut batch, n).await;
        if let Some(all_committing) = &phases.all_committing {
            all_committing.wait().await;
        }
        batch.commit(&self.cell).await.unwrap()
    }

    /// The tree once contender `n` won, leaving `cell`.
    fn won_by(&self, before: &TreeView, n: u8, cell: TreeCell) -> TreeView {
        let mut won = before.clone();
        won.cell = cell;
        won.rows = vec![self.row(n)];
        won.lookup.insert(entity(1), vec![self.row(n)]);
        won.tombstones = by_address(before.tombstones.iter().cloned().chain([self.tombstone(n)]));
        won.nodes = vec![(NodePrefix::root(), self.node(n))];
        won
    }
}

/// Where a contender tells the case how far it has got, so that the case can
/// order contenders on purpose.
#[derive(Default)]
struct Phases {
    /// Told once the contender's batch has begun.
    begun: Option<oneshot::Sender<()>>,
    /// Waited at, with the other contenders, once the batch has begun.
    all_begun: Option<Arc<Barrier>>,
    /// Waited at, with the other contenders, once the writes are made and
    /// just before the commit.
    all_committing: Option<Arc<Barrier>>,
}

impl Clone for Phases {
    /// A clone shares the barriers; a sender is one contender's alone.
    fn clone(&self) -> Self { Self { begun: None, all_begun: self.all_begun.clone(), all_committing: self.all_committing.clone() } }
}

/// The one contender that committed, and the cell it left. Every other
/// contender conflicted with that cell.
fn one_winner(outcomes: impl IntoIterator<Item = (u8, TreeBatchOutcome)>) -> (u8, TreeCell) {
    let outcomes: Vec<_> = outcomes.into_iter().collect();
    let winners: Vec<(u8, TreeCell)> = outcomes
        .iter()
        .filter_map(|(n, outcome)| match outcome {
            TreeBatchOutcome::Committed(cell) => Some((*n, *cell)),
            TreeBatchOutcome::Conflict { .. } => None,
        })
        .collect();
    let [(winner, cell)] = winners[..] else { panic!("exactly one of the batches against one cell commits: {outcomes:?}") };
    for (n, outcome) in &outcomes {
        if let TreeBatchOutcome::Conflict { observed } = outcome {
            assert_eq!(*observed, cell, "contender {n} conflicted with the winner's cell");
        }
    }
    (winner, cell)
}

fn entity(n: u8) -> EntityId { EntityId::from_bytes([n; 32]) }

/// One of many entities, for cases that need more than `entity` offers.
fn numbered(n: u16) -> EntityId {
    let mut bytes = [0xEE; 32];
    bytes[..2].copy_from_slice(&n.to_be_bytes());
    EntityId::from_bytes(bytes)
}

fn head(n: u8) -> Clock { Clock::from(EventId::from_bytes([n; 32])) }

fn text(value: &str) -> Value { Value::String(value.to_owned()) }

fn component() -> ModelId { ModelId::EntityId(entity(240)) }

fn title() -> PropertyId { PropertyId::EntityId(entity(241)) }

fn title_key_spec() -> KeySpec<PropertyId> { KeySpec::new(vec![IndexKeyPart::asc(title(), ValueType::String)]) }

/// The component's members, filed by title.
fn title_index() -> HashedIndex { HashedIndex::Component { component: component(), key_spec: title_key_spec() } }

fn rank() -> PropertyId { PropertyId::EntityId(entity(242)) }

/// The component's members, filed by an integer rank.
fn rank_index() -> HashedIndex {
    HashedIndex::Component { component: component(), key_spec: KeySpec::new(vec![IndexKeyPart::asc(rank(), ValueType::I64)]) }
}

/// The key the title index files a member with this title under.
fn title_key(value: &str) -> Vec<u8> { encode_tuple_values_with_key_spec(&[text(value)], &title_key_spec()).unwrap() }

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

/// Register a tree for `index` with the default options, which keep one.
async fn register<E: TreeStorage>(engine: &E, index: HashedIndex) -> TreeRegistration {
    engine.register_tree(index, TreeOptions::default()).await.unwrap().expect("an index that did not opt out keeps a tree")
}

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

/// Read everything in `range` a page of `limit` at a time, checking that each
/// page holds at most `limit` items and begins after the last page ended.
async fn scan<T, F, Fut>(range: &AddressRange, limit: usize, read: F) -> Vec<T>
where
    T: Addressed,
    F: Fn(AddressRange, usize) -> Fut,
    Fut: Future<Output = Vec<T>>,
{
    let (mut scanned, mut range) = (Vec::<T>::new(), range.clone());
    loop {
        let page = read(range.clone(), limit).await;
        assert!(page.len() <= limit, "a page holds at most its limit");
        let Some(last) = page.last() else { return scanned };
        assert!(scanned.last().is_none_or(|previous| previous.address() < page[0].address()), "each page continues after the last");
        range = range.after(&last.address());
        scanned.extend(page);
    }
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
