use ankql::ast::{Predicate, Resolved};
use std::collections::BTreeMap;
use std::future::Future;

use async_trait::async_trait;
use futures::Stream;

use crate::error::{MutationError, RetrievalError};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, Event, EventId, ModelId};

/// One raw logical record emitted by a storage dump.
#[derive(Debug)]
pub enum StorageDumpItem {
    Event(Attested<Event>),
    State(Attested<EntityState>),
}

/// Raw logical export implemented by storage engines that support portable
/// dumps. Physical tables, cursor pages, and materialized values remain an
/// implementation detail of the engine.
#[async_trait]
pub trait StorageDump: StorageEngine {
    type DumpStream: Stream<Item = Result<StorageDumpItem, RetrievalError>> + Send + 'static;

    /// Stream events followed by states without exposing the engine's
    /// physical partitioning.
    async fn dump(&self) -> Result<Self::DumpStream, RetrievalError>;
}

mod catalog;
pub use catalog::CatalogResolver;

mod read;
pub use read::GetStateResult;

mod state;
pub use state::{StateCursor, StateScan, StateSnapshot};

mod lock;
pub use lock::LockName;

mod map;
pub use map::{MapId, MapRead, MapRows, MapWrite, ScanDirection};

mod lock_table;
pub use lock_table::{HeldLocks, LockTable};

pub mod pending_writes;

/// One atomic storage transaction. Engines may execute writes as they arrive or buffer them until commit.
/// Dropping the handle without committing must leave its writes uncommitted.
///
/// Beside entity records, a transaction reads and writes ordered maps of bytes ([`MapId`]), and takes
/// named locks ([`LockName`]), under which its commit lands only if no other transaction that took
/// them has committed meanwhile: [`map_lock_and_read`](Self::map_lock_and_read) says how an engine
/// keeps that. Its map writes land with its entity records, all of them or none. A transaction that
/// makes no map call takes no lock, and commits as it would if engines kept no maps, writing the same
/// records the same way. On an engine with one writer for the whole store, its commit waits its turn
/// behind an open map transaction, as it would behind any other writer.
///
/// A task that holds open a transaction that made map calls, or a snapshot
/// ([`StorageEngine::map_snapshot`]), must not wait, before it ends it, on another transaction's map call
/// or commit, on another snapshot, or on a reset ([`StorageEngine::delete_all`]). On an engine whose locks
/// are fair, as sled's are, a commit or a reset that queues between the two waits for the first, the
/// second waits behind it, and neither ends; on an engine with one writer for the whole store, as SQLite
/// is, a second map transaction waits for the first's turn, whatever names it asks for.
#[async_trait]
pub trait StorageTransaction: Send {
    /// Write validated events with this transaction's states.
    async fn add_events(&mut self, events: &[Attested<Event>]) -> Result<(), MutationError>;

    /// Replace an entity's state, including its memberships and materializations.
    /// `expected_head` is the state this update was derived from, including prior writes in this transaction.
    /// A missing entity has an empty head. Repeated calls for an entity replace its preceding state.
    ///
    /// A transaction sets its states before its first map call: a state set after one fails with
    /// [`MutationError::InvalidUpdate`], since preparing a state can need the engine's other connections,
    /// which an open map transaction may hold up. On SQLite, a model's first state creates the model's
    /// tables through another connection, which would wait behind the transaction's own write lock.
    async fn set_state(&mut self, expected_head: &Clock, state: &Attested<EntityState>) -> Result<(), MutationError>;

    /// Compare every expected head, then atomically persist events, canonical
    /// states, memberships, materializations and map writes. Any conflict
    /// publishes none of those records and returns the observed stored states.
    /// A poisoned transaction fails with [`MutationError::TransactionFailed`].
    async fn commit(self) -> Result<StorageCommitOutcome, MutationError>;

    /// Take `locks`, then answer `reads`, one [`MapRows`] for each and in their order, in one request where
    /// the engine can. The reads see this transaction's own writes, ranges included.
    ///
    /// A lock name is exclusive in this sense: this transaction's commit lands only if no other
    /// transaction that took any of these names has committed since this one took them. An engine keeps
    /// that by making another transaction that asks for a name this one holds wait until this one ends.
    /// Where it can't make the other wait, as IndexedDB can't make another tab's, it notes each name's
    /// version as the name is taken, raises it at the commit, and refuses the commit of a transaction
    /// that finds one moved. An engine may make a transaction wait for more than its names ask: one
    /// with a single writer for the whole store makes every other writer wait behind an open map
    /// transaction, whatever names it asks for.
    ///
    /// Across all of a transaction's calls, its names come in strictly ascending order, so that every
    /// transaction takes its locks in one order: a name out of order, or taken twice, fails the call
    /// with [`MutationError::LockOutOfOrder`], before the engine takes any of them. Each engine's
    /// transaction makes its map calls through [`transaction_call`], which checks this and keeps the
    /// poison rule.
    async fn map_lock_and_read(&mut self, locks: &[LockName], reads: Vec<MapRead>) -> Result<Vec<MapRows>, MutationError>;

    /// Answer `reads` as [`map_lock_and_read`](Self::map_lock_and_read) does, taking no lock.
    async fn map_read(&mut self, reads: Vec<MapRead>) -> Result<Vec<MapRows>, MutationError> { self.map_lock_and_read(&[], reads).await }

    /// Write to maps, in order: the writes land with this transaction's entity records when it commits.
    /// An engine sends them into its native transaction now, or with the transaction's next request,
    /// where it can, and otherwise buffers them; either way later reads see them. A buffered write's
    /// failure may surface at a later read or commit. A write call that fails, or is dropped before it
    /// returns, poisons the transaction.
    async fn map_write(&mut self, writes: Vec<MapWrite>) -> Result<(), MutationError>;

    /// Permanently prevent this transaction's commit, after a compound operation on it failed or was
    /// cancelled part way. Every later call on it, [`add_events`](Self::add_events),
    /// [`set_state`](Self::set_state) and the map calls, and its [`commit`](Self::commit), fails with
    /// [`MutationError::TransactionFailed`].
    fn poison(&mut self);
}

/// Run an engine's map call with lock-order validation and transaction poisoning.
///
/// Reject a poisoned transaction or out-of-order `locks` before polling `call`; an ordering error
/// leaves the transaction usable. Otherwise mark it poisoned until `call` returns `Ok`, so failure
/// or cancellation prevents a later commit. `poisoned` and `last_lock` belong to the transaction;
/// taking the locks is the call's responsibility.
///
/// Constructing `call` must do no work: use an async function or block whose work starts when polled.
pub async fn transaction_call<T>(
    poisoned: &mut bool,
    last_lock: &mut Option<LockName>,
    locks: &[LockName],
    call: impl Future<Output = Result<T, MutationError>>,
) -> Result<T, MutationError> {
    if *poisoned {
        return Err(MutationError::TransactionFailed);
    }
    LockName::take_in_order(last_lock, locks)?;

    *poisoned = true;
    let answer = call.await?;
    *poisoned = false;
    Ok(answer)
}

/// Successful persistence details for one prepared entity.
// TODO: No caller uses this result; remove it unless per-entity commit reporting proves necessary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedEntityWrite {
    /// Canonical entity identity.
    pub entity_id: EntityId,
    /// Whether the canonical head changed.
    pub canonical_changed: bool,
}

/// Result of an atomically committed storage transaction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageCommitResult {
    /// One result per entity, in first-write order.
    pub entities: Vec<CommittedEntityWrite>,
}

/// Outcome of attempting an exact-head storage transaction.
#[derive(Debug, Clone)]
pub enum StorageCommitOutcome {
    /// Every expectation matched and the entire batch committed.
    Committed(StorageCommitResult),
    /// At least one expectation differed, so the engine rolled back the
    /// complete batch. Values are canonical states observed while checking
    /// this attempt; `None` means no canonical entity existed when checked.
    Conflict { observed: BTreeMap<EntityId, Option<Attested<EntityState>>> },
}

impl StorageCommitOutcome {
    /// Return the committed result, or a retryable write conflict.
    pub fn committed(self) -> Result<StorageCommitResult, MutationError> {
        match self {
            Self::Committed(result) => Ok(result),
            Self::Conflict { .. } => Err(MutationError::WriteConflict),
        }
    }
}

#[async_trait]
/// Semantic persistence boundary for canonical entity data, events,
/// and model-specific query materializations.
///
/// Physical tables, object stores, trees, naming registries, and
/// entity-to-model association layouts remain private to each implementation.
pub trait StorageEngine: Send + Sync {
    /// The engine's native value representation.
    type Value;

    /// This engine's transaction handle, borrowing only its engine handle.
    type Transaction<'a>: StorageTransaction + 'a
    where Self: 'a;

    /// A read over maps, apart from any transaction: see [`map_snapshot`](Self::map_snapshot).
    type Snapshot: MapSnapshot;

    /// Begin an atomic storage transaction; the engine controls native transaction timing.
    fn transaction(&self) -> Self::Transaction<'_>;

    /// Begin a read over maps, apart from any transaction, under the lock names `locks`.
    ///
    /// Its reads see one committed state of what transactions write under those names: the state some
    /// prefix of the landed commits left, every commit that completed before it began included. What they
    /// write under other names, or under none, a read may see as a later commit left it, or the read may
    /// fail as [`MapSnapshot`] says. An engine whose readers wait out a commit's apply holds the names'
    /// readers' locks while the snapshot is open. A task that holds it open keeps the rule
    /// [`StorageTransaction`] states, and waits on no other map transaction, snapshot or reset.
    async fn map_snapshot(&self, locks: &[LockName]) -> Result<Self::Snapshot, RetrievalError>;

    /// Read changed entities or a complete limited selection at one boundary.
    /// The engine validates a change cursor's
    /// format and storage incarnation; an unusable cursor requires a full scan.
    /// Events alone do not constitute an entity-state change.
    async fn state_snapshot(&self, scan: StateScan<'_>) -> Result<Box<dyn StateSnapshot>, RetrievalError>;

    /// Retrieve canonical state by entity identity.
    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError>;

    /// Fetch entities matching the selection, including its model-membership predicates.
    async fn fetch_states(&self, selection: &ankql::ast::Selection<Resolved>) -> Result<Vec<Attested<EntityState>>, RetrievalError>;

    /// Retrieve one outcome per requested identity, in input order, distinguishing absence from predicate mismatch.
    /// Engines may batch reads or join materializations; existence and predicate checks must use the same state.
    async fn get_states(&self, ids: Vec<EntityId>, predicate: &Predicate<Resolved>) -> Result<Vec<GetStateResult>, RetrievalError> {
        let mut states = Vec::with_capacity(ids.len());
        for id in ids {
            states.push(match self.get_state(id).await {
                Ok(state) => GetStateResult::matching(state, predicate)?,
                Err(RetrievalError::EntityNotFound(_)) => GetStateResult::NotFound(id),
                Err(e) => return Err(e),
            });
        }
        Ok(states)
    }

    /// Retrieve the stored events among `event_ids`, omitting missing ones.
    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError>;

    /// Retrieve all events for an entity.
    async fn dump_entity_events(&self, id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError>;

    /// Delete all engine-owned entity, event, association, materialization,
    /// and physical-name data while retaining engine compatibility metadata.
    ///
    /// It empties every map too, and no transaction that made map calls before it lands anything in the
    /// maps it emptied: it waits for such a transaction to end, or makes that transaction's commit fail.
    async fn delete_all(&self) -> Result<bool, MutationError>;

    /// List existing model materializations without creating any.
    async fn list_materializations(&self) -> Result<Vec<ModelId>, RetrievalError> { Ok(Vec::new()) }

    /// Supply optional labels for engines to seed their own persistent physical names.
    /// Injected after node construction; retained weakly to avoid an ownership cycle.
    fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn crate::schema::CatalogResolver>) { let _ = resolver; }
}

/// A read over maps, begun by [`StorageEngine::map_snapshot`], that sees one committed state of what
/// transactions write under the names it was begun under.
///
/// An engine that can't hold that state for all of a snapshot's reads fails the read that finds it
/// moved with a [`MutationError::WriteConflict`], carried as a [`RetrievalError::MutationError`], so that
/// its caller retries with another snapshot.
#[async_trait]
pub trait MapSnapshot: Send {
    /// Answer `reads`, one [`MapRows`] for each and in their order, in one request where the engine can.
    async fn map_read(&mut self, reads: Vec<MapRead>) -> Result<Vec<MapRows>, RetrievalError>;
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use super::*;
    use crate::test_helpers::map_conformance::fixtures::poll_once;

    /// An engine's call, counted in `calls`.
    async fn ran(calls: &mut usize) -> Result<(), MutationError> {
        *calls += 1;
        Ok(())
    }

    async fn failed(calls: &mut usize) -> Result<(), MutationError> {
        *calls += 1;
        Err(MutationError::General("the engine failed".into()))
    }

    async fn waits() -> Result<(), MutationError> { std::future::pending().await }

    #[tokio::test]
    async fn a_call_that_fails_poisons_the_transaction_and_no_later_call_runs() {
        let (mut poisoned, mut last_lock, mut calls) = (false, None, 0);
        transaction_call(&mut poisoned, &mut last_lock, &[], ran(&mut calls)).await.unwrap();
        assert!(matches!(transaction_call(&mut poisoned, &mut last_lock, &[], failed(&mut calls)).await, Err(MutationError::General(_))));

        assert!(poisoned);
        assert!(matches!(
            transaction_call(&mut poisoned, &mut last_lock, &[], ran(&mut calls)).await,
            Err(MutationError::TransactionFailed)
        ));
        assert_eq!(calls, 2);
    }

    #[tokio::test]
    async fn a_call_dropped_before_it_returns_poisons_the_transaction() {
        let (mut poisoned, mut last_lock) = (false, None);
        {
            let mut waiting = pin!(transaction_call(&mut poisoned, &mut last_lock, &[], waits()));
            assert!(poll_once(waiting.as_mut()).is_pending());
        }

        assert!(poisoned);
        assert!(matches!(transaction_call(&mut poisoned, &mut last_lock, &[], waits()).await, Err(MutationError::TransactionFailed)));
    }

    #[tokio::test]
    async fn names_out_of_order_fail_their_call_alone_before_it_runs() {
        let (mut poisoned, mut last_lock, mut calls) = (false, None, 0);
        transaction_call(&mut poisoned, &mut last_lock, &[LockName::new("b")], ran(&mut calls)).await.unwrap();

        let failure = transaction_call(&mut poisoned, &mut last_lock, &[LockName::new("a")], ran(&mut calls)).await;
        assert!(matches!(&failure, Err(MutationError::LockOutOfOrder { .. })), "{failure:?}");

        assert!(!poisoned);
        transaction_call(&mut poisoned, &mut last_lock, &[LockName::new("c")], ran(&mut calls)).await.unwrap();
        assert_eq!(calls, 2);
    }

    #[tokio::test]
    async fn a_poisoned_transaction_runs_no_call_of_names_in_order_or_not() {
        let (mut poisoned, mut last_lock, mut calls) = (true, None, 0);

        assert!(matches!(
            transaction_call(&mut poisoned, &mut last_lock, &[LockName::new("a")], ran(&mut calls)).await,
            Err(MutationError::TransactionFailed)
        ));
        assert!(matches!(
            transaction_call(&mut poisoned, &mut last_lock, &[LockName::new("a"), LockName::new("a")], ran(&mut calls)).await,
            Err(MutationError::TransactionFailed)
        ));
        assert_eq!(calls, 0);
    }
}
