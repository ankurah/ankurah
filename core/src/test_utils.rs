use crate::error::{MutationError, RetrievalError};
use crate::storage::{
    transaction_call, CommittedEntityWrite, LockName, LockTable, MapRead, MapRows, MapWrite, StorageCommitOutcome, StorageCommitResult,
    StorageEngine, StorageTransaction,
};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, Event, EventId, ModelId};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

mod maps;
use maps::{MapReads, Maps, OpenSnapshot, TestMapSnapshot, TransactionMaps};

mod states;
mod tests;

type Pause = (tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>);

/// In-memory storage for core lifecycle tests, not a query-engine substitute.
#[derive(Default)]
pub(crate) struct TestStorage {
    data: Mutex<TestData>,
    locks: LockTable,
    pub hold_fetch: Mutex<BTreeMap<ModelId, Pause>>,
    pub hold_set_state: Mutex<Option<Pause>>,
    pub hold_commit: Mutex<Option<Pause>>,
    pub hold_after_commit: Mutex<Option<Pause>>,
    /// Pause a canonical-state snapshot after capturing its read boundary.
    pub hold_state_snapshot: Mutex<Option<Pause>>,
    /// Pause the page that many calls from now, counting from zero.
    pub hold_state_page: Arc<Mutex<Option<(usize, Pause)>>>,
    pub reject_state_finish: Arc<AtomicBool>,
    /// Pauses the next map write part way, after the first of its writes.
    pub hold_map_write: Mutex<Option<Pause>>,
    /// Pauses a transaction's map call before it is answered: the call that
    /// many map calls from now, counting from zero.
    pub hold_map_call: Mutex<Option<(usize, Pause)>>,
    pub reject_state_read: Mutex<Option<EntityId>>,
    fail_next_commit: AtomicBool,
    fail_next_map_write: AtomicBool,
    fail_next_snapshot_read: Arc<AtomicBool>,

    /// The most snapshots open at once, past which one fails to begin, as on
    /// an engine whose snapshots each hold one of a bounded number of
    /// connections: `None` for no bound.
    pub most_open_snapshots: Mutex<Option<usize>>,

    /// How many snapshots are open: each counts from its beginning until it
    /// is dropped.
    open_snapshots: Arc<AtomicUsize>,

    /// Every map read that transactions and snapshots answer, while it
    /// records them.
    map_reads: MapReads,
}

struct TestData {
    states: BTreeMap<EntityId, Attested<EntityState>>,
    events: BTreeMap<EventId, Attested<Event>>,

    /// Every map's committed entries, shared with the snapshots begun on
    /// them: a commit that changes them replaces them.
    maps: Arc<Maps>,

    /// How many times `delete_all` has emptied the storage.
    resets: u64,
    incarnation: EntityId,
    revision: u64,
    state_revisions: BTreeMap<EntityId, u64>,
}

impl Default for TestData {
    fn default() -> Self {
        Self {
            states: BTreeMap::new(),
            events: BTreeMap::new(),
            maps: Arc::new(Maps::new()),
            resets: 0,
            incarnation: EntityId::random(),
            revision: 0,
            state_revisions: BTreeMap::new(),
        }
    }
}

impl TestStorage {
    /// Replace fixture data directly, including intentionally corrupt state.
    pub fn set_state(&self, state: Attested<EntityState>) {
        let mut data = self.data.lock().unwrap();
        data.revision += 1;
        let revision = data.revision;
        data.state_revisions.insert(state.payload.entity_id, revision);
        data.states.insert(state.payload.entity_id, state);
    }

    pub fn fail_next_commit(&self) { self.fail_next_commit.store(true, Ordering::SeqCst); }

    /// Fail the next map write part way, after the first of its writes.
    pub fn fail_next_map_write(&self) { self.fail_next_map_write.store(true, Ordering::SeqCst); }

    /// Fail the next read of a snapshot, as an engine fails one that finds
    /// the state it reads moved: a write conflict, to retry with another
    /// snapshot.
    pub fn fail_next_snapshot_read(&self) { self.fail_next_snapshot_read.store(true, Ordering::SeqCst); }

    /// Record every map read that transactions and snapshots answer from now
    /// on, until [`recorded_map_reads`](Self::recorded_map_reads).
    pub fn record_map_reads(&self) { *self.map_reads.lock().unwrap() = Some(Vec::new()); }

    /// The map reads answered since [`record_map_reads`](Self::record_map_reads),
    /// in order, and stop recording.
    pub fn recorded_map_reads(&self) -> Vec<MapRead> { self.map_reads.lock().unwrap().take().expect("map reads were being recorded") }
}

pub(crate) struct TestStorageTransaction<'a> {
    storage: &'a TestStorage,
    states: Vec<(Clock, Attested<EntityState>)>,
    events: Vec<Attested<Event>>,
    maps: TransactionMaps<'a>,

    /// Set by `poison`, and by each map call from its start until it returns
    /// `Ok`, so that a call that fails, or is dropped part way, leaves it set:
    /// every later call and the commit then fail.
    poisoned: bool,

    /// The greatest lock name taken so far, which each map call's names
    /// ascend past.
    last_lock: Option<LockName>,
}

#[async_trait::async_trait]
impl StorageTransaction for TestStorageTransaction<'_> {
    async fn add_events(&mut self, events: &[Attested<Event>]) -> Result<(), MutationError> {
        if self.poisoned {
            return Err(MutationError::TransactionFailed);
        }
        self.events.extend_from_slice(events);
        Ok(())
    }

    async fn set_state(&mut self, expected_head: &Clock, state: &Attested<EntityState>) -> Result<(), MutationError> {
        if self.poisoned {
            return Err(MutationError::TransactionFailed);
        }
        if self.maps.made_map_calls() {
            return Err(MutationError::InvalidUpdate("state set after the transaction's first map call"));
        }
        let hold = self.storage.hold_set_state.lock().unwrap().take();
        if let Some((entered, release)) = hold {
            entered.send(()).unwrap();
            release.await.unwrap();
        }
        if let Some((_, previous)) = self.states.iter_mut().find(|(_, previous)| previous.payload.entity_id == state.payload.entity_id) {
            if previous.payload.state.head != *expected_head {
                return Err(MutationError::InvalidUpdate("state does not follow the preceding transaction write"));
            }
            *previous = state.clone();
        } else {
            self.states.push((expected_head.clone(), state.clone()));
        }
        Ok(())
    }

    async fn commit(self) -> Result<StorageCommitOutcome, MutationError> {
        if self.poisoned {
            return Err(MutationError::TransactionFailed);
        }
        let hold = self.storage.hold_commit.lock().unwrap().take();
        if let Some((entered, release)) = hold {
            entered.send(()).unwrap();
            release.await.unwrap();
        }
        if self.storage.fail_next_commit.swap(false, Ordering::SeqCst) {
            return Err(MutationError::General("test storage commit failed".into()));
        }
        let storage = self.storage;
        let result = self.commit_to_storage()?;
        let hold = storage.hold_after_commit.lock().unwrap().take();
        if let Some((entered, release)) = hold {
            entered.send(()).unwrap();
            release.await.unwrap();
        }
        Ok(result)
    }

    async fn map_lock_and_read(&mut self, locks: &[LockName], reads: Vec<MapRead>) -> Result<Vec<MapRows>, MutationError> {
        self.storage.before_map_call().await;
        maps::record(&self.storage.map_reads, &reads);
        transaction_call(&mut self.poisoned, &mut self.last_lock, locks, self.maps.lock_and_read(self.storage, locks, reads)).await
    }

    async fn map_write(&mut self, writes: Vec<MapWrite>) -> Result<(), MutationError> {
        self.storage.before_map_call().await;
        transaction_call(&mut self.poisoned, &mut self.last_lock, &[], self.maps.write(self.storage, writes)).await
    }

    fn poison(&mut self) { self.poisoned = true; }
}

impl TestStorageTransaction<'_> {
    fn commit_to_storage(self) -> Result<StorageCommitOutcome, MutationError> {
        let mut data = self.storage.data.lock().unwrap();
        let mut maps = self.maps;
        maps.check(&data)?;
        let observed: BTreeMap<_, _> = self
            .states
            .iter()
            .map(|(_, state)| {
                let id = state.payload.entity_id;
                (id, data.states.get(&id).cloned())
            })
            .collect();
        if self.states.iter().any(|(expected_head, state)| {
            observed[&state.payload.entity_id].as_ref().map(|state| state.payload.state.head.clone()).unwrap_or_default() != *expected_head
        }) {
            return Ok(StorageCommitOutcome::Conflict { observed });
        }
        let mut entities = Vec::new();
        if !self.states.is_empty() {
            data.revision += 1;
        }
        for (_, attested_state) in self.states {
            let entity_id = attested_state.payload.entity_id;
            let previous = observed[&entity_id].as_ref();
            let state = &attested_state.payload.state;
            entities.push(CommittedEntityWrite {
                entity_id,
                canonical_changed: previous.is_none_or(|previous| previous.payload.state.head != state.head),
            });
            let revision = data.revision;
            data.state_revisions.insert(entity_id, revision);
            data.states.insert(entity_id, attested_state);
        }
        for event in self.events {
            data.events.insert(event.payload.id(), event);
        }
        maps.land(&mut data.maps);
        Ok(StorageCommitOutcome::Committed(StorageCommitResult { entities }))
    }
}

#[async_trait::async_trait]
impl StorageEngine for TestStorage {
    type Value = crate::value::Value;
    type Transaction<'a> = TestStorageTransaction<'a>;
    type Snapshot = TestMapSnapshot;

    fn transaction(&self) -> Self::Transaction<'_> {
        TestStorageTransaction {
            storage: self,
            states: Vec::new(),
            events: Vec::new(),
            maps: TransactionMaps::new(&self.locks),
            poisoned: false,
            last_lock: None,
        }
    }

    /// A snapshot holds the committed maps as it found them, so it waits on
    /// no lock, and no commit waits on it.
    async fn map_snapshot(&self, _locks: &[LockName]) -> Result<TestMapSnapshot, RetrievalError> {
        let (open, now_open) = OpenSnapshot::begin(&self.open_snapshots);
        if self.most_open_snapshots.lock().unwrap().is_some_and(|most| now_open > most) {
            return Err(RetrievalError::storage("test storage holds no more snapshots open at once"));
        }

        let maps = self.data.lock().unwrap().maps.clone();
        Ok(TestMapSnapshot {
            maps,
            fail_next_read: Arc::clone(&self.fail_next_snapshot_read),
            reads: Arc::clone(&self.map_reads),
            _open: open,
        })
    }

    async fn state_snapshot(&self, scan: crate::storage::StateScan<'_>) -> Result<Box<dyn crate::storage::StateSnapshot>, RetrievalError> {
        let snapshot =
            states::Snapshot::new(&self.data.lock().unwrap(), scan, self.hold_state_page.clone(), self.reject_state_finish.clone())?;
        let hold = self.hold_state_snapshot.lock().unwrap().take();
        if let Some((entered, release)) = hold {
            entered.send(()).unwrap();
            release.await.unwrap();
        }
        Ok(Box::new(snapshot))
    }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        if *self.reject_state_read.lock().unwrap() == Some(id) {
            return Err(RetrievalError::Other("unexpected state reload".into()));
        }
        self.data.lock().unwrap().states.get(&id).cloned().ok_or(RetrievalError::EntityNotFound(id))
    }

    async fn fetch_states(
        &self,
        selection: &ankql::ast::Selection<ankql::ast::Resolved>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        assert!(selection.order_by.is_none() && selection.limit.is_none());
        let predicate = selection.predicate.assume_null(&[]);
        let models = selection.predicate.referenced_models();
        let states: Vec<_> = self.data.lock().unwrap().states.values().cloned().collect();
        let hold = {
            let mut holds = self.hold_fetch.lock().unwrap();
            models.iter().find_map(|model| holds.remove(model))
        };
        if let Some((entered, release)) = hold {
            entered.send(()).unwrap();
            release.await.unwrap();
        }
        let mut matching = Vec::new();
        for state in states {
            // Membership-only scans do not decode unrelated property backends.
            if let ankql::ast::Predicate::MemberOf(model) = &predicate {
                if state.payload.state.memberships.contains(model) {
                    matching.push(state);
                }
                continue;
            }
            let entity = crate::entity::TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?;
            match crate::selection::filter::evaluate_predicate(&entity, &predicate) {
                Ok(true) => matching.push(state),
                Ok(false) | Err(crate::selection::filter::Error::PropertyNotFound(_)) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(matching)
    }

    async fn get_events(&self, ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let data = self.data.lock().unwrap();
        Ok(ids.into_iter().filter_map(|id| data.events.get(&id).cloned()).collect())
    }

    async fn dump_entity_events(&self, id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        Ok(self.data.lock().unwrap().events.values().filter(|event| event.payload.entity_id == id).cloned().collect())
    }

    /// Counting the reset fails the later map calls and the commit of every
    /// transaction that made map calls before it.
    async fn delete_all(&self) -> Result<bool, MutationError> {
        let mut data = self.data.lock().unwrap();
        *data = TestData { resets: data.resets + 1, ..TestData::default() };
        Ok(true)
    }

    async fn list_materializations(&self) -> Result<Vec<ModelId>, RetrievalError> {
        Ok(self
            .data
            .lock()
            .unwrap()
            .states
            .values()
            .flat_map(|state| state.payload.state.memberships.iter().copied())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect())
    }
}
