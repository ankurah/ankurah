use crate::error::{MutationError, RetrievalError};
use crate::storage::{CommittedEntityWrite, StorageCommitOutcome, StorageCommitResult, StorageEngine, StorageTransaction};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, Event, EventId, ModelId};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
};

type Pause = (tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>);

/// In-memory storage for core lifecycle tests, not a query-engine substitute.
#[derive(Default)]
pub(crate) struct TestStorage {
    data: Mutex<TestData>,
    pub hold_fetch: Mutex<BTreeMap<ModelId, Pause>>,
    pub hold_commit: Mutex<Option<Pause>>,
    pub hold_after_commit: Mutex<Option<Pause>>,
    pub reject_state_read: Mutex<Option<EntityId>>,
    fail_next_commit: AtomicBool,
}

#[derive(Default)]
struct TestData {
    states: BTreeMap<EntityId, Attested<EntityState>>,
    events: BTreeMap<EventId, Attested<Event>>,
}

impl TestStorage {
    /// Replace fixture data directly, including intentionally corrupt state.
    pub fn set_state(&self, state: Attested<EntityState>) { self.data.lock().unwrap().states.insert(state.payload.entity_id, state); }

    pub fn fail_next_commit(&self) { self.fail_next_commit.store(true, Ordering::SeqCst); }
}

pub(crate) struct TestStorageTransaction<'a> {
    storage: &'a TestStorage,
    states: Vec<(Clock, Attested<EntityState>)>,
    events: Vec<Attested<Event>>,
}

#[async_trait::async_trait]
impl StorageTransaction for TestStorageTransaction<'_> {
    async fn add_events(&mut self, events: &[Attested<Event>]) -> Result<(), MutationError> {
        self.events.extend_from_slice(events);
        Ok(())
    }

    async fn set_state(&mut self, expected_head: &Clock, state: &Attested<EntityState>) -> Result<(), MutationError> {
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
}

impl TestStorageTransaction<'_> {
    fn commit_to_storage(self) -> Result<StorageCommitOutcome, MutationError> {
        let mut data = self.storage.data.lock().unwrap();
        let observed: BTreeMap<_, _> = self
            .states
            .iter()
            .map(|(_, state)| {
                let id = state.payload.entity_id;
                (id, data.states.get(&id).cloned())
            })
            .collect();
        if self.states.iter().any(|(expected_head, state)| {
            observed[&state.payload.entity_id].as_ref().map(|state| state.payload.state.head.clone()).unwrap_or_default()
                != *expected_head
        }) {
            return Ok(StorageCommitOutcome::Conflict { observed });
        }
        let mut entities = Vec::new();
        for (_, attested_state) in self.states {
            let entity_id = attested_state.payload.entity_id;
            let previous = observed[&entity_id].as_ref();
            let state = &attested_state.payload.state;
            entities.push(CommittedEntityWrite {
                entity_id,
                canonical_changed: previous.is_none_or(|previous| previous.payload.state.head != state.head),
            });
            data.states.insert(entity_id, attested_state);
        }
        for event in self.events {
            data.events.insert(event.payload.id(), event);
        }
        Ok(StorageCommitOutcome::Committed(StorageCommitResult { entities }))
    }
}

#[async_trait::async_trait]
impl StorageEngine for TestStorage {
    type Value = crate::value::Value;
    type Transaction<'a> = TestStorageTransaction<'a>;

    fn transaction(&self) -> Self::Transaction<'_> { TestStorageTransaction { storage: self, states: Vec::new(), events: Vec::new() } }

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
                if state.payload.state.memberships.contains(model) { matching.push(state); }
                continue;
            }
            let entity = crate::entity::TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?;
            if crate::selection::filter::evaluate_predicate(&entity, &predicate)
                .map_err(|error| RetrievalError::Other(error.to_string()))?
            {
                matching.push(state);
            }
        }
        Ok(matching)
    }

    async fn get_events(&self, ids: Vec<EventId>, predicate: &ankql::ast::Predicate<ankql::ast::Resolved>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let events = {
            let data = self.data.lock().unwrap();
            ids.into_iter().filter_map(|id| data.events.get(&id).cloned()).collect()
        };
        crate::storage::filter_events(self, events, predicate).await
    }

    async fn dump_entity_events(&self, id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        Ok(self.data.lock().unwrap().events.values().filter(|event| event.payload.entity_id == id).cloned().collect())
    }

    async fn delete_all(&self) -> Result<bool, MutationError> {
        *self.data.lock().unwrap() = TestData::default();
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
