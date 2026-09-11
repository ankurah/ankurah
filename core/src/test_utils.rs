use crate::error::{MutationError, RetrievalError};
use crate::storage::{StorageCollection, StorageEngine};
use ankurah_proto::{Attested, CollectionId, EntityId, EntityState, Event, EventId};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

/// In-memory storage for system-root lifecycle tests, not a query-engine substitute.
#[derive(Default)]
pub(crate) struct TestStorage(Mutex<BTreeMap<CollectionId, Arc<TestCollection>>>);

#[derive(Default)]
pub(crate) struct TestCollection {
    states: Mutex<BTreeMap<EntityId, Attested<EntityState>>>,
    events: Mutex<BTreeMap<EventId, Attested<Event>>>,
    pub hold_fetch: Mutex<Option<(tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>)>>,
    pub hold_set_state: Mutex<Option<(tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>)>>,
}

impl TestStorage {
    pub fn table(&self, id: &CollectionId) -> Arc<TestCollection> { self.0.lock().unwrap().entry(id.clone()).or_default().clone() }
}

#[async_trait::async_trait]
impl StorageEngine for TestStorage {
    type Value = crate::value::Value;

    async fn collection(&self, id: &CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> { Ok(self.table(id)) }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        for table in self.0.lock().unwrap().values() {
            table.states.lock().unwrap().clear();
            table.events.lock().unwrap().clear();
        }
        Ok(true)
    }
}

#[async_trait::async_trait]
impl StorageCollection for TestCollection {
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, MutationError> {
        let hold = self.hold_set_state.lock().unwrap().take();
        if let Some((entered, release)) = hold {
            entered.send(()).unwrap();
            release.await.unwrap();
        }
        self.states.lock().unwrap().insert(state.payload.entity_id, state);
        Ok(true)
    }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        self.states.lock().unwrap().get(&id).cloned().ok_or(RetrievalError::EntityNotFound(id))
    }

    async fn fetch_states(
        &self,
        selection: &ankql::ast::Selection<ankql::ast::Resolved>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        assert!(matches!(selection.predicate, ankql::ast::Predicate::True));
        assert!(selection.order_by.is_none() && selection.limit.is_none());
        let states = self.states.lock().unwrap().values().cloned().collect();
        let hold = self.hold_fetch.lock().unwrap().take();
        if let Some((entered, release)) = hold {
            entered.send(()).unwrap();
            release.await.unwrap();
        }
        Ok(states)
    }

    async fn add_event(&self, event: &Attested<Event>) -> Result<bool, MutationError> {
        Ok(self.events.lock().unwrap().insert(event.payload.id(), event.clone()).is_none())
    }

    async fn get_events(&self, ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let events = self.events.lock().unwrap();
        ids.into_iter().map(|id| events.get(&id).cloned().ok_or(RetrievalError::EventNotFound(id))).collect()
    }

    async fn dump_entity_events(&self, id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        Ok(self.events.lock().unwrap().values().filter(|event| event.payload.entity_id == id).cloned().collect())
    }
}
