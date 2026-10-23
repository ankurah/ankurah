//! Sled storage that counts each node's read calls, so a record can report the storage work
//! behind its latency.

use std::sync::Mutex;

use ankql::ast::{Predicate, Resolved, Selection};
use ankurah::{
    core::storage::{CatalogResolver, GetStateResult, StorageEngine},
    error::{MutationError, RetrievalError},
    proto,
};
use ankurah_storage_sled::SledStorageEngine;
use async_trait::async_trait;
use serde::Serialize;

/// The read calls one node made through its storage engine. These count calls and requested ids,
/// not disk reads or cache misses.
#[derive(Default, Serialize)]
pub struct StorageReads {
    /// Calls that read states by selection.
    pub selections: usize,
    /// States those calls returned.
    pub returned_states: usize,
    /// The serialized size of those states.
    pub returned_state_bytes: usize,
    /// Entities read by id.
    pub point_reads: usize,
    /// Calls that read events by id.
    pub event_calls: usize,
    /// Events those calls requested.
    pub requested_events: usize,
}

/// A Sled storage engine that counts its read calls.
pub struct CountingStorage {
    inner: SledStorageEngine,
    reads: Mutex<StorageReads>,
}

impl CountingStorage {
    pub fn new() -> anyhow::Result<Self> { Ok(Self { inner: SledStorageEngine::new_test()?, reads: Mutex::default() }) }

    /// Returns the reads counted since the previous call, and counts from zero again.
    pub fn take_reads(&self) -> StorageReads { std::mem::take(&mut *self.reads.lock().unwrap()) }
}

#[async_trait]
impl StorageEngine for CountingStorage {
    type Value = <SledStorageEngine as StorageEngine>::Value;
    type Transaction<'a> = <SledStorageEngine as StorageEngine>::Transaction<'a>;

    fn transaction(&self) -> Self::Transaction<'_> { self.inner.transaction() }

    async fn get_state(&self, id: proto::EntityId) -> Result<proto::Attested<proto::EntityState>, RetrievalError> {
        self.reads.lock().unwrap().point_reads += 1;
        self.inner.get_state(id).await
    }

    async fn fetch_states(&self, selection: &Selection<Resolved>) -> Result<Vec<proto::Attested<proto::EntityState>>, RetrievalError> {
        let states = self.inner.fetch_states(selection).await?;
        let mut reads = self.reads.lock().unwrap();
        reads.selections += 1;
        reads.returned_states += states.len();
        reads.returned_state_bytes += states.iter().map(|state| bincode::serialized_size(state).unwrap() as usize).sum::<usize>();
        Ok(states)
    }

    async fn get_states(&self, ids: Vec<proto::EntityId>, predicate: &Predicate<Resolved>) -> Result<Vec<GetStateResult>, RetrievalError> {
        self.reads.lock().unwrap().point_reads += ids.len();
        self.inner.get_states(ids, predicate).await
    }

    async fn get_events(&self, ids: Vec<proto::EventId>) -> Result<Vec<proto::Attested<proto::Event>>, RetrievalError> {
        {
            let mut reads = self.reads.lock().unwrap();
            reads.event_calls += 1;
            reads.requested_events += ids.len();
        }
        self.inner.get_events(ids).await
    }

    async fn dump_entity_events(&self, id: proto::EntityId) -> Result<Vec<proto::Attested<proto::Event>>, RetrievalError> {
        self.inner.dump_entity_events(id).await
    }

    async fn delete_all(&self) -> Result<bool, MutationError> { self.inner.delete_all().await }

    async fn list_materializations(&self) -> Result<Vec<proto::ModelId>, RetrievalError> { self.inner.list_materializations().await }

    fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn CatalogResolver>) { self.inner.set_catalog_resolver(resolver); }
}
