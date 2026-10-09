//! Fixtures shared by the sled integration tests.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use ankurah_core::{
    indexing::KeySpec,
    property::backend::{LWWBackend, PropertyBackend},
    storage::{StorageCommitOutcome, StorageEngine, StorageTransaction},
    value::Value,
};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, EventId, ModelId, PropertyId, State, StateBuffers};
use ankurah_storage_sled::SledStorageEngine;

pub fn model() -> ModelId { ModelId::EntityId(EntityId::from_bytes([101; 32])) }

/// A property identity for a fixture, distinct per `tag`.
pub fn property(tag: u8) -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([tag; 32])) }

pub fn entity_id(byte: u8) -> EntityId { EntityId::from_bytes([byte; 32]) }

/// Commit an entity with id `entity_id(byte)`, a member of `model`, holding `values`.
pub async fn insert(engine: &SledStorageEngine, model: ModelId, byte: u8, values: &[(PropertyId, Value)]) -> anyhow::Result<()> {
    let backend = LWWBackend::new();
    for (property, value) in values {
        backend.set(*property, Some(value.clone()));
    }
    let event = EventId::from_bytes([byte; 32]);
    if let Some(operations) = backend.to_operations()? {
        backend.apply_operations_with_event(&operations, event.clone())?;
    }
    let state = Attested::opt(
        EntityState {
            entity_id: entity_id(byte),
            state: State {
                state_buffers: StateBuffers(BTreeMap::from([("lww".into(), backend.to_state_buffer()?)])),
                memberships: BTreeSet::from([model]),
                head: Clock::genesis(event),
            },
        },
        None,
    );
    let mut transaction = engine.transaction();
    transaction.set_state(&Clock::default(), &state).await?;
    assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));
    Ok(())
}

/// The key specs of the indexes the engine holds, to see which index a fetch built.
pub fn index_specs(engine: &SledStorageEngine) -> Vec<KeySpec<String>> {
    let database = engine.database.lock().unwrap().clone();
    let specs = database.index_manager.indexes.read().unwrap().values().map(|index| index.spec().clone()).collect();
    specs
}
