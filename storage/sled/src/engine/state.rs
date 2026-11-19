//! Canonical-state continuation with bounded reads validated against the commit counter.

use std::{num::NonZeroUsize, ops::Bound, sync::Arc};

use ankql::ast::{Resolved, Selection};
use ankurah_core::{
    error::{MutationError, RetrievalError},
    storage::{StateCursor, StateSnapshot},
};
use ankurah_proto::{Attested, EntityId, EntityState, StateFragment};
use ankurah_storage_common::{selection::LimitedSelection, state::CounterCursor};
use async_trait::async_trait;

use crate::{database::Database, error::sled_error, SledStorageEngine};

pub(super) const COUNTER: &[u8] = b"counter";
const INCARNATION: &[u8] = b"incarnation";
const ENGINE: &[u8] = b"sled";

pub(crate) fn initialize(changes: &sled::Tree, entities: &sled::Tree) -> Result<(), RetrievalError> {
    if changes.get(INCARNATION).map_err(sled_error)?.is_some() {
        return Ok(());
    }
    // An older store starts at zero. Until initialization finishes the engine
    // is not available for commits; a retry can safely repeat this backfill.
    for row in entities.iter() {
        let (key, _) = row.map_err(sled_error)?;
        let id = EntityId::from_bytes(key.as_ref().try_into().map_err(RetrievalError::storage)?);
        changes.insert(member_key(id), 0u64.to_be_bytes().as_slice()).map_err(sled_error)?;
        changes.insert(change_key(0, id), &[]).map_err(sled_error)?;
    }
    let mut batch = sled::Batch::default();
    batch.insert(COUNTER, 0u64.to_be_bytes().as_slice());
    batch.insert(INCARNATION, CounterCursor::fresh().incarnation.as_slice());
    changes.apply_batch(batch).map_err(sled_error)?;
    Ok(())
}

pub(super) fn decode_counter(bytes: &[u8]) -> Result<u64, RetrievalError> {
    Ok(u64::from_be_bytes(bytes.try_into().map_err(RetrievalError::storage)?))
}

pub(super) fn member_key(id: EntityId) -> Vec<u8> {
    let mut key = Vec::with_capacity(33);
    key.push(b'm');
    key.extend_from_slice(&id.to_bytes());
    key
}

pub(super) fn change_key(position: u64, id: EntityId) -> Vec<u8> {
    let mut key = Vec::with_capacity(41);
    key.push(b'r');
    key.extend_from_slice(&position.to_be_bytes());
    key.extend_from_slice(&id.to_bytes());
    key
}

fn current(database: &Database) -> Result<CounterCursor, RetrievalError> {
    let read = |key| {
        database
            .state_changes
            .get(key)
            .map_err(|error| match error {
                sled::Error::CollectionNotFound(_) => RetrievalError::InvalidStateCursor,
                error => sled_error(error),
            })?
            .ok_or(RetrievalError::InvalidStateCursor)
    };
    let incarnation = read(INCARNATION)?.as_ref().try_into().map_err(RetrievalError::storage)?;
    let position = decode_counter(&read(COUNTER)?)?;
    Ok(CounterCursor { incarnation, position })
}

fn validate(database: &Database, target: CounterCursor) -> Result<(), RetrievalError> {
    let now = current(database)?;
    if now.incarnation != target.incarnation {
        return Err(RetrievalError::InvalidStateCursor);
    }
    if now.position != target.position {
        return Err(MutationError::WriteConflict.into());
    }
    Ok(())
}

fn load(database: &Database, id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError> {
    database
        .entities_tree
        .get(id.to_bytes())
        .map_err(sled_error)?
        .map(|bytes| {
            let fragment: StateFragment = bincode::deserialize(&bytes)?;
            Ok(Attested::<EntityState>::from_parts(id, fragment))
        })
        .transpose()
}

/// A sequence of bounded reads at one counter value. A canonical commit
/// between reads invalidates the attempt; callers discard its provisional work.
pub(super) struct SledStateSnapshot {
    database: Arc<Database>,
    target: CounterCursor,
    after: Vec<u8>,
    exhausted: bool,
}

impl SledStateSnapshot {
    pub(super) async fn begin(engine: &SledStorageEngine, since: Option<&StateCursor>) -> Result<Self, RetrievalError> {
        let database = engine.database.lock().unwrap().clone();
        let since = since.cloned();
        tokio::task::spawn_blocking(move || {
            let guard = database.index_manager.mutation_lock.lock().unwrap();
            let target = current(&database)?;
            let since = target.resume(since.as_ref(), ENGINE)?;
            // The upper end of a counter's entity keys excludes that entire
            // commit, without incrementing a possibly maximal counter.
            let after = since.map_or_else(
                || vec![b'r'],
                |position| {
                    let mut key = change_key(position, EntityId::from_bytes([u8::MAX; 32]));
                    key.push(0);
                    key
                },
            );
            drop(guard);
            Ok(Self { database, target, after, exhausted: false })
        })
        .await?
    }
}

#[async_trait]
impl StateSnapshot for SledStateSnapshot {
    async fn next_page(&mut self, limit: NonZeroUsize) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let database = self.database.clone();
        let target = self.target;
        let after = self.after.clone();
        let exhausted = self.exhausted;
        let (states, last) = tokio::task::spawn_blocking(move || {
            let _guard = database.index_manager.mutation_lock.lock().unwrap();
            validate(&database, target)?;
            let mut states = Vec::new();
            let mut last = after.clone();
            if !exhausted {
                for row in database.state_changes.range((Bound::Excluded(after), Bound::Excluded(vec![b's']))).take(limit.get()) {
                    let (key, _) = row.map_err(sled_error)?;
                    let bytes = key.get(9..).ok_or_else(|| RetrievalError::storage("invalid state change key"))?;
                    let id = EntityId::from_bytes(bytes.try_into().map_err(RetrievalError::storage)?);
                    states.push(load(&database, id)?.ok_or_else(|| RetrievalError::storage("state change names a missing entity"))?);
                    last = key.to_vec();
                }
            }
            Ok::<_, RetrievalError>((states, last))
        })
        .await??;
        self.after = last;
        self.exhausted |= states.is_empty();
        Ok(states)
    }

    async fn get_states(&mut self, ids: &[EntityId]) -> Result<Vec<Option<Attested<EntityState>>>, RetrievalError> {
        let database = self.database.clone();
        let target = self.target;
        let ids = ids.to_vec();
        tokio::task::spawn_blocking(move || {
            let _guard = database.index_manager.mutation_lock.lock().unwrap();
            validate(&database, target)?;
            ids.into_iter().map(|id| load(&database, id)).collect()
        })
        .await?
    }

    async fn limited_states(&mut self, selection: &Selection<Resolved>) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let database = self.database.clone();
        let target = self.target;
        let selection = selection.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = database.index_manager.mutation_lock.lock().unwrap();
            validate(&database, target)?;
            let mut selected = LimitedSelection::new(&selection)?;
            if selection.limit == Some(0) {
                return Ok(selected.finish());
            }
            for row in database.entities_tree.iter() {
                let (key, bytes) = row.map_err(sled_error)?;
                let id = EntityId::from_bytes(key.as_ref().try_into().map_err(RetrievalError::storage)?);
                selected.consider(Attested::<EntityState>::from_parts(id, bincode::deserialize::<StateFragment>(&bytes)?))?;
            }
            Ok(selected.finish())
        })
        .await?
    }

    async fn finish(self: Box<Self>) -> Result<StateCursor, RetrievalError> {
        tokio::task::spawn_blocking(move || {
            let _guard = self.database.index_manager.mutation_lock.lock().unwrap();
            validate(&self.database, self.target)?;
            Ok(self.target.encode(ENGINE))
        })
        .await?
    }
}
