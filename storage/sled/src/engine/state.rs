//! Canonical-state continuation with bounded reads validated against the commit counter.

use std::{num::NonZeroUsize, ops::Bound, sync::Arc};

use ankurah_core::{
    entity::TemporaryEntity,
    error::{MutationError, RetrievalError},
    selection::{
        filter::{evaluate_predicate, membership_match},
        ordering::OrderBoundary,
    },
    storage::{StateCursor, StateScan, StateSnapshot, StorageEngine},
};
use ankurah_proto::{Attested, EntityId, EntityState, StateFragment};
use ankurah_storage_common::state::CounterCursor;
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
                sled::Error::CollectionNotFound(_) => MutationError::WriteConflict.into(),
                error => sled_error(error),
            })?
            .ok_or_else(|| RetrievalError::storage("missing canonical state cursor metadata"))
    };
    let incarnation = read(INCARNATION)?.as_ref().try_into().map_err(RetrievalError::storage)?;
    let position = decode_counter(&read(COUNTER)?)?;
    Ok(CounterCursor { incarnation, position })
}

fn validate(database: &Database, target: CounterCursor) -> Result<(), RetrievalError> {
    let now = current(database)?;
    if now != target {
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
    selected: Option<std::vec::IntoIter<Attested<EntityState>>>,
    selection: Option<ankql::ast::Selection<ankql::ast::Resolved>>,
}

impl SledStateSnapshot {
    pub(super) async fn begin(engine: &SledStorageEngine, scan: StateScan<'_>) -> Result<Self, RetrievalError> {
        let (since, selection) = match scan {
            StateScan::Changes(since) => (since.cloned(), None),
            StateScan::SelectionChanges { selection, since } => {
                if selection.limit.is_none() {
                    return Err(RetrievalError::Other("a selection change snapshot requires LIMIT".into()));
                }
                (since.cloned(), Some(ankql::selection::deterministic_order(selection).into_owned()))
            }
            StateScan::Selection(selection) => {
                if selection.limit.is_none() {
                    return Err(RetrievalError::Other("a selection snapshot requires LIMIT".into()));
                }
                (None, None)
            }
        };
        let database = engine.database.lock().unwrap().clone();
        let mut snapshot = tokio::task::spawn_blocking(move || {
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
            Ok::<_, RetrievalError>(Self { database, target, after, exhausted: false, selected: None, selection })
        })
        .await??;
        if let StateScan::Selection(selection) = scan {
            let selected = engine.fetch_states(selection).await?;
            snapshot.validate().await?;
            snapshot.selected = Some(selected.into_iter());
        }
        Ok(snapshot)
    }

    async fn validate(&self) -> Result<(), RetrievalError> {
        let database = self.database.clone();
        let target = self.target;
        tokio::task::spawn_blocking(move || {
            let _guard = database.index_manager.mutation_lock.lock().unwrap();
            validate(&database, target)
        })
        .await?
    }
}

#[async_trait]
impl StateSnapshot for SledStateSnapshot {
    async fn matching_ids(&mut self, ids: &[EntityId]) -> Result<Vec<EntityId>, RetrievalError> {
        let selection = self.selection.clone().ok_or_else(|| RetrievalError::InvalidState("snapshot has no prepared selection".into()))?;
        let states = self.get_states(ids).await?;
        let result = states
            .into_iter()
            .flatten()
            .filter_map(|state| {
                if let Some(matches) = membership_match(&selection.predicate, &|model| state.payload.state.memberships.contains(model)) {
                    return matches.then_some(Ok(state.payload.entity_id));
                }
                let entity = match TemporaryEntity::new(state.payload.entity_id, &state.payload.state) {
                    Ok(entity) => entity,
                    Err(error) => return Some(Err(error)),
                };
                evaluate_predicate(&entity, &selection.predicate).unwrap_or(false).then_some(Ok(state.payload.entity_id))
            })
            .collect();
        self.validate().await?;
        result
    }

    async fn select_after(
        &mut self,
        boundary: Option<&OrderBoundary>,
        limit: NonZeroUsize,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let mut selection =
            self.selection.clone().ok_or_else(|| RetrievalError::InvalidState("snapshot has no prepared selection".into()))?;
        selection.limit = Some(limit.get() as u64);
        self.validate().await?;
        let result = super::query::fetch_after(self.database.clone(), selection, boundary).await;
        self.validate().await?;
        result
    }

    async fn next_page(&mut self, limit: NonZeroUsize) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        if self.selected.is_some() {
            self.validate().await?;
            return Ok(self.selected.as_mut().unwrap().by_ref().take(limit.get()).collect());
        }
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

    async fn finish(self: Box<Self>) -> Result<StateCursor, RetrievalError> {
        tokio::task::spawn_blocking(move || {
            let _guard = self.database.index_manager.mutation_lock.lock().unwrap();
            validate(&self.database, self.target)?;
            Ok(self.target.encode(ENGINE))
        })
        .await?
    }
}
