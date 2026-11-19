use super::maps::{MapEntries, TransactionMaps};
use super::*;
use crate::index::Index;
use ankurah_core::{
    property::backend::backend_from_string,
    storage::{
        transaction_call, CommittedEntityWrite, LockName, MapRead, MapRows, MapWrite, StorageCommitOutcome, StorageCommitResult,
        StorageTransaction,
    },
    value::Value,
};
use ankurah_proto::{AttestationSet, Clock, PropertyId, State};
use sled::{transaction::ConflictableTransactionError, IVec};

/// Sled encodings and property values for one atomic commit.
pub struct SledTransaction<'a> {
    engine: &'a SledStorageEngine,
    entities: Vec<EntityRow>,
    events: Vec<(IVec, IVec)>,

    /// Its map calls: from the first, the locks it holds, and its map writes
    /// until its commit lands them.
    maps: TransactionMaps,

    /// Set by `poison`, and by each map call from its start until it returns
    /// `Ok`, so that a call that fails, or is dropped part way, leaves it set:
    /// every later call and the commit then fail.
    poisoned: bool,

    /// The greatest lock name taken so far, which each map call's names
    /// ascend past.
    last_lock: Option<LockName>,
}

struct EntityRow {
    original_index: usize,
    entity_id: EntityId,
    expected_head: Clock,
    head: Clock,
    memberships: BTreeSet<ModelId>,
    encoded_state: IVec,
    encoded_memberships: IVec,
    values: Vec<(PropertyId, Option<Value>)>,
    materializations: Vec<PreparedMaterialization>,
}

struct PreparedMaterialization {
    model: ModelId,
    tree: sled::Tree,
    values: Arc<Vec<(u32, Value)>>,
    encoded: IVec,
}

// Preserve StateFragment's field order without cloning its owned State.
#[derive(serde::Serialize)]
struct StateFragmentRef<'a> {
    state: &'a State,
    attestations: &'a AttestationSet,
}

enum SledCommitAbort {
    Conflict(BTreeMap<EntityId, Option<Attested<EntityState>>>),
    Invalid(String),
}

fn sled_abort(error: impl std::fmt::Display) -> ConflictableTransactionError<SledCommitAbort> {
    ConflictableTransactionError::Abort(SledCommitAbort::Invalid(error.to_string()))
}

pub(super) fn encode_events(events: &[Attested<Event>]) -> Result<Vec<(IVec, IVec)>, MutationError> {
    events.iter().map(|event| Ok((IVec::from(event.payload.id().as_bytes()), IVec::from(bincode::serialize(event)?)))).collect()
}

impl<'a> SledTransaction<'a> {
    pub(super) fn new(engine: &'a SledStorageEngine) -> Self {
        Self { engine, entities: Vec::new(), events: Vec::new(), maps: TransactionMaps::default(), poisoned: false, last_lock: None }
    }
}

#[async_trait]
impl StorageTransaction for SledTransaction<'_> {
    async fn add_events(&mut self, events: &[Attested<Event>]) -> Result<(), MutationError> {
        if self.poisoned {
            return Err(MutationError::TransactionFailed);
        }
        self.events.extend(encode_events(events)?);
        Ok(())
    }

    async fn set_state(&mut self, expected_head: &Clock, state: &Attested<EntityState>) -> Result<(), MutationError> {
        if self.poisoned {
            return Err(MutationError::TransactionFailed);
        }
        if self.maps.made_map_calls() {
            return Err(MutationError::InvalidUpdate("state set after the transaction's first map call"));
        }
        let entity_id = state.payload.entity_id;
        let index = self.entities.iter().position(|write| write.entity_id == entity_id);
        if index.is_some_and(|index| self.entities[index].head != *expected_head) {
            return Err(MutationError::InvalidUpdate("state does not follow the preceding transaction write"));
        }
        let mut values = Vec::new();
        for (name, buffer) in state.payload.state.state_buffers.iter() {
            let backend = backend_from_string(name, Some(buffer))?;
            values.extend(backend.property_values());
        }
        let mut write = EntityRow {
            original_index: index.unwrap_or(self.entities.len()),
            entity_id,
            expected_head: expected_head.clone(),
            head: state.payload.state.head.clone(),
            memberships: state.payload.state.memberships.clone(),
            encoded_state: bincode::serialize(&StateFragmentRef { state: &state.payload.state, attestations: &state.attestations })?.into(),
            encoded_memberships: bincode::serialize(&state.payload.state.memberships)?.into(),
            values,
            materializations: Vec::new(),
        };
        if let Some(index) = index {
            write.expected_head = self.entities[index].expected_head.clone();
            self.entities[index] = write;
        } else {
            self.entities.push(write);
        }
        Ok(())
    }

    async fn commit(self) -> Result<StorageCommitOutcome, MutationError> {
        if self.poisoned {
            return Err(MutationError::TransactionFailed);
        }
        let Self { engine, entities, events, maps, .. } = self;
        let mut landing = maps.landing(engine).await;
        if entities.is_empty() && events.is_empty() && landing.is_empty() {
            return Ok(StorageCommitOutcome::Committed(StorageCommitResult::default()));
        }

        let database = engine.database.lock().unwrap().clone();
        tokio::task::spawn_blocking(move || {
            let entries = landing.entries(&database.maps_tree).map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
            let outcome = commit_staged(database, entities, events, entries);

            drop(landing);
            outcome
        })
        .await?
    }

    async fn map_lock_and_read(&mut self, locks: &[LockName], reads: Vec<MapRead>) -> Result<Vec<MapRows>, MutationError> {
        transaction_call(&mut self.poisoned, &mut self.last_lock, locks, self.maps.lock_and_read(self.engine, locks, reads)).await
    }

    async fn map_write(&mut self, writes: Vec<MapWrite>) -> Result<(), MutationError> {
        let (maps, engine) = (&mut self.maps, self.engine);
        transaction_call(&mut self.poisoned, &mut self.last_lock, &[], async move { Ok(maps.write(engine, writes).await) }).await
    }

    fn poison(&mut self) { self.poisoned = true; }
}

fn commit_staged(
    database: Arc<Database>,
    mut entities: Vec<EntityRow>,
    events: Vec<(IVec, IVec)>,
    maps: Option<MapEntries>,
) -> Result<StorageCommitOutcome, MutationError> {
    // Index creation/backfill takes this same guard. Keep the current tree set
    // fixed from preparation through the native multi-tree transaction.
    let index_guard = database.index_manager.mutation_lock.lock().unwrap();
    let index_snapshot: Vec<Index> = database.index_manager.indexes.read().unwrap().values().cloned().collect();
    for entity in &mut entities {
        let mut values = Vec::new();
        let mut seen_slots = std::collections::HashSet::new();
        for (property_id, value) in std::mem::take(&mut entity.values) {
            let slot = database.property_manager.slot_for(&property_id)?;
            if seen_slots.insert(slot) {
                if let Some(value) = value {
                    values.push((slot, value));
                }
            }
        }
        let encoded = IVec::from(bincode::serialize(&values)?);
        let values = Arc::new(values);
        for model in &entity.memberships {
            let tree = database.db.open_tree(model_tree_name(model)).map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
            entity.materializations.push(PreparedMaterialization { model: *model, tree, values: values.clone(), encoded: encoded.clone() });
        }
    }
    entities.sort_by_key(|entity| entity.entity_id);
    let mut trees = vec![
        database.entities_tree.clone(),
        database.entity_models_tree.clone(),
        database.events_tree.clone(),
        database.state_changes.clone(),
    ];
    let mut tree_positions = BTreeMap::<Vec<u8>, usize>::new();
    for (position, tree) in trees.iter().enumerate() {
        tree_positions.insert(tree.name().to_vec(), position);
    }
    for entity in &entities {
        for materialization in &entity.materializations {
            for tree in std::iter::once(&materialization.tree).chain(index_snapshot.iter().map(Index::tree)) {
                let name = tree.name().to_vec();
                if !tree_positions.contains_key(&name) {
                    let position = trees.len();
                    trees.push(tree.clone());
                    tree_positions.insert(name, position);
                }
            }
        }
    }
    // The maps' tree joins the sled transaction only when it lands map writes.
    let maps = maps.map(|entries| {
        trees.push(database.maps_tree.clone());
        (trees.len() - 1, entries)
    });
    let attempt: Result<StorageCommitResult, TransactionError<SledCommitAbort>> = trees.as_slice().transaction(|transactional| {
        let canonical = &transactional[0];
        let associations = &transactional[1];
        let mut observed = BTreeMap::new();
        let mut conflict = false;
        for prepared in &entities {
            let entity_id = prepared.entity_id;
            let key = entity_id.to_bytes();
            let current = canonical
                .get(key)?
                .map(|bytes| {
                    bincode::deserialize::<StateFragment>(bytes.as_ref())
                        .map(|fragment| Attested::<EntityState>::from_parts(entity_id, fragment))
                        .map_err(sled_abort)
                })
                .transpose()?;
            let current_head = current.as_ref().map(|state| state.payload.state.head.clone()).unwrap_or_default();
            if current_head != prepared.expected_head {
                conflict = true;
            }
            observed.insert(entity_id, current);
        }
        if conflict {
            return Err(ConflictableTransactionError::Abort(SledCommitAbort::Conflict(observed)));
        }

        for (key, value) in &events {
            if transactional[2].get(key)?.is_none() {
                transactional[2].insert(key.clone(), value.clone())?;
            }
        }
        let changes = &transactional[3];
        let position = if entities.is_empty() {
            None
        } else {
            let current = changes.get(state::COUNTER)?.ok_or_else(|| sled_abort("missing canonical state counter"))?;
            let next = state::decode_counter(&current)
                .map_err(sled_abort)?
                .checked_add(1)
                .ok_or_else(|| sled_abort("canonical state counter exhausted"))?;
            changes.insert(state::COUNTER, next.to_be_bytes().as_slice())?;
            Some(next)
        };
        let mut results = Vec::with_capacity(entities.len());
        for prepared in &entities {
            let entity_id = prepared.entity_id;
            let key = entity_id.to_bytes().to_vec();
            let previous_models: BTreeSet<ModelId> = associations
                .get(&key)?
                .map(|bytes| bincode::deserialize(bytes.as_ref()).map_err(sled_abort))
                .transpose()?
                .unwrap_or_default();
            let models = &prepared.memberships;
            if !previous_models.is_subset(models) {
                return Err(ConflictableTransactionError::Abort(SledCommitAbort::Invalid(format!(
                    "canonical state for entity {entity_id} would remove durable memberships; membership removal is not supported"
                ))));
            }
            associations.insert(key.clone(), prepared.encoded_memberships.clone())?;
            canonical.insert(key.clone(), prepared.encoded_state.clone())?;
            if let Some(position) = position {
                let member_key = state::member_key(entity_id);
                if let Some(previous) = changes.get(&member_key)? {
                    let previous = state::decode_counter(&previous).map_err(sled_abort)?;
                    changes.remove(state::change_key(previous, entity_id))?;
                }
                changes.insert(member_key, position.to_be_bytes().as_slice())?;
                changes.insert(state::change_key(position, entity_id), &[])?;
            }

            for materialization in &prepared.materializations {
                let model_position = *tree_positions
                    .get(materialization.tree.name().as_ref())
                    .ok_or_else(|| sled_abort("prepared sled materialization tree is absent from transaction"))?;
                let model_tree = &transactional[model_position];
                let old_values = model_tree
                    .get(&key)?
                    .map(|bytes| bincode::deserialize::<Vec<(u32, ankurah_core::value::Value)>>(bytes.as_ref()).map_err(sled_abort))
                    .transpose()?;
                model_tree.insert(key.clone(), materialization.encoded.clone())?;

                for index in &index_snapshot {
                    let old_key = old_values
                        .as_deref()
                        .map(|values| index.build_key(&entity_id, &materialization.model, values))
                        .transpose()
                        .map_err(sled_abort)?
                        .flatten();
                    let new_key = index.build_key(&entity_id, &materialization.model, &materialization.values).map_err(sled_abort)?;
                    if old_key == new_key {
                        continue;
                    }
                    let index_position = *tree_positions
                        .get(index.tree().name().as_ref())
                        .ok_or_else(|| sled_abort("prepared sled index tree is absent from transaction"))?;
                    let index_tree = &transactional[index_position];
                    if let Some(old_key) = old_key {
                        index_tree.remove(old_key)?;
                    }
                    if let Some(new_key) = new_key {
                        index_tree.insert(new_key, Vec::new())?;
                    }
                }
            }

            results.push((
                prepared.original_index,
                CommittedEntityWrite { entity_id, canonical_changed: prepared.expected_head != prepared.head },
            ));
        }
        if let Some((position, entries)) = &maps {
            entries.land(&transactional[*position])?;
        }
        results.sort_by_key(|(index, _)| *index);
        let entities = results.into_iter().map(|(_, result)| result).collect();
        Ok(StorageCommitResult { entities })
    });
    drop(index_guard);

    match attempt {
        Ok(result) => Ok(StorageCommitOutcome::Committed(result)),
        Err(TransactionError::Abort(SledCommitAbort::Conflict(observed))) => Ok(StorageCommitOutcome::Conflict { observed }),
        Err(TransactionError::Abort(SledCommitAbort::Invalid(message))) => Err(MutationError::General(message.into())),
        Err(TransactionError::Storage(error)) => Err(MutationError::UpdateFailed(Box::new(error))),
    }
}
