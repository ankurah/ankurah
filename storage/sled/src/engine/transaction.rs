use super::*;
use ankurah_core::{
    property::backend::backend_from_string,
    storage::{CommittedEntityWrite, StorageCommitOutcome, StorageCommitResult, StorageTransaction},
    value::Value,
};
use ankurah_proto::{AttestationSet, Clock, PropertyId, State};
use crate::index::Index;
use sled::{IVec, transaction::ConflictableTransactionError};

/// Sled encodings and property values for one atomic commit.
pub struct SledTransaction<'a> {
    engine: &'a SledStorageEngine,
    entities: Vec<EntityRow>,
    events: Vec<(IVec, IVec)>,
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
    events.iter().map(|event| Ok((
        IVec::from(event.payload.id().as_bytes()),
        IVec::from(bincode::serialize(event)?),
    ))).collect()
}

impl<'a> SledTransaction<'a> {
    pub(super) fn new(engine: &'a SledStorageEngine) -> Self { Self { engine, entities: Vec::new(), events: Vec::new() } }
}

#[async_trait]
impl StorageTransaction for SledTransaction<'_> {
    async fn add_events(&mut self, events: &[Attested<Event>]) -> Result<(), MutationError> {
        self.events.extend(encode_events(events)?);
        Ok(())
    }

    async fn set_state(&mut self, expected_head: &Clock, state: &Attested<EntityState>) -> Result<(), MutationError> {
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
            encoded_state: bincode::serialize(&StateFragmentRef {
                state: &state.payload.state,
                attestations: &state.attestations,
            })?.into(),
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
        if self.entities.is_empty() && self.events.is_empty() {
            return Ok(StorageCommitOutcome::Committed(StorageCommitResult::default()));
        }
        let Self { engine, entities, events, .. } = self;
        let database = engine.database.lock().unwrap().clone();
        tokio::task::spawn_blocking(move || commit_staged(database, entities, events)).await?
    }
}

fn commit_staged(
    database: Arc<Database>,
    mut entities: Vec<EntityRow>,
    events: Vec<(IVec, IVec)>,
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
    let mut trees = vec![database.entities_tree.clone(), database.entity_models_tree.clone(), database.events_tree.clone()];
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
                CommittedEntityWrite {
                    entity_id,
                    canonical_changed: prepared.expected_head != prepared.head,
                },
            ));
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
