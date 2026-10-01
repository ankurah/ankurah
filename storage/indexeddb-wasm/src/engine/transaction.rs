use super::*;

/// IndexedDB objects prepared before opening their shared write transaction.
pub struct IndexedDbTransaction<'a> {
    engine: &'a IndexedDBStorageEngine,
    entities: Vec<EntityRow>,
    events: Vec<EventRow>,
}

struct EntityRow {
    entity_id: EntityId,
    expected_head: Clock,
    head: Clock,
    memberships: BTreeSet<ModelId>,
    object: Object,
    materializations: Vec<PreparedIndexedDbMaterialization>,
}

pub(super) struct EventRow {
    key: String,
    object: Object,
}

pub(super) fn encode_events(events: &[Attested<Event>]) -> Result<Vec<EventRow>, RetrievalError> {
    events.iter().map(|event| {
        let object = Object::new(js_sys::Object::new().into());
        object.set(&*ID_KEY, &event.payload.id())?;
        object.set(&*ENTITY_ID_KEY, event.payload.entity_id.to_base64())?;
        object.set(&*BODY_KEY, &event.payload.body)?;
        object.set(&*ATTESTATIONS_KEY, &event.attestations)?;
        object.set(&*PARENT_KEY, &event.payload.parent)?;
        Ok(EventRow { key: event.payload.id().to_base64(), object })
    }).collect()
}

pub(super) async fn insert_events(store: &web_sys::IdbObjectStore, events: &[EventRow]) -> Result<(), RetrievalError> {
    for event in events {
        let key = JsValue::from_str(&event.key);
        let existing = store.get(&key).require("get existing event")?;
        cb_future(&existing, "success", "error").await.require("await existing event")?;
        if existing.result().require("get existing event result")?.is_undefined() {
            let put = store.put_with_key(&event.object, &key).require("put event")?;
            cb_future(&put, "success", "error").await.require("await event put")?;
        }
    }
    Ok(())
}

impl<'a> IndexedDbTransaction<'a> {
    pub(super) fn new(engine: &'a IndexedDBStorageEngine) -> Self { Self { engine, entities: Vec::new(), events: Vec::new() } }
}

#[async_trait]
impl StorageTransaction for IndexedDbTransaction<'_> {
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
        let mut materializations = Vec::new();
        for model in &state.payload.state.memberships {
            let projection = self.engine.materialization(model).await
                .map_err(|error| MutationError::General(error.to_string().into()))?
                .prepare_state(state).await?;
            materializations.push(projection);
        }
        let object = Object::new(js_sys::Object::new().into());
        object.set(&*ID_KEY, entity_id.to_base64())?;
        object.set(&*STATE_BUFFER_KEY, &state.payload.state.state_buffers)?;
        let memberships = serde_json::to_string(&state.payload.state.memberships).map_err(RetrievalError::storage)?;
        object.set(&*MEMBERSHIPS_KEY, memberships)?;
        object.set(&*HEAD_KEY, &state.payload.state.head)?;
        object.set(&*ATTESTATIONS_KEY, &state.attestations)?;
        let mut write = EntityRow {
            entity_id,
            expected_head: expected_head.clone(),
            head: state.payload.state.head.clone(),
            memberships: state.payload.state.memberships.clone(),
            object,
            materializations,
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
        let Self { engine, entities: staged_entities, events: staged_events, .. } = self;
        let db_connection = engine.db.get_connection().await;
        SendWrapper::new(async move {
            let store_names = js_sys::Array::new();
            for name in ["entities", "entity_models", "materializations", "events"] {
                store_names.push(&JsValue::from_str(name));
            }
            let transaction = db_connection
                .transaction_with_str_sequence_and_mode(store_names.as_ref(), web_sys::IdbTransactionMode::Readwrite)
                .require("create prepared state transaction")?;
            let _abort = AbortUnfinishedTransaction(transaction.clone());
            let entities = transaction.object_store("entities").require("get canonical entities store")?;
            let associations = transaction.object_store("entity_models").require("get entity-model association store")?;
            let materializations = transaction.object_store("materializations").require("get materializations store")?;

            let mut ordered = staged_entities.iter().enumerate().collect::<Vec<_>>();
            ordered.sort_by_key(|(_, write)| write.entity_id);
            let mut observed = BTreeMap::new();
            let mut conflict = false;
            for (_, write) in &ordered {
                let entity_id = write.entity_id;
                let request = entities.get(&JsValue::from_str(&entity_id.to_base64())).require("get canonical entity for comparison")?;
                cb_future(&request, "success", "error").await.require("await canonical entity comparison")?;
                let value = request.result().require("get canonical entity comparison result")?;
                let current = if value.is_null() || value.is_undefined() {
                    None
                } else {
                    Some(entity_state_from_object(entity_id, &Object::new(value))?)
                };
                let current_head = current.as_ref().map(|state| state.payload.state.head.clone()).unwrap_or_default();
                if current_head != write.expected_head {
                    conflict = true;
                }
                observed.insert(entity_id, current);
            }
            if conflict {
                cb_future(&transaction, "complete", "error").await.require("complete conflicting state transaction")?;
                return Ok::<_, RetrievalError>(StorageCommitOutcome::Conflict { observed });
            }

            let mut prior_models = BTreeMap::<EntityId, BTreeSet<ModelId>>::new();
            for (_, write) in &ordered {
                let entity_id = write.entity_id;
                let prior = associated_models_in_store(&associations, entity_id).await?;
                if !prior.is_subset(&write.memberships) {
                    return Err(RetrievalError::Other(format!(
                        "canonical state for entity {entity_id} would remove durable memberships; membership removal is not supported"
                    )));
                }
                prior_models.insert(entity_id, prior);
            }

            let events = transaction.object_store("events").require("get events store")?;
            insert_events(&events, &staged_events).await?;
            let mut committed = Vec::with_capacity(staged_entities.len());
            for (original_index, write) in ordered {
                let entity_id = write.entity_id;
                let id_key = JsValue::from_str(&entity_id.to_base64());
                let put = entities.put_with_key(&write.object, &id_key).require("put canonical entity")?;
                cb_future(&put, "success", "error").await.require("await canonical entity put")?;

                let prior = prior_models
                    .remove(&entity_id)
                    .ok_or_else(|| RetrievalError::Other(format!("storage batch omitted the prior model set for entity {entity_id}")))?;
                for model in &write.memberships {
                    if !prior.contains(model) {
                        let request = associations
                            .put_with_key(&JsValue::TRUE, &JsValue::from_str(&entity_model_key(entity_id, model)))
                            .require("put entity-model association")?;
                        cb_future(&request, "success", "error").await.require("await entity-model association")?;
                    }
                }
                for projection in &write.materializations {
                    let request = materializations
                        .put_with_key(&projection.object, &JsValue::from_str(&projection.key))
                        .require("put materialized entity")?;
                    cb_future(&request, "success", "error").await.require("await materialized entity")?;
                }

                committed.push((
                    original_index,
                    CommittedEntityWrite {
                        entity_id,
                        canonical_changed: write.expected_head != write.head,
                    },
                ));
            }
            committed.sort_by_key(|(index, _)| *index);
            let entities = committed.into_iter().map(|(_, result)| result).collect();
            cb_future(&transaction, "complete", "error").await.require("complete prepared state transaction")?;
            Ok(StorageCommitOutcome::Committed(StorageCommitResult { entities }))
        })
        .await
        .map_err(MutationError::from)
    }
}
