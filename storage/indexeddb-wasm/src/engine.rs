use ankurah_core::{
    error::{MutationError, RetrievalError},
    schema::CatalogResolver,
    storage::{naming, CommitBatchOutcome, CommittedEntityWrite, StorageCommitResult, StorageEngine, StorageWriteBatch},
};
use ankurah_proto::{Attested, EntityId, EntityState, Event, EventId, ModelId, State, SystemModel};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use send_wrapper::SendWrapper;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};
use wasm_bindgen::prelude::*;

use crate::{
    bucket::{IndexedDBBucket, PreparedIndexedDbMaterialization},
    database::Database,
    statics::*,
    util::{cb_future::cb_future, cb_stream::cb_stream, object::Object, require::WBGRequire},
};
#[cfg(debug_assertions)]
use std::sync::atomic::{AtomicBool, Ordering};

const MODEL_REGISTRATIONS_STORE: &str = "model_registrations";

fn system_label(model: SystemModel) -> &'static str {
    match model {
        SystemModel::System => "_ankurah_system",
        SystemModel::Model => "_ankurah_model",
        SystemModel::Property => "_ankurah_property",
        SystemModel::ModelProperty => "_ankurah_model_property",
    }
}

fn model_registration_key(model: &ModelId) -> String {
    format!("model\0{}", serde_json::to_string(model).expect("ModelId always serializes"))
}

fn name_registration_key(name: &str) -> String { format!("name\0{name}") }

fn entity_model_prefix(entity: EntityId) -> String { format!("{}\0", entity.to_base64()) }

fn entity_model_key(entity: EntityId, model: &ModelId) -> String {
    format!("{}{}", entity_model_prefix(entity), serde_json::to_string(model).expect("ModelId always serializes"))
}

async fn associated_models_in_store(
    associations: &web_sys::IdbObjectStore,
    entity_id: EntityId,
) -> Result<BTreeSet<ModelId>, RetrievalError> {
    let prefix = entity_model_prefix(entity_id);
    let upper = format!("{prefix}{}", '\u{ffff}');
    let range =
        web_sys::IdbKeyRange::bound(&JsValue::from_str(&prefix), &JsValue::from_str(&upper)).require("create entity-model range")?;
    let request = associations.open_key_cursor_with_range(&range).require("scan entity models")?;
    let mut stream = cb_stream(&request, "success", "error");
    let mut models = BTreeSet::new();
    while let Some(result) = stream.next().await {
        let cursor_result = result.require("entity-model cursor error")?;
        if cursor_result.is_null() || cursor_result.is_undefined() {
            break;
        }
        let cursor = cursor_result.dyn_into::<web_sys::IdbCursor>().require("cast entity-model cursor")?;
        let key = cursor.key().require("get entity-model key")?.as_string().require("entity-model key is string")?;
        let encoded = key.strip_prefix(&prefix).require("entity-model key prefix")?;
        models.insert(serde_json::from_str(encoded).map_err(RetrievalError::storage)?);
        cursor.continue_().require("advance entity-model cursor")?;
    }
    Ok(models)
}

enum IndexedDbBatchAttempt {
    Committed(StorageCommitResult),
    Conflict(BTreeMap<EntityId, Option<Attested<EntityState>>>),
    MissingSchema,
}

/// IndexedDB implementation of the model-independent storage contract.
pub struct IndexedDBStorageEngine {
    // We need SendWrapper because despite the ability to declare an async trait as ?Send,
    // we can't define StorageEngine as optionally Send or !Send.
    // This appears not to be an issue with the macro, but rather the inability to add supplemental bindings on Generic associated types?
    // A lot of time could be potentially burned on this, so we're just going to use SendWrapper for now.
    // See this thread for more information
    // https://users.rust-lang.org/t/send-not-send-variant-of-async-trait-object-without-duplication/115294
    /// Shared IndexedDB database handle.
    pub db: Database,
    /// The catalog resolver, injected post-construction by `Node` (see
    /// `StorageEngine::set_catalog_resolver`). Shared with every bucket: the
    /// name SOURCE for the engine-owned durable id-to-field map (the
    /// `property_columns` object store). Weak so storage never keeps the node
    /// alive. (Wasm is single-threaded, but the trait signature requires these
    /// `Sync` types; `std::sync` works fine here.)
    resolver: Arc<RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
    #[cfg(debug_assertions)]
    /// Runtime test switch for disabling open-ended scan prefix guards.
    pub prefix_guard_disabled: std::sync::Arc<AtomicBool>,
}

impl IndexedDBStorageEngine {
    /// Open or create an IndexedDB-backed storage engine.
    pub async fn open(name: &str) -> anyhow::Result<Self> {
        let db = Database::open(name).await?;
        Ok(Self {
            db,
            resolver: Arc::new(RwLock::new(None)),
            #[cfg(debug_assertions)]
            prefix_guard_disabled: std::sync::Arc::new(AtomicBool::new(false)),
        })
    }

    /// Delete the named IndexedDB database.
    pub async fn cleanup(name: &str) -> anyhow::Result<()> { Database::cleanup(name).await }

    /// Get the database name
    pub fn name(&self) -> &str { self.db.name() }

    fn catalog_registered_label(&self, model: &ModelId) -> Result<String, RetrievalError> {
        if let ModelId::System(system) = model {
            return Ok(system_label(*system).to_owned());
        }
        let resolver = self
            .resolver
            .read()
            .unwrap()
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| RetrievalError::Other(format!("catalog resolver is unavailable for model {model}")))?;
        resolver.model_name(model).map_err(|error| RetrievalError::Other(error.to_string()))
    }

    async fn registered_materialization_name(&self, model: &ModelId) -> Result<Option<String>, RetrievalError> {
        let db_connection = self.db.get_connection().await;
        let key = model_registration_key(model);
        SendWrapper::new(async move {
            let transaction =
                db_connection.transaction_with_str(MODEL_REGISTRATIONS_STORE).require("create model registration transaction")?;
            let store = transaction.object_store(MODEL_REGISTRATIONS_STORE).require("get model registration store")?;
            let request = store.get(&JsValue::from_str(&key)).require("get model registration")?;
            cb_future(&request, "success", "error").await.require("await model registration")?;
            Ok(request.result().require("get model registration result")?.as_string())
        })
        .await
    }

    /// Return the immutable physical materialization name for `model`,
    /// assigning it on first use. The IndexedDB-private registry is
    /// authoritative, so reopening an existing model does not require a ready
    /// catalog resolver.
    async fn get_or_insert_unique_durable_materialization_name(&self, model: &ModelId) -> Result<String, RetrievalError> {
        if let Some(name) = self.registered_materialization_name(model).await? {
            return Ok(name);
        }
        // Resolver access is intentionally after the durable miss.
        let desired = naming::sanitize(&self.catalog_registered_label(model)?);
        let model = *model;
        let model_key = model_registration_key(&model);
        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction = db_connection
                .transaction_with_str_and_mode(MODEL_REGISTRATIONS_STORE, web_sys::IdbTransactionMode::Readwrite)
                .require("create writable model registration transaction")?;
            let store = transaction.object_store(MODEL_REGISTRATIONS_STORE).require("get model registration store")?;

            let get = store.get(&JsValue::from_str(&model_key)).require("recheck model registration")?;
            cb_future(&get, "success", "error").await.require("await model registration recheck")?;
            if let Some(existing) = get.result().require("get model registration recheck result")?.as_string() {
                return Ok(existing);
            }

            let prefix = "name\0";
            let upper = format!("{prefix}{}", '\u{ffff}');
            let range = web_sys::IdbKeyRange::bound(&JsValue::from_str(prefix), &JsValue::from_str(&upper))
                .require("create model-name registration range")?;
            let cursor_request = store.open_key_cursor_with_range(&range).require("scan registered materialization names")?;
            let mut stream = crate::util::cb_stream::cb_stream(&cursor_request, "success", "error");
            let mut taken = std::collections::HashSet::new();
            while let Some(result) = stream.next().await {
                let cursor_result = result.require("model-name registration cursor error")?;
                if cursor_result.is_null() || cursor_result.is_undefined() {
                    break;
                }
                let cursor = cursor_result.dyn_into::<web_sys::IdbCursor>().require("cast model-name registration cursor")?;
                if let Some(key) = cursor.key().require("get model-name registration key")?.as_string() {
                    if let Some(name) = key.strip_prefix(prefix) {
                        taken.insert(name.to_owned());
                    }
                }
                cursor.continue_().require("advance model-name registration cursor")?;
            }
            for system in [SystemModel::System, SystemModel::Model, SystemModel::Property, SystemModel::ModelProperty] {
                taken.insert(naming::sanitize(system_label(system)));
            }

            let materialization_name = match model {
                ModelId::EntityId(id) => {
                    naming::dedupe(&desired, &id, |name| taken.contains(name)).map_err(|error| RetrievalError::Other(error.to_string()))?
                }
                ModelId::System(system) => {
                    let fixed = naming::sanitize(system_label(system));
                    let reverse = store.get(&JsValue::from_str(&name_registration_key(&fixed))).require("check reserved system name")?;
                    cb_future(&reverse, "success", "error").await.require("await reserved system name check")?;
                    if !reverse.result().require("get reserved system name result")?.is_undefined() {
                        return Err(RetrievalError::Other(format!(
                            "reserved system model {model} cannot claim its physical materialization name {fixed:?}"
                        )));
                    }
                    fixed
                }
            };
            let put_model = store
                .put_with_key(&JsValue::from_str(&materialization_name), &JsValue::from_str(&model_key))
                .require("write model registration")?;
            cb_future(&put_model, "success", "error").await.require("await model registration write")?;
            let put_name = store
                .put_with_key(&JsValue::from_str(&model_key), &JsValue::from_str(&name_registration_key(&materialization_name)))
                .require("write materialization-name registration")?;
            cb_future(&put_name, "success", "error").await.require("await materialization-name registration write")?;
            cb_future(&transaction, "complete", "error").await.require("complete model registration transaction")?;
            Ok(materialization_name)
        })
        .await
    }

    async fn materialization(&self, model: &ModelId) -> Result<IndexedDBBucket, RetrievalError> {
        let materialization_name = self.get_or_insert_unique_durable_materialization_name(model).await?;
        Ok(IndexedDBBucket {
            db: self.db.clone(),
            model_id: *model,
            materialization_name,
            mutex: tokio::sync::Mutex::new(()),
            invocation_count: std::sync::atomic::AtomicUsize::new(0),
            resolver: self.resolver.clone(),
            property_columns: Arc::new(RwLock::new(BTreeMap::new())),
            property_columns_loaded: std::sync::atomic::AtomicBool::new(false),
            #[cfg(debug_assertions)]
            prefix_guard_disabled: self.prefix_guard_disabled.clone(),
        })
    }

    async fn associated_models(&self, entity_id: EntityId) -> Result<BTreeSet<ModelId>, RetrievalError> {
        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction = db_connection.transaction_with_str("entity_models").require("create entity-model read transaction")?;
            let store = transaction.object_store("entity_models").require("get entity-model association store")?;
            let models = associated_models_in_store(&store, entity_id).await?;
            cb_future(&transaction, "complete", "error").await.require("complete entity-model read transaction")?;
            Ok(models)
        })
        .await
    }

    /// For wasm tests: enable/disable prefix guard at runtime
    #[cfg(debug_assertions)]
    pub fn set_prefix_guard_disabled(&self, disabled: bool) { self.prefix_guard_disabled.store(disabled, Ordering::Relaxed); }
}

#[async_trait]
impl StorageEngine for IndexedDBStorageEngine {
    type Value = JsValue;

    async fn append_events(&self, events: &[Attested<Event>]) -> Result<Vec<bool>, MutationError> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let events = events.to_vec();
        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction = db_connection
                .transaction_with_str_and_mode("events", web_sys::IdbTransactionMode::Readwrite)
                .require("create event append transaction")?;
            let store = transaction.object_store("events").require("get events store")?;
            let mut inserted = Vec::with_capacity(events.len());
            for event in events {
                let key = JsValue::from_str(&event.payload.id().to_base64());
                let existing = store.get(&key).require("get existing event")?;
                cb_future(&existing, "success", "error").await.require("await existing event")?;
                if existing.result().require("get existing event result")?.is_undefined() {
                    let event_obj = Object::new(js_sys::Object::new().into());
                    event_obj.set(&*ID_KEY, &event.payload.id())?;
                    event_obj.set(&*ENTITY_ID_KEY, event.payload.entity_id.to_base64())?;
                    event_obj.set(&*OPERATIONS_KEY, &event.payload.operations)?;
                    event_obj.set(&*ATTESTATIONS_KEY, &event.attestations)?;
                    event_obj.set(&*PARENT_KEY, &event.payload.parent)?;
                    let put = store.put_with_key(&event_obj, &key).require("put event")?;
                    cb_future(&put, "success", "error").await.require("await event put")?;
                    inserted.push(true);
                } else {
                    inserted.push(false);
                }
            }
            cb_future(&transaction, "complete", "error").await.require("complete event append transaction")?;
            Ok(inserted)
        })
        .await
    }

    async fn commit_batch(&self, batch: StorageWriteBatch) -> Result<CommitBatchOutcome, MutationError> {
        if batch.entities.is_empty() {
            return Ok(CommitBatchOutcome::Committed(StorageCommitResult::default()));
        }
        let mut seen = BTreeSet::new();
        for write in &batch.entities {
            if !seen.insert(write.state.payload.entity_id) {
                return Err(MutationError::General(
                    format!("storage write batch contains duplicate entity {}", write.state.payload.entity_id).into(),
                ));
            }
        }

        loop {
            // Durable name/field assignments are harmless preflight metadata;
            // the objects themselves are not published until the transaction
            // below also commits canonical state and associations.
            let mut prepared = BTreeMap::<(EntityId, ModelId), PreparedIndexedDbMaterialization>::new();
            for write in &batch.entities {
                let entity_id = write.state.payload.entity_id;
                let mut models = self.associated_models(entity_id).await?;
                models.extend(write.associate_with.iter().copied());
                for model in models {
                    let projection = self
                        .materialization(&model)
                        .await
                        .map_err(|error| MutationError::General(error.to_string().into()))?
                        .prepare_state(&write.state)
                        .await?;
                    prepared.insert((entity_id, model), projection);
                }
            }

            let attempt_batch = batch.clone();
            let db_connection = self.db.get_connection().await;
            let attempt = SendWrapper::new(async move {
                let store_names = js_sys::Array::new();
                for name in ["entities", "entity_models", "materializations"] {
                    store_names.push(&JsValue::from_str(name));
                }
                let transaction = db_connection
                    .transaction_with_str_sequence_and_mode(store_names.as_ref(), web_sys::IdbTransactionMode::Readwrite)
                    .require("create prepared state transaction")?;
                let entities = transaction.object_store("entities").require("get canonical entities store")?;
                let associations = transaction.object_store("entity_models").require("get entity-model association store")?;
                let materializations = transaction.object_store("materializations").require("get materializations store")?;

                let mut ordered = attempt_batch.entities.iter().enumerate().collect::<Vec<_>>();
                ordered.sort_by_key(|(_, write)| write.state.payload.entity_id);
                let mut observed = BTreeMap::new();
                let mut conflict = false;
                for (_, write) in &ordered {
                    let entity_id = write.state.payload.entity_id;
                    let request =
                        entities.get(&JsValue::from_str(&entity_id.to_base64())).require("get canonical entity for comparison")?;
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
                    return Ok::<_, RetrievalError>(IndexedDbBatchAttempt::Conflict(observed));
                }

                let mut prior_models = BTreeMap::<EntityId, BTreeSet<ModelId>>::new();
                let mut complete_models = BTreeMap::<EntityId, BTreeSet<ModelId>>::new();
                for (_, write) in &ordered {
                    let entity_id = write.state.payload.entity_id;
                    let prior = associated_models_in_store(&associations, entity_id).await?;
                    let models = prior.union(&write.associate_with).copied().collect::<BTreeSet<_>>();
                    if models.iter().any(|model| !prepared.contains_key(&(entity_id, *model))) {
                        cb_future(&transaction, "complete", "error").await.require("complete schema-retry state transaction")?;
                        return Ok(IndexedDbBatchAttempt::MissingSchema);
                    }
                    prior_models.insert(entity_id, prior);
                    complete_models.insert(entity_id, models);
                }

                let mut committed = Vec::with_capacity(attempt_batch.entities.len());
                for (original_index, write) in ordered {
                    let state = &write.state;
                    let entity_id = state.payload.entity_id;
                    let id_key = JsValue::from_str(&entity_id.to_base64());
                    let entity = Object::new(js_sys::Object::new().into());
                    entity.set(&*ID_KEY, entity_id.to_base64())?;
                    entity.set(&*STATE_BUFFER_KEY, &state.payload.state.state_buffers)?;
                    entity.set(&*HEAD_KEY, &state.payload.state.head)?;
                    entity.set(&*ATTESTATIONS_KEY, &state.attestations)?;
                    let put = entities.put_with_key(&entity, &id_key).require("put canonical entity")?;
                    cb_future(&put, "success", "error").await.require("await canonical entity put")?;

                    let prior = prior_models.remove(&entity_id).ok_or_else(|| {
                        RetrievalError::Other(format!("storage batch omitted the prior model set for entity {entity_id}"))
                    })?;
                    let models = complete_models.remove(&entity_id).ok_or_else(|| {
                        RetrievalError::Other(format!("storage batch omitted the complete model set for entity {entity_id}"))
                    })?;
                    for model in &write.associate_with {
                        if !prior.contains(model) {
                            let request = associations
                                .put_with_key(&JsValue::TRUE, &JsValue::from_str(&entity_model_key(entity_id, model)))
                                .require("put entity-model association")?;
                            cb_future(&request, "success", "error").await.require("await entity-model association")?;
                        }
                    }
                    for model in &models {
                        let projection = prepared.get(&(entity_id, *model)).ok_or_else(|| {
                            RetrievalError::Other(format!(
                                "storage batch omitted the prepared materialization for entity {entity_id} under model {model}"
                            ))
                        })?;
                        let request = materializations
                            .put_with_key(&projection.object, &JsValue::from_str(&projection.key))
                            .require("put materialized entity")?;
                        cb_future(&request, "success", "error").await.require("await materialized entity")?;
                    }

                    committed.push((
                        original_index,
                        CommittedEntityWrite {
                            entity_id,
                            canonical_changed: write.expected_head != state.payload.state.head,
                            associations_added: write.associate_with.difference(&prior).copied().collect(),
                            materialized_as: models.into_iter().collect(),
                        },
                    ));
                }
                committed.sort_by_key(|(index, _)| *index);
                let entities = committed.into_iter().map(|(_, result)| result).collect();
                cb_future(&transaction, "complete", "error").await.require("complete prepared state transaction")?;
                Ok(IndexedDbBatchAttempt::Committed(StorageCommitResult { entities }))
            })
            .await?;

            match attempt {
                IndexedDbBatchAttempt::Committed(result) => return Ok(CommitBatchOutcome::Committed(result)),
                IndexedDbBatchAttempt::Conflict(observed) => return Ok(CommitBatchOutcome::Conflict { observed }),
                IndexedDbBatchAttempt::MissingSchema => continue,
            }
        }
    }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> { load_state(&self.db, id).await }

    async fn fetch_states(&self, model: &ModelId, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        self.materialization(model).await?.fetch_states(selection).await
    }

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction = db_connection.transaction_with_str("events").require("create event read transaction")?;
            let store = transaction.object_store("events").require("get events store")?;
            let mut events = Vec::new();
            for event_id in event_ids {
                let request = store.get(&JsValue::from_str(&event_id.to_base64())).require("get event")?;
                cb_future(&request, "success", "error").await.require("await event")?;
                let value = request.result().require("get event result")?;
                if !value.is_null() && !value.is_undefined() {
                    events.push(event_from_object(&Object::new(value))?);
                }
            }
            Ok(events)
        })
        .await
    }

    async fn dump_entity_events(&self, id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction = db_connection.transaction_with_str("events").require("create event read transaction")?;
            let store = transaction.object_store("events").require("get events store")?;
            let index = store.index("by_entity_id").require("get entity id index")?;
            let range = web_sys::IdbKeyRange::only(&JsValue::from_str(&id.to_base64())).require("create entity id range")?;
            let request = index.open_cursor_with_range(&range).require("scan entity events")?;
            let mut stream = cb_stream(&request, "success", "error");
            let mut events = Vec::new();
            while let Some(result) = stream.next().await {
                let cursor_result = result.require("entity event cursor error")?;
                if cursor_result.is_null() || cursor_result.is_undefined() {
                    break;
                }
                let cursor = cursor_result.dyn_into::<web_sys::IdbCursorWithValue>().require("cast entity event cursor")?;
                events.push(event_from_object(&Object::new(cursor.value().require("get event cursor value")?))?);
                cursor.continue_().require("advance entity event cursor")?;
            }
            Ok(events)
        })
        .await
    }

    fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn CatalogResolver>) { *self.resolver.write().unwrap() = Some(resolver); }

    async fn delete_all(&self) -> Result<bool, MutationError> {
        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            // One transaction across all stores: a reset must not be
            // observable half-done. Wiping every materialization must wipe the
            // engine-owned durable id-to-field map (property_columns) with it,
            // or an interruption between per-store transactions would leave a
            // re-created materialization finding stale rows; a single transaction
            // commits the clears together or not at all.
            let store_names = js_sys::Array::new();
            for name in ["entities", "materializations", "events", "entity_models", "property_columns", MODEL_REGISTRATIONS_STORE] {
                store_names.push(&name.into());
            }
            let transaction = db_connection
                .transaction_with_str_sequence_and_mode(store_names.as_ref(), web_sys::IdbTransactionMode::Readwrite)
                .require("create reset transaction")?;
            for name in ["entities", "materializations", "events", "entity_models", "property_columns", MODEL_REGISTRATIONS_STORE] {
                let store = transaction.object_store(name).require("get store for reset")?;
                let request = store.clear().require("clear store")?;
                cb_future(&request, "success", "error").await.require("await store clear")?;
            }
            cb_future(&transaction, "complete", "error").await.require("complete reset transaction")?;

            // Return true since we cleared everything
            Ok(true)
        })
        .await
    }

    async fn list_materializations(&self) -> Result<Vec<ModelId>, RetrievalError> {
        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction =
                db_connection.transaction_with_str(MODEL_REGISTRATIONS_STORE).require("create model registration transaction")?;
            let store = transaction.object_store(MODEL_REGISTRATIONS_STORE).require("get model registration store")?;
            let prefix = "model\0";
            let upper = format!("{prefix}{}", '\u{ffff}');
            let range = web_sys::IdbKeyRange::bound(&JsValue::from_str(prefix), &JsValue::from_str(&upper))
                .require("create model registration range")?;
            let request = store.open_key_cursor_with_range(&range).require("scan model registrations")?;
            let mut stream = crate::util::cb_stream::cb_stream(&request, "success", "error");
            let mut models = Vec::new();
            while let Some(result) = stream.next().await {
                let cursor_result = result.require("model registration cursor error")?;
                if cursor_result.is_null() || cursor_result.is_undefined() {
                    break;
                }
                let cursor = cursor_result.dyn_into::<web_sys::IdbCursor>().require("cast model registration cursor")?;
                let key = cursor.key().require("get model registration key")?.as_string().require("model registration key is a string")?;
                let encoded = key.strip_prefix(prefix).require("model registration key prefix")?;
                models.push(serde_json::from_str(encoded).map_err(RetrievalError::storage)?);
                cursor.continue_().require("advance model registration cursor")?;
            }
            Ok(models)
        })
        .await
    }
}

fn entity_state_from_object(id: EntityId, entity: &Object) -> Result<Attested<EntityState>, RetrievalError> {
    Ok(Attested {
        payload: EntityState {
            entity_id: id,
            state: State { state_buffers: entity.get(&STATE_BUFFER_KEY)?, head: entity.get(&HEAD_KEY)? },
        },
        attestations: entity.get(&ATTESTATIONS_KEY)?,
    })
}

/// Load one canonical entity state from the IndexedDB entity store.
pub(crate) async fn load_state(db: &Database, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
    let db_connection = db.get_connection().await;
    SendWrapper::new(async move {
        let transaction = db_connection.transaction_with_str("entities").require("create canonical entity read transaction")?;
        let store = transaction.object_store("entities").require("get canonical entities store")?;
        let request = store.get(&JsValue::from_str(&id.to_base64())).require("get canonical entity")?;
        cb_future(&request, "success", "error").await.require("await canonical entity")?;
        let value = request.result().require("get canonical entity result")?;
        if value.is_null() || value.is_undefined() {
            return Err(RetrievalError::EntityNotFound(id));
        }
        entity_state_from_object(id, &Object::new(value))
    })
    .await
}

fn event_from_object(object: &Object) -> Result<Attested<Event>, RetrievalError> {
    Ok(Attested {
        payload: Event {
            entity_id: object.get(&ENTITY_ID_KEY)?,
            operations: object.get(&OPERATIONS_KEY)?,
            parent: object.get(&PARENT_KEY)?,
        },
        attestations: object.get(&ATTESTATIONS_KEY)?,
    })
}
