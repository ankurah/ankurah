use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use ankurah_core::{
    action_debug,
    error::{MutationError, RetrievalError},
    property::PropertyKey,
    schema::CatalogResolver,
    selection::filter::{evaluate_predicate, Filterable},
    storage::{naming, StorageCollection},
};
use ankurah_proto::{self as proto, Attested, EntityId, EntityState, EventId, State};
use async_trait::async_trait;
use send_wrapper::SendWrapper;
use wasm_bindgen::{JsCast, JsValue};

use crate::{
    database::Database,
    statics::*,
    util::{cb_future::cb_future, cb_stream::cb_stream, object::Object, require::WBGRequire},
};
use ankurah_storage_common::{filtering::ValueSetStream, OrderByComponents, Plan};
// Import tracing for debug macro and futures for StreamExt
use futures::StreamExt;
use tracing::{debug, warn};

/// Read the mandatory u32 generation off a stored event object. Numbers round-trip
/// through JS as f64 and a u32 is exactly representable, so this is lossless. There
/// is no numeric `TryFrom<JsValue>` to route through `Object::get`, hence the direct
/// reflect-and-`as_f64` read.
fn read_generation(event_obj: &Object) -> Result<u32, RetrievalError> {
    let raw = js_sys::Reflect::get(event_obj, &GENERATION_KEY)
        .map_err(|_| RetrievalError::StorageError(anyhow::anyhow!("Failed to get generation").into()))?;
    raw.as_f64()
        .map(|n| n as u32)
        .ok_or_else(|| RetrievalError::StorageError(anyhow::anyhow!("event generation missing or not a number").into()))
}

#[derive(Debug)]
pub struct IndexedDBBucket {
    pub(crate) db: Database,
    pub(crate) collection_id: proto::CollectionId,
    pub(crate) mutex: tokio::sync::Mutex<()>, // should probably be implemented by Database, but not certain
    pub(crate) invocation_count: AtomicUsize,
    /// The injected catalog resolver (shared with the engine): the NAME SOURCE
    /// for [`Self::field_for_key`]. Weak so storage never keeps the node alive.
    pub(crate) resolver: Arc<RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
    /// This collection's slice of the engine-owned durable id-to-field map (the
    /// `property_columns` object store), cached in memory. The map -- not the
    /// display name -- is what addresses a property's field once assigned:
    /// renames never move fields, collisions were deduped at assignment.
    pub(crate) property_columns: Arc<RwLock<BTreeMap<EntityId, String>>>,
    /// Whether [`Self::ensure_property_columns_loaded`] has hydrated
    /// `property_columns` from the store yet (lazy, once per bucket).
    pub(crate) property_columns_loaded: AtomicBool,
    #[cfg(debug_assertions)]
    pub(crate) prefix_guard_disabled: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

/// Reserved entity-object fields a property field must never shadow: the
/// primary `id` plus the double-underscore system columns that `set_state`
/// writes on every entity object.
const RESERVED_FIELDS: [&str; 5] = ["id", "__collection", "__state_buffer", "__head", "__attestations"];

/// Store key for a property id's field-name assignment in `collection`:
/// `{collection}\0{id base64}`. A concatenated string key (matching the store's
/// other string keys) so a collection's whole slice is one prefix range.
fn property_columns_key(collection: &str, id: &EntityId) -> String { format!("{}\0{}", collection, id.to_base64()) }

/// Capture ORDER BY types while the selection still carries catalog identity,
/// then key them by the physical field name the planner will receive. A
/// durable field assignment is sticky across catalog renames and may be
/// collision-suffixed, so resolving that physical name back through the
/// catalog is not reliable.
fn order_types_in_column_space(
    collection: &str,
    selection: &ankql::ast::Selection,
    resolver: Option<&dyn CatalogResolver>,
    column_of: &dyn Fn(&EntityId) -> Option<String>,
) -> BTreeMap<String, ankurah_core::value::ValueType> {
    selection
        .order_by
        .iter()
        .flatten()
        .filter_map(|item| {
            let name = item.path.first();
            if name == "id" || name.starts_with("__") {
                return None;
            }
            let resolver = resolver?;
            let id = item.property.map(EntityId::from_ulid).or_else(|| resolver.resolve(collection, name))?;
            let value_type = ankurah_core::value::ValueType::from_property_str(&resolver.canonical_value_type(&id)?)?;
            let column = column_of(&id).unwrap_or_else(|| name.to_string());
            Some((column, value_type))
        })
        .collect()
}

impl std::fmt::Display for IndexedDBBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "IndexedDBBucket({})", self.collection_id) }
}

#[async_trait]
impl StorageCollection for IndexedDBBucket {
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, MutationError> {
        self.invocation_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Lock the mutex to prevent concurrent updates
        let _lock = self.mutex.lock().await;

        // Assign + persist durable field names for this state's property keys
        // BEFORE opening the entities transaction below. This does every
        // `property_columns` write up front on its own transaction, so the
        // entities transaction never awaits a foreign transaction (IndexedDB
        // auto-commits a transaction the moment an await doesn't belong to it),
        // and it warms the cache so `extract_all_fields` only ever hits it.
        self.assign_property_fields(&state.payload).await?;

        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            use web_sys::IdbTransactionMode::Readwrite;
            action_debug!(self, "set_state {}", "{}", &self.collection_id);
            // Get the old entity if it exists to check for changes
            let transaction = db_connection.transaction_with_str_and_mode("entities", Readwrite).require("create transaction")?;
            let store = transaction.object_store("entities").require("get object store")?;
            let old_request = store.get(&state.payload.entity_id.to_string().into()).require("get old entity")?;
            let foo = cb_future(&old_request, "success", "error").await;
            let _: () = foo.require("get old entity")?;

            let old_entity: JsValue = old_request.result().require("get old entity result")?;

            // Check if the entity changed
            if !old_entity.is_undefined() && !old_entity.is_null() {
                let old_entity_obj = Object::new(old_entity);

                if let Some(old_clock) = old_entity_obj.get_opt::<proto::Clock>(&HEAD_KEY)? {
                    // let old_clock: proto::Clock = old_head.try_into()?;
                    if old_clock == state.payload.state.head {
                        // return false if the head is the same. This was formerly disabled for IndexedDB because it was breaking things
                        // by *accurately* reporting that the stored entity had not changed, because it was applied by another browser window moments earlier.
                        // Now we are checking the resident entity to see if it has been updated, which is more correct.
                        return Ok(false);
                    }
                }
            }

            let entity = Object::new(js_sys::Object::new().into());
            entity.set(&*ID_KEY, state.payload.entity_id.to_string())?;
            entity.set(&*COLLECTION_KEY, self.collection_id.as_str())?;
            entity.set(&*STATE_BUFFER_KEY, &state.payload.state.state_buffers)?;
            entity.set(&*HEAD_KEY, &state.payload.state.head)?;
            // Per-tip head generations (D2 M4, REV 5 section K home 3):
            // self-contained [generation, eventIdBase64] pairs under one
            // key, so rehydration reconstitutes the materialization without
            // reading events. A pre-M4 record lacks the key and state reads
            // fail loudly (fresh-database ruling).
            entity.set(&*GENERATIONS_KEY, &state.payload.state.head_generations)?;
            entity.set(&*ATTESTATIONS_KEY, &state.attestations)?;

            // Extract all fields for indexing (durable field names resolved via
            // the engine-owned map; the cache was warmed above so every lookup
            // here is a pure hit that never touches this transaction).
            self.extract_all_fields(&entity, &state.payload).await?;

            // Put the entity in the store
            let request = store.put_with_key(&entity, &state.payload.entity_id.to_string().into()).require("put entity in store")?;

            cb_future(&request, "success", "error").await.require("put entity in store")?;
            cb_future(&transaction, "complete", "error").await.require("complete transaction")?;

            Ok(true) // It was updated
        })
        .await
    }

    async fn get_state(&self, id: proto::EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            // Create transaction and get object store
            let transaction = db_connection.transaction_with_str("entities").require("create transaction")?;
            let store = transaction.object_store("entities").require("get object store")?;
            let request = store.get(&id.to_string().into()).require("get entity")?;

            cb_future(&request, "success", "error").await.require("await request")?;

            let result = request.result().require("get result")?;

            // Check if the entity exists
            if result.is_undefined() || result.is_null() {
                return Err(RetrievalError::EntityNotFound(id));
            }

            let entity = Object::new(result);

            Ok(Attested {
                payload: EntityState {
                    entity_id: id,
                    model: self.model_id()?,
                    state: State {
                        state_buffers: entity.get(&STATE_BUFFER_KEY)?,
                        head: entity.get(&HEAD_KEY)?,
                        head_generations: entity.get(&GENERATIONS_KEY)?,
                    },
                },
                attestations: entity.get(&ATTESTATIONS_KEY)?,
            })
        })
        .await
    }

    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let _invocation = self.invocation_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _lock = self.mutex.lock().await; // TODO why are we locking here?

        // Translate into this engine's column space FIRST (ids -> assigned
        // field names via the durable map, order-by names via the catalog
        // resolver), so the planner's index columns, the record field reads,
        // and the sort comparators all address the fields writes actually
        // created (sticky under rename, deduped under collision, synthetic
        // under fallback).
        self.ensure_property_columns_loaded().await.map_err(|e| RetrievalError::StorageError(format!("{e:?}").into()))?;
        let resolver = self.resolver.read().expect("RwLock poisoned").as_ref().and_then(|weak| weak.upgrade());
        let assigned = self.property_columns.read().expect("RwLock poisoned").clone();
        let column_of = |id: &EntityId| assigned.get(id).cloned();
        let order_types = order_types_in_column_space(self.collection_id.as_str(), selection, resolver.as_deref(), &column_of);
        let selection =
            ankurah_core::storage::selection_to_column_space(self.collection_id.as_str(), selection, resolver.as_deref(), &column_of);
        let selection = &selection;

        // Step 1: Amend predicate with __collection comparison
        let amended_selection = add_collection(selection, &self.collection_id);

        // Step 2: Use planner to generate query plans
        // ORDER BY key parts collate in each property's CANONICAL value_type
        // (the canonical value_type ruling); unresolvable columns keep the
        // historical String collation.
        let order_type_of = |name: &str| -> Option<ankurah_core::value::ValueType> {
            if let Some(value_type) = order_types.get(name) {
                return Some(*value_type);
            }
            let resolver = resolver.as_deref()?;
            let id = resolver.resolve(self.collection_id.as_str(), name)?;
            ankurah_core::value::ValueType::from_property_str(&resolver.canonical_value_type(&id)?)
        };
        let planner = ankurah_storage_common::planner::Planner::new(ankurah_storage_common::planner::PlannerConfig::indexeddb());
        let plans = planner.plan_with_types(&amended_selection, "id", &order_type_of);

        // Step 3: Pick the first plan (always)
        let plan = plans.first().ok_or_else(|| RetrievalError::StorageError("No plan generated".into()))?;

        // Handle Plan enum
        match plan {
            Plan::EmptyScan => {
                // Empty scan - return empty results immediately
                return Ok(Vec::new());
            }
            Plan::Index { index_spec, bounds, scan_direction, remaining_predicate, order_by_spill } => {
                // Step 4: Ensure index exists using plan's IndexSpec
                self.db
                    .assure_index_exists(index_spec)
                    .await
                    .map_err(|e| RetrievalError::StorageError(format!("ensure index exists: {}", e).into()))?;

                // Step 6: Execute the query using the plan
                let db_connection = self.db.get_connection().await;
                let collection_id = self.collection_id.clone();
                let limit = selection.limit;

                SendWrapper::new(async move {
                    let transaction = db_connection.transaction_with_str("entities").require("create transaction")?;
                    let store = transaction.object_store("entities").require("get object store")?;

                    // Get the index specified by the plan
                    let index = store.index(&index_spec.name_with("", "__")).require("get index")?;

                    // Convert plan bounds to IndexedDB key range using new pipeline
                    let (key_range, upper_open_ended, eq_prefix_len, eq_prefix_values) =
                        crate::planner_integration::plan_bounds_to_idb_range(bounds, scan_direction)
                            .map_err(|e| RetrievalError::StorageError(format!("bounds conversion: {}", e).into()))?;
                    // Convert scan direction to cursor direction
                    let cursor_direction = crate::planner_integration::scan_direction_to_cursor_direction(scan_direction);

                    let results = self
                        .execute_plan_query(
                            &index,
                            Some(key_range),
                            remaining_predicate,
                            cursor_direction,
                            limit,
                            &collection_id,
                            upper_open_ended,
                            eq_prefix_len,
                            eq_prefix_values,
                            &order_by_spill,
                        )
                        .await?;

                    Ok(results)
                })
            }
            Plan::TableScan { .. } => {
                unreachable!(
                    "We should always have an IndexPlan or EmptyScan due to the amendment of the selection to include the collection"
                )
            }
        }
        .await
    }

    async fn add_event(&self, attested_event: &Attested<ankurah_proto::Event>) -> Result<bool, MutationError> {
        let invocation = self.invocation_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        debug!("IndexedDBBucket({}).add_event({})", self.collection_id, invocation);
        let _lock = self.mutex.lock().await;
        debug!("IndexedDBBucket({}).add_event({}) LOCKED", self.collection_id, invocation);

        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction = db_connection
                .transaction_with_str_and_mode("events", web_sys::IdbTransactionMode::Readwrite)
                .require("create transaction")?;

            let store = transaction.object_store("events").require("get object store")?;

            // Create a JS object to store the event data
            let event_obj = Object::new(js_sys::Object::new().into());
            let payload = &attested_event.payload;
            event_obj.set(&*ID_KEY, &payload.id())?;
            event_obj.set(&*ENTITY_ID_KEY, payload.entity_id.to_base64())?;
            event_obj.set(&*OPERATIONS_KEY, &payload.operations)?;
            event_obj.set(&*ATTESTATIONS_KEY, &attested_event.attestations)?;
            event_obj.set(&*PARENT_KEY, &payload.parent)?;
            // generation is a u32, stored as a JS number (exactly representable).
            event_obj.set(&*GENERATION_KEY, payload.generation)?;

            let request = store.put_with_key(&event_obj, &(&payload.id()).into()).require("put event in store")?;

            cb_future(&request, "success", "error").await.require("await request")?;
            cb_future(&transaction, "complete", "error").await.require("complete transaction")?;

            Ok(true)
        })
        .await
    }

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<ankurah_proto::Event>>, RetrievalError> {
        if event_ids.is_empty() {
            return Ok(Vec::new());
        }

        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction = db_connection.transaction_with_str("events").require("create transaction")?;
            let store = transaction.object_store("events").require("get object store")?;

            // TODO - do we want to use a cursor? The id space is pretty sparse, so we would probably need benchmarks to see if it's worth it
            let mut events = Vec::new();
            for event_id in event_ids {
                let request = store.get(&event_id.to_base64().into()).require("get event")?;
                cb_future(&request, "success", "error").await.require("await event request")?;
                let result = request.result().require("get result")?;

                // Skip if event not found
                if result.is_undefined() || result.is_null() {
                    continue;
                }

                let event_obj = Object::new(result);

                let event = Attested {
                    payload: ankurah_proto::Event {
                        model: self.model_id()?,
                        entity_id: event_obj.get(&ENTITY_ID_KEY)?,
                        operations: event_obj.get(&OPERATIONS_KEY)?,
                        parent: event_obj.get(&PARENT_KEY)?,
                        generation: read_generation(&event_obj)?,
                    },
                    attestations: event_obj.get(&ATTESTATIONS_KEY)?,
                };
                events.push(event);
            }

            Ok(events)
        })
        .await
    }

    async fn has_event(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        let db_connection = self.db.get_connection().await;
        let key = event_id.to_base64();
        SendWrapper::new(async move {
            let transaction = db_connection.transaction_with_str("events").require("create transaction")?;
            let store = transaction.object_store("events").require("get object store")?;
            let request = store.get(&key.into()).require("get event")?;
            cb_future(&request, "success", "error").await.require("await event request")?;
            let result = request.result().require("get result")?;
            Ok(!(result.is_undefined() || result.is_null()))
        })
        .await
    }

    async fn dump_entity_events(&self, id: ankurah_proto::EntityId) -> Result<Vec<Attested<ankurah_proto::Event>>, RetrievalError> {
        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction = db_connection.transaction_with_str("events").require("create transaction")?;
            let store = transaction.object_store("events").require("get object store")?;
            let index = store.index("by_entity_id").require("get entity_id index")?;
            let key_range = web_sys::IdbKeyRange::only(&id.into()).require("create key range")?;
            let request = index.open_cursor_with_range(&key_range).require("open cursor")?;

            let mut events = Vec::new();
            let mut stream = cb_stream(&request, "success", "error");

            while let Some(result) = stream.next().await {
                let cursor_result = result.require("Cursor error")?;

                // Check if we've reached the end
                if cursor_result.is_null() || cursor_result.is_undefined() {
                    break;
                }

                let cursor = cursor_result.dyn_into::<web_sys::IdbCursorWithValue>().require("cast cursor")?;
                let event_obj = Object::new(cursor.value().require("get cursor value")?);

                let event = Attested {
                    payload: ankurah_proto::Event {
                        model: self.model_id()?,
                        // id: event_obj.get(&ID_KEY)?.try_into()?,
                        entity_id: event_obj.get(&ENTITY_ID_KEY)?,
                        operations: event_obj.get(&OPERATIONS_KEY)?,
                        parent: event_obj.get(&PARENT_KEY)?,
                        generation: read_generation(&event_obj)?,
                    },
                    attestations: event_obj.get(&ATTESTATIONS_KEY)?,
                };
                events.push(event);

                cursor.continue_().require("Failed to advance cursor")?;
            }

            Ok(events)
        })
        .await
    }
}

// We always use index cursors for fetch operations
// Store cursors are only used for direct ID lookups (get_state, not fetch_states)

/// Execute queries using index cursors (we always use indexes for fetch operations)
/// Convert IndexDirection to IdbCursorDirection
// pub fn to_idb_cursor_direction(direction: ankurah_core::indexing::IndexDirection) -> web_sys::IdbCursorDirection {
//     match direction {
//         ankurah_core::indexing::IndexDirection::Asc => web_sys::IdbCursorDirection::Next,
//         ankurah_core::indexing::IndexDirection::Desc => web_sys::IdbCursorDirection::Prev,
//     }
// }

impl IndexedDBBucket {
    /// The model id stamped on envelopes this bucket reconstructs (#330):
    /// well-knowns, then the injected catalog resolver.
    fn model_id(&self) -> Result<ankurah_proto::EntityId, RetrievalError> {
        let resolver = self.resolver.read().expect("RwLock poisoned").as_ref().and_then(|weak| weak.upgrade());
        ankurah_core::storage::bucket_model_id(&self.collection_id, resolver.as_deref())
    }

    /// Hydrate `property_columns` with this collection's durable id-to-field
    /// assignments, once per bucket. Runs BEFORE any write transaction so the
    /// loaded cache is the authoritative taken-set for [`Self::field_for_key`]
    /// (and so a field lookup never has to read the store while the entities
    /// transaction is open -- IndexedDB auto-commits a transaction the moment an
    /// await doesn't belong to it).
    async fn ensure_property_columns_loaded(&self) -> Result<(), MutationError> {
        if self.property_columns_loaded.load(Ordering::Relaxed) {
            return Ok(());
        }
        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction = db_connection.transaction_with_str("property_columns").require("create property_columns transaction")?;
            let store = transaction.object_store("property_columns").require("get property_columns store")?;

            // This collection's rows are exactly the keys under the
            // `{collection}\0` prefix; base64 chars all sort below U+FFFF, so
            // this bound captures the slice and nothing else.
            let prefix = format!("{}\0", self.collection_id.as_str());
            let upper = format!("{}{}", prefix, '\u{ffff}');
            let range = web_sys::IdbKeyRange::bound(&JsValue::from_str(&prefix), &JsValue::from_str(&upper))
                .require("create property_columns key range")?;
            let request = store.open_cursor_with_range(&range).require("open property_columns cursor")?;

            let mut map = BTreeMap::new();
            let mut stream = cb_stream(&request, "success", "error");
            while let Some(result) = stream.next().await {
                let cursor_result = result.require("property_columns cursor error")?;
                if cursor_result.is_null() || cursor_result.is_undefined() {
                    break;
                }
                let cursor = cursor_result.dyn_into::<web_sys::IdbCursorWithValue>().require("cast property_columns cursor")?;
                let key = cursor.key().require("get property_columns cursor key")?;
                let value = cursor.value().require("get property_columns cursor value")?;
                // key = "{collection}\0{id base64}", value = the field name.
                if let (Some(key_str), Some(name)) = (key.as_string(), value.as_string()) {
                    if let Some(base64) = key_str.strip_prefix(&prefix) {
                        if let Ok(id) = EntityId::from_base64(base64) {
                            map.insert(id, name);
                        }
                    }
                }
                cursor.continue_().require("advance property_columns cursor")?;
            }

            *self.property_columns.write().unwrap() = map;
            self.property_columns_loaded.store(true, Ordering::Relaxed);
            Ok(())
        })
        .await
    }

    /// Assign + persist a durable field name for every property key in a state's
    /// buffers, hydrating the cache first. Runs BEFORE the entities transaction
    /// in `set_state`: all of the assignment path's `property_columns` writes
    /// happen here, on their own transaction, so the entities transaction never
    /// awaits a foreign transaction (which would silently auto-commit it). After
    /// this, every [`Self::field_for_key`] call inside `extract_all_fields` is a
    /// pure cache hit.
    async fn assign_property_fields(&self, entity_state: &EntityState) -> Result<(), MutationError> {
        use ankurah_core::property::backend::backend_from_string;
        self.ensure_property_columns_loaded().await?;
        for (backend_name, state_buffer) in entity_state.state.state_buffers.iter() {
            let backend = backend_from_string(backend_name, Some(state_buffer)).map_err(|e| MutationError::General(Box::new(e)))?;
            for (key, _value) in backend.property_values() {
                self.field_for_key(&key).await?;
            }
        }
        Ok(())
    }

    /// The durable field name for a property key in this collection.
    ///
    /// `Name` keys (system/catalog collections, legacy residue) use the name
    /// directly. `Id` keys resolve through the durable map
    /// ([`Self::property_columns`]); a miss assigns a field NOW: seed from the
    /// catalog resolver's display name (sanitized), dedupe against the reserved
    /// fields and this collection's other assignments (`{name}_{trailing id
    /// chars}`, the ratified collision rule), or -- when the resolver cannot
    /// name the id (the intra-node descriptor race; should effectively never
    /// fire) -- a synthetic `p_{trailing id chars}` name, logged loudly.
    ///
    /// PRECONDITION: the cache is already loaded (via
    /// [`Self::ensure_property_columns_loaded`]). A cache HIT returns WITHOUT
    /// awaiting, so it is safe to call while the entities transaction is open;
    /// the miss/assignment path opens the `property_columns` transaction and so
    /// must only be reached before that (see [`Self::assign_property_fields`]).
    async fn field_for_key(&self, key: &PropertyKey) -> Result<String, MutationError> {
        let id = match key {
            PropertyKey::Name(name) => return Ok(name.clone()),
            PropertyKey::Id(id) => *id,
        };
        if let Some(name) = self.property_columns.read().unwrap().get(&id) {
            return Ok(name.clone());
        }

        // Assignment path. The loaded cache is the complete taken-set: wasm is
        // single-threaded, so no other writer is assigning concurrently.
        let seed = {
            let resolver = self.resolver.read().unwrap().as_ref().and_then(|weak| weak.upgrade());
            resolver.and_then(|r| r.name_for(&id)).map(|name| naming::sanitize(&name))
        };
        let field = {
            let assigned = self.property_columns.read().unwrap();
            let is_taken = |candidate: &str| {
                RESERVED_FIELDS.contains(&candidate) || assigned.iter().any(|(other, name)| *other != id && name == candidate)
            };
            match &seed {
                Some(seed) => naming::dedupe(seed, &id, is_taken),
                None => {
                    warn!(
                        "IndexedDBBucket({}): catalog cannot name property {}; assigning fallback field (descriptor race?)",
                        self.collection_id,
                        id.to_base64()
                    );
                    naming::fallback("p", &id, is_taken)
                }
            }
        };
        let stored = self.persist_property_column(&id, &field).await?;
        self.property_columns.write().unwrap().insert(id, stored.clone());
        Ok(stored)
    }

    /// Persist an id-to-field assignment to the `property_columns` store,
    /// returning the durable name. Get-then-put rather than a CAS loop: wasm is
    /// single-threaded, so between the get and the put no other writer can
    /// intervene. The get still runs -- belt and suspenders -- and yields to any
    /// assignment a prior session already durably made for this id.
    async fn persist_property_column(&self, id: &EntityId, proposed: &str) -> Result<String, MutationError> {
        let map_key = property_columns_key(self.collection_id.as_str(), id);
        let proposed = proposed.to_string();
        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction = db_connection
                .transaction_with_str_and_mode("property_columns", web_sys::IdbTransactionMode::Readwrite)
                .require("create property_columns transaction")?;
            let store = transaction.object_store("property_columns").require("get property_columns store")?;

            let get_request = store.get(&JsValue::from_str(&map_key)).require("get property_columns entry")?;
            cb_future(&get_request, "success", "error").await.require("await property_columns get")?;
            let existing = get_request.result().require("get property_columns result")?;
            if let Some(name) = existing.as_string() {
                // Already assigned durably (e.g. by a prior session this bucket
                // never loaded); keep the durable name.
                return Ok(name);
            }

            let put_request =
                store.put_with_key(&JsValue::from_str(&proposed), &JsValue::from_str(&map_key)).require("put property_columns entry")?;
            cb_future(&put_request, "success", "error").await.require("await property_columns put")?;
            cb_future(&transaction, "complete", "error").await.require("complete property_columns transaction")?;
            Ok(proposed)
        })
        .await
    }

    /// Extract all fields from entity state and set them directly on the
    /// IndexedDB entity object, keyed by their durable field names.
    async fn extract_all_fields(&self, entity_obj: &Object, entity_state: &EntityState) -> Result<(), MutationError> {
        use ankurah_core::property::backend::backend_from_string;
        use std::collections::HashSet;

        let mut seen_fields = HashSet::new();

        // Process all property values from state buffers
        for (backend_name, state_buffer) in entity_state.state.state_buffers.iter() {
            let backend = backend_from_string(backend_name, Some(state_buffer)).map_err(|e| MutationError::General(Box::new(e)))?;

            for (key, value) in backend.property_values() {
                // Resolve the durable field name through the engine-owned map.
                // The cache was warmed by `assign_property_fields` before the
                // entities transaction opened, so this is always a hit here and
                // never opens a transaction that would evict the entities one.
                let field_name = self.field_for_key(&key).await?;
                // First occurrence wins on same-NAME collisions (cross-backend
                // Name residue, or a Name and an Id that resolve to the same
                // string). Same-collection id collisions can't reach here: they
                // were deduped to distinct names at assignment.
                if !seen_fields.insert(field_name.clone()) {
                    continue;
                }

                // Set field directly on entity object (no prefix - they become the primary fields)
                // Use IdbValue encoding to ensure fields are IndexedDB-key-compatible (bool as 0/1, etc.)
                let js_value = match value {
                    Some(ref prop_value) => crate::idb_value::IdbValue::from(prop_value).into(),
                    None => JsValue::NULL,
                };
                entity_obj.set(&field_name, js_value)?;
            }
        }

        Ok(())
    }

    async fn execute_plan_query(
        &self,
        index: &web_sys::IdbIndex,
        key_range: Option<web_sys::IdbKeyRange>,
        predicate: &ankql::ast::Predicate,
        cursor_direction: web_sys::IdbCursorDirection,
        limit: Option<u64>,
        collection_id: &ankurah_proto::CollectionId,
        upper_open_ended: bool,
        eq_prefix_len: usize,
        eq_prefix_values: Vec<ankurah_core::value::Value>,
        order_by_spill: &OrderByComponents,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        // Resolved on the first row that needs stamping: an empty scan on a
        // cold catalog must return empty, not a model-resolution error.
        let mut model_memo: Option<ankurah_proto::EntityId> = None;
        let needs_spill_sort = !order_by_spill.spill.is_empty();

        // Determine effective prefix guard config (can be disabled in debug builds for testing)
        #[cfg(debug_assertions)]
        let effective_prefix_len =
            if upper_open_ended && eq_prefix_len > 0 && !self.prefix_guard_disabled.load(std::sync::atomic::Ordering::Relaxed) {
                eq_prefix_len
            } else {
                0
            };
        #[cfg(not(debug_assertions))]
        let effective_prefix_len = if upper_open_ended && eq_prefix_len > 0 { eq_prefix_len } else { 0 };

        // Use IdbIndexScanner for cursor iteration with prefix guard
        let scanner =
            crate::scanner::IdbIndexScanner::new(index.clone(), key_range, cursor_direction, effective_prefix_len, eq_prefix_values);

        let mut stream = std::pin::pin!(scanner.scan());
        let mut count = 0u64;
        let mut rows: Vec<IdbRecord> = Vec::new();
        let mut direct_results: Vec<Attested<EntityState>> = Vec::new();

        while let Some(result) = stream.next().await {
            let entity_obj = result?;

            let model = match model_memo {
                Some(model) => model,
                None => {
                    let model = self.model_id()?;
                    model_memo = Some(model);
                    model
                }
            };

            // Create IdbRecord - wraps JS object with lazy value extraction
            let record = match IdbRecord::new(entity_obj, collection_id.clone(), model) {
                Ok(r) => r,
                Err(_) => continue,
            };

            // Apply predicate filtering (uses lazy extraction from IdbRecord)
            if evaluate_predicate(&record, predicate)
                .map_err(|e| RetrievalError::StorageError(format!("Predicate evaluation failed: {}", e).into()))?
            {
                if needs_spill_sort {
                    // Collect for sorting
                    rows.push(record);
                } else {
                    // No sorting needed - extract entity state and apply limit during scan
                    if let Ok(entity_state) = record.entity_state() {
                        direct_results.push(entity_state);
                        count += 1;

                        if let Some(limit_val) = limit {
                            if count >= limit_val {
                                break;
                            }
                        }
                    }
                }
            }
        }

        // If we need to sort by spilled columns, use partition-aware sorting
        if needs_spill_sort {
            // Use ValueSetStream trait methods for partition-aware sorting
            let results: Vec<Attested<EntityState>> = match limit {
                Some(limit_val) => {
                    // Use partition-aware TopK
                    futures::stream::iter(rows)
                        .top_k(order_by_spill.clone(), limit_val as usize)
                        .filter_map(|r| async move { r.entity_state().ok() })
                        .collect()
                        .await
                }
                None => {
                    // Use partition-aware sort
                    futures::stream::iter(rows)
                        .sort_by(order_by_spill.clone())
                        .filter_map(|r| async move { r.entity_state().ok() })
                        .collect()
                        .await
                }
            };
            Ok(results)
        } else {
            Ok(direct_results)
        }
    }
}

/// A record from the IndexedDB entities store.
///
/// Wraps the raw JS object with lazy extraction for filtering and sorting.
/// Implements `Filterable` and `HasEntityId` for use with stream combinators.
struct IdbRecord {
    id: ankurah_proto::EntityId,
    object: Object,
    collection_id: ankurah_proto::CollectionId,
    /// The model id stamped on reconstructed envelopes (#330), resolved once
    /// by the bucket at record construction.
    model: ankurah_proto::EntityId,
}

impl IdbRecord {
    /// Create a new IdbRecord from a JS object
    fn new(object: Object, collection_id: ankurah_proto::CollectionId, model: ankurah_proto::EntityId) -> Result<Self, RetrievalError> {
        let id: ankurah_proto::EntityId = object.get(&ID_KEY)?;
        Ok(Self { id, object, collection_id, model })
    }

    /// Get the entity state (converts from JS object on demand)
    fn entity_state(&self) -> Result<Attested<EntityState>, RetrievalError> { js_object_to_entity_state(&self.object, self.model) }

    /// Extract property values needed for sorting
    fn extract_sort_properties(&self, order_by: &OrderByComponents) -> std::collections::BTreeMap<String, ankurah_core::value::Value> {
        extract_sort_properties(&self.object, order_by)
    }
}

impl Filterable for IdbRecord {
    fn collection(&self) -> &str { self.collection_id.as_str() }

    fn value(&self, name: &str) -> Option<ankurah_core::value::Value> {
        // Lazy extraction from JS object
        let idb_val: crate::idb_value::IdbValue = self.object.get_opt(&name.into()).ok()??;
        Some(idb_val.into_value())
    }
}

impl ankurah_storage_common::filtering::HasEntityId for IdbRecord {
    fn entity_id(&self) -> ankurah_proto::EntityId { self.id }
}

/// Extract property values needed for sorting from a JS object
fn extract_sort_properties(
    entity_obj: &Object,
    order_by: &OrderByComponents,
) -> std::collections::BTreeMap<String, ankurah_core::value::Value> {
    let mut map = std::collections::BTreeMap::new();
    // Extract all ORDER BY columns - presort for partition detection, spill for sorting
    for item in &order_by.presort {
        let property_name = item.path.property();
        if let Ok(Some(idb_val)) = entity_obj.get_opt::<crate::idb_value::IdbValue>(&property_name.into()) {
            map.insert(property_name.to_string(), idb_val.into_value());
        }
    }
    for item in &order_by.spill {
        let property_name = item.path.property();
        if let Ok(Some(idb_val)) = entity_obj.get_opt::<crate::idb_value::IdbValue>(&property_name.into()) {
            map.insert(property_name.to_string(), idb_val.into_value());
        }
    }
    map
}

/// Convert JS object to EntityState using the correct field extraction.
/// `model` is the model id stamped on the reconstructed envelope (#330).
fn js_object_to_entity_state(entity_obj: &Object, model: ankurah_proto::EntityId) -> Result<Attested<EntityState>, RetrievalError> {
    use crate::statics::{ATTESTATIONS_KEY, GENERATIONS_KEY, HEAD_KEY, ID_KEY, STATE_BUFFER_KEY};
    use ankurah_proto::{Attested, EntityId, EntityState, State};

    // Extract the specific fields that are stored in IndexedDB using Object::get
    let id: EntityId = entity_obj.get(&ID_KEY)?;

    let entity_state = EntityState {
        model,
        entity_id: id,
        state: State {
            state_buffers: entity_obj.get(&STATE_BUFFER_KEY)?,
            head: entity_obj.get(&HEAD_KEY)?,
            head_generations: entity_obj.get(&GENERATIONS_KEY)?,
        },
    };

    let attestations = entity_obj.get(&ATTESTATIONS_KEY)?;
    let attested_state = Attested { payload: entity_state, attestations };

    Ok(attested_state)
}

/// Amend a selection with __collection = 'value' comparison
pub fn add_collection(selection: &ankql::ast::Selection, collection_id: &ankurah_proto::CollectionId) -> ankql::ast::Selection {
    use ankql::ast::{ComparisonOperator, Expr, Literal, PathExpr, Predicate};

    let collection_comparison = Predicate::Comparison {
        left: Box::new(Expr::Path(PathExpr::simple("__collection"))),
        operator: ComparisonOperator::Equal,
        right: Box::new(Expr::Literal(Literal::String(collection_id.to_string()))),
    };

    ankql::ast::Selection {
        predicate: Predicate::And(Box::new(collection_comparison), Box::new(selection.predicate.clone())),
        order_by: selection.order_by.clone(),
        limit: selection.limit,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::ast::{OrderByItem, OrderDirection, PathExpr, Predicate, Selection};
    use ankurah_core::value::ValueType;

    struct RenamedCatalogResolver {
        property: EntityId,
    }

    impl CatalogResolver for RenamedCatalogResolver {
        fn resolve(&self, collection: &str, name: &str) -> Option<EntityId> {
            (collection == "record" && name == "score").then_some(self.property)
        }

        fn name_for(&self, id: &EntityId) -> Option<String> { (*id == self.property).then(|| "score".to_string()) }

        fn canonical_value_type(&self, id: &EntityId) -> Option<String> { (*id == self.property).then(|| "i32".to_string()) }
    }

    #[test]
    fn resolved_order_type_survives_sticky_physical_name() {
        let property = EntityId::from_bytes([7; 16]);
        let resolver = RenamedCatalogResolver { property };
        let selection = Selection {
            predicate: Predicate::True,
            order_by: Some(vec![OrderByItem {
                path: PathExpr::simple("score"),
                direction: OrderDirection::Asc,
                property: Some(property.to_ulid()),
            }]),
            limit: None,
        };

        // Simulate a column assigned before the catalog display name changed,
        // or a collision-suffixed assignment that was never a catalog name.
        let assigned = BTreeMap::from([(property, "legacy_score".to_string())]);
        let column_of = |id: &EntityId| assigned.get(id).cloned();
        let order_types = order_types_in_column_space("record", &selection, Some(&resolver), &column_of);
        let translated = ankurah_core::storage::selection_to_column_space("record", &selection, Some(&resolver), &column_of);
        let physical_name = translated.order_by.as_ref().unwrap()[0].path.first();

        assert_eq!(physical_name, "legacy_score");
        assert_eq!(resolver.resolve("record", physical_name), None, "the physical name is not catalog-resolvable");
        assert_eq!(order_types.get(physical_name), Some(&ValueType::I32));

        // Pin the actual planner boundary: the physical field still gets the
        // canonical numeric collation rather than String fallback.
        let amended = add_collection(&translated, &proto::CollectionId::fixed_name("record"));
        let planner = ankurah_storage_common::planner::Planner::new(ankurah_storage_common::planner::PlannerConfig::indexeddb());
        let plans = planner.plan_with_types(&amended, "id", &|name| order_types.get(name).copied());
        let index_spec = plans
            .iter()
            .find_map(|plan| match plan {
                Plan::Index { index_spec, .. } => Some(index_spec),
                _ => None,
            })
            .expect("ORDER BY should produce an IndexedDB index plan");
        let score_key = index_spec.keyparts.iter().find(|part| part.column == "legacy_score").expect("physical ORDER BY key");
        assert_eq!(score_key.value_type, ValueType::I32);
    }
}
