use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use ankql::ast::{OrderKey, PropertyId};
use ankurah_core::{
    action_debug,
    error::{MutationError, RetrievalError},
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

#[derive(Debug)]
pub struct IndexedDBBucket {
    pub(crate) db: Database,
    pub(crate) collection_id: proto::CollectionId,
    pub(crate) mutex: tokio::sync::Mutex<()>, // should probably be implemented by Database, but not certain
    pub(crate) invocation_count: AtomicUsize,
    /// The injected catalog resolver (shared with the engine): the NAME SOURCE
    /// for [`Self::column_for_key`]. Weak so storage never keeps the node alive.
    pub(crate) resolver: Arc<RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
    /// This collection's slice of the engine-owned durable identity-to-field map
    /// (the `property_columns` object store), cached in memory and keyed by
    /// durable [`PropertyId`] (NOT `EntityId`) so the write side (a backend's
    /// `property_values()` id) and the read side (a `PropertyId` off the resolved
    /// AST) address the same row. The map -- not the display name -- is what
    /// addresses a property's field once assigned: renames never move fields,
    /// collisions were deduped at assignment.
    pub(crate) property_columns: Arc<RwLock<BTreeMap<PropertyId, String>>>,
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

/// Store key for a property identity's field-name assignment in `collection`:
/// `{collection}\0{serialized PropertyId}`. The suffix is the property's durable
/// serde identity (JSON, see [`property_key_text`]) -- NOT an `EntityId` -- so
/// the write side (a backend's `property_values()` id) and the read side (a
/// `PropertyId` off the resolved AST) address the byte-identical row, and a
/// system property gets a row exactly like a registered one. A concatenated
/// string key (matching the store's other string keys) so a collection's whole
/// slice is one prefix range; collection names carry no NUL and JSON escapes any
/// control byte, so the NUL cleanly separates the scope from the identity suffix.
fn property_columns_key(collection: &str, id: &PropertyId) -> String { format!("{}\0{}", collection, property_key_text(id)) }

/// The durable, serialized address of a property identity: the JSON a
/// [`PropertyId`] serializes to (mirrors the sqlite engine's `property_key`
/// text). Legible and stable across the serde boundary the durable map key
/// crosses; the write side (the id a backend yields) and the read side (an id
/// straight off the resolved AST) both go through this, so a field assigned on
/// write is found by the byte-identical key on read.
fn property_key_text(id: &PropertyId) -> String { serde_json::to_string(id).expect("PropertyId always serializes") }

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
                    state: State { state_buffers: entity.get(&STATE_BUFFER_KEY)?, head: entity.get(&HEAD_KEY)? },
                },
                attestations: entity.get(&ATTESTATIONS_KEY)?,
            })
        })
        .await
    }

    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        // This engine can only address a property by identity. Refuse a
        // selection that still carries a name, before taking the lock.
        selection.check()?;
        let _invocation = self.invocation_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _lock = self.mutex.lock().await; // TODO why are we locking here?

        // Resolve, through this engine's own durable map, the field for every
        // property the selection references (assigned on write, sticky under
        // rename). There is NO name fallback: a property with no assigned field
        // is ABSENT (evaluates NULL, folded below), never re-derived from a raw
        // name (the #374 fix). The planner then translates each surviving
        // identity to its field at build time via `column_of`, and the in-memory
        // post-filter/sort keep reading by identity, so a rename is harmless end
        // to end.
        self.ensure_property_columns_loaded().await.map_err(|e| RetrievalError::StorageError(format!("{e:?}").into()))?;
        let resolver = self.resolver.read().expect("RwLock poisoned").as_ref().and_then(|weak| weak.upgrade());
        let assigned = std::sync::Arc::new(self.property_columns.read().expect("RwLock poisoned").clone());

        // Translate a resolved identity to this engine's physical field through
        // the durable map (a miss = absent, no name fallback); `PropertyId::Id`
        // is pinned to the reserved "id" field so a primary-key read is a uniform
        // map hit and never wrongly absent.
        let column_of = {
            let assigned = assigned.clone();
            move |pid: &PropertyId| -> Option<String> {
                if matches!(pid, PropertyId::Id) {
                    return Some("id".to_string());
                }
                assigned.get(pid).cloned()
            }
        };

        // Read-side absence, by durable identity: fold every referenced property
        // this engine never materialized to NULL, and drop any ORDER BY key on
        // one.
        let referenced = selection.referenced_properties();
        let absent: Vec<PropertyId> = referenced.into_iter().filter(|pid| column_of(pid).is_none()).collect();
        let selection = if absent.is_empty() { selection.clone() } else { selection.assume_null(&absent) };

        // Bind each resolved ORDER BY key's canonical value_type (the canonical
        // value_type ruling), keyed by the PHYSICAL field the planner resolves it
        // to via `column_of`, so ordered index scans collate numerics
        // numerically. A key whose canonical type is unknown (system properties,
        // the primary key) keeps the historical String collation.
        let order_types: BTreeMap<String, ankurah_core::value::ValueType> = selection
            .order_by
            .iter()
            .flatten()
            .filter_map(|item| {
                let OrderKey::Property(pp) = &item.key else { return None };
                let property_id = pp.id();
                let PropertyId::EntityId(ulid) = property_id else { return None };
                let resolver = resolver.as_deref()?;
                let value_type =
                    ankurah_core::value::ValueType::from_property_str(&resolver.canonical_value_type(&EntityId::from_ulid(ulid))?)?;
                let column = column_of(&property_id)?;
                Some((column, value_type))
            })
            .collect();

        // Step 1: Amend predicate with __collection comparison
        let amended_selection = add_collection(&selection, &self.collection_id);

        // Step 2: Use planner to generate query plans. ORDER BY key parts collate
        // in each property's CANONICAL value_type (keyed by physical field);
        // unresolvable fields keep the historical String collation. `column_of`
        // resolves each identity the planner touches to its field (no name
        // fallback), so a rename never moves the field a plan addresses.
        let order_type_of = |column: &str| -> Option<ankurah_core::value::ValueType> { order_types.get(column).copied() };
        let planner = ankurah_storage_common::planner::Planner::new(ankurah_storage_common::planner::PlannerConfig::indexeddb());
        let plans = planner.plan_with_types(&amended_selection, "id", &order_type_of, &column_of);

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
                // The durable identity-to-field snapshot the post-filter/sort use
                // to read each referenced property by resolved identity.
                let columns = assigned.clone();

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
                            columns,
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
                    },
                    attestations: event_obj.get(&ATTESTATIONS_KEY)?,
                };
                events.push(event);
            }

            Ok(events)
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
    /// The model id written on envelopes this bucket reconstructs (#330):
    /// well-knowns, then the injected catalog resolver.
    fn model_id(&self) -> Result<ankurah_proto::ModelId, RetrievalError> {
        let resolver = self.resolver.read().expect("RwLock poisoned").as_ref().and_then(|weak| weak.upgrade());
        ankurah_core::storage::bucket_model_id(&self.collection_id, resolver.as_deref())
    }

    /// [`Self::model_id`] as a clonable outcome for the scan loop, which
    /// checks it only when a row actually needs its model id: a scan that matches
    /// nothing must not fail for want of a model id (cold catalog, e.g. a
    /// fetch over persisted rows whose post-filter rejects every one).
    /// Mirrors sled's `model_id_lazy`.
    fn model_id_lazy(&self) -> Result<ankurah_proto::ModelId, String> { self.model_id().map_err(|e| e.to_string()) }

    /// Hydrate `property_columns` with this collection's durable
    /// identity-to-field assignments, once per bucket. Runs BEFORE any write
    /// transaction so the loaded cache is the authoritative taken-set for
    /// [`Self::column_for_key`] (and so a field lookup never has to read the
    /// store while the entities transaction is open -- IndexedDB auto-commits a
    /// transaction the moment an await doesn't belong to it).
    async fn ensure_property_columns_loaded(&self) -> Result<(), MutationError> {
        if self.property_columns_loaded.load(Ordering::Relaxed) {
            return Ok(());
        }
        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction = db_connection.transaction_with_str("property_columns").require("create property_columns transaction")?;
            let store = transaction.object_store("property_columns").require("get property_columns store")?;

            // This collection's rows are exactly the keys under the
            // `{collection}\0` prefix; the serialized-PropertyId suffix is JSON
            // (all chars sort below U+FFFF), so this bound captures the slice and
            // nothing else.
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
                // key = "{collection}\0{serialized PropertyId}", value = the field
                // name. A suffix we cannot parse is skipped rather than poisoning
                // the whole map.
                if let (Some(key_str), Some(name)) = (key.as_string(), value.as_string()) {
                    if let Some(suffix) = key_str.strip_prefix(&prefix) {
                        if let Ok(property_id) = serde_json::from_str::<PropertyId>(suffix) {
                            map.insert(property_id, name);
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

    /// Assign + persist a durable field name for every property identity in a
    /// state's buffers, hydrating the cache first. Runs BEFORE the entities
    /// transaction in `set_state`: all of the assignment path's
    /// `property_columns` writes happen here, on their own transaction, so the
    /// entities transaction never awaits a foreign transaction (which would
    /// silently auto-commit it). After this, every [`Self::column_for_key`] call
    /// inside `extract_all_fields` is a pure cache hit.
    async fn assign_property_fields(&self, entity_state: &EntityState) -> Result<(), MutationError> {
        use ankurah_core::property::backend::backend_from_string;
        self.ensure_property_columns_loaded().await?;
        for (backend_name, state_buffer) in entity_state.state.state_buffers.iter() {
            let backend = backend_from_string(backend_name, Some(state_buffer)).map_err(|e| MutationError::General(Box::new(e)))?;
            for (property_id, _value) in backend.property_values() {
                self.column_for_key(&property_id).await?;
            }
        }
        Ok(())
    }

    /// The durable field name for a property identity in this collection.
    ///
    /// Both arms route through the durable map (there is no name short-circuit),
    /// so a system property is addressed by identity on reads exactly like a
    /// registered one. A cache miss assigns a field NOW, mirroring the sqlite
    /// engine:
    /// - a **system** property (`System { name }`) has no entity id to
    ///   suffix-dedupe with, so its sanitized name is the field directly -- and
    ///   a collision with a reserved record field or a field already assigned
    ///   to a different identity is a HARD ERROR rather than a silent
    ///   absorption (ingest is catalog-free, so a state buffer from an
    ///   untrusted peer can carry any system name; absorbing a collision is
    ///   how aliasing arose). Re-assignment by the same identity is idempotent
    ///   (the cache hit above);
    /// - a **registered** property (`EntityId`) seeds from the catalog resolver's
    ///   display name (sanitized), deduped against the reserved fields and this
    ///   collection's other assignments (`{name}_{trailing id chars}`, the
    ///   ratified collision rule), or -- when the resolver cannot name the id
    ///   (the intra-node descriptor race; should effectively never fire) -- a
    ///   synthetic `p_{trailing id chars}` name, logged loudly;
    /// - the **`id`** pseudo-property is the primary key, never a stored value, so
    ///   it never reaches field assignment (it is pinned to the `"id"` field in
    ///   the read path's `column_of`).
    ///
    /// PRECONDITION: the cache is already loaded (via
    /// [`Self::ensure_property_columns_loaded`]). A cache HIT returns WITHOUT
    /// awaiting, so it is safe to call while the entities transaction is open;
    /// the miss/assignment path opens the `property_columns` transaction and so
    /// must only be reached before that (see [`Self::assign_property_fields`]).
    async fn column_for_key(&self, property_id: &PropertyId) -> Result<String, MutationError> {
        if let Some(field) = self.property_columns.read().unwrap().get(property_id) {
            return Ok(field.clone());
        }

        // Assignment path. The loaded cache is the complete taken-set: wasm is
        // single-threaded, so no other writer is assigning concurrently.
        let field = match property_id {
            // System property: its sanitized name is the field directly. There
            // is no entity id to suffix-dedupe with, so a collision cannot be
            // widened away like the EntityId arm's -- and silently absorbing
            // one is how aliasing arose -- so a collision with a reserved
            // record field, or with a field already assigned to a DIFFERENT
            // identity, is a hard error. (The SAME identity re-asserting its
            // field is idempotent and already returned from the cache hit
            // above.)
            PropertyId::System { name } => {
                let field = naming::sanitize(name);
                if RESERVED_FIELDS.contains(&field.as_str()) {
                    return Err(MutationError::General(
                        anyhow::anyhow!(
                            "system property {:?} sanitizes to {:?}, a reserved record field of collection {}; refusing the assignment",
                            name,
                            field,
                            self.collection_id
                        )
                        .into(),
                    ));
                }
                let assigned = self.property_columns.read().unwrap();
                if let Some((owner, _)) = assigned.iter().find(|(owner, taken)| *owner != property_id && taken.as_str() == field) {
                    return Err(MutationError::General(
                        anyhow::anyhow!(
                            "system property {:?} sanitizes to field {:?}, which is already assigned to property {} in collection {}; refusing the assignment",
                            name,
                            field,
                            property_key_text(owner),
                            self.collection_id
                        )
                        .into(),
                    ));
                }
                drop(assigned);
                field
            }
            // Registered property: seed from the resolver, dedupe by id.
            PropertyId::EntityId(ulid) => {
                let id = EntityId::from_ulid(*ulid);
                let seed = {
                    let resolver = self.resolver.read().unwrap().as_ref().and_then(|weak| weak.upgrade());
                    resolver.and_then(|r| r.name_for(&id)).map(|name| naming::sanitize(&name))
                };
                let assigned = self.property_columns.read().unwrap();
                let is_taken = |candidate: &str| {
                    RESERVED_FIELDS.contains(&candidate) || assigned.iter().any(|(other, name)| other != property_id && name == candidate)
                };
                match &seed {
                    Some(seed) => naming::dedupe(seed, &id, is_taken).map_err(|e| MutationError::General(anyhow::anyhow!(e).into()))?,
                    None => {
                        warn!(
                            "IndexedDBBucket({}): catalog cannot name property {}; assigning fallback field (descriptor race?)",
                            self.collection_id,
                            id.to_base64()
                        );
                        naming::fallback("p", &id, is_taken).map_err(|e| MutationError::General(anyhow::anyhow!(e).into()))?
                    }
                }
            }
            // The `id` pseudo-property is the primary key, never a stored value.
            PropertyId::Id => {
                return Err(MutationError::General(
                    anyhow::anyhow!("the id pseudo-property is never materialized as a stored value").into(),
                ))
            }
        };
        let stored = self.persist_property_column(property_id, &field).await?;
        self.property_columns.write().unwrap().insert(property_id.clone(), stored.clone());
        Ok(stored)
    }

    /// Persist an identity-to-field assignment to the `property_columns` store,
    /// returning the durable name. Get-then-put rather than a CAS loop: wasm is
    /// single-threaded, so between the get and the put no other writer can
    /// intervene. The get still runs -- belt and suspenders -- and yields to any
    /// assignment a prior session already durably made for this identity.
    async fn persist_property_column(&self, property_id: &PropertyId, proposed: &str) -> Result<String, MutationError> {
        let map_key = property_columns_key(self.collection_id.as_str(), property_id);
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

            for (property_id, value) in backend.property_values() {
                // Resolve the durable field name through the engine-owned map by
                // identity. The cache was warmed by `assign_property_fields`
                // before the entities transaction opened, so this is always a hit
                // here and never opens a transaction that would evict the entities
                // one.
                let field_name = self.column_for_key(&property_id).await?;
                // First occurrence wins on same-field collisions (a cross-backend
                // duplicate). Same-collection identity collisions can't reach
                // here: assignment deduped them to distinct fields (EntityId) or
                // refused the state outright (System), so no property field ever
                // lands on a reserved record field or another identity's field.
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
        columns: std::sync::Arc<BTreeMap<PropertyId, String>>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        // The model-id resolution OUTCOME, captured up front and checked only
        // when a row actually needs its model id (mirrors sled's model_id_lazy):
        // a fetch over persisted rows whose post-filter rejects every one must
        // return empty on a cold catalog, not a model-resolution error.
        let model = self.model_id_lazy();
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

            // Create IdbRecord - wraps JS object with lazy value extraction,
            // carrying the identity-to-field snapshot so it reads by identity.
            let record = match IdbRecord::new(entity_obj, collection_id.clone(), model.clone(), columns.clone()) {
                Ok(r) => r,
                Err(_) => continue,
            };

            // Apply predicate filtering (uses lazy extraction from IdbRecord)
            if evaluate_predicate(&record, predicate)
                .map_err(|e| RetrievalError::StorageError(format!("Predicate evaluation failed: {}", e).into()))?
            {
                // This row matched, so its envelope needs the model id NOW: an
                // unresolved model is only an error once there is an envelope to
                // fill in. (The loud check lives here because the hydration
                // calls below swallow per-row errors.)
                if let Err(e) = &record.model {
                    return Err(RetrievalError::Other(e.clone()));
                }
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
/// The object's fields are keyed by physical field name, so the `Filterable`
/// impl translates each resolved IDENTITY to its field through the durable map
/// snapshot ([`Self::columns`]) -- reading by identity exactly like the entity's
/// own evaluation, with NO fallback to a name (the #374 fix).
struct IdbRecord {
    id: ankurah_proto::EntityId,
    object: Object,
    collection_id: ankurah_proto::CollectionId,
    /// The model-id resolution OUTCOME written on reconstructed envelopes
    /// (#330), captured by the bucket at scan construction and checked only
    /// when a row actually needs its model id (mirrors sled's model_id_lazy): an
    /// empty result set never demands a model id.
    model: Result<ankurah_proto::ModelId, String>,
    /// This collection's durable identity-to-field map (snapshot), the sole
    /// translation from a resolved property identity to the physical field the
    /// JS object carries it under.
    columns: std::sync::Arc<BTreeMap<PropertyId, String>>,
}

impl IdbRecord {
    /// Create a new IdbRecord from a JS object
    fn new(
        object: Object,
        collection_id: ankurah_proto::CollectionId,
        model: Result<ankurah_proto::ModelId, String>,
        columns: std::sync::Arc<BTreeMap<PropertyId, String>>,
    ) -> Result<Self, RetrievalError> {
        let id: ankurah_proto::EntityId = object.get(&ID_KEY)?;
        Ok(Self { id, object, collection_id, model, columns })
    }

    /// Get the entity state (converts from JS object on demand). This row is
    /// having its envelope built, so an unresolved model id is an error here.
    fn entity_state(&self) -> Result<Attested<EntityState>, RetrievalError> {
        let model = self.model.clone().map_err(RetrievalError::Other)?;
        js_object_to_entity_state(&self.object, model)
    }

    /// Lazily decode the physical field `field` off the JS object, if present.
    fn field_value(&self, field: &str) -> Option<ankurah_core::value::Value> {
        let idb_val: crate::idb_value::IdbValue = self.object.get_opt(&field.into()).ok()??;
        Some(idb_val.into_value())
    }
}

impl Filterable for IdbRecord {
    fn collection(&self) -> &str { self.collection_id.as_str() }

    /// Read the `id` pseudo-property, a reserved system object field
    /// (`__collection` and friends, stored verbatim), or a SYSTEM property. `id`
    /// is the record's own EntityId; a system property is addressed through the
    /// durable map by identity (a miss is absent, no name fallback). A registered
    /// property is read by id via [`Filterable::value_by_id`], not here.
    fn value(&self, name: &str) -> Option<ankurah_core::value::Value> {
        if name == "id" {
            return Some(ankurah_core::value::Value::EntityId(self.id));
        }
        if name.starts_with("__") {
            return self.field_value(name);
        }
        let field = self.columns.get(&PropertyId::System { name: name.to_string() })?;
        self.field_value(field)
    }

    /// Read a registered property by its stable id -- the identity a resolved
    /// `Expr::PropertyPath` evaluates through. Identity -> field via the
    /// durable map; a miss is absent (NULL), no name fallback.
    fn value_by_id(&self, property_id: ankurah_proto::EntityId) -> Option<ankurah_core::value::Value> {
        let field = self.columns.get(&PropertyId::EntityId(property_id.to_ulid()))?;
        self.field_value(field)
    }
}

impl ankurah_storage_common::filtering::HasEntityId for IdbRecord {
    fn entity_id(&self) -> ankurah_proto::EntityId { self.id }
}

/// Convert JS object to EntityState using the correct field extraction.
/// `model` is the model id written on the reconstructed envelope (#330).
fn js_object_to_entity_state(entity_obj: &Object, model: ankurah_proto::ModelId) -> Result<Attested<EntityState>, RetrievalError> {
    use crate::statics::{ATTESTATIONS_KEY, HEAD_KEY, ID_KEY, STATE_BUFFER_KEY};
    use ankurah_proto::{Attested, EntityId, EntityState, State};

    // Extract the specific fields that are stored in IndexedDB using Object::get
    let id: EntityId = entity_obj.get(&ID_KEY)?;

    let entity_state = EntityState {
        model,
        entity_id: id,
        state: State { state_buffers: entity_obj.get(&STATE_BUFFER_KEY)?, head: entity_obj.get(&HEAD_KEY)? },
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
    use ankql::ast::{OrderByItem, OrderDirection, OrderKey, Predicate, PropertyPath, Selection};
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

    /// The durable field an ORDER BY property was assigned is sticky across a
    /// catalog rename and may be collision-suffixed, so it is NOT catalog
    /// resolvable. Binding the canonical value_type by IDENTITY (before touching
    /// the physical field) and keying `order_type_of` by that field must still
    /// give the planner the canonical numeric collation -- never the String
    /// fallback, and never a name-derived field (the #374 fix).
    #[test]
    fn resolved_order_type_survives_sticky_physical_field() {
        let property = EntityId::from_bytes([7; 16]);
        let resolver = RenamedCatalogResolver { property };

        // A resolved ORDER BY on the registered property. Its catalog display
        // name is now "score", but the resolved identity carries only the id (the
        // "score" label is Display-only and never seeds a read).
        let selection = Selection {
            predicate: Predicate::True,
            order_by: Some(vec![OrderByItem {
                key: OrderKey::Property(PropertyPath::registered(property.to_ulid(), "score", vec![])),
                direction: OrderDirection::Asc,
            }]),
            limit: None,
        };

        // A field assigned before the catalog display name changed (or a
        // collision-suffixed assignment that was never a catalog name), keyed by
        // durable identity exactly like the engine's own cache.
        let assigned: BTreeMap<PropertyId, String> =
            BTreeMap::from([(PropertyId::EntityId(property.to_ulid()), "legacy_score".to_string())]);
        let column_of = |pid: &PropertyId| -> Option<String> {
            if matches!(pid, PropertyId::Id) {
                return Some("id".to_string());
            }
            assigned.get(pid).cloned()
        };

        // Bind the ORDER BY canonical value_type by identity, keyed by the
        // physical field (mirrors `fetch_states`).
        let order_types: BTreeMap<String, ValueType> = selection
            .order_by
            .iter()
            .flatten()
            .filter_map(|item| {
                let OrderKey::Property(pp) = &item.key else { return None };
                let property_id = pp.id();
                let PropertyId::EntityId(ulid) = property_id else { return None };
                let value_type = ValueType::from_property_str(&resolver.canonical_value_type(&EntityId::from_ulid(ulid))?)?;
                let column = column_of(&property_id)?;
                Some((column, value_type))
            })
            .collect();

        assert_eq!(order_types.get("legacy_score"), Some(&ValueType::I32));
        assert_eq!(resolver.resolve("record", "legacy_score"), None, "the physical field is not catalog-resolvable");

        // Pin the actual planner boundary: the physical field still gets the
        // canonical numeric collation rather than String fallback.
        let amended = add_collection(&selection, &proto::CollectionId::fixed_name("record"));
        let order_type_of = |column: &str| order_types.get(column).copied();
        let planner = ankurah_storage_common::planner::Planner::new(ankurah_storage_common::planner::PlannerConfig::indexeddb());
        let plans = planner.plan_with_types(&amended, "id", &order_type_of, &column_of);
        let index_spec = plans
            .iter()
            .find_map(|plan| match plan {
                Plan::Index { index_spec, .. } => Some(index_spec),
                _ => None,
            })
            .expect("ORDER BY should produce an IndexedDB index plan");
        let score_key = index_spec.keyparts.iter().find(|part| part.key == "legacy_score").expect("physical ORDER BY key");
        assert_eq!(score_key.value_type, ValueType::I32);
    }
}
