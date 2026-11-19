//! Canonical-state pages validated against a transactional counter.

use std::num::NonZeroUsize;

use ankql::ast::{Resolved, Selection};
use ankurah_core::{
    error::{MutationError, RetrievalError},
    storage::{StateCursor, StateSnapshot},
};
use ankurah_proto::{Attested, EntityId, EntityState};
use ankurah_storage_common::{selection::LimitedSelection, state::CounterCursor};
use async_trait::async_trait;
use futures::StreamExt;
use js_sys::{Array, Uint8Array};
use send_wrapper::SendWrapper;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{IdbCursorWithValue, IdbDatabase, IdbObjectStore, IdbTransaction, IdbTransactionMode};

use super::{completed, entity_state_from_object, AbortUnfinishedTransaction};
use crate::{
    database::Database,
    util::{cb_future::cb_future, cb_stream::cb_stream, object::Object, require::WBGRequire},
};

pub(crate) const META: &str = "_ankurah_meta";
pub(crate) const CHANGES: &str = "_ankurah_state_changes";
const CURSOR: &str = "state_cursor";
const INDEX: &str = "by_position";
const ENGINE: &[u8] = b"indexeddb";

pub(crate) fn has_object_stores(database: &IdbDatabase) -> bool { database.object_store_names().contains(CHANGES) }

pub(crate) fn create_object_stores(database: &IdbDatabase) -> Result<(), JsValue> {
    database.create_object_store(CHANGES)?.create_index_with_str_sequence(INDEX, &Array::of2(&"position".into(), &"id".into()))?;
    Ok(())
}

fn decode(value: JsValue) -> Result<CounterCursor, RetrievalError> {
    let bytes = value.dyn_into::<Uint8Array>().map_err(|_| RetrievalError::InvalidStateCursor)?.to_vec();
    CounterCursor::decode(&StateCursor::new(bytes), ENGINE)
}

fn write_cursor(transaction: &IdbTransaction, cursor: CounterCursor) -> Result<(), RetrievalError> {
    transaction
        .object_store(META)
        .require("get state cursor metadata")?
        .put_with_key(&Uint8Array::from(cursor.encode(ENGINE).as_bytes()), &CURSOR.into())
        .require("write state cursor")?;
    Ok(())
}

async fn current(transaction: &IdbTransaction) -> Result<CounterCursor, RetrievalError> {
    let request = transaction.object_store(META).require("get state cursor metadata")?.get(&CURSOR.into()).require("read state cursor")?;
    cb_future(&request, "success", "error").await.require("await state cursor")?;
    decode(request.result().require("get state cursor result")?)
}

async fn validate(transaction: &IdbTransaction, target: CounterCursor) -> Result<(), RetrievalError> {
    let now = current(transaction).await?;
    if now.incarnation != target.incarnation {
        return Err(RetrievalError::InvalidStateCursor);
    }
    if now.position != target.position {
        return Err(MutationError::WriteConflict.into());
    }
    Ok(())
}

/// Initialize older stores atomically before the engine becomes available.
pub(crate) async fn initialize(database: &Database) -> Result<(), RetrievalError> {
    let database = database.get_connection().await;
    SendWrapper::new(async move {
        let stores = Array::of3(&"entities".into(), &META.into(), &CHANGES.into());
        let transaction = database
            .transaction_with_str_sequence_and_mode(&stores, IdbTransactionMode::Readwrite)
            .require("initialize state continuation")?;
        let _abort = AbortUnfinishedTransaction(transaction.clone());
        let metadata = transaction.object_store(META).require("get state cursor metadata")?;
        let request = metadata.get(&CURSOR.into()).require("read state cursor")?;
        cb_future(&request, "success", "error").await.require("await state cursor")?;
        let value = request.result().require("get state cursor result")?;
        if value.is_undefined() {
            let changes = transaction.object_store(CHANGES).require("get state changes")?;
            changes.clear().require("clear obsolete state changes")?;
            write_cursor(&transaction, CounterCursor::fresh())?;
            let request =
                transaction.object_store("entities").require("get canonical entities")?.open_cursor().require("backfill state changes")?;
            let mut stream = cb_stream(&request, "success", "error");
            while let Some(result) = stream.next().await {
                let value = result.require("read canonical entity for backfill")?;
                if value.is_null() || value.is_undefined() {
                    break;
                }
                let cursor = value.dyn_into::<IdbCursorWithValue>().require("cast state backfill cursor")?;
                let key = cursor.key().require("read entity key")?.as_string().require("entity key is text")?;
                write_change(&changes, EntityId::from_base64(key).map_err(RetrievalError::storage)?, 0)?;
                cursor.continue_().require("advance state backfill")?;
            }
        } else {
            decode(value)?;
        }
        completed(&transaction).await
    })
    .await
}

pub(super) async fn advance(transaction: &IdbTransaction) -> Result<u64, RetrievalError> {
    let mut cursor = current(transaction).await?;
    cursor.position = cursor.position.checked_add(1).ok_or_else(|| RetrievalError::storage("canonical state counter exhausted"))?;
    write_cursor(transaction, cursor)?;
    Ok(cursor.position)
}

pub(super) fn reset(transaction: &IdbTransaction) -> Result<(), RetrievalError> { write_cursor(transaction, CounterCursor::fresh()) }

pub(super) fn write_change(store: &IdbObjectStore, id: EntityId, position: u64) -> Result<(), RetrievalError> {
    let object = js_sys::Object::new();
    let id = id.to_base64();
    // Fixed-width hex preserves the entire u64 range in an IndexedDB string key.
    js_sys::Reflect::set(&object, &"position".into(), &format!("{position:016x}").into()).require("set state position")?;
    js_sys::Reflect::set(&object, &"id".into(), &id.clone().into()).require("set changed entity id")?;
    store.put_with_key(&object, &id.into()).require("write latest state change")?;
    Ok(())
}

async fn load(store: &IdbObjectStore, id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError> {
    let request = store.get(&id.to_base64().into()).require("read snapshot entity")?;
    cb_future(&request, "success", "error").await.require("await snapshot entity")?;
    let value = request.result().require("get snapshot entity result")?;
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }
    Ok(Some(entity_state_from_object(id, &Object::new(value))?))
}

pub(super) struct IndexedDbStateSnapshot {
    database: Database,
    target: CounterCursor,
    after: (String, String),
}

impl IndexedDbStateSnapshot {
    pub(super) async fn begin(database: Database, since: Option<&StateCursor>) -> Result<Self, RetrievalError> {
        let connection = database.get_connection().await;
        let target = SendWrapper::new(async move {
            let transaction = connection.transaction_with_str(META).require("begin state snapshot")?;
            let target = current(&transaction).await?;
            completed(&transaction).await?;
            Ok::<_, RetrievalError>(target)
        })
        .await?;
        let since = target.resume(since, ENGINE)?;
        let after = since.map_or((String::new(), String::new()), |position| (format!("{position:016x}"), "\u{ffff}".into()));
        Ok(Self { database, target, after })
    }

    async fn transaction(&self) -> Result<SendWrapper<IdbTransaction>, RetrievalError> {
        let connection = self.database.get_connection().await;
        let stores = Array::of3(&"entities".into(), &META.into(), &CHANGES.into());
        Ok(SendWrapper::new(connection.transaction_with_str_sequence(&stores).require("read canonical state snapshot")?))
    }
}

#[async_trait]
impl StateSnapshot for IndexedDbStateSnapshot {
    async fn next_page(&mut self, limit: NonZeroUsize) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let transaction = self.transaction().await?;
        let target = self.target;
        let after = self.after.clone();
        let (states, last) = SendWrapper::new(async move {
            let _abort = AbortUnfinishedTransaction((*transaction).clone());
            validate(&transaction, target).await?;
            let entities = transaction.object_store("entities").require("get snapshot entities")?;
            let changes =
                transaction.object_store(CHANGES).require("get state changes")?.index(INDEX).require("get state position index")?;
            let bound = Array::of2(&after.0.clone().into(), &after.1.clone().into());
            let range = web_sys::IdbKeyRange::lower_bound_with_open(&bound, true).require("bound changed states")?;
            let request = changes.open_cursor_with_range(&range).require("scan changed states")?;
            let mut stream = cb_stream(&request, "success", "error");
            let mut states = Vec::new();
            let mut last = after;
            while let Some(result) = stream.next().await {
                let value = result.require("read state change")?;
                if value.is_null() || value.is_undefined() {
                    break;
                }
                let cursor = value.dyn_into::<IdbCursorWithValue>().require("cast state change cursor")?;
                let key = cursor.key().require("get state change key")?.dyn_into::<Array>().require("state change key is an array")?;
                last = (key.get(0).as_string().require("state position is text")?, key.get(1).as_string().require("entity id is text")?);
                let id = EntityId::from_base64(&last.1).map_err(RetrievalError::storage)?;
                states.push(load(&entities, id).await?.ok_or_else(|| RetrievalError::storage("state change names a missing entity"))?);
                if states.len() == limit.get() {
                    break;
                }
                cursor.continue_().require("advance state change cursor")?;
            }
            completed(&transaction).await?;
            Ok::<_, RetrievalError>((states, last))
        })
        .await?;
        self.after = last;
        Ok(states)
    }

    async fn get_states(&mut self, ids: &[EntityId]) -> Result<Vec<Option<Attested<EntityState>>>, RetrievalError> {
        let transaction = self.transaction().await?;
        let target = self.target;
        SendWrapper::new(async move {
            let _abort = AbortUnfinishedTransaction((*transaction).clone());
            validate(&transaction, target).await?;
            let entities = transaction.object_store("entities").require("get snapshot entities")?;
            let mut states = Vec::with_capacity(ids.len());
            for id in ids {
                states.push(load(&entities, *id).await?);
            }
            completed(&transaction).await?;
            Ok(states)
        })
        .await
    }

    async fn limited_states(&mut self, selection: &Selection<Resolved>) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let transaction = self.transaction().await?;
        let target = self.target;
        SendWrapper::new(async move {
            let _abort = AbortUnfinishedTransaction((*transaction).clone());
            validate(&transaction, target).await?;
            let mut selected = LimitedSelection::new(selection)?;
            if selection.limit == Some(0) {
                completed(&transaction).await?;
                return Ok(selected.finish());
            }
            let request =
                transaction.object_store("entities").require("get snapshot entities")?.open_cursor().require("scan snapshot entities")?;
            let mut stream = cb_stream(&request, "success", "error");
            while let Some(result) = stream.next().await {
                let value = result.require("read snapshot entity")?;
                if value.is_null() || value.is_undefined() {
                    break;
                }
                let cursor = value.dyn_into::<IdbCursorWithValue>().require("cast snapshot entity cursor")?;
                let key = cursor.key().require("get entity key")?.as_string().require("entity key is text")?;
                let id = EntityId::from_base64(key).map_err(RetrievalError::storage)?;
                selected.consider(entity_state_from_object(id, &Object::new(cursor.value().require("get snapshot entity")?))?)?;
                cursor.continue_().require("advance snapshot entities")?;
            }
            completed(&transaction).await?;
            Ok(selected.finish())
        })
        .await
    }

    async fn finish(self: Box<Self>) -> Result<StateCursor, RetrievalError> {
        let transaction = self.transaction().await?;
        SendWrapper::new(async move {
            let _abort = AbortUnfinishedTransaction((*transaction).clone());
            validate(&transaction, self.target).await?;
            completed(&transaction).await?;
            Ok(self.target.encode(ENGINE))
        })
        .await
    }
}
