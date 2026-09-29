//! Cursor-backed logical dumps for SQLite storage.

use std::{collections::VecDeque, pin::Pin};

use ankurah_core::{
    error::RetrievalError,
    storage::{StorageDump, StorageDumpItem},
};
use ankurah_proto::{AttestationSet, Attested, Clock, EntityId, EntityState, Event, EventBody, EventId, ModelId, State, StateBuffers};
use async_trait::async_trait;
use futures_util::{stream, Stream};

use crate::{
    engine::{Pool, ENTITY_MODEL_TABLE, ENTITY_TABLE, EVENT_TABLE},
    exec::SqliteExecutor,
    value::SqliteValue,
    SqliteNodeError, SqliteNodeStorageEngine,
};

const PAGE_SIZE: i64 = 512;

type BoxDumpStream = Pin<Box<dyn Stream<Item = Result<StorageDumpItem, RetrievalError>> + Send + 'static>>;

#[async_trait]
impl StorageDump for SqliteNodeStorageEngine {
    type DumpStream = BoxDumpStream;

    async fn dump(&self) -> Result<Self::DumpStream, RetrievalError> {
        let conn = self.pool.get().await.map_err(|error| SqliteNodeError::Pool(error.to_string()))?;
        self.ensure_shared_tables(&conn).await?;
        let cursor = SqliteDumpCursor { pool: self.pool.clone(), phase: DumpPhase::Events, after: None, pending: VecDeque::new() };
        Ok(Box::pin(stream::try_unfold(cursor, |mut cursor| async move { Ok(cursor.next().await?.map(|item| (item, cursor))) })))
    }
}

#[derive(Clone, Copy)]
enum DumpPhase {
    Events,
    States,
    Done,
}

struct SqliteDumpCursor {
    pool: Pool,
    phase: DumpPhase,
    after: Option<String>,
    pending: VecDeque<StorageDumpItem>,
}

impl SqliteDumpCursor {
    async fn next(&mut self) -> Result<Option<StorageDumpItem>, RetrievalError> {
        loop {
            if let Some(item) = self.pending.pop_front() {
                return Ok(Some(item));
            }
            let conn = self.pool.get().await.map_err(|error| SqliteNodeError::Pool(error.to_string()))?;
            let after = self.after.clone();
            let (last, page) = match self.phase {
                DumpPhase::Events => conn.with_executor(move |executor| event_page(executor, after.as_deref())).await?,
                DumpPhase::States => conn.with_executor(move |executor| state_page(executor, after.as_deref())).await?,
                DumpPhase::Done => return Ok(None),
            };
            if page.is_empty() {
                self.after = None;
                self.phase = match self.phase {
                    DumpPhase::Events => DumpPhase::States,
                    DumpPhase::States | DumpPhase::Done => DumpPhase::Done,
                };
                continue;
            }
            self.after = last;
            self.pending.extend(page);
        }
    }
}

/// The paging clause shared by both pages: everything after the cursor, in id order.
fn page_query(columns: &str, table: &str, after: Option<&str>) -> (String, Vec<SqliteValue>) {
    match after {
        Some(after) => (
            format!(r#"SELECT {columns} FROM "{table}" WHERE "id" > ? ORDER BY "id" LIMIT ?"#),
            vec![SqliteValue::Text(after.to_owned()), SqliteValue::Integer(PAGE_SIZE)],
        ),
        None => (format!(r#"SELECT {columns} FROM "{table}" ORDER BY "id" LIMIT ?"#), vec![SqliteValue::Integer(PAGE_SIZE)]),
    }
}

fn event_page(executor: &dyn SqliteExecutor, after: Option<&str>) -> Result<(Option<String>, Vec<StorageDumpItem>), SqliteNodeError> {
    let (query, arguments) = page_query(r#""id", "entity_id", "body", "parent", "attestations""#, EVENT_TABLE, after);
    let mut last = None;
    let mut page = Vec::new();
    for row in executor.query(&query, &arguments)? {
        let stored_id = row.text(0)?;
        let declared_id = EventId::from_base64(&stored_id).map_err(|error| SqliteNodeError::Dump(error.to_string()))?;
        let entity_id = EntityId::from_base64(&row.text(1)?).map_err(|error| SqliteNodeError::Dump(error.to_string()))?;
        let body = bincode::deserialize::<EventBody>(&row.blob(2)?)?;
        let parent = serde_json::from_str::<Clock>(&row.text(3)?)?;
        let attestations = bincode::deserialize::<AttestationSet>(&row.blob(4)?)?;
        let event = Attested { payload: Event { entity_id, body, parent }, attestations };
        if event.payload.id() != declared_id {
            return Err(SqliteNodeError::Dump(format!("stored event id does not match payload for {declared_id}")));
        }
        last = Some(stored_id);
        page.push(StorageDumpItem::Event(event));
    }
    Ok((last, page))
}

fn state_page(executor: &dyn SqliteExecutor, after: Option<&str>) -> Result<(Option<String>, Vec<StorageDumpItem>), SqliteNodeError> {
    let (query, arguments) = page_query(r#""id", "state_buffer", "head", "attestations""#, ENTITY_TABLE, after);
    let mut last = None;
    let mut page = Vec::new();
    for row in executor.query(&query, &arguments)? {
        let stored_id = row.text(0)?;
        let entity_id = EntityId::from_base64(&stored_id).map_err(|error| SqliteNodeError::Dump(error.to_string()))?;
        let state_buffers = bincode::deserialize::<StateBuffers>(&row.blob(1)?)?;
        let head = serde_json::from_str::<Clock>(&row.text(2)?)?;
        let attestations = bincode::deserialize::<AttestationSet>(&row.blob(3)?)?;
        let mut memberships = std::collections::BTreeSet::new();
        let keys = executor.query(
            &format!(r#"SELECT "model_key" FROM "{ENTITY_MODEL_TABLE}" WHERE "entity_id" = ? ORDER BY "model_key""#),
            &[SqliteValue::Text(stored_id.clone())],
        )?;
        for key in keys {
            memberships.insert(bincode::deserialize::<ModelId>(&key.blob(0)?)?);
        }
        let state = State { state_buffers, memberships, head };
        page.push(StorageDumpItem::State(Attested { payload: EntityState { entity_id, state }, attestations }));
        last = Some(stored_id);
    }
    Ok((last, page))
}
