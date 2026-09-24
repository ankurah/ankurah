//! Cursor-backed logical dumps for SQLite storage.

use std::{collections::VecDeque, pin::Pin};

use ankurah_core::{
    error::RetrievalError,
    storage::{StorageDump, StorageDumpItem},
};
use ankurah_proto::{AttestationSet, Attested, Clock, EntityId, EntityState, Event, EventBody, EventId, ModelId, State, StateBuffers};
use async_trait::async_trait;
use futures_util::{stream, Stream};
use rusqlite::Connection;

use crate::{
    engine::{ENTITY_MODEL_TABLE, ENTITY_TABLE, EVENT_TABLE},
    SqliteConnectionManager, SqliteError, SqliteStorageEngine,
};

const PAGE_SIZE: i64 = 512;

type Pool = bb8::Pool<SqliteConnectionManager>;
type BoxDumpStream = Pin<Box<dyn Stream<Item = Result<StorageDumpItem, RetrievalError>> + Send + 'static>>;

#[async_trait]
impl StorageDump for SqliteStorageEngine {
    type DumpStream = BoxDumpStream;

    async fn dump(&self) -> Result<Self::DumpStream, RetrievalError> {
        let conn = self.pool().get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
        self.ensure_shared_tables(&conn).await?;
        let cursor = SqliteDumpCursor { pool: self.pool().clone(), phase: DumpPhase::Events, after: None, pending: VecDeque::new() };
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
            let conn = self.pool.get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
            let after = self.after.clone();
            let (last, page) = match self.phase {
                DumpPhase::Events => conn.with_connection(move |conn| event_page(conn, after.as_deref())).await?,
                DumpPhase::States => conn.with_connection(move |conn| state_page(conn, after.as_deref())).await?,
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

fn event_page(conn: &Connection, after: Option<&str>) -> Result<(Option<String>, Vec<StorageDumpItem>), SqliteError> {
    let (query, arguments): (String, Vec<rusqlite::types::Value>) = if let Some(after) = after {
        (
            format!(
                r#"SELECT "id", "entity_id", "body", "parent", "attestations"
                   FROM "{EVENT_TABLE}" WHERE "id" > ? ORDER BY "id" LIMIT ?"#
            ),
            vec![after.to_owned().into(), PAGE_SIZE.into()],
        )
    } else {
        (
            format!(
                r#"SELECT "id", "entity_id", "body", "parent", "attestations"
                   FROM "{EVENT_TABLE}" ORDER BY "id" LIMIT ?"#
            ),
            vec![PAGE_SIZE.into()],
        )
    };
    let mut statement = conn.prepare(&query)?;
    let mut rows = statement.query(rusqlite::params_from_iter(arguments))?;
    let mut last = None;
    let mut page = Vec::new();
    while let Some(row) = rows.next()? {
        let stored_id: String = row.get(0)?;
        let declared_id = EventId::from_base64(&stored_id).map_err(|error| SqliteError::Dump(error.to_string()))?;
        let entity_id = EntityId::from_base64(row.get::<_, String>(1)?).map_err(|error| SqliteError::Dump(error.to_string()))?;
        let body = bincode::deserialize::<EventBody>(&row.get::<_, Vec<u8>>(2)?)?;
        let parent = serde_json::from_str::<Clock>(&row.get::<_, String>(3)?)?;
        let attestations = bincode::deserialize::<AttestationSet>(&row.get::<_, Vec<u8>>(4)?)?;
        let event = Attested { payload: Event { entity_id, body, parent }, attestations };
        if event.payload.id() != declared_id {
            return Err(SqliteError::Dump(format!("stored event id does not match payload for {declared_id}")));
        }
        last = Some(stored_id);
        page.push(StorageDumpItem::Event(event));
    }
    Ok((last, page))
}

fn state_page(conn: &Connection, after: Option<&str>) -> Result<(Option<String>, Vec<StorageDumpItem>), SqliteError> {
    let (query, arguments): (String, Vec<rusqlite::types::Value>) = if let Some(after) = after {
        (
            format!(
                r#"SELECT "id", "state_buffer", "head", "attestations"
                   FROM "{ENTITY_TABLE}" WHERE "id" > ? ORDER BY "id" LIMIT ?"#
            ),
            vec![after.to_owned().into(), PAGE_SIZE.into()],
        )
    } else {
        (
            format!(
                r#"SELECT "id", "state_buffer", "head", "attestations"
                   FROM "{ENTITY_TABLE}" ORDER BY "id" LIMIT ?"#
            ),
            vec![PAGE_SIZE.into()],
        )
    };
    let mut statement = conn.prepare(&query)?;
    let mut rows = statement.query(rusqlite::params_from_iter(arguments))?;
    let mut last = None;
    let mut page = Vec::new();
    while let Some(row) = rows.next()? {
        let stored_id: String = row.get(0)?;
        let entity_id = EntityId::from_base64(&stored_id).map_err(|error| SqliteError::Dump(error.to_string()))?;
        let state_buffers = bincode::deserialize::<StateBuffers>(&row.get::<_, Vec<u8>>(1)?)?;
        let head = serde_json::from_str::<Clock>(&row.get::<_, String>(2)?)?;
        let attestations = bincode::deserialize::<AttestationSet>(&row.get::<_, Vec<u8>>(3)?)?;
        let mut memberships = std::collections::BTreeSet::new();
        let mut membership_statement =
            conn.prepare(&format!(r#"SELECT "model_key" FROM "{ENTITY_MODEL_TABLE}" WHERE "entity_id" = ? ORDER BY "model_key""#))?;
        let keys = membership_statement.query_map([&stored_id], |row| row.get::<_, Vec<u8>>(0))?.collect::<Result<Vec<_>, _>>()?;
        for key in keys {
            memberships.insert(bincode::deserialize::<ModelId>(&key)?);
        }
        let state = State { state_buffers, memberships, head };
        page.push(StorageDumpItem::State(Attested { payload: EntityState { entity_id, state }, attestations }));
        last = Some(stored_id);
    }
    Ok((last, page))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use ankurah_core::storage::{StorageCommitOutcome, StorageEngine, StorageTransaction};
    use ankurah_proto::{AuthorId, OperationSet};
    use futures_util::{pin_mut, StreamExt};

    use super::*;

    #[tokio::test]
    async fn dump_crosses_cursor_pages_without_skipping_records() -> anyhow::Result<()> {
        const RECORDS: usize = PAGE_SIZE as usize + 1;

        let storage = SqliteStorageEngine::open_in_memory().await?;
        let mut expected = BTreeSet::new();
        for _ in 0..RECORDS {
            let event = Event::genesis(None, AuthorId::Unknown, OperationSet::default());
            let entity_id = event.entity_id;
            let event_id = event.id();
            let state = Attested::opt(
                EntityState {
                    entity_id,
                    state: State {
                        state_buffers: StateBuffers::default(),
                        memberships: BTreeSet::new(),
                        head: Clock::from(vec![event_id]),
                    },
                },
                None,
            );
            let mut transaction = storage.transaction();
            transaction.add_events(&[Attested::opt(event, None)]).await?;
            transaction.set_state(&Clock::default(), &state).await?;
            assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));
            expected.insert(entity_id);
        }

        let items = storage.dump().await?;
        pin_mut!(items);
        let mut events = BTreeSet::new();
        let mut states = BTreeSet::new();
        let mut saw_state = false;
        while let Some(item) = items.next().await {
            match item? {
                StorageDumpItem::Event(event) => {
                    assert!(!saw_state, "dump emitted an event after a state");
                    events.insert(event.payload.entity_id);
                }
                StorageDumpItem::State(state) => {
                    saw_state = true;
                    states.insert(state.payload.entity_id);
                }
            }
        }
        assert_eq!(events, expected);
        assert_eq!(states, expected);
        Ok(())
    }
}
