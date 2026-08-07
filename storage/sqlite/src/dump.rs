//! Cursor-backed logical dumps for SQLite storage.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    pin::Pin,
};

use ankurah_core::{
    error::RetrievalError,
    storage::{StorageDump, StorageDumpItem},
};
use ankurah_proto::{
    AttestationSet, Attested, Clock, CollectionId, EntityId, EntityState, Event, EventId, OperationSet, State, StateBuffers,
};
use async_trait::async_trait;
use futures_util::{stream, Stream};
use rusqlite::Connection;

use crate::{SqliteConnectionManager, SqliteError, SqliteStorageEngine};

const PAGE_SIZE: i64 = 512;

type Pool = bb8::Pool<SqliteConnectionManager>;
type BoxDumpStream = Pin<Box<dyn Stream<Item = Result<StorageDumpItem, RetrievalError>> + Send + 'static>>;

#[async_trait]
impl StorageDump for SqliteStorageEngine {
    type DumpStream = BoxDumpStream;

    async fn dump(&self) -> Result<Self::DumpStream, RetrievalError> {
        let conn = self.pool().get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
        let (event_collections, state_collections) = conn.with_connection(dump_collections).await?;
        drop(conn);

        let cursor = SqliteDumpCursor {
            pool: self.pool().clone(),
            phase: DumpPhase::Events,
            event_collections,
            state_collections,
            collection_index: 0,
            after: None,
            pending: VecDeque::new(),
        };
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
    event_collections: Vec<CollectionId>,
    state_collections: Vec<CollectionId>,
    collection_index: usize,
    after: Option<String>,
    pending: VecDeque<StorageDumpItem>,
}

impl SqliteDumpCursor {
    async fn next(&mut self) -> Result<Option<StorageDumpItem>, RetrievalError> {
        loop {
            if let Some(item) = self.pending.pop_front() {
                return Ok(Some(item));
            }
            match self.phase {
                DumpPhase::Events => {
                    let Some(collection) = self.event_collections.get(self.collection_index).cloned() else {
                        self.phase = DumpPhase::States;
                        self.collection_index = 0;
                        self.after = None;
                        continue;
                    };
                    let conn = self.pool.get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
                    let after = self.after.clone();
                    let (last, page) = conn.with_connection(move |conn| event_page(conn, &collection, after.as_deref())).await?;
                    if page.is_empty() {
                        self.collection_index += 1;
                        self.after = None;
                        continue;
                    }
                    self.after = last;
                    self.pending.extend(page);
                }
                DumpPhase::States => {
                    let Some(collection) = self.state_collections.get(self.collection_index).cloned() else {
                        self.phase = DumpPhase::Done;
                        continue;
                    };
                    let conn = self.pool.get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
                    let after = self.after.clone();
                    let (last, page) = conn.with_connection(move |conn| state_page(conn, &collection, after.as_deref())).await?;
                    if page.is_empty() {
                        self.collection_index += 1;
                        self.after = None;
                        continue;
                    }
                    self.after = last;
                    self.pending.extend(page);
                }
                DumpPhase::Done => return Ok(None),
            }
        }
    }
}

fn event_page(
    conn: &Connection,
    collection: &CollectionId,
    after: Option<&str>,
) -> Result<(Option<String>, Vec<StorageDumpItem>), SqliteError> {
    let table = quote_identifier(&format!("{collection}_event"));
    let (query, arguments): (String, Vec<rusqlite::types::Value>) = if let Some(after) = after {
        (
            format!("SELECT id, entity_id, operations, parent, attestations FROM {table} WHERE id > ? ORDER BY id LIMIT ?"),
            vec![after.to_owned().into(), PAGE_SIZE.into()],
        )
    } else {
        (format!("SELECT id, entity_id, operations, parent, attestations FROM {table} ORDER BY id LIMIT ?"), vec![PAGE_SIZE.into()])
    };
    let mut statement = conn.prepare(&query)?;
    let mut rows = statement.query(rusqlite::params_from_iter(arguments))?;
    let mut last = None;
    let mut page = Vec::new();
    while let Some(row) = rows.next()? {
        let stored_id: String = row.get(0)?;
        let declared_id = EventId::from_base64(&stored_id).map_err(|error| SqliteError::Dump(error.to_string()))?;
        let entity_id = EntityId::from_base64(row.get::<_, String>(1)?).map_err(|error| SqliteError::Dump(error.to_string()))?;
        let operations = bincode::deserialize::<OperationSet>(&row.get::<_, Vec<u8>>(2)?)?;
        let parent = serde_json::from_str::<Clock>(&row.get::<_, String>(3)?)?;
        let attestations = bincode::deserialize::<AttestationSet>(&row.get::<_, Vec<u8>>(4)?)?;
        let event = Attested { payload: Event { collection: collection.clone(), entity_id, operations, parent }, attestations };
        if event.payload.id() != declared_id {
            return Err(SqliteError::Dump(format!("stored event id does not match payload for {collection}/{declared_id}")));
        }
        last = Some(stored_id);
        page.push(StorageDumpItem::Event(event));
    }
    Ok((last, page))
}

fn state_page(
    conn: &Connection,
    collection: &CollectionId,
    after: Option<&str>,
) -> Result<(Option<String>, Vec<StorageDumpItem>), SqliteError> {
    let table = quote_identifier(collection.as_str());
    let (query, arguments): (String, Vec<rusqlite::types::Value>) = if let Some(after) = after {
        (
            format!("SELECT id, state_buffer, memberships, head, attestations FROM {table} WHERE id > ? ORDER BY id LIMIT ?"),
            vec![after.to_owned().into(), PAGE_SIZE.into()],
        )
    } else {
        (format!("SELECT id, state_buffer, memberships, head, attestations FROM {table} ORDER BY id LIMIT ?"), vec![PAGE_SIZE.into()])
    };
    let mut statement = conn.prepare(&query)?;
    let mut rows = statement.query(rusqlite::params_from_iter(arguments))?;
    let mut last = None;
    let mut page = Vec::new();
    while let Some(row) = rows.next()? {
        let stored_id: String = row.get(0)?;
        let entity_id = EntityId::from_base64(&stored_id).map_err(|error| SqliteError::Dump(error.to_string()))?;
        let state_buffers = bincode::deserialize::<BTreeMap<String, Vec<u8>>>(&row.get::<_, Vec<u8>>(1)?)?;
        let memberships = bincode::deserialize(&row.get::<_, Vec<u8>>(2)?)?;
        let head = serde_json::from_str::<Clock>(&row.get::<_, String>(3)?)?;
        let attestations = bincode::deserialize::<AttestationSet>(&row.get::<_, Vec<u8>>(4)?)?;
        last = Some(stored_id);
        page.push(StorageDumpItem::State(Attested {
            payload: EntityState {
                entity_id,
                collection: collection.clone(),
                state: State { state_buffers: StateBuffers(state_buffers), memberships, head },
            },
            attestations,
        }));
    }
    Ok((last, page))
}

fn dump_collections(conn: &Connection) -> Result<(Vec<CollectionId>, Vec<CollectionId>), SqliteError> {
    let columns = table_columns(conn)?;
    let state_columns = ["id", "state_buffer", "memberships", "head", "attestations"];
    let event_columns = ["id", "entity_id", "operations", "parent", "attestations"];
    let mut events = Vec::new();
    let mut states = Vec::new();
    for (name, present) in columns {
        if state_columns.iter().all(|column| present.contains(*column)) {
            states.push(CollectionId::from(name.clone()));
        }
        if event_columns.iter().all(|column| present.contains(*column)) {
            if let Some(collection) = name.strip_suffix("_event") {
                events.push(CollectionId::from(collection));
            }
        }
    }
    Ok((events, states))
}

fn table_columns(conn: &Connection) -> Result<BTreeMap<String, BTreeSet<String>>, SqliteError> {
    let mut tables = conn.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")?;
    let names = tables.query_map([], |row| row.get::<_, String>(0))?.collect::<Result<Vec<_>, _>>()?;
    let mut columns = BTreeMap::new();
    for name in names {
        let query = format!(r#"PRAGMA table_info("{}")"#, name.replace('"', "\"\""));
        let mut statement = conn.prepare(&query)?;
        let present = statement.query_map([], |row| row.get::<_, String>(1))?.collect::<Result<BTreeSet<_>, _>>()?;
        columns.insert(name, present);
    }
    Ok(columns)
}

fn quote_identifier(identifier: &str) -> String { format!("\"{}\"", identifier.replace('"', "\"\"")) }

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc};

    use ankurah::{policy::DEFAULT_CONTEXT, Model, Node, PermissiveAgent};
    use futures_util::{pin_mut, StreamExt};
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct DumpAlbum {
        name: String,
    }

    #[tokio::test]
    async fn dump_crosses_cursor_pages_without_skipping_records() -> anyhow::Result<()> {
        const RECORDS: usize = PAGE_SIZE as usize + 1;

        let storage = Arc::new(SqliteStorageEngine::open_in_memory().await?);
        let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
        node.system.create().await?;
        let context = node.context(DEFAULT_CONTEXT)?;

        let mut expected = BTreeSet::new();
        for index in 0..RECORDS {
            let transaction = context.begin();
            let album = transaction.create(&DumpAlbum { name: format!("album {index}") }).await?;
            expected.insert(album.id());
            transaction.commit().await?;
        }
        drop(context);
        drop(node);

        let items = storage.dump().await?;
        pin_mut!(items);
        let collection = CollectionId::from("dumpalbum");
        let mut events = BTreeSet::new();
        let mut states = BTreeSet::new();
        let mut saw_state = false;
        while let Some(item) = items.next().await {
            match item? {
                StorageDumpItem::Event(event) => {
                    assert!(!saw_state, "dump emitted an event after a state");
                    if event.payload.collection == collection {
                        events.insert(event.payload.entity_id);
                    }
                }
                StorageDumpItem::State(state) => {
                    saw_state = true;
                    if state.payload.collection == collection {
                        states.insert(state.payload.entity_id);
                    }
                }
            }
        }
        assert_eq!(events, expected);
        assert_eq!(states, expected);
        Ok(())
    }
}
