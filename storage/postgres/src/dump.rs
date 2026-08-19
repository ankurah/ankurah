//! Cursor-backed logical dumps for PostgreSQL storage.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    pin::Pin,
};

use ankurah_core::{
    error::RetrievalError,
    storage::{StorageDump, StorageDumpItem},
};
use ankurah_proto::{
    Attestation, AttestationSet, Attested, Clock, EntityId, EntityState, Event, EventBody, EventFragment, EventId, ModelId, State,
    StateBuffers, StateFragment,
};
use async_trait::async_trait;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};
use futures_util::{stream, Stream};

use crate::Postgres;

const PAGE_SIZE: i64 = 512;

type Pool = bb8::Pool<PostgresConnectionManager<NoTls>>;
type BoxDumpStream = Pin<Box<dyn Stream<Item = Result<StorageDumpItem, RetrievalError>> + Send + 'static>>;

#[async_trait]
impl StorageDump for Postgres {
    type DumpStream = BoxDumpStream;

    async fn dump(&self) -> Result<Self::DumpStream, RetrievalError> {
        let (event_collections, state_collections) = {
            let client = self.pool.get().await.map_err(RetrievalError::storage)?;
            discover_collections_from_schema(&client).await?
        };

        let cursor = PostgresDumpCursor {
            pool: self.pool.clone(),
            phase: DumpPhase::Events,
            event_collections,
            state_collections,
            collection_index: 0,
            after_event: None,
            after_state: None,
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

struct PostgresDumpCursor {
    pool: Pool,
    phase: DumpPhase,
    event_collections: Vec<ModelId>,
    state_collections: Vec<ModelId>,
    collection_index: usize,
    after_event: Option<EventId>,
    after_state: Option<EntityId>,
    pending: VecDeque<StorageDumpItem>,
}

impl PostgresDumpCursor {
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
                        continue;
                    };
                    let client = self.pool.get().await.map_err(RetrievalError::storage)?;
                    let page = event_page(&client, &collection, self.after_event.as_ref()).await?;
                    if page.is_empty() {
                        self.collection_index += 1;
                        self.after_event = None;
                        continue;
                    }
                    self.after_event = page.last().map(|(id, _)| id.clone());
                    self.pending.extend(page.into_iter().map(|(_, event)| StorageDumpItem::Event(event)));
                }
                DumpPhase::States => {
                    let Some(collection) = self.state_collections.get(self.collection_index).cloned() else {
                        self.phase = DumpPhase::Done;
                        continue;
                    };
                    let client = self.pool.get().await.map_err(RetrievalError::storage)?;
                    let page = state_page(&client, &collection, self.after_state.as_ref()).await?;
                    if page.is_empty() {
                        self.collection_index += 1;
                        self.after_state = None;
                        continue;
                    }
                    self.after_state = page.last().map(|(id, _)| *id);
                    self.pending.extend(page.into_iter().map(|(_, state)| StorageDumpItem::State(state)));
                }
                DumpPhase::Done => return Ok(None),
            }
        }
    }
}

async fn event_page(
    client: &tokio_postgres::Client,
    collection: &ModelId,
    after: Option<&EventId>,
) -> anyhow::Result<Vec<(EventId, Attested<Event>)>> {
    let table = quote_identifier(&format!("{collection}_event"));
    let rows = if let Some(after) = after {
        client
            .query(
                &format!("SELECT id, entity_id, body, parent, attestations FROM {table} WHERE id > $1 ORDER BY id LIMIT $2"),
                &[after, &PAGE_SIZE],
            )
            .await?
    } else {
        client.query(&format!("SELECT id, entity_id, body, parent, attestations FROM {table} ORDER BY id LIMIT $1"), &[&PAGE_SIZE]).await?
    };

    rows.into_iter()
        .map(|row| {
            let id: EventId = row.try_get("id")?;
            let entity_id: EntityId = row.try_get("entity_id")?;
            let body: EventBody = row.try_get("body")?;
            let parent: Clock = row.try_get("parent")?;
            let attestations: Vec<u8> = row.try_get("attestations")?;
            let fragment = EventFragment { body, parent, attestations: bincode::deserialize(&attestations)? };
            let event = Attested::<Event>::from_parts(entity_id, collection.clone(), fragment);
            anyhow::ensure!(event.payload.id() == id, "stored event id does not match payload for {collection}/{id}");
            Ok((id, event))
        })
        .collect()
}

async fn state_page(
    client: &tokio_postgres::Client,
    collection: &ModelId,
    after: Option<&EntityId>,
) -> anyhow::Result<Vec<(EntityId, Attested<EntityState>)>> {
    let table = quote_identifier(&collection.to_string());
    let rows = if let Some(after) = after {
        client
            .query(
                &format!("SELECT id, state_buffer, memberships, head, attestations FROM {table} WHERE id > $1 ORDER BY id LIMIT $2"),
                &[after, &PAGE_SIZE],
            )
            .await?
    } else {
        client
            .query(&format!("SELECT id, state_buffer, memberships, head, attestations FROM {table} ORDER BY id LIMIT $1"), &[&PAGE_SIZE])
            .await?
    };

    rows.into_iter()
        .map(|row| {
            let entity_id: EntityId = row.try_get("id")?;
            let state_buffers: Vec<u8> = row.try_get("state_buffer")?;
            let memberships: Vec<u8> = row.try_get("memberships")?;
            let head: Clock = row.try_get("head")?;
            let attestations: Vec<Vec<u8>> = row.try_get("attestations")?;
            let attestations =
                attestations.into_iter().map(|bytes| bincode::deserialize::<Attestation>(&bytes)).collect::<Result<Vec<_>, _>>()?;
            let fragment = StateFragment {
                state: State {
                    state_buffers: bincode::deserialize::<StateBuffers>(&state_buffers)?,
                    memberships: bincode::deserialize(&memberships)?,
                    head,
                },
                attestations: AttestationSet(attestations),
            };
            Ok((entity_id, Attested::<EntityState>::from_parts(entity_id, collection.clone(), fragment)))
        })
        .collect()
}

// Temporary until StorageEngine exposes its collection registry: recognize
// current Ankurah tables by their required columns.
async fn discover_collections_from_schema(client: &tokio_postgres::Client) -> anyhow::Result<(Vec<ModelId>, Vec<ModelId>)> {
    let columns = table_columns(client).await?;
    let state_columns = ["id", "state_buffer", "memberships", "head", "attestations"];
    let event_columns = ["id", "entity_id", "body", "parent", "attestations"];
    let mut events = Vec::new();
    let mut states = Vec::new();
    for (name, columns) in columns {
        // A table is named for the model it holds, so the identity reads
        // back off the name this engine wrote; a table that is not one of
        // ours does not name a model and is not part of the dump.
        if state_columns.iter().all(|column| columns.contains(*column)) {
            if let Ok(collection) = name.parse::<ModelId>() {
                states.push(collection);
            }
        }
        if event_columns.iter().all(|column| columns.contains(*column)) {
            if let Some(Ok(collection)) = name.strip_suffix("_event").map(str::parse::<ModelId>) {
                events.push(collection);
            }
        }
    }
    Ok((events, states))
}

async fn table_columns(client: &tokio_postgres::Client) -> anyhow::Result<BTreeMap<String, BTreeSet<String>>> {
    let rows = client
        .query(
            "SELECT columns.table_name, columns.column_name
             FROM information_schema.columns AS columns
             INNER JOIN information_schema.tables AS tables
                ON tables.table_schema = columns.table_schema
               AND tables.table_name = columns.table_name
             WHERE columns.table_schema = current_schema()
               AND tables.table_type = 'BASE TABLE'
             ORDER BY columns.table_name, columns.ordinal_position",
            &[],
        )
        .await?;
    let mut columns = BTreeMap::<String, BTreeSet<String>>::new();
    for row in rows {
        columns.entry(row.try_get("table_name")?).or_default().insert(row.try_get("column_name")?);
    }
    Ok(columns)
}

fn quote_identifier(identifier: &str) -> String { format!("\"{}\"", identifier.replace('"', "\"\"")) }

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use ankurah_core::storage::StorageEngine;
    use ankurah_proto::{AuthorId, OperationSet, State, StateFragment};
    use futures_util::{pin_mut, StreamExt};
    use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

    use super::*;

    #[tokio::test]
    async fn dump_crosses_cursor_pages_without_skipping_records() -> anyhow::Result<()> {
        const RECORDS: usize = PAGE_SIZE as usize + 1;

        let container =
            postgres::Postgres::default().with_db_name("ankurah").with_user("postgres").with_password("postgres").start().await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(5432).await?;
        let storage = Postgres::open(&format!("host={host} port={port} user=postgres password=postgres dbname=ankurah")).await?;
        // A stand-in model identity: this test needs SOME collection, never a
        // particular one.
        let collection_id = ModelId::EntityId(EntityId::from_bytes([0xfc; 32]));
        let collection = storage.collection(&collection_id).await?;

        let mut expected_events = BTreeSet::new();
        let mut expected_states = BTreeSet::new();
        for _ in 0..RECORDS {
            let event = Attested {
                payload: Event::genesis(collection_id.clone(), None, AuthorId::Unknown, OperationSet::default()),
                attestations: AttestationSet::default(),
            };
            let entity_id = event.payload.entity_id;
            collection.add_event(&event).await?;
            collection
                .set_state(Attested::<EntityState>::from_parts(
                    entity_id,
                    collection_id.clone(),
                    StateFragment { state: State::default(), attestations: AttestationSet::default() },
                ))
                .await?;
            expected_events.insert(event.payload.id());
            expected_states.insert(entity_id);
        }

        let client = storage.pool.get().await?;
        client
            .batch_execute(
                "CREATE VIEW dump_decoy_event AS
                     SELECT id, entity_id, body, parent, attestations FROM dump_pages_event;
                 CREATE VIEW dump_decoy_state AS
                     SELECT id, state_buffer, memberships, head, attestations FROM dump_pages;",
            )
            .await?;
        drop(client);

        let items = storage.dump().await?;
        pin_mut!(items);
        let mut events = BTreeSet::new();
        let mut states = BTreeSet::new();
        let mut event_count = 0;
        let mut state_count = 0;
        let mut saw_state = false;
        while let Some(item) = items.next().await {
            match item? {
                StorageDumpItem::Event(event) => {
                    assert!(!saw_state, "dump emitted an event after a state");
                    event_count += 1;
                    events.insert(event.payload.id());
                }
                StorageDumpItem::State(state) => {
                    saw_state = true;
                    state_count += 1;
                    states.insert(state.payload.entity_id);
                }
            }
        }
        assert_eq!(event_count, RECORDS, "views must not be discovered as event tables");
        assert_eq!(state_count, RECORDS, "views must not be discovered as state tables");
        assert_eq!(events, expected_events);
        assert_eq!(states, expected_states);
        Ok(())
    }
}
