//! Cursor-backed logical dumps for PostgreSQL storage.

use std::{
    collections::{BTreeMap, VecDeque},
    pin::Pin,
};

use ankurah_core::{
    error::RetrievalError,
    storage::{StorageDump, StorageDumpItem},
};
use ankurah_proto::{
    Attestation, AttestationSet, Attested, Clock, CollectionId, EntityId, EntityState, Event, EventId, OperationSet, State, StateBuffers,
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
        let client = self.pool.get().await.map_err(RetrievalError::storage)?;
        let (event_collections, state_collections) = dump_collections(&client).await.map_err(RetrievalError::from)?;
        drop(client);

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
    event_collections: Vec<CollectionId>,
    state_collections: Vec<CollectionId>,
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
                    let page = event_page(&client, &collection, self.after_event.as_ref()).await.map_err(RetrievalError::from)?;
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
                    let page = state_page(&client, &collection, self.after_state.as_ref()).await.map_err(RetrievalError::from)?;
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
    collection: &CollectionId,
    after: Option<&EventId>,
) -> anyhow::Result<Vec<(EventId, Attested<Event>)>> {
    let table = quote_identifier(&format!("{collection}_event"));
    let rows = if let Some(after) = after {
        client
            .query(
                &format!("SELECT id, entity_id, operations, parent, attestations FROM {table} WHERE id > $1 ORDER BY id LIMIT $2"),
                &[after, &PAGE_SIZE],
            )
            .await?
    } else {
        client
            .query(&format!("SELECT id, entity_id, operations, parent, attestations FROM {table} ORDER BY id LIMIT $1"), &[&PAGE_SIZE])
            .await?
    };

    rows.into_iter()
        .map(|row| {
            let id: EventId = row.try_get("id")?;
            let entity_id: EntityId = row.try_get("entity_id")?;
            let operations: Vec<u8> = row.try_get("operations")?;
            let parent: Clock = row.try_get("parent")?;
            let attestations: Vec<u8> = row.try_get("attestations")?;
            let event = Attested {
                payload: Event {
                    collection: collection.clone(),
                    entity_id,
                    operations: bincode::deserialize::<OperationSet>(&operations)?,
                    parent,
                },
                attestations: bincode::deserialize::<AttestationSet>(&attestations)?,
            };
            anyhow::ensure!(event.payload.id() == id, "stored event id does not match payload for {collection}/{id}");
            Ok((id, event))
        })
        .collect()
}

async fn state_page(
    client: &tokio_postgres::Client,
    collection: &CollectionId,
    after: Option<&EntityId>,
) -> anyhow::Result<Vec<(EntityId, Attested<EntityState>)>> {
    let table = quote_identifier(collection.as_str());
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
            Ok((
                entity_id,
                Attested {
                    payload: EntityState {
                        entity_id,
                        collection: collection.clone(),
                        state: State {
                            state_buffers: StateBuffers(bincode::deserialize::<BTreeMap<String, Vec<u8>>>(&state_buffers)?),
                            memberships: bincode::deserialize(&memberships)?,
                            head,
                        },
                    },
                    attestations: AttestationSet(attestations),
                },
            ))
        })
        .collect()
}

async fn dump_collections(client: &tokio_postgres::Client) -> anyhow::Result<(Vec<CollectionId>, Vec<CollectionId>)> {
    let columns = table_columns(client).await?;
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

async fn table_columns(client: &tokio_postgres::Client) -> anyhow::Result<BTreeMap<String, std::collections::BTreeSet<String>>> {
    let rows = client
        .query(
            "SELECT table_name, column_name
             FROM information_schema.columns
             WHERE table_schema = current_schema()
             ORDER BY table_name, ordinal_position",
            &[],
        )
        .await?;
    let mut columns = BTreeMap::<String, std::collections::BTreeSet<String>>::new();
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
    use ankurah_proto::{State, StateFragment};
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
        let collection_id = CollectionId::from("dump_pages");
        let collection = storage.collection(&collection_id).await?;

        let mut expected_events = BTreeSet::new();
        let mut expected_states = BTreeSet::new();
        for _ in 0..RECORDS {
            let entity_id = EntityId::new();
            let event = Attested {
                payload: Event { collection: collection_id.clone(), entity_id, operations: Default::default(), parent: Default::default() },
                attestations: AttestationSet::default(),
            };
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

        let items = storage.dump().await?;
        pin_mut!(items);
        let mut events = BTreeSet::new();
        let mut states = BTreeSet::new();
        let mut saw_state = false;
        while let Some(item) = items.next().await {
            match item? {
                StorageDumpItem::Event(event) => {
                    assert!(!saw_state, "dump emitted an event after a state");
                    events.insert(event.payload.id());
                }
                StorageDumpItem::State(state) => {
                    saw_state = true;
                    states.insert(state.payload.entity_id);
                }
            }
        }
        assert_eq!(events, expected_events);
        assert_eq!(states, expected_states);
        Ok(())
    }
}
