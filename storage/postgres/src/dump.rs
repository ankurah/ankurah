//! Cursor-backed logical dumps for PostgreSQL storage.

use std::{collections::VecDeque, pin::Pin};

use ankurah_core::{
    error::RetrievalError,
    storage::{StorageDump, StorageDumpItem},
};
use ankurah_proto::{
    Attestation, AttestationSet, Attested, Clock, EntityId, EntityState, Event, EventBody, EventId, ModelId, State, StateBuffers,
};
use async_trait::async_trait;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};
use futures_util::{stream, Stream};

use crate::{Postgres, ENTITY_MODEL_TABLE, ENTITY_TABLE, EVENT_TABLE};

const PAGE_SIZE: i64 = 512;

type Pool = bb8::Pool<PostgresConnectionManager<NoTls>>;
type BoxDumpStream = Pin<Box<dyn Stream<Item = Result<StorageDumpItem, RetrievalError>> + Send + 'static>>;

#[async_trait]
impl StorageDump for Postgres {
    type DumpStream = BoxDumpStream;

    async fn dump(&self) -> Result<Self::DumpStream, RetrievalError> {
        let client = self.pool.get().await.map_err(RetrievalError::storage)?;
        self.ensure_shared_tables(&client).await.map_err(RetrievalError::storage)?;
        let cursor = PostgresDumpCursor {
            pool: self.pool.clone(),
            phase: DumpPhase::Events,
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
            let client = self.pool.get().await.map_err(RetrievalError::storage)?;
            let page = match self.phase {
                DumpPhase::Events => event_page(&client, self.after_event.as_ref()).await?,
                DumpPhase::States => state_page(&client, self.after_state.as_ref()).await?,
                DumpPhase::Done => return Ok(None),
            };
            if page.is_empty() {
                self.phase = match self.phase {
                    DumpPhase::Events => DumpPhase::States,
                    DumpPhase::States | DumpPhase::Done => DumpPhase::Done,
                };
                continue;
            }
            match self.phase {
                DumpPhase::Events => {
                    if let Some(StorageDumpItem::Event(event)) = page.last() {
                        self.after_event = Some(event.payload.id());
                    }
                }
                DumpPhase::States => {
                    if let Some(StorageDumpItem::State(state)) = page.last() {
                        self.after_state = Some(state.payload.entity_id);
                    }
                }
                DumpPhase::Done => {}
            }
            self.pending.extend(page);
        }
    }
}

async fn event_page(client: &tokio_postgres::Client, after: Option<&EventId>) -> Result<Vec<StorageDumpItem>, RetrievalError> {
    let rows = if let Some(after) = after {
        client
            .query(
                &format!(
                    r#"SELECT "id", "entity_id", "body", "parent", "attestations"
                       FROM "{EVENT_TABLE}" WHERE "id" > $1 ORDER BY "id" LIMIT $2"#
                ),
                &[after, &PAGE_SIZE],
            )
            .await
    } else {
        client
            .query(
                &format!(
                    r#"SELECT "id", "entity_id", "body", "parent", "attestations"
                       FROM "{EVENT_TABLE}" ORDER BY "id" LIMIT $1"#
                ),
                &[&PAGE_SIZE],
            )
            .await
    }
    .map_err(RetrievalError::storage)?;

    rows.into_iter()
        .map(|row| {
            let declared_id: EventId = row.try_get("id").map_err(RetrievalError::storage)?;
            let entity_id: EntityId = row.try_get("entity_id").map_err(RetrievalError::storage)?;
            let body = bincode::deserialize::<EventBody>(&row.try_get::<_, Vec<u8>>("body").map_err(RetrievalError::storage)?)?;
            let parent: Clock = row.try_get("parent").map_err(RetrievalError::storage)?;
            let attestations =
                bincode::deserialize::<AttestationSet>(&row.try_get::<_, Vec<u8>>("attestations").map_err(RetrievalError::storage)?)?;
            let event = Attested { payload: Event { entity_id, body, parent }, attestations };
            if event.payload.id() != declared_id {
                return Err(RetrievalError::Other(format!("stored event id does not match payload for {declared_id}")));
            }
            Ok(StorageDumpItem::Event(event))
        })
        .collect()
}

async fn state_page(client: &tokio_postgres::Client, after: Option<&EntityId>) -> Result<Vec<StorageDumpItem>, RetrievalError> {
    let rows = if let Some(after) = after {
        client
            .query(
                &format!(
                    r#"SELECT "id", "state_buffer", "head", "attestations"
                       FROM "{ENTITY_TABLE}" WHERE "id" > $1 ORDER BY "id" LIMIT $2"#
                ),
                &[after, &PAGE_SIZE],
            )
            .await
    } else {
        client
            .query(
                &format!(
                    r#"SELECT "id", "state_buffer", "head", "attestations"
                       FROM "{ENTITY_TABLE}" ORDER BY "id" LIMIT $1"#
                ),
                &[&PAGE_SIZE],
            )
            .await
    }
    .map_err(RetrievalError::storage)?;

    let mut page = Vec::with_capacity(rows.len());
    for row in rows {
        let entity_id: EntityId = row.try_get("id").map_err(RetrievalError::storage)?;
        let state_buffers =
            bincode::deserialize::<StateBuffers>(&row.try_get::<_, Vec<u8>>("state_buffer").map_err(RetrievalError::storage)?)?;
        let head: Clock = row.try_get("head").map_err(RetrievalError::storage)?;
        let attestation_bytes: Vec<Vec<u8>> = row.try_get("attestations").map_err(RetrievalError::storage)?;
        let attestations =
            attestation_bytes.into_iter().map(|bytes| bincode::deserialize::<Attestation>(&bytes)).collect::<Result<Vec<_>, _>>()?;
        let memberships = client
            .query(&format!(r#"SELECT "model_key" FROM "{ENTITY_MODEL_TABLE}" WHERE "entity_id" = $1 ORDER BY "model_key""#), &[&entity_id])
            .await
            .map_err(RetrievalError::storage)?
            .into_iter()
            .map(|row| {
                let bytes: Vec<u8> = row.get("model_key");
                bincode::deserialize::<ModelId>(&bytes)
            })
            .collect::<Result<_, _>>()?;
        let state = State { state_buffers, memberships, head };
        page.push(StorageDumpItem::State(Attested {
            payload: EntityState { entity_id, state },
            attestations: AttestationSet(attestations),
        }));
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use ankurah_core::storage::{StorageCommitOutcome, StorageEngine, StorageTransaction};
    use ankurah_proto::{AuthorId, OperationSet};
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
