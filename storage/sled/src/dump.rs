//! Cursor-backed logical dumps for Sled storage.

use std::{collections::VecDeque, pin::Pin};

use ankurah_core::{
    error::RetrievalError,
    storage::{StorageDump, StorageDumpItem},
};
use ankurah_proto::{Attested, EntityId, EntityState, Event, StateFragment};
use async_trait::async_trait;
use futures::{stream, Stream};

use crate::{error::sled_error, SledStorageEngine};

const PAGE_SIZE: usize = 512;

type BoxDumpStream = Pin<Box<dyn Stream<Item = Result<StorageDumpItem, RetrievalError>> + Send + 'static>>;

#[async_trait]
impl StorageDump for SledStorageEngine {
    type DumpStream = BoxDumpStream;

    async fn dump(&self) -> Result<Self::DumpStream, RetrievalError> {
        let database = self.database.lock().unwrap().clone();
        let cursor = SledDumpCursor {
            phase: DumpPhase::Events,
            event_entries: Some(database.events_tree.iter()),
            state_entries: Some(database.entities_tree.iter()),
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

struct SledDumpCursor {
    phase: DumpPhase,
    event_entries: Option<sled::Iter>,
    state_entries: Option<sled::Iter>,
    pending: VecDeque<StorageDumpItem>,
}

impl SledDumpCursor {
    async fn next(&mut self) -> Result<Option<StorageDumpItem>, RetrievalError> {
        loop {
            if let Some(item) = self.pending.pop_front() {
                return Ok(Some(item));
            }
            match self.phase {
                DumpPhase::Events => {
                    let entries = self.event_entries.take().expect("event iterator is present during event phase");
                    let (entries, page) = tokio::task::spawn_blocking(move || event_page(entries)).await??;
                    if page.is_empty() {
                        self.event_entries = None;
                        self.phase = DumpPhase::States;
                        continue;
                    }
                    self.event_entries = Some(entries);
                    self.pending.extend(page);
                }
                DumpPhase::States => {
                    let entries = self.state_entries.take().expect("state iterator is present during state phase");
                    let (entries, page) = tokio::task::spawn_blocking(move || state_page(entries)).await??;
                    if page.is_empty() {
                        self.state_entries = None;
                        self.phase = DumpPhase::Done;
                        continue;
                    }
                    self.state_entries = Some(entries);
                    self.pending.extend(page);
                }
                DumpPhase::Done => return Ok(None),
            }
        }
    }
}

fn event_page(mut entries: sled::Iter) -> Result<(sled::Iter, Vec<StorageDumpItem>), RetrievalError> {
    let mut page = Vec::new();
    for entry in entries.by_ref().take(PAGE_SIZE) {
        let (key, bytes) = entry.map_err(sled_error)?;
        let event = bincode::deserialize::<Attested<Event>>(&bytes)?;
        let payload_id = event.payload.id();
        if key.as_ref() != payload_id.as_bytes() {
            return Err(key_mismatch("event", key.as_ref(), payload_id.as_bytes()));
        }
        page.push(StorageDumpItem::Event(event));
    }
    Ok((entries, page))
}

fn state_page(mut entries: sled::Iter) -> Result<(sled::Iter, Vec<StorageDumpItem>), RetrievalError> {
    let mut page = Vec::new();
    for entry in entries.by_ref().take(PAGE_SIZE) {
        let (key, bytes) = entry.map_err(sled_error)?;
        let entity_id = EntityId::try_from(key.to_vec()).map_err(RetrievalError::storage)?;
        let fragment = bincode::deserialize::<StateFragment>(&bytes)?;
        let state = Attested::<EntityState>::from_parts(entity_id, fragment);
        page.push(StorageDumpItem::State(state));
    }
    Ok((entries, page))
}

fn key_mismatch(kind: &str, stored: &[u8], payload: &[u8]) -> RetrievalError {
    let stored = stored.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
    let payload = payload.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
    RetrievalError::Other(format!("Sled {kind} key {stored} does not match payload id bytes {payload}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use ankurah_core::storage::StorageDump;
    use ankurah_proto::{AttestationSet, AuthorId, OperationSet, State};
    use futures::{pin_mut, StreamExt};

    use super::*;

    #[tokio::test]
    async fn dump_identifies_both_sides_of_an_event_key_mismatch() -> anyhow::Result<()> {
        let storage = SledStorageEngine::new_test()?;
        let event =
            Attested { payload: Event::genesis(None, AuthorId::Unknown, OperationSet::default()), attestations: AttestationSet::default() };
        let stored_key = vec![0xab; event.payload.id().as_bytes().len()];
        {
            let database = storage.database.lock().unwrap();
            database.events_tree.insert(&stored_key, bincode::serialize(&event)?)?;
        }

        let items = storage.dump().await?;
        pin_mut!(items);
        let error = items.next().await.expect("corrupt event record").expect_err("mismatched event key must fail");
        let message = error.to_string();
        let stored_key = stored_key.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
        let payload_key = event.payload.id().as_bytes().iter().map(|byte| format!("{byte:02x}")).collect::<String>();
        assert!(message.contains(&stored_key));
        assert!(message.contains(&payload_key));
        Ok(())
    }

    #[tokio::test]
    async fn dump_crosses_cursor_pages_without_skipping_records() -> anyhow::Result<()> {
        const RECORDS: usize = PAGE_SIZE + 1;

        let storage = SledStorageEngine::new_test()?;
        let mut expected_events = BTreeSet::new();
        let mut expected_states = BTreeSet::new();
        {
            let database = storage.database.lock().unwrap();
            for _ in 0..RECORDS {
                let event = Attested {
                    payload: Event::genesis(None, AuthorId::Unknown, OperationSet::default()),
                    attestations: AttestationSet::default(),
                };
                let entity_id = event.payload.entity_id;
                let state =
                    Attested { payload: EntityState { entity_id, state: State::default() }, attestations: AttestationSet::default() };
                database.events_tree.insert(event.payload.id().as_bytes(), bincode::serialize(&event)?)?;
                let (_, fragment) = state.to_parts();
                database.entities_tree.insert(entity_id.to_bytes(), bincode::serialize(&fragment)?)?;
                expected_events.insert(event.payload.id());
                expected_states.insert(entity_id);
            }
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
