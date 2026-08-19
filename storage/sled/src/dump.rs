//! Cursor-backed logical dumps for Sled storage.

use std::{
    collections::{BTreeMap, VecDeque},
    ops::Bound::Included,
    pin::Pin,
    sync::Arc,
};

use ankurah_core::{
    error::RetrievalError,
    storage::{StorageDump, StorageDumpItem},
};
use ankurah_proto::{Attested, EntityId, EntityState, Event, ModelId, StateFragment};
use async_trait::async_trait;
use futures::{stream, Stream};

use crate::{database::Database, error::sled_error, SledStorageEngine};

const PAGE_SIZE: usize = 512;

type BoxDumpStream = Pin<Box<dyn Stream<Item = Result<StorageDumpItem, RetrievalError>> + Send + 'static>>;

#[async_trait]
impl StorageDump for SledStorageEngine {
    type DumpStream = BoxDumpStream;

    async fn dump(&self) -> Result<Self::DumpStream, RetrievalError> {
        let database = self.database.lock().unwrap().clone();
        let collection_trees = Arc::new(collection_trees(&database)?);
        let cursor = SledDumpCursor {
            phase: DumpPhase::Events,
            event_entries: Some(database.events_tree.iter()),
            state_entries: Some(database.entities_tree.iter()),
            collection_trees,
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
    collection_trees: Arc<Vec<(ModelId, sled::Tree)>>,
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
                    let collection_trees = self.collection_trees.clone();
                    let (entries, page) = tokio::task::spawn_blocking(move || state_page(entries, collection_trees)).await??;
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
            let stored_key = key.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
            let payload_key = payload_id.as_bytes().iter().map(|byte| format!("{byte:02x}")).collect::<String>();
            return Err(RetrievalError::Other(format!("Sled event key {stored_key} does not match payload id bytes {payload_key}")));
        }
        page.push(StorageDumpItem::Event(event));
    }
    Ok((entries, page))
}

fn state_page(
    mut entries: sled::Iter,
    collection_trees: Arc<Vec<(ModelId, sled::Tree)>>,
) -> Result<(sled::Iter, Vec<StorageDumpItem>), RetrievalError> {
    let mut raw_states = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    for entry in entries.by_ref().take(PAGE_SIZE) {
        let (key, bytes) = entry.map_err(sled_error)?;
        raw_states.insert(key.to_vec(), bytes.to_vec());
    }
    let Some(first) = raw_states.keys().next().cloned() else {
        return Ok((entries, Vec::new()));
    };
    let last = raw_states.keys().next_back().cloned().expect("non-empty state page");

    // Canonical states are global and StateFragment does not carry its
    // collection. Use the per-collection materialization trees only to recover
    // ownership, while keeping the canonical state bytes as the dump source.
    let mut ownership = BTreeMap::<Vec<u8>, ModelId>::new();
    for (collection, tree) in collection_trees.iter() {
        for entry in tree.range::<Vec<u8>, _>((Included(first.clone()), Included(last.clone()))) {
            let (key, _) = entry.map_err(sled_error)?;
            let key = key.to_vec();
            if !raw_states.contains_key(&key) {
                continue;
            }
            if let Some(previous) = ownership.insert(key.clone(), collection.clone()) {
                let entity = entity_id_from_key(&key)?;
                return Err(RetrievalError::Other(format!("Sled raw state {entity} belongs to both {previous} and {collection}")));
            }
        }
    }

    let mut page = Vec::with_capacity(raw_states.len());
    for (key, bytes) in raw_states {
        let entity_id = entity_id_from_key(&key)?;
        let collection = ownership
            .remove(&key)
            .ok_or_else(|| RetrievalError::Other(format!("Sled raw state {entity_id} has no collection membership")))?;
        let fragment = bincode::deserialize::<StateFragment>(&bytes)?;
        page.push(StorageDumpItem::State(Attested::<EntityState>::from_parts(entity_id, collection, fragment)));
    }
    Ok((entries, page))
}

fn collection_trees(database: &Database) -> Result<Vec<(ModelId, sled::Tree)>, RetrievalError> {
    let mut collections = Vec::new();
    for name in database.db.tree_names() {
        let Some(collection) = name.as_ref().strip_prefix(b"collection_") else {
            continue;
        };
        let collection = std::str::from_utf8(collection)
            .map_err(|error| RetrievalError::Other(format!("invalid UTF-8 in Sled collection tree name: {error}")))?;
        // A tree is named by the rendering of the model it holds, so the
        // identity reads straight back off the name this engine wrote.
        let collection = collection
            .parse::<ModelId>()
            .map_err(|error| RetrievalError::Other(format!("Sled collection tree '{collection}' does not name a model: {error}")))?;
        let tree = database.db.open_tree(name).map_err(sled_error)?;
        collections.push((collection, tree));
    }
    collections.sort_by(|(left, _), (right, _)| left.cmp(right));
    Ok(collections)
}

fn entity_id_from_key(key: &[u8]) -> Result<EntityId, RetrievalError> {
    let bytes: [u8; EntityId::BYTE_LEN] =
        key.try_into().map_err(|_| RetrievalError::Other(format!("invalid Sled entity key length {}", key.len())))?;
    Ok(EntityId::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use ankurah_core::storage::StorageEngine;
    use ankurah_proto::{AttestationSet, AuthorId, OperationSet, State};
    use futures::{pin_mut, StreamExt};

    use super::*;

    #[tokio::test]
    async fn dump_rejects_a_raw_state_without_collection_ownership() -> anyhow::Result<()> {
        let storage = SledStorageEngine::new_test()?;
        let entity = EntityId::random();
        let fragment = StateFragment { state: State::default(), attestations: AttestationSet::default() };
        {
            let database = storage.database.lock().unwrap();
            database.entities_tree.insert(entity.to_bytes(), bincode::serialize(&fragment)?)?;
        }

        let items = storage.dump().await?;
        pin_mut!(items);
        assert!(matches!(items.next().await, Some(Err(RetrievalError::Other(message))) if message.contains("no collection membership")));
        Ok(())
    }

    /// A stand-in model identity for fixtures: these tests need records that
    /// belong to SOME model, never a particular one.
    fn fixture_collection() -> ModelId { ModelId::EntityId(EntityId::from_bytes([0xfc; 32])) }

    #[tokio::test]
    async fn dump_identifies_both_sides_of_an_event_key_mismatch() -> anyhow::Result<()> {
        let storage = SledStorageEngine::new_test()?;
        let event = Attested {
            payload: Event::genesis(fixture_collection(), None, AuthorId::Unknown, OperationSet::default()),
            attestations: AttestationSet::default(),
        };
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
        let collection_id = fixture_collection();
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
                .set_state(Attested {
                    payload: EntityState { entity_id, collection: collection_id.clone(), state: State::default() },
                    attestations: AttestationSet::default(),
                })
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
