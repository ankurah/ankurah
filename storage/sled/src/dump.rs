//! Cursor-backed logical dumps for Sled storage.

use std::{
    collections::{BTreeMap, VecDeque},
    ops::Bound::{Excluded, Included, Unbounded},
    pin::Pin,
    sync::Arc,
};

use ankurah_core::{
    error::RetrievalError,
    storage::{StorageDump, StorageDumpItem},
};
use ankurah_proto::{Attested, CollectionId, EntityId, EntityState, Event, StateFragment};
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
        let state_collections = collection_trees(&database)?;
        let cursor = SledDumpCursor { database, phase: DumpPhase::Events, state_collections, after: None, pending: VecDeque::new() };
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
    database: Arc<Database>,
    phase: DumpPhase,
    state_collections: Vec<CollectionId>,
    after: Option<Vec<u8>>,
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
                    let database = self.database.clone();
                    let after = self.after.clone();
                    let (last, page) = tokio::task::spawn_blocking(move || event_page(database, after)).await??;
                    if page.is_empty() {
                        self.phase = DumpPhase::States;
                        self.after = None;
                        continue;
                    }
                    self.after = last;
                    self.pending.extend(page);
                }
                DumpPhase::States => {
                    let database = self.database.clone();
                    let collections = self.state_collections.clone();
                    let after = self.after.clone();
                    let (last, page) = tokio::task::spawn_blocking(move || state_page(database, collections, after)).await??;
                    if page.is_empty() {
                        self.phase = DumpPhase::Done;
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

fn event_page(database: Arc<Database>, after: Option<Vec<u8>>) -> Result<(Option<Vec<u8>>, Vec<StorageDumpItem>), RetrievalError> {
    let mut entries = match after {
        Some(after) => database.events_tree.range::<Vec<u8>, _>((Excluded(after), Unbounded)),
        None => database.events_tree.iter(),
    };
    let mut last = None;
    let mut page = Vec::new();
    for entry in entries.by_ref().take(PAGE_SIZE) {
        let (key, bytes) = entry.map_err(sled_error)?;
        let event = bincode::deserialize::<Attested<Event>>(&bytes)?;
        if key.as_ref() != event.payload.id().as_bytes() {
            return Err(RetrievalError::Other("Sled event key does not match its payload id".to_owned()));
        }
        last = Some(key.to_vec());
        page.push(StorageDumpItem::Event(event));
    }
    Ok((last, page))
}

fn state_page(
    database: Arc<Database>,
    collections: Vec<CollectionId>,
    after: Option<Vec<u8>>,
) -> Result<(Option<Vec<u8>>, Vec<StorageDumpItem>), RetrievalError> {
    let mut entries = match after {
        Some(after) => database.entities_tree.range::<Vec<u8>, _>((Excluded(after), Unbounded)),
        None => database.entities_tree.iter(),
    };
    let mut raw_states = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    for entry in entries.by_ref().take(PAGE_SIZE) {
        let (key, bytes) = entry.map_err(sled_error)?;
        raw_states.insert(key.to_vec(), bytes.to_vec());
    }
    let Some(first) = raw_states.keys().next().cloned() else {
        return Ok((None, Vec::new()));
    };
    let last = raw_states.keys().next_back().cloned().expect("non-empty state page");

    // StateFragment does not carry its collection in the 0.9 Sled layout.
    // Collection trees supply ownership only; their materialized values are
    // deliberately ignored, and the raw state always comes from entities.
    let mut ownership = BTreeMap::<Vec<u8>, CollectionId>::new();
    for collection in collections {
        let tree = database.db.open_tree(format!("collection_{collection}")).map_err(sled_error)?;
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
    Ok((Some(last), page))
}

fn collection_trees(database: &Database) -> Result<Vec<CollectionId>, RetrievalError> {
    let mut collections = Vec::new();
    for name in database.db.tree_names() {
        let Some(collection) = name.as_ref().strip_prefix(b"collection_") else {
            continue;
        };
        let collection = std::str::from_utf8(collection)
            .map_err(|error| RetrievalError::Other(format!("invalid UTF-8 in Sled collection tree name: {error}")))?;
        collections.push(CollectionId::from(collection));
    }
    collections.sort();
    collections.dedup();
    Ok(collections)
}

fn entity_id_from_key(key: &[u8]) -> Result<EntityId, RetrievalError> {
    let bytes: [u8; 16] = key.try_into().map_err(|_| RetrievalError::Other(format!("invalid Sled entity key length {}", key.len())))?;
    Ok(EntityId::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use ankurah_proto::{AttestationSet, State};
    use futures::{pin_mut, StreamExt};

    use super::*;

    #[tokio::test]
    async fn dump_rejects_a_raw_state_without_collection_ownership() -> anyhow::Result<()> {
        let storage = SledStorageEngine::new_test()?;
        let entity = EntityId::new();
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
}
