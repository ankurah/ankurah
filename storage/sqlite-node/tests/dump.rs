//! The cursor-backed dump crosses its page boundary without skipping records.

use std::collections::BTreeSet;

use ankurah_core::storage::{StorageCommitOutcome, StorageDump, StorageDumpItem, StorageEngine, StorageTransaction};
use ankurah_proto::{Attested, AuthorId, Clock, EntityState, Event, OperationSet, State, StateBuffers};
use ankurah_storage_sqlite_node::SqliteNodeStorageEngine;
use futures_util::{pin_mut, StreamExt};
use wasm_bindgen_test::wasm_bindgen_test;

/// One more than the dump's page size, so the cursor has to continue.
const RECORDS: usize = 513;

#[wasm_bindgen_test]
async fn dump_crosses_cursor_pages_without_skipping_records() -> anyhow::Result<()> {
    let storage = SqliteNodeStorageEngine::open_in_memory().await?;
    let mut expected = BTreeSet::new();
    for _ in 0..RECORDS {
        let event = Event::genesis(None, AuthorId::Unknown, OperationSet::default());
        let entity_id = event.entity_id;
        let event_id = event.id();
        let state = Attested::opt(
            EntityState {
                entity_id,
                state: State { state_buffers: StateBuffers::default(), memberships: BTreeSet::new(), head: Clock::from(vec![event_id]) },
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
