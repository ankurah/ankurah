mod common;
use ankurah::core::storage::{StorageEngine, StorageTransaction};
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
use anyhow::Result;
use common::proto::{Attested, Event};
use std::sync::Arc;

/// RT165: PostgreSQL storage should be idempotent when inserting duplicate events
///
/// This test demonstrates that duplicate event insertions (e.g., from network retries,
/// peer sync, etc.) should not cause errors. EventIDs are content-addressed (SHA256 hash
/// of entity_id + operations + parent), so duplicate insertions are safe and should be
/// idempotent rather than erroring.
#[tokio::test]
async fn postgres_duplicate_event_idempotency() -> Result<()> {
    use common::*;

    let (_container, storage_engine) = create_postgres_container().await?;

    let storage_engine = Arc::new(storage_engine);
    let node = Node::new_durable(storage_engine.clone(), PermissiveAgent::new());
    node.system.create().await?;
    let context = node.context_async(c).await?;

    // Create an album
    let trx = context.begin();
    let album = trx.create(&Album { name: "Test Album".to_owned(), year: "2024".to_owned() }).await?;
    let album_id = album.id();
    trx.commit().await?;

    // Get the first event that was created
    let events = storage_engine.dump_entity_events(album_id).await?;
    assert_eq!(events.len(), 1, "Should have exactly one event");
    let event: Attested<Event> = events[0].clone();

    // Repeated event-only transactions remain idempotent.
    for _ in 0..2 {
        let mut transaction = storage_engine.transaction();
        transaction.add_events(std::slice::from_ref(&event)).await?;
        transaction.commit().await?.committed()?;
    }

    // Verify we still only have one event
    let events_after = storage_engine.dump_entity_events(album_id).await?;
    assert_eq!(events_after.len(), 1, "Should still have exactly one event");

    Ok(())
}
