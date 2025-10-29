#![cfg(feature = "postgres")]
mod common;
use anyhow::Result;
use std::sync::Arc;
mod pg_common;
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
use common::{
    proto::{Attested, Event},
    Album,
};

/// RT165: PostgreSQL storage should be idempotent when inserting duplicate events
///
/// This test demonstrates that duplicate event insertions (e.g., from network retries,
/// peer sync, etc.) should not cause errors. EventIDs are content-addressed (SHA256 hash
/// of entity_id + operations + parent), so duplicate insertions are safe and should be
/// idempotent - returning false on subsequent attempts rather than erroring.
#[tokio::test]
async fn postgres_duplicate_event_idempotency() -> Result<()> {
    use common::*;

    let (_container, storage_engine) = pg_common::create_postgres_container().await?;

    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());
    node.system.create().await?;
    let context = node.context(c)?;

    // Create an album
    let trx = context.begin();
    let album = trx.create(&Album { name: "Test Album".to_owned(), year: "2024".to_owned() }).await?;
    let album_id = album.id();
    trx.commit().await?;

    // Get the collection to access storage directly
    let collection = context.collection(&"album".into()).await?;

    // Get the first event that was created
    let events = collection.dump_entity_events(album_id).await?;
    assert_eq!(events.len(), 1, "Should have exactly one event");
    let event: Attested<Event> = events[0].clone();

    // Try to add the same event again - this should be idempotent
    let result1 = collection.add_event(&event).await;
    assert!(result1.is_ok(), "First duplicate insert should succeed (idempotent): {:?}", result1.err());
    assert_eq!(result1.unwrap(), false, "Should return false since event already exists");

    // Try again to ensure it's consistently idempotent
    let result2 = collection.add_event(&event).await;
    assert!(result2.is_ok(), "Second duplicate insert should succeed (idempotent): {:?}", result2.err());
    assert_eq!(result2.unwrap(), false, "Should return false since event already exists");

    // Verify we still only have one event
    let events_after = collection.dump_entity_events(album_id).await?;
    assert_eq!(events_after.len(), 1, "Should still have exactly one event");

    Ok(())
}
