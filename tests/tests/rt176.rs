#![cfg(feature = "postgres")]
mod common;
mod pg_common;

use ankurah::error::RetrievalError;
use ankurah::{policy::DEFAULT_CONTEXT as c, EntityId, Node, PermissiveAgent};
use anyhow::Result;
use std::sync::Arc;

/// RT176: get_state should return EntityNotFound for non-existent entities (postgres)
///
/// When `get_state` is called for an entity that doesn't exist in postgres storage,
/// it should return `EntityNotFound` (not a generic StorageError). This is critical
/// because the `Retrieve` trait implementation converts `EntityNotFound` to `Ok(None)`,
/// which allows entity creation to proceed normally.
///
/// Regression: commit c48b639 changed `error_kind` to use `err.as_db_error().message()`
/// instead of `err.to_string()`, breaking detection of the client-side "unexpected
/// number of rows" error from `query_one`.
#[tokio::test]
async fn postgres_get_state_returns_entity_not_found() -> Result<()> {
    let (_container, storage_engine) = pg_common::create_postgres_container().await?;

    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());
    node.system.create().await?;
    let context = node.context(c)?;

    // Get a collection (this creates the tables)
    let collection = context.collection(&"album".into()).await?;

    // Generate a random entity ID that definitely doesn't exist
    let non_existent_id = EntityId::new();

    // Call get_state directly on the storage collection
    let result = collection.get_state(non_existent_id).await;

    // Should be EntityNotFound, NOT a generic StorageError
    match result {
        Err(RetrievalError::EntityNotFound(id)) => {
            assert_eq!(id, non_existent_id, "EntityNotFound should contain the requested ID");
        }
        Err(RetrievalError::StorageError(e)) => {
            panic!(
                "get_state returned StorageError instead of EntityNotFound: {}. \
                This indicates the postgres storage is not properly handling the \
                'query returned an unexpected number of rows' error from query_one.",
                e
            );
        }
        Err(e) => {
            panic!("get_state returned unexpected error: {:?}", e);
        }
        Ok(_) => {
            panic!("get_state returned Ok for non-existent entity - this should not happen");
        }
    }

    Ok(())
}
