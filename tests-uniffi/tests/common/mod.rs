use std::sync::Arc;

use ankurah::{policy::DEFAULT_CONTEXT, Context, Node, PermissiveAgent};
use ankurah_storage_sqlite::SqliteStorageEngine;
use anyhow::Result;

/// A context on a new durable node over in-memory SQLite
pub async fn context() -> Result<Context> {
    let node = Node::new_durable(Arc::new(SqliteStorageEngine::open_in_memory().await?), PermissiveAgent::new());
    Ok(node.context_async(DEFAULT_CONTEXT).await?)
}
