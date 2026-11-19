use std::sync::Arc;

use ankurah::{policy::DEFAULT_CONTEXT, Context, Node, PermissiveAgent};
use ankurah_storage_sqlite::SqliteStorageEngine;
use anyhow::Result;

/// A context on a new durable node whose storage is a temporary SQLite database
pub async fn context() -> Result<Context> {
    let node = Node::new_durable(Arc::new(SqliteStorageEngine::open_temporary().await?), PermissiveAgent::new());
    node.system.create().await?;
    Ok(node.context_async(DEFAULT_CONTEXT).await?)
}
