//! The index a query's plan reads is created on the model's table the first
//! time the plan asks for it, decided by the shared planner as for sled and
//! IndexedDB, and reused after. Every assertion reads SQLite's own catalog.

use ankurah::property::Json;
use ankurah::{policy::DEFAULT_CONTEXT as c, Context, Model, Node, PermissiveAgent};
use ankurah_storage_sqlite::SqliteStorageEngine;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Notification {
    pub recipient: String,
    pub status: String,
    pub kind: String,
    pub detail: Json,
    /// Never set, so the table never gains its column.
    #[active_type(LWW)]
    pub anchor: Option<String>,
}

/// A node over `storage` holding one unread notification per recipient.
async fn notified(storage: Arc<SqliteStorageEngine>, recipients: &[&str]) -> Result<Context> {
    let node = Node::new_durable(storage, PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await?;
    let trx = ctx.begin();
    for recipient in recipients {
        trx.create(&Notification {
            recipient: recipient.to_string(),
            status: "unread".to_string(),
            kind: "mention".to_string(),
            detail: Json::new(serde_json::json!({ "kind": "mention" })),
            anchor: None,
        })
        .await?;
    }
    trx.commit().await?;
    Ok(ctx)
}

/// The recipients of the notifications a query returns, sorted.
async fn recipients(ctx: &Context, query: &str) -> Result<Vec<String>> {
    let mut found = ctx.fetch::<NotificationView>(query).await?.iter().map(|view| view.recipient()).collect::<Result<Vec<_>, _>>()?;
    found.sort();
    Ok(found)
}

/// The indexes on the notification table as the catalog lists them: each
/// name with its key columns (None for an expression) and whether each
/// column sorts descending.
async fn catalog(storage: &SqliteStorageEngine) -> Result<Vec<(String, Vec<(Option<String>, bool)>)>> {
    let conn = storage.pool().get().await?;
    let rows = conn
        .with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT il.name, ix.name, ix.desc FROM pragma_index_list('notification') AS il JOIN pragma_index_xinfo(il.name) AS ix \
                 WHERE ix.key AND il.origin = 'c' ORDER BY il.name, ix.seqno",
            )?;
            let rows = statement
                .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?, row.get::<_, bool>(2)?)))?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await?;
    let mut indexes: Vec<(String, Vec<(Option<String>, bool)>)> = Vec::new();
    for (name, column, descending) in rows {
        match indexes.last_mut() {
            Some((last, columns)) if *last == name => columns.push((column, descending)),
            _ => indexes.push((name, vec![(column, descending)])),
        }
    }
    Ok(indexes)
}

async fn index_names(storage: &SqliteStorageEngine) -> Result<Vec<String>> {
    Ok(catalog(storage).await?.into_iter().map(|(name, _)| name).collect())
}

/// The statement the catalog keeps for the index named `name`.
async fn statement(storage: &SqliteStorageEngine, name: &str) -> Result<String> {
    let conn = storage.pool().get().await?;
    let name = name.to_owned();
    Ok(conn
        .with_connection(move |connection| {
            Ok(connection.query_row("SELECT sql FROM sqlite_master WHERE name = ?1", [name], |row| row.get(0))?)
        })
        .await?)
}

async fn drop_index(storage: &SqliteStorageEngine, name: &str) -> Result<()> {
    let conn = storage.pool().get().await?;
    let statement = format!(r#"DROP INDEX "{name}""#);
    conn.with_connection(move |connection| {
        connection.execute(&statement, [])?;
        Ok(())
    })
    .await?;
    Ok(())
}

const RECIPIENT_STATUS: &str = "notification__recipient asc__status asc";

/// A query's first run creates the index its plan reads; its second run finds
/// it in the catalog and runs no DDL, so an index dropped behind the engine's
/// back stays dropped while the query still returns its rows.
#[tokio::test]
async fn a_first_use_creates_the_index_and_later_uses_run_no_ddl() -> Result<()> {
    let storage = Arc::new(SqliteStorageEngine::open_in_memory().await?);
    let ctx = notified(storage.clone(), &["alice", "bob"]).await?;
    assert!(index_names(&storage).await?.is_empty());

    assert_eq!(recipients(&ctx, "recipient = 'alice' AND status = 'unread'").await?, ["alice"]);
    assert_eq!(index_names(&storage).await?, [RECIPIENT_STATUS]);
    assert_eq!(recipients(&ctx, "recipient = 'bob' AND status = 'unread'").await?, ["bob"]);
    assert_eq!(index_names(&storage).await?, [RECIPIENT_STATUS]);

    drop_index(&storage, RECIPIENT_STATUS).await?;
    assert_eq!(recipients(&ctx, "recipient = 'alice' AND status = 'unread'").await?, ["alice"]);
    assert!(index_names(&storage).await?.is_empty(), "a known index is not created again");
    Ok(())
}

/// An index outlives its engine: a reopened engine reads it from the catalog
/// and its first query wanting the index runs no DDL either.
#[tokio::test]
async fn a_reopened_engine_finds_its_indexes_in_the_catalog() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("indexes.sqlite");
    let ctx = notified(Arc::new(SqliteStorageEngine::open(&path).await?), &["alice", "bob"]).await?;
    assert_eq!(recipients(&ctx, "recipient = 'alice' AND status = 'unread'").await?, ["alice"]);
    drop(ctx);

    let reopened = Arc::new(SqliteStorageEngine::open(&path).await?);
    let ctx = Node::new_durable(reopened.clone(), PermissiveAgent::new()).context_async(c).await?;
    assert_eq!(recipients(&ctx, "recipient = 'bob' AND status = 'unread'").await?, ["bob"]);
    drop_index(&reopened, RECIPIENT_STATUS).await?;
    assert_eq!(recipients(&ctx, "recipient = 'bob' AND status = 'unread'").await?, ["bob"]);
    assert!(index_names(&reopened).await?.is_empty(), "the reopened engine knew the index from the catalog");
    Ok(())
}

/// Sled's guard decides prefix reuse: an index whose trailing parts are the id
/// column serves a shorter key, one with any other trailing part does not.
#[tokio::test]
async fn a_prefix_is_reused_only_past_a_trailing_id() -> Result<()> {
    let storage = Arc::new(SqliteStorageEngine::open_in_memory().await?);
    let ctx = notified(storage.clone(), &["alice", "bob"]).await?;

    assert_eq!(recipients(&ctx, "recipient = 'alice' ORDER BY id").await?, ["alice"]);
    assert_eq!(index_names(&storage).await?, ["notification__recipient asc__id asc"]);
    assert_eq!(recipients(&ctx, "recipient = 'alice'").await?, ["alice"]);
    assert_eq!(index_names(&storage).await?, ["notification__recipient asc__id asc"], "a trailing id serves the prefix");

    assert_eq!(recipients(&ctx, "status = 'unread' ORDER BY kind DESC").await?, ["alice", "bob"]);
    assert_eq!(recipients(&ctx, "status = 'unread'").await?, ["alice", "bob"]);
    assert_eq!(
        index_names(&storage).await?,
        ["notification__recipient asc__id asc", "notification__status asc", "notification__status asc__kind desc"],
        "a trailing property refuses the prefix"
    );
    Ok(())
}

/// A composite key keeps each part's direction, and a JSON sub-path part is
/// the `json_extract` expression the query uses, read back from the catalog
/// so that a reopened engine recognizes it.
#[tokio::test]
async fn composite_and_json_sub_path_keys_are_rendered_and_read_back() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("keys.sqlite");
    let storage = Arc::new(SqliteStorageEngine::open(&path).await?);
    let ctx = notified(storage.clone(), &["alice", "bob"]).await?;

    assert_eq!(recipients(&ctx, "status = 'unread' ORDER BY kind DESC, recipient ASC").await?, ["alice", "bob"]);
    assert_eq!(recipients(&ctx, "detail.kind = 'mention' AND status = 'unread'").await?, ["alice", "bob"]);
    let column = |name: &str, descending| (Some(name.to_owned()), descending);
    assert_eq!(
        catalog(&storage).await?,
        [
            ("notification__detail.kind asc__status asc".to_owned(), vec![(None, false), column("status", false)]),
            (
                "notification__status asc__kind desc__recipient asc".to_owned(),
                vec![column("status", false), column("kind", true), column("recipient", false)]
            ),
        ]
    );
    assert!(statement(&storage, "notification__detail.kind asc__status asc").await?.contains(r#"json_extract("detail", '$.kind')"#));
    drop(ctx);

    let reopened = Arc::new(SqliteStorageEngine::open(&path).await?);
    let ctx = Node::new_durable(reopened.clone(), PermissiveAgent::new()).context_async(c).await?;
    assert_eq!(recipients(&ctx, "detail.kind = 'mention' AND status = 'unread'").await?, ["alice", "bob"]);
    drop_index(&reopened, "notification__detail.kind asc__status asc").await?;
    assert_eq!(recipients(&ctx, "detail.kind = 'mention' AND status = 'unread'").await?, ["alice", "bob"]);
    assert_eq!(index_names(&reopened).await?, ["notification__status asc__kind desc__recipient asc"], "the expression index was read back");
    Ok(())
}

/// A query whose plan reads no index creates none: the planner indexes
/// neither an inequality on its own nor a property the table has no column
/// for, and a predicate on such a property is evaluated on stored states.
#[tokio::test]
async fn a_plan_without_an_index_creates_nothing() -> Result<()> {
    let storage = Arc::new(SqliteStorageEngine::open_in_memory().await?);
    let ctx = notified(storage.clone(), &["alice", "bob"]).await?;
    assert_eq!(recipients(&ctx, "status != 'dismissed'").await?, ["alice", "bob"]);
    assert_eq!(recipients(&ctx, "recipient = 'alice' AND anchor = 'a1'").await?, Vec::<String>::new());
    assert!(index_names(&storage).await?.is_empty());
    Ok(())
}

/// First uses racing on one table create its index once, and all succeed:
/// the engine's index DDL lock serializes them, and the later ones find the
/// index the first created.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_first_uses_create_one_index() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let storage = Arc::new(SqliteStorageEngine::open(directory.path().join("race.sqlite")).await?);
    let ctx = notified(storage.clone(), &["alice", "bob"]).await?;
    let query = "recipient = 'alice' AND status = 'unread'";
    let (first, second, third) = tokio::join!(recipients(&ctx, query), recipients(&ctx, query), recipients(&ctx, query));
    for found in [first?, second?, third?] {
        assert_eq!(found, ["alice"]);
    }
    assert_eq!(index_names(&storage).await?, [RECIPIENT_STATUS]);
    Ok(())
}
