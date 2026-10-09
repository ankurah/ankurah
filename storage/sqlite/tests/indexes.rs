//! The index a query's plan reads is created on the model's table the first
//! time the plan asks for it, decided by the shared planner as for sled and
//! IndexedDB, and reused after. Every assertion reads SQLite's own catalog,
//! or the statements a traced connection ran.

use ankurah::property::Json;
use ankurah::{policy::DEFAULT_CONTEXT as c, Context, Model, Node, PermissiveAgent};
use ankurah_storage_sqlite::{SqliteConnectionManager, SqliteStorageEngine};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex};

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

/// Each recipient's unread notification and its kind.
const ALICE_AND_BOB: &[(&str, &str)] = &[("alice", "mention"), ("bob", "reply")];

/// A node over `storage` holding one unread notification per recipient, of the kind beside it.
async fn notified(storage: Arc<SqliteStorageEngine>, notifications: &[(&str, &str)]) -> Result<Context> {
    let node = Node::new_durable(storage, PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await?;
    let trx = ctx.begin();
    for (recipient, kind) in notifications {
        trx.create(&Notification {
            recipient: recipient.to_string(),
            status: "unread".to_string(),
            kind: kind.to_string(),
            detail: Json::new(serde_json::json!({ "kind": kind })),
            anchor: None,
        })
        .await?;
    }
    trx.commit().await?;
    Ok(ctx)
}

/// A node over an engine already holding the notifications, as a reopen makes one.
async fn reopened(storage: Arc<SqliteStorageEngine>) -> Result<Context> {
    Ok(Node::new_durable(storage, PermissiveAgent::new()).context_async(c).await?)
}

/// The recipients of the notifications a query returns, in the order returned.
async fn recipients_in_order(ctx: &Context, query: &str) -> Result<Vec<String>> {
    Ok(ctx.fetch::<NotificationView>(query).await?.iter().map(|view| view.recipient()).collect::<Result<Vec<_>, _>>()?)
}

/// The recipients of the notifications a query returns, sorted, for a query that orders nothing.
async fn recipients(ctx: &Context, query: &str) -> Result<Vec<String>> {
    let mut found = recipients_in_order(ctx, query).await?;
    found.sort();
    Ok(found)
}

/// One index as the catalog lists it: its name, then each key column's name
/// (None for an expression) with whether that column sorts descending.
type CatalogIndex = (String, Vec<(Option<String>, bool)>);

/// The indexes on the notification table as the catalog lists them.
async fn catalog(storage: &SqliteStorageEngine) -> Result<Vec<CatalogIndex>> {
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
    let mut indexes: Vec<CatalogIndex> = Vec::new();
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

async fn execute(storage: &SqliteStorageEngine, statement: String) -> Result<()> {
    let conn = storage.pool().get().await?;
    conn.with_connection(move |connection| {
        connection.execute(&statement, [])?;
        Ok(())
    })
    .await?;
    Ok(())
}

/// Every statement a traced connection ran. Tests that trace hold `TRACING`
/// for their whole run, so that one test's statements never land in another's.
static STATEMENTS: Mutex<Vec<String>> = Mutex::new(Vec::new());
static TRACING: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn record(statement: &str) { STATEMENTS.lock().unwrap().push(statement.to_owned()); }

/// Record every statement the engine's connection runs. For an engine on a
/// file, `traced_engine` makes that its one connection.
async fn trace(storage: &SqliteStorageEngine) -> Result<()> {
    let conn = storage.pool().get().await?;
    conn.with_connection_mut(|connection| {
        connection.trace(Some(record));
        Ok(())
    })
    .await?;
    Ok(())
}

/// An engine on the file at `path` with one connection, every statement of which is recorded.
async fn traced_engine(path: &Path) -> Result<Arc<SqliteStorageEngine>> {
    let pool = bb8::Pool::builder().max_size(1).build(SqliteConnectionManager::file(path)).await?;
    let storage = SqliteStorageEngine::new(pool).await?;
    trace(&storage).await?;
    Ok(Arc::new(storage))
}

/// The statements creating one of the engine's own indexes traced since the
/// last call; the shared tables' index, which every query makes sure of, is
/// not one.
fn index_ddl_traced() -> Vec<String> {
    STATEMENTS
        .lock()
        .unwrap()
        .drain(..)
        .filter(|statement| statement.starts_with("CREATE INDEX") && statement.contains("_ankurah_index__"))
        .collect()
}

const RECIPIENT_STATUS: &str = "_ankurah_index__notification__recipient asc__status asc";

/// A query's first run creates the index its plan reads; its later runs find
/// it in the catalog and run no DDL, so an index dropped behind the engine's
/// back stays dropped while the query still returns its rows.
#[tokio::test]
async fn a_first_use_creates_the_index_and_later_uses_run_no_ddl() -> Result<()> {
    let _tracing = TRACING.lock().await;
    let storage = Arc::new(SqliteStorageEngine::open_in_memory().await?);
    let ctx = notified(storage.clone(), ALICE_AND_BOB).await?;
    trace(&storage).await?;
    assert!(index_names(&storage).await?.is_empty());

    assert_eq!(recipients(&ctx, "recipient = 'alice' AND status = 'unread'").await?, ["alice"]);
    assert_eq!(index_ddl_traced().len(), 1);
    assert_eq!(index_names(&storage).await?, [RECIPIENT_STATUS]);
    assert_eq!(recipients(&ctx, "recipient = 'bob' AND status = 'unread'").await?, ["bob"]);
    assert!(index_ddl_traced().is_empty());

    execute(&storage, format!(r#"DROP INDEX "{RECIPIENT_STATUS}""#)).await?;
    assert_eq!(recipients(&ctx, "recipient = 'alice' AND status = 'unread'").await?, ["alice"]);
    assert!(index_ddl_traced().is_empty(), "a known index is not created again");
    assert!(index_names(&storage).await?.is_empty());
    Ok(())
}

/// An index outlives its engine: a reopened engine reads it from the catalog,
/// and its first query wanting the index runs no DDL at all.
#[tokio::test]
async fn a_reopened_engine_s_first_query_runs_no_ddl() -> Result<()> {
    let _tracing = TRACING.lock().await;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("indexes.sqlite");
    let ctx = notified(traced_engine(&path).await?, ALICE_AND_BOB).await?;
    assert_eq!(recipients(&ctx, "recipient = 'alice' AND status = 'unread'").await?, ["alice"]);
    assert_eq!(index_ddl_traced().len(), 1, "the first engine created the index");
    drop(ctx);

    let storage = traced_engine(&path).await?;
    let ctx = reopened(storage.clone()).await?;
    assert_eq!(recipients(&ctx, "recipient = 'bob' AND status = 'unread'").await?, ["bob"]);
    assert!(index_ddl_traced().is_empty(), "the reopened engine knew the index from the catalog");
    assert_eq!(index_names(&storage).await?, [RECIPIENT_STATUS]);
    Ok(())
}

/// Sled's guard decides prefix reuse: an index whose trailing parts are the id
/// column serves a shorter key, one with any other trailing part does not.
#[tokio::test]
async fn a_prefix_is_reused_only_past_a_trailing_id() -> Result<()> {
    let storage = Arc::new(SqliteStorageEngine::open_in_memory().await?);
    let ctx = notified(storage.clone(), ALICE_AND_BOB).await?;

    assert_eq!(recipients(&ctx, "recipient = 'alice' ORDER BY id").await?, ["alice"]);
    assert_eq!(index_names(&storage).await?, ["_ankurah_index__notification__recipient asc__id asc"]);
    assert_eq!(recipients(&ctx, "recipient = 'alice'").await?, ["alice"]);
    assert_eq!(index_names(&storage).await?, ["_ankurah_index__notification__recipient asc__id asc"], "a trailing id serves the prefix");

    assert_eq!(recipients_in_order(&ctx, "status = 'unread' ORDER BY kind DESC").await?, ["bob", "alice"]);
    assert_eq!(recipients(&ctx, "status = 'unread'").await?, ["alice", "bob"]);
    assert_eq!(
        index_names(&storage).await?,
        [
            "_ankurah_index__notification__recipient asc__id asc",
            "_ankurah_index__notification__status asc",
            "_ankurah_index__notification__status asc__kind desc"
        ],
        "a trailing property refuses the prefix"
    );
    Ok(())
}

/// A composite key keeps each part's direction, and the result comes in the
/// order asked, with or without the index.
#[tokio::test]
async fn composite_keys_keep_each_part_s_direction_and_the_result_its_order() -> Result<()> {
    let storage = Arc::new(SqliteStorageEngine::open_in_memory().await?);
    let ctx = notified(storage.clone(), ALICE_AND_BOB).await?;

    let descending = "status = 'unread' ORDER BY kind DESC, recipient ASC";
    assert_eq!(recipients_in_order(&ctx, descending).await?, ["bob", "alice"]);
    let column = |name: &str, descending| (Some(name.to_owned()), descending);
    assert_eq!(
        catalog(&storage).await?,
        [(
            "_ankurah_index__notification__status asc__kind desc__recipient asc".to_owned(),
            vec![column("status", false), column("kind", true), column("recipient", false)]
        )]
    );
    assert_eq!(recipients_in_order(&ctx, descending).await?, ["bob", "alice"], "the same order read through the index");
    assert_eq!(recipients_in_order(&ctx, "status = 'unread' ORDER BY kind ASC, recipient ASC").await?, ["alice", "bob"]);
    Ok(())
}

/// A JSON sub-path part gets no index on SQLite, whose declared column types
/// cannot establish that a column holds only JSON; the query answers, and the
/// plan's other parts get no index on their own either.
#[tokio::test]
async fn a_json_sub_path_part_gets_no_index() -> Result<()> {
    let storage = Arc::new(SqliteStorageEngine::open_in_memory().await?);
    let ctx = notified(storage.clone(), ALICE_AND_BOB).await?;
    assert_eq!(recipients(&ctx, "detail.kind = 'mention' AND status = 'unread'").await?, ["alice"]);
    assert_eq!(recipients(&ctx, "detail.kind = 'reply'").await?, ["bob"]);
    assert!(index_names(&storage).await?.is_empty());
    assert_eq!(recipients(&ctx, "status = 'unread'").await?, ["alice", "bob"]);
    assert_eq!(index_names(&storage).await?, ["_ankurah_index__notification__status asc"], "a key without a sub-path gets its index");
    Ok(())
}

/// A query whose plan reads no index creates none: the planner indexes
/// neither an inequality on its own nor a property the table has no column
/// for, and a predicate on such a property is evaluated on stored states.
#[tokio::test]
async fn a_plan_without_an_index_creates_nothing() -> Result<()> {
    let storage = Arc::new(SqliteStorageEngine::open_in_memory().await?);
    let ctx = notified(storage.clone(), ALICE_AND_BOB).await?;
    assert_eq!(recipients(&ctx, "status != 'dismissed'").await?, ["alice", "bob"]);
    assert_eq!(recipients(&ctx, "recipient = 'alice' AND anchor = 'a1'").await?, Vec::<String>::new());
    assert!(index_names(&storage).await?.is_empty());
    Ok(())
}

/// First uses racing on one table within one engine create its index once,
/// and all succeed: the engine's index DDL lock serializes them, and the
/// later ones find the index the first created.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_first_uses_create_one_index() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let storage = Arc::new(SqliteStorageEngine::open(directory.path().join("race.sqlite")).await?);
    let ctx = notified(storage.clone(), ALICE_AND_BOB).await?;
    let query = "recipient = 'alice' AND status = 'unread'";
    let (first, second, third) = tokio::join!(recipients(&ctx, query), recipients(&ctx, query), recipients(&ctx, query));
    for found in [first?, second?, third?] {
        assert_eq!(found, ["alice"]);
    }
    assert_eq!(index_names(&storage).await?, [RECIPIENT_STATUS]);
    Ok(())
}

/// Two engines on one file deciding at once, one wanting the key ascending
/// and the other descending, create one index that serves both: each decides
/// inside an immediate write transaction, so the second finds the first's.
#[tokio::test(flavor = "multi_thread")]
async fn two_engines_deciding_at_once_create_one_index_that_serves_both() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("both.sqlite");
    let first = Arc::new(SqliteStorageEngine::open(&path).await?);
    let ctx_first = notified(first.clone(), ALICE_AND_BOB).await?;
    let ctx_second = reopened(Arc::new(SqliteStorageEngine::open(&path).await?)).await?;
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let ascending = tokio::spawn({
        let barrier = barrier.clone();
        async move {
            barrier.wait().await;
            recipients_in_order(&ctx_first, "kind > 'a' ORDER BY kind").await
        }
    });
    let descending = tokio::spawn(async move {
        barrier.wait().await;
        recipients_in_order(&ctx_second, "kind > 'a' ORDER BY kind DESC").await
    });
    assert_eq!(ascending.await??, ["alice", "bob"]);
    assert_eq!(descending.await??, ["bob", "alice"]);
    let names = index_names(&first).await?;
    assert_eq!(names.len(), 1, "{names:?}");
    assert!(names[0].starts_with("_ankurah_index__notification__kind "), "{names:?}");
    Ok(())
}

/// An application's index on the name the engine would use, serving the
/// plan's key or not, is left alone: the engine tries the name once, and when
/// the index there does not serve the plan runs no DDL for that key again
/// while the query keeps answering.
#[tokio::test]
async fn an_application_index_on_the_engine_s_name_is_left_alone() -> Result<()> {
    let _tracing = TRACING.lock().await;
    let directory = tempfile::tempdir()?;
    let storage = traced_engine(&directory.path().join("stranger.sqlite")).await?;
    let ctx = notified(storage.clone(), ALICE_AND_BOB).await?;
    execute(&storage, format!(r#"CREATE INDEX "{RECIPIENT_STATUS}" ON "notification" ("kind")"#)).await?;
    index_ddl_traced();

    assert_eq!(recipients(&ctx, "recipient = 'alice' AND status = 'unread'").await?, ["alice"]);
    assert_eq!(index_ddl_traced().len(), 1, "the name is tried once");
    assert_eq!(catalog(&storage).await?, [(RECIPIENT_STATUS.to_owned(), vec![(Some("kind".to_owned()), false)])]);
    assert_eq!(recipients(&ctx, "recipient = 'bob' AND status = 'unread'").await?, ["bob"]);
    assert!(index_ddl_traced().is_empty(), "the key is not tried again");
    Ok(())
}
