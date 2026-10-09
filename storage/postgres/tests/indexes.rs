//! The index a query's plan reads is created on the model's table the first
//! time the plan asks for it, decided by the shared planner as for sled and
//! IndexedDB, and reused after. Every assertion reads PostgreSQL's own
//! catalog. Like every test here, these need the Docker-backed test server.

mod common;

use ankurah::property::Json;
use ankurah::{policy::DEFAULT_CONTEXT as c, Context, Model, Node, PermissiveAgent};
use ankurah_storage_postgres::Postgres;
use anyhow::Result;
use common::TestPool;
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
async fn notified(storage: Postgres, recipients: &[&str]) -> Result<Context> {
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
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

/// A second node over the same database, as another process would open one.
async fn reopened(pool: &TestPool) -> Result<Context> {
    Ok(Node::new_durable(Arc::new(Postgres::new(pool.clone()).await?), PermissiveAgent::new()).context_async(c).await?)
}

/// The recipients of the notifications a query returns, sorted.
async fn recipients(ctx: &Context, query: &str) -> Result<Vec<String>> {
    let mut found = ctx.fetch::<NotificationView>(query).await?.iter().map(|view| view.recipient()).collect::<Result<Vec<_>, _>>()?;
    found.sort();
    Ok(found)
}

/// The indexes on the notification table as the catalog lists them, besides
/// its primary key: each name with its key columns (None for an expression)
/// and whether each column sorts descending.
async fn catalog(pool: &TestPool) -> Result<Vec<(String, Vec<(Option<String>, bool)>)>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT ic.relname AS name, CASE WHEN i.indkey[k - 1] = 0 THEN NULL ELSE pg_get_indexdef(i.indexrelid, k, true) END AS column, \
                    (i.indoption[k - 1] & 1) = 1 AS descending \
             FROM pg_index AS i JOIN pg_class AS ic ON ic.oid = i.indexrelid, generate_series(1, i.indnkeyatts) AS k \
             WHERE i.indrelid = 'notification'::regclass AND NOT i.indisprimary ORDER BY ic.relname, k",
            &[],
        )
        .await?;
    let mut indexes: Vec<(String, Vec<(Option<String>, bool)>)> = Vec::new();
    for row in rows {
        let (name, column, descending): (String, Option<String>, bool) = (row.get("name"), row.get("column"), row.get("descending"));
        match indexes.last_mut() {
            Some((last, columns)) if *last == name => columns.push((column, descending)),
            _ => indexes.push((name, vec![(column, descending)])),
        }
    }
    Ok(indexes)
}

async fn index_names(pool: &TestPool) -> Result<Vec<String>> { Ok(catalog(pool).await?.into_iter().map(|(name, _)| name).collect()) }

/// The definition the catalog keeps for the index named `name`.
async fn definition(pool: &TestPool, name: &str) -> Result<String> {
    Ok(pool.get().await?.query_one("SELECT pg_get_indexdef(to_regclass($1))", &[&format!(r#""{name}""#)]).await?.get(0))
}

async fn drop_index(pool: &TestPool, name: &str) -> Result<()> {
    pool.get().await?.execute(&format!(r#"DROP INDEX "{name}""#), &[]).await?;
    Ok(())
}

const RECIPIENT_STATUS: &str = "notification__recipient asc__status asc";

/// A query's first run creates the index its plan reads; its second run finds
/// it in the catalog and runs no DDL, so an index dropped behind the engine's
/// back stays dropped while the query still returns its rows.
#[tokio::test]
async fn a_first_use_creates_the_index_and_later_uses_run_no_ddl() -> Result<()> {
    let (_container, storage, pool) = common::create_postgres_container_with_pool().await?;
    let ctx = notified(storage, &["alice", "bob"]).await?;
    assert!(index_names(&pool).await?.is_empty());

    assert_eq!(recipients(&ctx, "recipient = 'alice' AND status = 'unread'").await?, ["alice"]);
    assert_eq!(index_names(&pool).await?, [RECIPIENT_STATUS]);
    assert_eq!(recipients(&ctx, "recipient = 'bob' AND status = 'unread'").await?, ["bob"]);
    assert_eq!(index_names(&pool).await?, [RECIPIENT_STATUS]);

    drop_index(&pool, RECIPIENT_STATUS).await?;
    assert_eq!(recipients(&ctx, "recipient = 'alice' AND status = 'unread'").await?, ["alice"]);
    assert!(index_names(&pool).await?.is_empty(), "a known index is not created again");
    Ok(())
}

/// An index outlives its engine: a second engine on the database reads it
/// from the catalog and its first query wanting the index runs no DDL either.
#[tokio::test]
async fn a_reopened_engine_finds_its_indexes_in_the_catalog() -> Result<()> {
    let (_container, storage, pool) = common::create_postgres_container_with_pool().await?;
    let ctx = notified(storage, &["alice", "bob"]).await?;
    assert_eq!(recipients(&ctx, "recipient = 'alice' AND status = 'unread'").await?, ["alice"]);

    let ctx = reopened(&pool).await?;
    assert_eq!(recipients(&ctx, "recipient = 'bob' AND status = 'unread'").await?, ["bob"]);
    drop_index(&pool, RECIPIENT_STATUS).await?;
    assert_eq!(recipients(&ctx, "recipient = 'bob' AND status = 'unread'").await?, ["bob"]);
    assert!(index_names(&pool).await?.is_empty(), "the reopened engine knew the index from the catalog");
    Ok(())
}

/// Sled's guard decides prefix reuse: an index whose trailing parts are the id
/// column serves a shorter key, one with any other trailing part does not.
#[tokio::test]
async fn a_prefix_is_reused_only_past_a_trailing_id() -> Result<()> {
    let (_container, storage, pool) = common::create_postgres_container_with_pool().await?;
    let ctx = notified(storage, &["alice", "bob"]).await?;

    assert_eq!(recipients(&ctx, "recipient = 'alice' ORDER BY id").await?, ["alice"]);
    assert_eq!(index_names(&pool).await?, ["notification__recipient asc__id asc"]);
    assert_eq!(recipients(&ctx, "recipient = 'alice'").await?, ["alice"]);
    assert_eq!(index_names(&pool).await?, ["notification__recipient asc__id asc"], "a trailing id serves the prefix");

    assert_eq!(recipients(&ctx, "status = 'unread' ORDER BY kind DESC").await?, ["alice", "bob"]);
    assert_eq!(recipients(&ctx, "status = 'unread'").await?, ["alice", "bob"]);
    assert_eq!(
        index_names(&pool).await?,
        ["notification__recipient asc__id asc", "notification__status asc", "notification__status asc__kind desc"],
        "a trailing property refuses the prefix"
    );
    Ok(())
}

/// A composite key keeps each part's direction, and a JSON sub-path part is
/// the `->` expression the query uses, read back from the catalog so that a
/// second engine recognizes it.
#[tokio::test]
async fn composite_and_json_sub_path_keys_are_rendered_and_read_back() -> Result<()> {
    let (_container, storage, pool) = common::create_postgres_container_with_pool().await?;
    let ctx = notified(storage, &["alice", "bob"]).await?;

    assert_eq!(recipients(&ctx, "status = 'unread' ORDER BY kind DESC, recipient ASC").await?, ["alice", "bob"]);
    assert_eq!(recipients(&ctx, "detail.kind = 'mention' AND status = 'unread'").await?, ["alice", "bob"]);
    let column = |name: &str, descending| (Some(name.to_owned()), descending);
    assert_eq!(
        catalog(&pool).await?,
        [
            ("notification__detail.kind asc__status asc".to_owned(), vec![(None, false), column("status", false)]),
            (
                "notification__status asc__kind desc__recipient asc".to_owned(),
                vec![column("status", false), column("kind", true), column("recipient", false)]
            ),
        ]
    );
    assert!(definition(&pool, "notification__detail.kind asc__status asc").await?.contains("(detail -> 'kind'::text)"));

    let ctx = reopened(&pool).await?;
    assert_eq!(recipients(&ctx, "detail.kind = 'mention' AND status = 'unread'").await?, ["alice", "bob"]);
    drop_index(&pool, "notification__detail.kind asc__status asc").await?;
    assert_eq!(recipients(&ctx, "detail.kind = 'mention' AND status = 'unread'").await?, ["alice", "bob"]);
    assert_eq!(index_names(&pool).await?, ["notification__status asc__kind desc__recipient asc"], "the expression index was read back");
    Ok(())
}

/// A query whose plan reads no index creates none: the planner indexes
/// neither an inequality on its own nor a property the table has no column
/// for, and a predicate on such a property is evaluated on stored states.
#[tokio::test]
async fn a_plan_without_an_index_creates_nothing() -> Result<()> {
    let (_container, storage, pool) = common::create_postgres_container_with_pool().await?;
    let ctx = notified(storage, &["alice", "bob"]).await?;
    assert_eq!(recipients(&ctx, "status != 'dismissed'").await?, ["alice", "bob"]);
    assert_eq!(recipients(&ctx, "recipient = 'alice' AND anchor = 'a1'").await?, Vec::<String>::new());
    assert!(index_names(&pool).await?.is_empty());
    Ok(())
}

/// First uses racing on one table from two engines on one database, as two
/// nodes race, create its index once and all succeed: each engine's own DDL
/// lock lets both reach the catalog, and the advisory lock on the index's
/// name makes the second wait and find the index the first created.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_first_uses_from_two_engines_create_one_index() -> Result<()> {
    let (_container, storage, pool) = common::create_postgres_container_with_pool().await?;
    let first = notified(storage, &["alice", "bob"]).await?;
    let second = reopened(&pool).await?;
    let query = "recipient = 'alice' AND status = 'unread'";
    let (a, b, c2, d) =
        tokio::join!(recipients(&first, query), recipients(&second, query), recipients(&first, query), recipients(&second, query));
    for found in [a?, b?, c2?, d?] {
        assert_eq!(found, ["alice"]);
    }
    assert_eq!(index_names(&pool).await?, [RECIPIENT_STATUS]);
    Ok(())
}
