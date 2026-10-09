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

/// Each recipient's unread notification and its kind.
const ALICE_AND_BOB: &[(&str, &str)] = &[("alice", "mention"), ("bob", "reply")];

/// A node over `storage` holding one unread notification per recipient, of the kind beside it.
async fn notified(storage: Postgres, notifications: &[(&str, &str)]) -> Result<Context> {
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
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

/// A second node over the same database, as another process would open one.
async fn reopened(pool: &TestPool) -> Result<Context> {
    Ok(Node::new_durable(Arc::new(Postgres::new(pool.clone()).await?), PermissiveAgent::new()).context_async(c).await?)
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

/// The indexes on the notification table as the catalog lists them, besides
/// its primary key.
async fn catalog(pool: &TestPool) -> Result<Vec<CatalogIndex>> {
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
    let mut indexes: Vec<CatalogIndex> = Vec::new();
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

const RECIPIENT_STATUS: &str = "_ankurah_index__notification__recipient asc__status asc";

/// A query's first run creates the index its plan reads; its second run finds
/// it in the catalog and runs no DDL, so an index dropped behind the engine's
/// back stays dropped while the query still returns its rows.
#[tokio::test]
async fn a_first_use_creates_the_index_and_later_uses_run_no_ddl() -> Result<()> {
    let (_container, storage, pool) = common::create_postgres_container_with_pool().await?;
    let ctx = notified(storage, ALICE_AND_BOB).await?;
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
    let ctx = notified(storage, ALICE_AND_BOB).await?;
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
    let ctx = notified(storage, ALICE_AND_BOB).await?;

    assert_eq!(recipients(&ctx, "recipient = 'alice' ORDER BY id").await?, ["alice"]);
    assert_eq!(index_names(&pool).await?, ["_ankurah_index__notification__recipient asc__id asc"]);
    assert_eq!(recipients(&ctx, "recipient = 'alice'").await?, ["alice"]);
    assert_eq!(index_names(&pool).await?, ["_ankurah_index__notification__recipient asc__id asc"], "a trailing id serves the prefix");

    assert_eq!(recipients_in_order(&ctx, "status = 'unread' ORDER BY kind DESC").await?, ["bob", "alice"]);
    assert_eq!(recipients(&ctx, "status = 'unread'").await?, ["alice", "bob"]);
    assert_eq!(
        index_names(&pool).await?,
        [
            "_ankurah_index__notification__recipient asc__id asc",
            "_ankurah_index__notification__status asc",
            "_ankurah_index__notification__status asc__kind desc"
        ],
        "a trailing property refuses the prefix"
    );
    Ok(())
}

/// A composite key keeps each part's direction and the result comes in the
/// order asked, and a JSON sub-path part is the `->` expression the query
/// uses, read back from the catalog so that a second engine recognizes it.
#[tokio::test]
async fn composite_and_json_sub_path_keys_are_rendered_and_read_back() -> Result<()> {
    let (_container, storage, pool) = common::create_postgres_container_with_pool().await?;
    let ctx = notified(storage, ALICE_AND_BOB).await?;

    let descending = "status = 'unread' ORDER BY kind DESC, recipient ASC";
    assert_eq!(recipients_in_order(&ctx, descending).await?, ["bob", "alice"]);
    assert_eq!(recipients(&ctx, "detail.kind = 'mention' AND status = 'unread'").await?, ["alice"]);
    let column = |name: &str, descending| (Some(name.to_owned()), descending);
    let listed = catalog(&pool).await?;
    assert_eq!(listed.len(), 2, "{listed:?}");
    assert_eq!(
        listed[0],
        ("_ankurah_index__notification__detail.kind asc__status asc".to_owned(), vec![(None, false), column("status", false)])
    );
    // The three-part name exceeds PostgreSQL's identifier limit, so a hash ends its retained head.
    assert!(listed[1].0.starts_with("_ankurah_index__notification__status asc__kind"), "{listed:?}");
    assert_eq!(listed[1].0.len(), 63);
    assert_eq!(listed[1].1, [column("status", false), column("kind", true), column("recipient", false)]);
    let composite = listed[1].0.clone();
    assert!(definition(&pool, "_ankurah_index__notification__detail.kind asc__status asc").await?.contains("(detail -> 'kind'::text)"));
    assert_eq!(recipients_in_order(&ctx, descending).await?, ["bob", "alice"], "the same order read through the index");

    let ctx = reopened(&pool).await?;
    assert_eq!(recipients(&ctx, "detail.kind = 'mention' AND status = 'unread'").await?, ["alice"]);
    drop_index(&pool, "_ankurah_index__notification__detail.kind asc__status asc").await?;
    assert_eq!(recipients(&ctx, "detail.kind = 'mention' AND status = 'unread'").await?, ["alice"]);
    assert_eq!(index_names(&pool).await?, [composite], "the expression index was read back");
    Ok(())
}

/// A query whose plan reads no index creates none: the planner indexes
/// neither an inequality on its own nor a property the table has no column
/// for, and a predicate on such a property is evaluated on stored states.
#[tokio::test]
async fn a_plan_without_an_index_creates_nothing() -> Result<()> {
    let (_container, storage, pool) = common::create_postgres_container_with_pool().await?;
    let ctx = notified(storage, ALICE_AND_BOB).await?;
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
    let first = notified(storage, ALICE_AND_BOB).await?;
    let second = reopened(&pool).await?;
    let query = "recipient = 'alice' AND status = 'unread'";
    let results =
        tokio::join!(recipients(&first, query), recipients(&second, query), recipients(&first, query), recipients(&second, query));
    for found in [results.0?, results.1?, results.2?, results.3?] {
        assert_eq!(found, ["alice"]);
    }
    assert_eq!(index_names(&pool).await?, [RECIPIENT_STATUS]);
    Ok(())
}

/// Two engines on one database deciding at once, one wanting the key
/// ascending and the other descending, create one index that serves both:
/// each decides under the table's advisory lock, so the second finds the
/// first's.
#[tokio::test(flavor = "multi_thread")]
async fn two_engines_deciding_at_once_create_one_index_that_serves_both() -> Result<()> {
    let (_container, storage, pool) = common::create_postgres_container_with_pool().await?;
    let ctx_first = notified(storage, ALICE_AND_BOB).await?;
    let ctx_second = reopened(&pool).await?;
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
    let names = index_names(&pool).await?;
    assert_eq!(names.len(), 1, "{names:?}");
    assert!(names[0].starts_with("_ankurah_index__notification__kind "), "{names:?}");
    Ok(())
}

/// An application's index on the name the engine would use is left alone:
/// the engine finds the name taken by an index that does not serve the plan,
/// creates nothing, and the query keeps answering.
#[tokio::test]
async fn an_application_index_on_the_engine_s_name_is_left_alone() -> Result<()> {
    let (_container, storage, pool) = common::create_postgres_container_with_pool().await?;
    let ctx = notified(storage, ALICE_AND_BOB).await?;
    pool.get().await?.execute(&format!(r#"CREATE INDEX "{RECIPIENT_STATUS}" ON "notification" ("kind")"#), &[]).await?;

    assert_eq!(recipients(&ctx, "recipient = 'alice' AND status = 'unread'").await?, ["alice"]);
    assert_eq!(catalog(&pool).await?, [(RECIPIENT_STATUS.to_owned(), vec![(Some("kind".to_owned()), false)])]);
    assert_eq!(recipients(&ctx, "recipient = 'bob' AND status = 'unread'").await?, ["bob"]);
    assert_eq!(catalog(&pool).await?, [(RECIPIENT_STATUS.to_owned(), vec![(Some("kind".to_owned()), false)])]);
    Ok(())
}
