//! The table that holds every map, the locks map transactions take, and the
//! statements each of the maps' connections prepares once.
//!
//! Every statement that finds entries by key does so one key at a time, as a
//! probe of the whole primary key: a scalar subquery per key, whose rows a
//! deletion then deletes by their row ids. The table holds every map, and
//! PostgreSQL's planner, knowing little of a map written moments ago,
//! otherwise reads every entry of a map to join or filter a handful of keys,
//! and does it while the transaction holds its locks.

use std::ops::Bound;

use ankurah_core::error::StateError;
use ankurah_core::storage::ScanDirection;
use futures_util::future::try_join_all;
use tokio_postgres::{Client, Statement};

use crate::maps::MapError;
use crate::{acquire_ddl_lock, release_ddl_lock, MAP_TABLE};

/// Create the table on `client`, if it doesn't exist yet, under the engine's
/// lock on its DDL, since `IF NOT EXISTS` doesn't keep two sessions creating
/// one table at once from failing.
///
/// An entry of a few kilobytes, such as a stored digest tree's row in the
/// LtHash16 group, which holds a 2 KB element, is past the size at which
/// PostgreSQL would move it out of line into the table's TOAST table, where
/// every read of it costs another lookup: `toast_tuple_target` keeps entries
/// of up to 8 KB in the table itself.
pub(crate) async fn create_table(client: &Client) -> Result<(), StateError> {
    let lock_key = acquire_ddl_lock(client, MAP_TABLE).await?;

    let created = client
        .batch_execute(&format!(
            r#"CREATE TABLE IF NOT EXISTS "{MAP_TABLE}" (
                "map"   bytea NOT NULL,
                "key"   bytea NOT NULL,
                "value" bytea NOT NULL,
                PRIMARY KEY ("map", "key")
            ) WITH (toast_tuple_target = 8160)"#
        ))
        .await;

    release_ddl_lock(client, lock_key).await?;
    created.map_err(|error| StateError::DDLError(Box::new(error)))
}

/// The reset lock, which `delete_all` takes exclusively and every map
/// transaction takes shared at its first map call, each until it ends: a
/// reset waits for the map transactions under way, and none begins while it
/// runs. It is one key, "ak_r" and "eset" in ASCII, in the space of advisory
/// locks keyed by two numbers, which no lock name's key shares.
const RESET_LOCK: (i32, i32) = (0x616b_5f72, 0x6573_6574);

/// A map transaction's share of the reset lock.
pub(crate) fn share_reset_lock() -> String {
    let (class, object) = RESET_LOCK;
    format!("SELECT pg_advisory_xact_lock_shared({class}, {object})")
}

/// Empty every map, in a reset's transaction open on `client`, holding the
/// reset lock until the transaction ends: once every map transaction under
/// way has ended, and before any other begins. Whether any map held an
/// entry.
///
/// `client` is one of the maps' connections, which open only once the table
/// is made, and whose lock timeout fails a reset that waits past it with
/// [`MapError::LockTimeout`].
pub(crate) async fn reset(client: &Client) -> Result<bool, MapError> {
    let (class, object) = RESET_LOCK;
    client.batch_execute(&format!("SELECT pg_advisory_xact_lock({class}, {object})")).await.map_err(MapError::database)?;

    let removed = client.execute(&format!(r#"DELETE FROM "{MAP_TABLE}""#), &[]).await.map_err(MapError::database)?;
    Ok(removed > 0)
}

/// One lock name, which the transaction holds until it ends: an advisory
/// lock on a 64-bit hash of the name under a prefix of its own, in the space
/// of single keys that the engine's entity and DDL locks also take. Two names
/// whose keys collide share a lock, so that a transaction may wait on another
/// that holds neither's name, and two that take such names in opposite orders
/// deadlock, which PostgreSQL detects, failing one of them; at 64 bits a
/// collision is improbable, not impossible.
fn lock() -> String { "SELECT pg_advisory_xact_lock(hashtextextended('map lock ' || encode($1, 'hex'), 0))".to_owned() }

/// The value under each of several keys of one map, or none, one row for
/// each key, in their order.
fn get() -> String {
    format!(
        r#"SELECT (SELECT "value" FROM "{MAP_TABLE}" WHERE "map" = $1 AND "key" = wanted.key)
           FROM unnest($2::bytea[]) WITH ORDINALITY AS wanted (key, place) ORDER BY wanted.place"#
    )
}

/// Values put under keys of one map, each over any value under its key.
fn put() -> String {
    format!(
        r#"INSERT INTO "{MAP_TABLE}" ("map", "key", "value")
           SELECT $1, key, value FROM unnest($2::bytea[], $3::bytea[]) AS written (key, value)
           ON CONFLICT ("map", "key") DO UPDATE SET "value" = EXCLUDED."value""#
    )
}

/// Keys deleted from one map: each entry found by its key, and every entry
/// found deleted by its row id, so that one statement deletes them all.
/// Deleting by row id is safe: every write to a digest tree's maps is made
/// under the tree's lock, and of two writers of one key that hold no lock, a
/// delete that waited on the other's update skips the entry it updated, as
/// if the delete had come first.
fn delete() -> String {
    format!(
        r#"DELETE FROM "{MAP_TABLE}" WHERE ctid = ANY (ARRAY(
               SELECT (SELECT ctid FROM "{MAP_TABLE}" WHERE "map" = $1 AND "key" = gone.key) FROM unnest($2::bytea[]) AS gone (key)
           ))"#
    )
}

fn clear() -> String { format!(r#"DELETE FROM "{MAP_TABLE}" WHERE "map" = $1"#) }

/// The statements a connection prepares once, so that each later request is
/// one round trip.
pub(crate) struct Statements {
    pub(crate) lock: Statement,
    pub(crate) get: Statement,
    pub(crate) put: Statement,
    pub(crate) delete: Statement,
    pub(crate) clear: Statement,

    /// A range read, for each kind of start, end and direction.
    ranges: Vec<Statement>,
}

/// How a range read's bound limits it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Edge {
    Open,
    Included,
    Excluded,
}

const EDGES: [Edge; 3] = [Edge::Open, Edge::Included, Edge::Excluded];
const DIRECTIONS: [ScanDirection; 2] = [ScanDirection::Forward, ScanDirection::Reverse];

impl Statements {
    /// Prepare every statement on `client`, in one round trip.
    pub(crate) async fn prepare(client: &Client) -> Result<Self, tokio_postgres::Error> {
        let fixed = [lock(), get(), put(), delete(), clear()];
        let fixed = try_join_all(fixed.iter().map(|sql| client.prepare(sql)));
        let ranges = try_join_all(range_sqls().into_iter().map(|sql| async move { client.prepare(&sql).await }));
        let (fixed, ranges) = futures_util::try_join!(fixed, ranges)?;

        let [lock, get, put, delete, clear]: [Statement; 5] = fixed.try_into().expect("one statement prepared for each text");
        Ok(Self { lock, get, put, delete, clear, ranges })
    }

    /// The range read between bounds of the kinds of `start` and `end`, in
    /// `direction`. Its parameters are the map, each bound that limits it,
    /// start first, and the limit.
    pub(crate) fn range(&self, start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>, direction: ScanDirection) -> &Statement {
        let index = |edge| EDGES.iter().position(|&each| each == edge).expect("every edge is listed");
        let direction = DIRECTIONS.iter().position(|&each| each == direction).expect("every direction is listed");

        &self.ranges[(index(edge(start)) * EDGES.len() + index(edge(end))) * DIRECTIONS.len() + direction]
    }
}

fn edge(bound: &Bound<Vec<u8>>) -> Edge {
    match bound {
        Bound::Unbounded => Edge::Open,
        Bound::Included(_) => Edge::Included,
        Bound::Excluded(_) => Edge::Excluded,
    }
}

/// The text of every range read, for each kind of start, end and direction,
/// in the order [`Statements::range`] finds them.
fn range_sqls() -> Vec<String> {
    EDGES
        .iter()
        .flat_map(|&start| EDGES.iter().flat_map(move |&end| DIRECTIONS.iter().map(move |&direction| range_sql(start, end, direction))))
        .collect()
}

/// The text of a range read limited by bounds of these kinds, read in
/// `direction`.
fn range_sql(start: Edge, end: Edge, direction: ScanDirection) -> String {
    let mut sql = format!(r#"SELECT "key", "value" FROM "{MAP_TABLE}" WHERE "map" = $1"#);
    let mut parameter = 2;

    for (edge, inclusive, exclusive) in [(start, ">=", ">"), (end, "<=", "<")] {
        let operator = match edge {
            Edge::Open => continue,
            Edge::Included => inclusive,
            Edge::Excluded => exclusive,
        };

        sql.push_str(&format!(r#" AND "key" {operator} ${parameter}"#));
        parameter += 1;
    }

    let order = match direction {
        ScanDirection::Forward => "ASC",
        ScanDirection::Reverse => "DESC",
    };

    sql.push_str(&format!(r#" ORDER BY "key" {order} LIMIT ${parameter}"#));
    sql
}

#[cfg(test)]
mod tests;
