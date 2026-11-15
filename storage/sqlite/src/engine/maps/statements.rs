//! The maps' table, and the statements that read and write it.
//!
//! A map's id and a key are BLOBs, which SQLite compares byte by byte,
//! unsigned, a shorter one first where it begins the longer: the order of
//! maps' keys.

use std::ops::Bound;

use ankurah_core::storage::{MapRead, MapRows, MapWrite, ScanDirection};
use rusqlite::{named_params, Connection, OptionalExtension, ToSql};

use crate::engine::MAP_TABLE;
use crate::error::SqliteError;

pub(super) fn create_table(connection: &Connection) -> Result<(), SqliteError> {
    connection.execute_batch(&format!(
        r#"CREATE TABLE IF NOT EXISTS "{MAP_TABLE}" (
            "map" BLOB NOT NULL,
            "key" BLOB NOT NULL,
            "value" BLOB NOT NULL,
            PRIMARY KEY ("map", "key")
        ) WITHOUT ROWID"#
    ))?;

    Ok(())
}

/// Empty the maps' table, where the database has one: whether anything was
/// removed.
pub(in crate::engine) fn empty(connection: &Connection) -> Result<bool, SqliteError> {
    let exists = connection
        .query_row("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", [MAP_TABLE], |_| Ok(()))
        .optional()?
        .is_some();

    Ok(exists && connection.execute(&format!(r#"DELETE FROM "{MAP_TABLE}""#), [])? > 0)
}

pub(super) fn read(connection: &Connection, read: &MapRead) -> Result<MapRows, SqliteError> {
    match read {
        MapRead::Get { map, keys } => {
            let mut get =
                connection.prepare_cached(&format!(r#"SELECT "value" FROM "{MAP_TABLE}" WHERE "map" = :map AND "key" = :key"#))?;
            let values = keys
                .iter()
                .map(|key| get.query_row(named_params! { ":map": map.as_bytes(), ":key": key }, |row| row.get(0)).optional())
                .collect::<Result<_, _>>()?;

            Ok(MapRows::Got(values))
        }

        MapRead::Range { map, start, end, direction, limit } => {
            let (after, from) = match start {
                Bound::Included(key) => (r#"AND "key" >= :start"#, Some(key)),
                Bound::Excluded(key) => (r#"AND "key" > :start"#, Some(key)),
                Bound::Unbounded => ("", None),
            };

            let (before, to) = match end {
                Bound::Included(key) => (r#"AND "key" <= :end"#, Some(key)),
                Bound::Excluded(key) => (r#"AND "key" < :end"#, Some(key)),
                Bound::Unbounded => ("", None),
            };

            let order = match direction {
                ScanDirection::Forward => "ASC",
                ScanDirection::Reverse => "DESC",
            };

            let mut range = connection.prepare_cached(&format!(
                r#"SELECT "key", "value" FROM "{MAP_TABLE}" WHERE "map" = :map {after} {before} ORDER BY "key" {order} LIMIT :limit"#
            ))?;

            let (map, limit) = (map.as_bytes(), i64::try_from(*limit).unwrap_or(i64::MAX));
            let mut params: Vec<(&str, &dyn ToSql)> = vec![(":map", &map), (":limit", &limit)];
            params.extend(from.map(|key| (":start", key as &dyn ToSql)));
            params.extend(to.map(|key| (":end", key as &dyn ToSql)));

            let entries = range.query_map(params.as_slice(), |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<Result<_, _>>()?;
            Ok(MapRows::Ranged(entries))
        }
    }
}

pub(super) fn write(connection: &Connection, write: &MapWrite) -> Result<(), SqliteError> {
    match write {
        MapWrite::Put { map, key, value } => connection
            .prepare_cached(&format!(
                r#"INSERT INTO "{MAP_TABLE}" ("map", "key", "value") VALUES (:map, :key, :value)
               ON CONFLICT ("map", "key") DO UPDATE SET "value" = excluded."value""#
            ))?
            .execute(named_params! { ":map": map.as_bytes(), ":key": key, ":value": value })?,

        MapWrite::Delete { map, key } => connection
            .prepare_cached(&format!(r#"DELETE FROM "{MAP_TABLE}" WHERE "map" = :map AND "key" = :key"#))?
            .execute(named_params! { ":map": map.as_bytes(), ":key": key })?,

        MapWrite::Clear { map } => connection
            .prepare_cached(&format!(r#"DELETE FROM "{MAP_TABLE}" WHERE "map" = :map"#))?
            .execute(named_params! { ":map": map.as_bytes() })?,
    };

    Ok(())
}
