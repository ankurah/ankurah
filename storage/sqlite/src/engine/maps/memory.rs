//! Optimistic map transactions on SQLite's single private-memory connection.

use ankurah_core::storage::{pending_writes::PendingWrites, MapRead, MapRows, MapWrite};
use rusqlite::{Connection, OptionalExtension};

use super::statements;
use crate::{connection::PooledConnection, engine::META_TABLE, error::SqliteError};

const REVISION: &str = "map_revision";

pub(in crate::engine) fn revision(connection: &Connection) -> Result<u64, SqliteError> {
    let bytes = connection
        .query_row(&format!(r#"SELECT "value" FROM "{META_TABLE}" WHERE "key" = ?"#), [REVISION], |row| row.get::<_, Vec<u8>>(0))
        .optional()?;
    bytes
        .map(|bytes| {
            bytes.as_slice().try_into().map(u64::from_be_bytes).map_err(|_| SqliteError::CorruptRecord("invalid map revision".into()))
        })
        .unwrap_or(Ok(0))
}

pub(in crate::engine) fn advance(connection: &Connection) -> Result<(), SqliteError> {
    let next = revision(connection)?.checked_add(1).ok_or_else(|| SqliteError::CorruptRecord("map revision exhausted".into()))?;
    connection.execute(
        &format!(
            r#"INSERT INTO "{META_TABLE}" ("key", "value") VALUES (?, ?)
            ON CONFLICT ("key") DO UPDATE SET "value" = excluded."value""#
        ),
        rusqlite::params![REVISION, next.to_be_bytes().as_slice()],
    )?;
    Ok(())
}

pub(super) async fn read(
    connection: &PooledConnection,
    expected: Option<u64>,
    reads: Vec<MapRead>,
) -> Result<(u64, Vec<MapRows>), SqliteError> {
    connection
        .with_connection_mut(move |connection| {
            statements::create_table(connection)?;
            let snapshot = connection.transaction()?;
            let version = revision(&snapshot)?;
            if expected.is_some_and(|expected| expected != version) {
                return Err(SqliteError::SnapshotChanged);
            }
            let rows = reads.iter().map(|read| statements::read(&snapshot, read)).collect::<Result<_, _>>()?;
            Ok((version, rows))
        })
        .await
}

#[derive(Default)]
pub(in crate::engine) struct Writes {
    revision: Option<u64>,
    pending: PendingWrites,
}

impl Writes {
    pub(in crate::engine) async fn read(
        &mut self,
        connection: &PooledConnection,
        reads: Vec<MapRead>,
    ) -> Result<Vec<MapRows>, SqliteError> {
        let (reads, overlay) = self.pending.read(reads);
        let (revision, rows) = read(connection, self.revision, reads).await?;
        self.revision = Some(revision);
        Ok(overlay.answer(rows))
    }

    pub(in crate::engine) async fn write(&mut self, connection: &PooledConnection, writes: Vec<MapWrite>) -> Result<(), SqliteError> {
        self.read(connection, Vec::new()).await?;
        self.pending.add(writes);
        Ok(())
    }

    pub(in crate::engine) fn validate(&self, connection: &Connection) -> Result<(), SqliteError> {
        if self.revision != Some(revision(connection)?) {
            return Err(SqliteError::SnapshotChanged);
        }
        Ok(())
    }

    pub(in crate::engine) fn land(mut self, connection: &Connection) -> Result<(), SqliteError> {
        let changes = self.pending.take();
        if changes.is_empty() {
            return Ok(());
        }
        for (map, changes) in changes {
            if changes.cleared {
                statements::write(connection, &MapWrite::Clear { map: map.clone() })?;
            }
            for (key, value) in changes.keys {
                let write = match value {
                    Some(value) => MapWrite::Put { map: map.clone(), key, value },
                    None => MapWrite::Delete { map: map.clone(), key },
                };
                statements::write(connection, &write)?;
            }
        }
        advance(connection)
    }
}
