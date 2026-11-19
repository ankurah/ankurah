//! Canonical-state continuation through a WAL read transaction, or validated
//! short reads on the sole connection of a private-memory database.

use std::{collections::BTreeMap, num::NonZeroUsize};

use ankql::ast::{Resolved, Selection};
use ankurah_core::{
    error::RetrievalError,
    storage::{StateCursor, StateSnapshot},
};
use ankurah_proto::{Attested, EntityId, EntityState};
use ankurah_storage_common::{selection::LimitedSelection, state::CounterCursor};
use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension};

use super::{
    decode_state_row, load_states, maps::Lease, memberships_from_sqlite_connection, raw_state_from_sqlite_row, SqliteStorageEngine,
    ENTITY_TABLE, META_TABLE,
};
use crate::error::SqliteError;

pub(super) const CHANGES_TABLE: &str = "_ankurah_state_changes";
pub(super) const CURSOR_KEY: &str = "state_cursor";
const ENGINE: &[u8] = b"sqlite";

pub(super) fn initialize(connection: &Connection) -> Result<(), SqliteError> {
    let present =
        connection.query_row(&format!(r#"SELECT 1 FROM "{META_TABLE}" WHERE "key" = ?"#), [CURSOR_KEY], |_| Ok(())).optional()?.is_some();
    if present {
        return Ok(());
    }
    connection.execute_batch("SAVEPOINT ankurah_state_cursor")?;
    let result = (|| {
        let inserted = connection.execute(
            &format!(r#"INSERT OR IGNORE INTO "{META_TABLE}" ("key", "value") VALUES (?, ?)"#),
            rusqlite::params![CURSOR_KEY, CounterCursor::fresh().encode(ENGINE).as_bytes()],
        )?;
        if inserted != 0 {
            connection.execute_batch(&format!(
                r#"
                CREATE TABLE IF NOT EXISTS "{CHANGES_TABLE}" (
                    "entity_id" TEXT PRIMARY KEY,
                    "position" BLOB NOT NULL
                );
                CREATE INDEX IF NOT EXISTS "{CHANGES_TABLE}_position" ON "{CHANGES_TABLE}" ("position", "entity_id");
                INSERT OR IGNORE INTO "{CHANGES_TABLE}" ("entity_id", "position")
                    SELECT "id", X'0000000000000000' FROM "{ENTITY_TABLE}";
            "#
            ))?;
        }
        Ok::<_, SqliteError>(())
    })();
    if result.is_err() {
        connection.execute_batch("ROLLBACK TO ankurah_state_cursor")?;
    }
    connection.execute_batch("RELEASE ankurah_state_cursor")?;
    result
}

fn current(connection: &Connection) -> Result<CounterCursor, SqliteError> {
    let bytes = connection
        .query_row(&format!(r#"SELECT "value" FROM "{META_TABLE}" WHERE "key" = ?"#), [CURSOR_KEY], |row| row.get::<_, Vec<u8>>(0))
        .optional()?
        .ok_or(SqliteError::InvalidStateCursor)?;
    CounterCursor::decode(&StateCursor::new(bytes), ENGINE).map_err(|error| SqliteError::CorruptRecord(error.to_string()))
}

pub(super) fn advance(connection: &Connection) -> Result<u64, SqliteError> {
    let mut cursor = current(connection)?;
    cursor.position =
        cursor.position.checked_add(1).ok_or_else(|| SqliteError::CorruptRecord("canonical state counter exhausted".into()))?;
    connection.execute(
        &format!(r#"UPDATE "{META_TABLE}" SET "value" = ? WHERE "key" = ?"#),
        rusqlite::params![cursor.encode(ENGINE).as_bytes(), CURSOR_KEY],
    )?;
    Ok(cursor.position)
}

pub(super) struct SqliteStateSnapshot {
    source: Source,
    target: CounterCursor,
    after: (u64, String),
}

enum Source {
    File(Lease),
    Memory(crate::connection::PooledConnection),
}

impl SqliteStateSnapshot {
    pub(super) async fn begin(engine: &SqliteStorageEngine, since: Option<&StateCursor>) -> Result<Self, RetrievalError> {
        let (source, target) = if let Some(connection) = engine.maps.memory_connection().await? {
            engine.ensure_shared_tables(&connection).await?;
            let target = connection.with_connection(current).await?;
            (Source::Memory(connection), target)
        } else {
            let mut lease = engine.maps.lease().await?;
            engine.ensure_shared_tables(lease.connection()).await?;
            let target = lease
                .run(|connection| {
                    connection.execute_batch("BEGIN")?;
                    current(connection)
                })
                .await?;
            (Source::File(lease), target)
        };
        let since = target.resume(since, ENGINE)?;
        let after = since.map_or((0, String::new()), |position| (position, "\u{10ffff}".into()));
        Ok(Self { source, target, after })
    }

    async fn read<T: Send + 'static>(
        &mut self,
        call: impl FnOnce(&Connection) -> Result<T, SqliteError> + Send + 'static,
    ) -> Result<T, SqliteError> {
        let target = self.target;
        match &mut self.source {
            Source::File(lease) => lease.run(call).await,
            Source::Memory(connection) => {
                connection
                    .with_connection_mut(move |connection| {
                        let snapshot = connection.transaction()?;
                        let now = current(&snapshot)?;
                        if now.incarnation != target.incarnation {
                            return Err(SqliteError::InvalidStateCursor);
                        }
                        if now.position != target.position {
                            return Err(SqliteError::SnapshotChanged);
                        }
                        call(&snapshot)
                    })
                    .await
            }
        }
    }
}

#[async_trait]
impl StateSnapshot for SqliteStateSnapshot {
    async fn next_page(&mut self, limit: NonZeroUsize) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let after = self.after.clone();
        let (states, last) = self
            .read(move |connection| {
                let mut statement = connection.prepare(&format!(
                    r#"
                SELECT "position", "entity_id" FROM "{CHANGES_TABLE}"
                WHERE ("position", "entity_id") > (?, ?) ORDER BY "position", "entity_id" LIMIT ?
            "#
                ))?;
                let limit = i64::try_from(limit.get()).unwrap_or(i64::MAX);
                let rows = statement.query_map(rusqlite::params![after.0.to_be_bytes().as_slice(), after.1, limit], |row| {
                    Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?))
                })?;
                let mut last = after;
                let mut ids = Vec::new();
                for row in rows {
                    let (position, key) = row?;
                    let position = u64::from_be_bytes(
                        position.as_slice().try_into().map_err(|_| SqliteError::CorruptRecord("invalid state position".into()))?,
                    );
                    ids.push(EntityId::from_base64(&key).map_err(|error| SqliteError::CorruptRecord(error.to_string()))?);
                    last = (position, key);
                }
                let states = load_states(connection, &ids)?;
                if states.len() != ids.len() {
                    return Err(SqliteError::CorruptRecord("state change names a missing entity".into()));
                }
                Ok((states, last))
            })
            .await?;
        self.after = last;
        Ok(states)
    }

    async fn get_states(&mut self, ids: &[EntityId]) -> Result<Vec<Option<Attested<EntityState>>>, RetrievalError> {
        let ids = ids.to_vec();
        Ok(self
            .read(move |connection| {
                let states: BTreeMap<_, _> =
                    load_states(connection, &ids)?.into_iter().map(|state| (state.payload.entity_id, state)).collect();
                Ok(ids.iter().map(|id| states.get(id).cloned()).collect())
            })
            .await?)
    }

    async fn limited_states(&mut self, selection: &Selection<Resolved>) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let selection = selection.clone();
        self.read(move |connection| {
            let mut selected = LimitedSelection::new(&selection).map_err(|error| SqliteError::CorruptRecord(error.to_string()))?;
            if selection.limit == Some(0) {
                return Ok(selected.finish());
            }
            let mut statement =
                connection.prepare(&format!(r#"SELECT "id", "state_buffer", "head", "attestations" FROM "{ENTITY_TABLE}""#))?;
            for row in statement.query_map([], raw_state_from_sqlite_row)? {
                let row = row?;
                let memberships = memberships_from_sqlite_connection(connection, &row.0)?;
                selected.consider(decode_state_row(row, memberships)?).map_err(|error| SqliteError::CorruptRecord(error.to_string()))?;
            }
            Ok(selected.finish())
        })
        .await
        .map_err(Into::into)
    }

    async fn finish(mut self: Box<Self>) -> Result<StateCursor, RetrievalError> {
        match &mut self.source {
            Source::File(lease) => lease.run(|connection| Ok(connection.execute_batch("ROLLBACK")?)).await?,
            Source::Memory(_) => self.read(|_| Ok(())).await?,
        }
        Ok(self.target.encode(ENGINE))
    }
}
