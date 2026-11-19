//! Canonical-state continuation through a WAL read transaction.

use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc};

use ankurah_core::{
    error::RetrievalError,
    selection::ordering::OrderBoundary,
    storage::{StateCursor, StateScan, StateSnapshot},
};
use ankurah_proto::{Attested, EntityId, EntityState};
use ankurah_storage_common::state::CounterCursor;
use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension};

use super::{load_states, maps::Lease, query::Query, SqliteStorageEngine, ENTITY_TABLE, META_TABLE};
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
    lease: Lease,
    target: CounterCursor,
    after: (u64, String),
    selected: Option<std::vec::IntoIter<Attested<EntityState>>>,
    query: Option<Arc<Query>>,
}

impl SqliteStateSnapshot {
    pub(super) async fn begin(engine: &SqliteStorageEngine, scan: StateScan<'_>) -> Result<Self, RetrievalError> {
        // Query preparation may create catalog/materialization columns. Complete
        // it before the read transaction fixes the schema and state boundary.
        let (since, selection, selection_changes) = match scan {
            StateScan::Changes(since) => (since, None, false),
            StateScan::SelectionChanges { selection, since } => {
                if selection.limit.is_none() {
                    return Err(RetrievalError::Other("a selection change snapshot requires LIMIT".into()));
                }
                (since, Some(selection), true)
            }
            StateScan::Selection(selection) => {
                if selection.limit.is_none() {
                    return Err(RetrievalError::Other("a selection snapshot requires LIMIT".into()));
                }
                (None, Some(selection), false)
            }
        };
        let (query, prepared_at) = match selection {
            None => (None, None),
            Some(selection) => {
                let prepared_at = {
                    let connection = engine.pool.get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
                    engine.ensure_shared_tables(&connection).await?;
                    connection.with_connection(current).await?
                };
                (Some(Arc::new(Query::prepare(engine, selection).await?)), Some(prepared_at))
            }
        };
        let mut lease = engine.maps.lease().await?;
        engine.ensure_shared_tables(lease.connection()).await?;
        let target = lease
            .run(|connection| {
                connection.execute_batch("BEGIN")?;
                current(connection)
            })
            .await?;
        // A commit during planning may introduce a materialization the query
        // did not include. It cannot be certified by the later source cursor.
        if prepared_at.is_some_and(|prepared_at| prepared_at != target) {
            return Err(SqliteError::SnapshotChanged.into());
        }
        let since = target.resume(since, ENGINE)?;
        let after = since.map_or((0, String::new()), |position| (position, "\u{10ffff}".into()));
        let mut snapshot = Self { lease, target, after, selected: None, query: None };
        if let Some(query) = query {
            if selection_changes {
                snapshot.query = Some(query);
                return Ok(snapshot);
            }
            let selected = snapshot.lease.run(move |connection| Ok(query.states_in(connection))).await??;
            snapshot.selected = Some(selected.into_iter());
        }
        Ok(snapshot)
    }
}

#[async_trait]
impl StateSnapshot for SqliteStateSnapshot {
    async fn next_page(&mut self, limit: NonZeroUsize) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        if self.selected.is_some() {
            return Ok(self.selected.as_mut().unwrap().by_ref().take(limit.get()).collect());
        }
        let after = self.after.clone();
        let (states, last) = self
            .lease
            .run(move |connection| {
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
            .lease
            .run(move |connection| {
                let states: BTreeMap<_, _> =
                    load_states(connection, &ids)?.into_iter().map(|state| (state.payload.entity_id, state)).collect();
                Ok(ids.iter().map(|id| states.get(id).cloned()).collect())
            })
            .await?)
    }

    async fn matching_ids(&mut self, ids: &[EntityId]) -> Result<Vec<EntityId>, RetrievalError> {
        let query = self.query.clone().ok_or_else(|| RetrievalError::Other("snapshot has no prepared selection".into()))?;
        let ids = ids.to_vec();
        self.lease.run(move |connection| Ok(query.matching_ids_in(connection, &ids))).await?
    }

    async fn select_after(
        &mut self,
        after: Option<&OrderBoundary>,
        limit: NonZeroUsize,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let query = self.query.clone().ok_or_else(|| RetrievalError::Other("snapshot has no prepared selection".into()))?;
        let after = after.cloned();
        self.lease.run(move |connection| Ok(query.select_after_in(connection, after.as_ref(), limit))).await?
    }

    async fn finish(mut self: Box<Self>) -> Result<StateCursor, RetrievalError> {
        self.lease.run(|connection| Ok(connection.execute_batch("ROLLBACK")?)).await?;
        Ok(self.target.encode(ENGINE))
    }
}
