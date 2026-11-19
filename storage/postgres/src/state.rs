//! Canonical-state continuation through PostgreSQL transaction snapshots.
//!
//! A state's writer is a top-level transaction ID, not a commit sequence. A
//! saved snapshot includes the transactions that were still running, so a
//! later scan can find their writes even when they commit out of ID order.

use std::{collections::VecDeque, num::NonZeroUsize, sync::Arc, time::Duration};

use ankurah_core::{
    error::{MutationError, RetrievalError, StateError},
    selection::ordering::OrderBoundary,
    storage::{StateCursor, StateScan, StateSnapshot},
};
use ankurah_proto::{Attested, EntityId, EntityState};
use async_trait::async_trait;
use bincode::Options;
use tokio_postgres::{Client, NoTls, Row};

use crate::{query::Query, state_from_row, Postgres, ENTITY_MODEL_TABLE, ENTITY_TABLE, META_TABLE};

const CURSOR_FORMAT: &[u8] = b"ankurah:postgres:state:1\0";
const INCARNATION: &str = "state_incarnation";

/// Add continuation metadata to an existing store under its shared DDL lock.
/// PostgreSQL 11's bigint/txid_snapshot functions also work on newer servers.
pub(super) async fn ensure(client: &Client) -> Result<(), StateError> {
    // ALTER TABLE takes an exclusive relation lock even when IF NOT EXISTS
    // finds the column. Reopening an initialized store must not wait for
    // another node's long-running canonical-state snapshot.
    let has_writer: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = $1::text::regclass AND attname = 'state_writer' AND NOT attisdropped)",
            &[&ENTITY_TABLE],
        )
        .await
        .map_err(|error| StateError::DDLError(Box::new(error)))?
        .get(0);
    if !has_writer {
        client
            .batch_execute(&format!(r#"ALTER TABLE "{ENTITY_TABLE}" ADD COLUMN "state_writer" bigint NOT NULL DEFAULT txid_current()"#))
            .await
            .map_err(|error| StateError::DDLError(Box::new(error)))?;
    }
    client
        .batch_execute(&format!(
            r#"CREATE INDEX IF NOT EXISTS "{ENTITY_TABLE}_state_writer_idx" ON "{ENTITY_TABLE}" ("state_writer", "id")"#
        ))
        .await
        .map_err(|error| StateError::DDLError(Box::new(error)))?;
    client
        .execute(
            &format!(r#"INSERT INTO "{META_TABLE}" ("key", "value") VALUES ($1, $2) ON CONFLICT ("key") DO NOTHING"#),
            &[&INCARNATION, &format!("{:032x}", rand::random::<u128>())],
        )
        .await
        .map_err(|error| StateError::DDLError(Box::new(error)))?;
    Ok(())
}

/// Retire saved cursors in the same transaction that deletes canonical state.
pub(super) async fn reset(client: &Client) -> Result<(), MutationError> {
    client
        .execute(
            &format!(r#"UPDATE "{META_TABLE}" SET "value" = $1 WHERE "key" = $2"#),
            &[&format!("{:032x}", rand::random::<u128>()), &INCARNATION],
        )
        .await
        .map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
    Ok(())
}

/// One fixed read view, on a connection apart from the normal and map pools.
/// Holding it cannot consume the connection a tree write needs to finish.
pub(super) struct Snapshot {
    client: Client,
    cursor: StateCursor,
    scopes: VecDeque<WriterScope>,
    after: (i64, String),
    selected: Option<std::vec::IntoIter<Attested<EntityState>>>,
    ended: bool,
    query: Option<Arc<Query>>,
    incarnation: String,
}

/// Separate exact in-progress writers from the new-ID range. Each query can
/// seek the writer index directly, including under a generic prepared plan.
enum WriterScope {
    Transaction(i64),
    Since(i64),
}

impl Snapshot {
    fn ensure_active(&self) -> Result<(), RetrievalError> {
        if self.ended {
            return Err(MutationError::WriteConflict.into());
        }
        Ok(())
    }

    pub(super) async fn begin(engine: &Postgres, scan: StateScan<'_>) -> Result<Self, RetrievalError> {
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
        let client = tokio::time::timeout(Duration::from_secs(crate::DEFAULT_CONNECTION_TIMEOUT_SECS), engine.pool.dedicated_connection())
            .await
            .map_err(RetrievalError::storage)?
            .map_err(RetrievalError::storage)?;
        let mut snapshot = Self {
            client,
            cursor: StateCursor::new(Vec::new()),
            scopes: VecDeque::new(),
            after: (0, String::new()),
            selected: None,
            ended: false,
            query: None,
            incarnation: String::new(),
        };
        engine.ensure_shared_tables(&snapshot.client).await.map_err(RetrievalError::storage)?;
        // Planning may prepare materializations and their columns. Do that
        // before opening the fixed read view or holding its relation locks.
        // Capture coverage before planning: a later canonical commit can add
        // materializations/properties the prepared query did not include.
        let prepared = if let Some(selection) = selection {
            let (incarnation, boundary) = read_boundary(&snapshot.client).await?;
            let query = match Query::prepare(engine, selection).await {
                Ok(query) => query,
                Err(error) => return Err(after_possible_reset(&snapshot.client, &incarnation, error).await),
            };
            Some((incarnation, boundary, query))
        } else {
            None
        };
        // Take relation locks before reading the cursor metadata. A reset
        // cannot remove these relations between the boundary and its pages.
        snapshot
            .client
            .batch_execute(&format!(
                r#"BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
                   LOCK TABLE "{ENTITY_TABLE}", "{ENTITY_MODEL_TABLE}" IN ACCESS SHARE MODE"#
            ))
            .await
            .map_err(RetrievalError::storage)?;
        let (incarnation, target) = read_boundary(&snapshot.client).await?;
        if prepared.as_ref().is_some_and(|(before, _, _)| *before != incarnation) {
            return Err(MutationError::WriteConflict.into());
        }

        let previous = if let Some(since) = since {
            let bytes = since.as_bytes().strip_prefix(CURSOR_FORMAT).ok_or(RetrievalError::InvalidStateCursor)?;
            Some(
                bincode::DefaultOptions::new()
                    .with_fixint_encoding()
                    .with_limit(bytes.len() as u64)
                    .reject_trailing_bytes()
                    .deserialize::<(String, String)>(bytes)
                    .map_err(|_| RetrievalError::InvalidStateCursor)?,
            )
        } else {
            None
        };
        if let Some((old_incarnation, old_snapshot)) = previous {
            if old_incarnation != incarnation {
                return Err(RetrievalError::InvalidStateCursor);
            }
            snapshot.scopes = scopes_since(&old_snapshot, &target)?;
        } else {
            snapshot.scopes.push_back(WriterScope::Since(0));
        }

        snapshot.incarnation = incarnation.clone();
        let mut encoded = CURSOR_FORMAT.to_vec();
        encoded.extend(bincode::serialize(&(incarnation, &target))?);
        snapshot.cursor = StateCursor::new(encoded);
        if let Some((incarnation, planned_at, query)) = prepared {
            // Compare canonical changes, not raw snapshot text: planning DDL
            // and unrelated transactions can allocate IDs without changing any
            // state. The ordinary change scan also catches older late commits.
            let requested_scopes = std::mem::replace(&mut snapshot.scopes, scopes_since(&planned_at, &target)?);
            let changed_during_planning = !snapshot.next_page(NonZeroUsize::MIN).await?.is_empty();
            snapshot.scopes = requested_scopes;
            snapshot.after = (0, String::new());
            if changed_during_planning {
                return Err(MutationError::WriteConflict.into());
            }
            if selection_changes {
                snapshot.query = Some(Arc::new(query));
                return Ok(snapshot);
            }
            // Reset drops materialization tables. Keep their read locks only
            // while collecting the bounded selection, so reset cannot wait on
            // this source snapshot while its later tree-map flush waits on reset.
            // Canonical relation locks and the read boundary precede this savepoint.
            snapshot.client.batch_execute("SAVEPOINT selected_states").await.map_err(RetrievalError::storage)?;
            let states = match query.states_in(&snapshot.client).await {
                Ok(states) => states,
                Err(error) => {
                    // Outside the failed snapshot, distinguish a real reset
                    // from SQL/query errors that must keep their original cause.
                    if snapshot.client.batch_execute("ROLLBACK").await.is_ok() {
                        snapshot.ended = true;
                        return Err(after_possible_reset(&snapshot.client, &incarnation, error).await);
                    }
                    return Err(error);
                }
            };
            snapshot
                .client
                .batch_execute("ROLLBACK TO SAVEPOINT selected_states; RELEASE SAVEPOINT selected_states")
                .await
                .map_err(RetrievalError::storage)?;
            snapshot.selected = Some(states.into_iter());
        }
        Ok(snapshot)
    }
    async fn complete_query<T>(&mut self, result: Result<T, RetrievalError>) -> Result<T, RetrievalError> {
        let result = match result {
            Ok(value) => self
                .client
                .batch_execute("ROLLBACK TO SAVEPOINT selection_query; RELEASE SAVEPOINT selection_query")
                .await
                .map(|_| value)
                .map_err(RetrievalError::storage),
            Err(error) => Err(error),
        };
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                if self.client.batch_execute("ROLLBACK").await.is_ok() {
                    self.ended = true;
                    return Err(after_possible_reset(&self.client, &self.incarnation, error).await);
                }
                Err(error)
            }
        }
    }
}

fn scopes_since(previous: &str, current: &str) -> Result<VecDeque<WriterScope>, RetrievalError> {
    let previous = Boundary::parse(previous)?;
    let current = Boundary::parse(current)?;
    if previous.end > current.end || current.running.iter().any(|xid| *xid < previous.end && previous.running.binary_search(xid).is_err()) {
        return Err(RetrievalError::InvalidStateCursor);
    }
    let mut scopes: VecDeque<_> = previous.running.into_iter().map(WriterScope::Transaction).collect();
    scopes.push_back(WriterScope::Since(previous.end));
    Ok(scopes)
}

async fn read_boundary(client: &Client) -> Result<(String, String), RetrievalError> {
    let row = client
        .query_one(
            &format!(r#"SELECT "value", txid_current_snapshot()::text AS snapshot FROM "{META_TABLE}" WHERE "key" = $1"#),
            &[&INCARNATION],
        )
        .await
        .map_err(RetrievalError::storage)?;
    Ok((row.get("value"), row.get("snapshot")))
}

async fn current_incarnation(client: &Client) -> Result<String, RetrievalError> {
    Ok(client
        .query_one(&format!(r#"SELECT "value" FROM "{META_TABLE}" WHERE "key" = $1"#), &[&INCARNATION])
        .await
        .map_err(RetrievalError::storage)?
        .get(0))
}

async fn after_possible_reset(client: &Client, before: &str, error: RetrievalError) -> RetrievalError {
    if current_incarnation(client).await.is_ok_and(|current| current != before) {
        MutationError::WriteConflict.into()
    } else {
        error
    }
}

#[async_trait]
impl StateSnapshot for Snapshot {
    async fn next_page(&mut self, limit: NonZeroUsize) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        self.ensure_active()?;
        if let Some(selected) = &mut self.selected {
            return Ok(selected.by_ref().take(limit.get()).collect());
        }
        let mut states = Vec::new();
        while states.len() < limit.get() {
            let Some(scope) = self.scopes.front() else { break };
            let remaining = i64::try_from(limit.get() - states.len()).unwrap_or(i64::MAX);
            let rows = match scope {
                WriterScope::Transaction(writer) => {
                    self.client
                        .query(
                            &format!(r#"{} WHERE e."state_writer" = $1 AND e."id" > $2 ORDER BY e."id" LIMIT $3"#, select_states()),
                            &[writer, &self.after.1, &remaining],
                        )
                        .await
                }
                WriterScope::Since(writer) => {
                    self.client
                        .query(
                            &format!(
                                r#"{} WHERE e."state_writer" >= $1 AND (e."state_writer", e."id") > ($2, $3)
                                   ORDER BY e."state_writer", e."id" LIMIT $4"#,
                                select_states()
                            ),
                            &[writer, &self.after.0, &self.after.1, &remaining],
                        )
                        .await
                }
            }
            .map_err(RetrievalError::storage)?;
            if let Some(last) = rows.last() {
                self.after = (last.get("state_writer"), last.get("id"));
            }
            let exhausted = rows.len() < remaining as usize;
            states.extend(rows.into_iter().map(decode_state).collect::<Result<Vec<_>, _>>()?);
            if exhausted {
                self.scopes.pop_front();
                self.after = (0, String::new());
            }
        }
        Ok(states)
    }

    async fn get_states(&mut self, ids: &[EntityId]) -> Result<Vec<Option<Attested<EntityState>>>, RetrievalError> {
        self.ensure_active()?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows =
            self.client.query(&format!(r#"{} WHERE e."id" = ANY($1)"#, select_states()), &[&ids]).await.map_err(RetrievalError::storage)?;
        let mut states = std::collections::BTreeMap::new();
        for row in rows {
            let state = decode_state(row)?;
            states.insert(state.payload.entity_id, state);
        }
        Ok(ids.iter().map(|id| states.get(id).cloned()).collect())
    }

    async fn matching_ids(&mut self, ids: &[EntityId]) -> Result<Vec<EntityId>, RetrievalError> {
        self.ensure_active()?;
        let query = self.query.clone().ok_or_else(|| RetrievalError::Other("snapshot has no prepared selection".into()))?;
        self.client.batch_execute("SAVEPOINT selection_query").await.map_err(RetrievalError::storage)?;
        let result = query.matching_ids_in(&self.client, ids).await;
        self.complete_query(result).await
    }

    async fn select_after(
        &mut self,
        after: Option<&OrderBoundary>,
        limit: NonZeroUsize,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        self.ensure_active()?;
        let query = self.query.clone().ok_or_else(|| RetrievalError::Other("snapshot has no prepared selection".into()))?;
        self.client.batch_execute("SAVEPOINT selection_query").await.map_err(RetrievalError::storage)?;
        let result = query.select_after_in(&self.client, after, limit).await;
        self.complete_query(result).await
    }

    async fn finish(mut self: Box<Self>) -> Result<StateCursor, RetrievalError> {
        self.ensure_active()?;
        self.client.batch_execute("ROLLBACK").await.map_err(RetrievalError::storage)?;
        self.ended = true;
        Ok(self.cursor.clone())
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        if !self.ended {
            // Closing the dedicated client rolls back. Cancel a statement
            // still waiting for a lock so it does not keep the session alive.
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                let cancel = self.client.cancel_token();
                runtime.spawn(async move { tokio::time::timeout(Duration::from_secs(30), cancel.cancel_query(NoTls)).await });
            }
        }
    }
}

fn select_states() -> String {
    format!(
        r#"SELECT e."id", e."state_buffer", e."head", e."attestations", e."state_writer",
                  ARRAY(SELECT m."model_key" FROM "{ENTITY_MODEL_TABLE}" m WHERE m."entity_id" = e."id") AS memberships
           FROM "{ENTITY_TABLE}" e"#
    )
}

fn decode_state(row: Row) -> Result<Attested<EntityState>, RetrievalError> {
    let memberships: Vec<Vec<u8>> = row.try_get("memberships").map_err(RetrievalError::storage)?;
    state_from_row(&row, memberships.into_iter().map(|bytes| bincode::deserialize(&bytes)).collect::<Result<_, _>>()?)
}

/// PostgreSQL emits xmin:xmax:xip. Only the upper boundary and in-progress
/// writers are needed to find states not visible in that snapshot.
struct Boundary {
    end: i64,
    running: Vec<i64>,
}

impl Boundary {
    fn parse(snapshot: &str) -> Result<Self, RetrievalError> {
        let invalid = || RetrievalError::InvalidStateCursor;
        let mut fields = snapshot.split(':');
        let start: i64 = fields.next().ok_or_else(invalid)?.parse().map_err(|_| invalid())?;
        let end: i64 = fields.next().ok_or_else(invalid)?.parse().map_err(|_| invalid())?;
        let running = fields.next().ok_or_else(invalid)?;
        if fields.next().is_some() || start < 0 || start > end {
            return Err(invalid());
        }
        let running: Vec<i64> = if running.is_empty() {
            Vec::new()
        } else {
            running.split(',').map(|xid| xid.parse().map_err(|_| invalid())).collect::<Result<_, _>>()?
        };
        if running.iter().any(|xid| *xid < start || *xid >= end) || running.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(invalid());
        }
        Ok(Self { end, running })
    }
}

#[cfg(test)]
mod tests;
