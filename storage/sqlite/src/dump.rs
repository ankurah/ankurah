//! Cursor-backed logical dumps for SQLite storage.

use std::{collections::VecDeque, pin::Pin};

use ankurah_core::{
    error::RetrievalError,
    storage::{StorageDump, StorageDumpItem},
};
use ankurah_proto::{AttestationSet, Attested, Clock, EntityId, EntityState, Event, EventBody, EventId, ModelId, State, StateBuffers};
use async_trait::async_trait;
use futures_util::{stream, Stream};

use crate::{
    engine::{ENTITY_MODEL_TABLE, ENTITY_TABLE, EVENT_TABLE},
    executor::{Pool, SqliteExecutor},
    value::SqliteValueRef,
    SqliteError, SqliteStorageEngine,
};

const PAGE_SIZE: i64 = 512;

type BoxDumpStream = Pin<Box<dyn Stream<Item = Result<StorageDumpItem, RetrievalError>> + Send + 'static>>;

#[async_trait]
impl StorageDump for SqliteStorageEngine {
    type DumpStream = BoxDumpStream;

    async fn dump(&self) -> Result<Self::DumpStream, RetrievalError> {
        let conn = self.pool().get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
        self.ensure_shared_tables(&conn).await?;
        let cursor = SqliteDumpCursor { pool: self.pool().clone(), phase: DumpPhase::Events, after: None, pending: VecDeque::new() };
        Ok(Box::pin(stream::try_unfold(cursor, |mut cursor| async move { Ok(cursor.next().await?.map(|item| (item, cursor))) })))
    }
}

#[derive(Clone, Copy)]
enum DumpPhase {
    Events,
    States,
    Done,
}

struct SqliteDumpCursor {
    pool: Pool,
    phase: DumpPhase,
    after: Option<String>,
    pending: VecDeque<StorageDumpItem>,
}

impl SqliteDumpCursor {
    async fn next(&mut self) -> Result<Option<StorageDumpItem>, RetrievalError> {
        loop {
            if let Some(item) = self.pending.pop_front() {
                return Ok(Some(item));
            }
            let conn = self.pool.get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
            let after = self.after.clone();
            let (last, page) = match self.phase {
                DumpPhase::Events => conn.with_executor(move |conn| event_page(conn, after.as_deref())).await?,
                DumpPhase::States => conn.with_executor(move |conn| state_page(conn, after.as_deref())).await?,
                DumpPhase::Done => return Ok(None),
            };
            if page.is_empty() {
                self.after = None;
                self.phase = match self.phase {
                    DumpPhase::Events => DumpPhase::States,
                    DumpPhase::States | DumpPhase::Done => DumpPhase::Done,
                };
                continue;
            }
            self.after = last;
            self.pending.extend(page);
        }
    }
}

fn event_page(conn: &dyn SqliteExecutor, after: Option<&str>) -> Result<(Option<String>, Vec<StorageDumpItem>), SqliteError> {
    let (query, arguments) = if let Some(after) = after {
        (
            format!(
                r#"SELECT "id", "entity_id", "body", "parent", "attestations"
                   FROM "{EVENT_TABLE}" WHERE "id" > ? ORDER BY "id" LIMIT ?"#
            ),
            vec![SqliteValueRef::Text(after), SqliteValueRef::Integer(PAGE_SIZE)],
        )
    } else {
        (
            format!(
                r#"SELECT "id", "entity_id", "body", "parent", "attestations"
                   FROM "{EVENT_TABLE}" ORDER BY "id" LIMIT ?"#
            ),
            vec![SqliteValueRef::Integer(PAGE_SIZE)],
        )
    };
    let rows = conn.query(&query, &arguments)?;
    let mut last = None;
    let mut page = Vec::new();
    // Each row is decoded and then dropped, so that the rows read and the
    // events decoded are not all held at once.
    for row in rows {
        let stored_id = row.text(0)?;
        let declared_id = EventId::from_base64(stored_id).map_err(|error| SqliteError::Dump(error.to_string()))?;
        let entity_id = EntityId::from_base64(row.text(1)?).map_err(|error| SqliteError::Dump(error.to_string()))?;
        let body = bincode::deserialize::<EventBody>(row.blob(2)?)?;
        let parent = serde_json::from_str::<Clock>(row.text(3)?)?;
        let attestations = bincode::deserialize::<AttestationSet>(row.blob(4)?)?;
        let event = Attested { payload: Event { entity_id, body, parent }, attestations };
        if event.payload.id() != declared_id {
            return Err(SqliteError::Dump(format!("stored event id does not match payload for {declared_id}")));
        }
        last = Some(stored_id.to_owned());
        page.push(StorageDumpItem::Event(event));
    }
    Ok((last, page))
}

fn state_page(conn: &dyn SqliteExecutor, after: Option<&str>) -> Result<(Option<String>, Vec<StorageDumpItem>), SqliteError> {
    let (query, arguments) = if let Some(after) = after {
        (
            format!(
                r#"SELECT "id", "state_buffer", "head", "attestations"
                   FROM "{ENTITY_TABLE}" WHERE "id" > ? ORDER BY "id" LIMIT ?"#
            ),
            vec![SqliteValueRef::Text(after), SqliteValueRef::Integer(PAGE_SIZE)],
        )
    } else {
        (
            format!(
                r#"SELECT "id", "state_buffer", "head", "attestations"
                   FROM "{ENTITY_TABLE}" ORDER BY "id" LIMIT ?"#
            ),
            vec![SqliteValueRef::Integer(PAGE_SIZE)],
        )
    };
    // One read snapshot for the page, so each state's memberships are read as
    // of the state itself. Main had that snapshot without asking for one: its
    // page's statement was still stepping while each membership was read, and
    // SQLite ends a read only when the connection's last statement finishes.
    // The executor reads the whole page first, so a savepoint keeps it here: it
    // begins a transaction on a connection outside one, and nests in one an
    // embedder left open, which main's page read within too. A failed read's
    // error wins over the savepoint's ROLLBACK TO and RELEASE (see `savepoint`).
    let snapshot = conn.savepoint("_ankurah_dump_page")?;
    let page = states_of_page(&snapshot, &query, &arguments)?;
    snapshot.release()?;
    Ok(page)
}

/// The states of a page, and the id of its last, read through `snapshot`.
fn states_of_page(
    snapshot: &dyn SqliteExecutor,
    query: &str,
    arguments: &[SqliteValueRef<'_>],
) -> Result<(Option<String>, Vec<StorageDumpItem>), SqliteError> {
    let rows = snapshot.query(query, arguments)?;
    let mut last = None;
    let mut page = Vec::new();
    // Each row is decoded and then dropped, as the events are.
    for row in rows {
        let stored_id = row.text(0)?;
        let entity_id = EntityId::from_base64(stored_id).map_err(|error| SqliteError::Dump(error.to_string()))?;
        let state_buffers = bincode::deserialize::<StateBuffers>(row.blob(1)?)?;
        let head = serde_json::from_str::<Clock>(row.text(2)?)?;
        let attestations = bincode::deserialize::<AttestationSet>(row.blob(3)?)?;
        let mut memberships = std::collections::BTreeSet::new();
        let keys = snapshot.query(
            &format!(r#"SELECT "model_key" FROM "{ENTITY_MODEL_TABLE}" WHERE "entity_id" = ? ORDER BY "model_key""#),
            &[SqliteValueRef::Text(stored_id)],
        )?;
        let keys = keys.iter().map(|row| row.blob(0)).collect::<Result<Vec<_>, _>>()?;
        for key in keys {
            memberships.insert(bincode::deserialize::<ModelId>(key)?);
        }
        let state = State { state_buffers, memberships, head };
        page.push(StorageDumpItem::State(Attested { payload: EntityState { entity_id, state }, attestations }));
        last = Some(stored_id.to_owned());
    }
    Ok((last, page))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use ankurah_core::storage::{StorageCommitOutcome, StorageEngine, StorageTransaction};
    use ankurah_proto::{AuthorId, OperationSet};
    use futures_util::{pin_mut, StreamExt};

    use super::*;
    use crate::test_driver::{self, engine_test};

    #[engine_test]
    async fn dump_crosses_cursor_pages_without_skipping_records() -> anyhow::Result<()> {
        const RECORDS: usize = PAGE_SIZE as usize + 1;

        let storage = test_driver::open_in_memory().await?;
        let mut expected = BTreeSet::new();
        for _ in 0..RECORDS {
            let event = Event::genesis(None, AuthorId::Unknown, OperationSet::default());
            let entity_id = event.entity_id;
            let state = Attested::opt(
                EntityState {
                    entity_id,
                    state: State { state_buffers: StateBuffers::default(), memberships: BTreeSet::new(), head: Clock::singleton(&event) },
                },
                None,
            );
            let mut transaction = storage.transaction();
            transaction.add_events(&[Attested::opt(event, None)]).await?;
            transaction.set_state(&Clock::default(), &state).await?;
            assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));
            expected.insert(entity_id);
        }

        let items = storage.dump().await?;
        pin_mut!(items);
        let mut events = BTreeSet::new();
        let mut states = BTreeSet::new();
        let mut saw_state = false;
        while let Some(item) = items.next().await {
            match item? {
                StorageDumpItem::Event(event) => {
                    assert!(!saw_state, "dump emitted an event after a state");
                    events.insert(event.payload.entity_id);
                }
                StorageDumpItem::State(state) => {
                    saw_state = true;
                    states.insert(state.payload.entity_id);
                }
            }
        }
        assert_eq!(events, expected);
        assert_eq!(states, expected);
        Ok(())
    }

    /// A dump on a connection an embedder left inside an open transaction
    /// reads its states within that transaction, as main's dump did, and
    /// leaves the transaction open for the embedder to end: each page's
    /// snapshot is a savepoint, which nests in it.
    #[engine_test]
    async fn dump_reads_within_a_transaction_left_open() -> anyhow::Result<()> {
        let storage = test_driver::open_in_memory().await?;
        let event = Event::genesis(None, AuthorId::Unknown, OperationSet::default());
        let entity_id = event.entity_id;
        let head = Clock::singleton(&event);
        let state = Attested::opt(
            EntityState { entity_id, state: State { state_buffers: StateBuffers::default(), memberships: BTreeSet::new(), head } },
            None,
        );
        let mut transaction = storage.transaction();
        transaction.add_events(&[Attested::opt(event, None)]).await?;
        transaction.set_state(&Clock::default(), &state).await?;
        assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));

        storage.with_executor(|executor| executor.execute("BEGIN", &[]).map(drop)).await?;
        let items = storage.dump().await?;
        pin_mut!(items);
        let mut states = Vec::new();
        while let Some(item) = items.next().await {
            if let StorageDumpItem::State(state) = item? {
                states.push(state.payload.entity_id);
            }
        }
        assert_eq!(states, [entity_id]);
        storage.with_executor(|executor| executor.execute("COMMIT", &[]).map(drop)).await?;
        Ok(())
    }

    /// A page reads each state's memberships as they stood when it read the
    /// state, though another connection commits a change to them in between,
    /// since the page's savepoint holds one read snapshot: here the executor
    /// the page reads through commits a membership through a second
    /// connection on the WAL file once the query of the states returns.
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn a_page_reads_its_memberships_in_the_snapshot_of_its_states() -> anyhow::Result<()> {
        use std::cell::Cell;

        use ankurah_proto::SystemModel;

        use crate::executor::Row;

        /// An executor on `reader` that runs `change` once its first query
        /// has returned.
        struct ChangesAfterTheFirstQuery<'c> {
            reader: &'c rusqlite::Connection,
            change: Cell<Option<Box<dyn FnOnce() + 'c>>>,
        }

        impl SqliteExecutor for ChangesAfterTheFirstQuery<'_> {
            fn execute(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<usize, SqliteError> {
                SqliteExecutor::execute(self.reader, sql, params)
            }

            fn query(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<Vec<Row>, SqliteError> {
                let rows = SqliteExecutor::query(self.reader, sql, params)?;
                if let Some(change) = self.change.take() {
                    change();
                }
                Ok(rows)
            }

            fn query_row(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<Option<Row>, SqliteError> {
                SqliteExecutor::query_row(self.reader, sql, params)
            }
        }

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("dump.sqlite");
        // The engine's connections put the file in WAL mode, where a writer
        // commits while a reader holds its snapshot.
        let storage = SqliteStorageEngine::open(&path).await?;
        let event = Event::genesis(None, AuthorId::Unknown, OperationSet::default());
        let entity_id = event.entity_id;
        let model = ModelId::System(SystemModel::Model);
        let state = Attested::opt(
            EntityState {
                entity_id,
                state: State { state_buffers: StateBuffers::default(), memberships: [model].into(), head: Clock::singleton(&event) },
            },
            None,
        );
        let mut transaction = storage.transaction();
        transaction.add_events(&[Attested::opt(event, None)]).await?;
        transaction.set_state(&Clock::default(), &state).await?;
        assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));

        let reader = rusqlite::Connection::open(&path)?;
        let writer = rusqlite::Connection::open(&path)?;
        let id = entity_id.to_base64();
        let joined = bincode::serialize(&ModelId::System(SystemModel::Property))?;
        let join = || {
            writer
                .execute(
                    &format!(r#"INSERT INTO "{ENTITY_MODEL_TABLE}" ("entity_id", "model_key") VALUES (?1, ?2)"#),
                    rusqlite::params![id, joined],
                )
                .expect("the second connection commits a membership");
        };
        let executor = ChangesAfterTheFirstQuery { reader: &reader, change: Cell::new(Some(Box::new(join))) };
        let (_, page) = state_page(&executor, None)?;
        let memberships: Vec<_> = page
            .into_iter()
            .map(|item| match item {
                StorageDumpItem::State(state) => state.payload.state.memberships,
                StorageDumpItem::Event(_) => unreachable!("a state page holds only states"),
            })
            .collect();
        assert_eq!(memberships, [BTreeSet::from([model])], "the page reads the memberships as of its states");
        let now: i64 = writer.query_row(&format!(r#"SELECT count(*) FROM "{ENTITY_MODEL_TABLE}""#), [], |row| row.get(0))?;
        assert_eq!(now, 2, "the second connection's membership committed");
        Ok(())
    }
}
