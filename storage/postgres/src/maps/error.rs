//! What goes wrong in the engine's map calls, and what each failure is to
//! core.

use std::time::Duration;

use ankurah_core::error::{MutationError, RetrievalError, StateError};
use tokio_postgres::error::SqlState;

use super::request::Failure;

/// A failure of one of the engine's map calls, carried as the source of
/// core's [`MutationError`] or [`RetrievalError`].
#[derive(Debug, thiserror::Error)]
pub enum MapError {
    /// The database refused a statement.
    #[error("the database refused a map statement")]
    Statement(#[source] tokio_postgres::Error),

    /// The database couldn't be reached, or ended the session: the connection
    /// failed, the server shut down or ran out of resources, or the session
    /// sat idle in its transaction past the engine's idle limit.
    #[error("the database is unavailable for map calls")]
    Unavailable(#[source] tokio_postgres::Error),

    /// A lock wasn't granted within the engine's lock timeout: another
    /// transaction held it longer.
    #[error("a lock wasn't granted within the lock timeout")]
    LockTimeout(#[source] tokio_postgres::Error),

    /// The transaction's session ended before its `COMMIT` was sent, as when
    /// it sat idle past the engine's idle limit: nothing of it landed.
    #[error("the transaction's session ended before its COMMIT was sent, so nothing of it landed")]
    SessionEnded,

    /// A statement ahead of the transaction's `COMMIT` failed before it was
    /// sent, as one whose parameter doesn't encode does, so the `COMMIT`
    /// wasn't sent either: nothing of the transaction landed.
    #[error("a statement ahead of the COMMIT failed before it was sent, so the COMMIT wasn't sent and nothing of the transaction landed")]
    CommitUnsent(#[source] tokio_postgres::Error),

    /// The connection failed once the transaction's `COMMIT` was sent: the
    /// transaction may have landed or not, and the engine can't tell which.
    #[error("the connection failed once the COMMIT was sent, so the transaction may or may not have landed")]
    OutcomeUnknown(#[source] tokio_postgres::Error),

    /// No connection for map calls came free or opened within the connection
    /// timeout.
    #[error("no connection for map calls came free or opened within {waited:?}")]
    ConnectionTimeout { waited: Duration },

    /// A snapshot found every one of the engine's connections for map calls
    /// in use for longer than the snapshot pool timeout.
    #[error("all {most} of the engine's connections for map calls stayed in use")]
    ConnectionsInUse { most: usize },

    /// The table that holds the maps couldn't be created.
    #[error("the table that holds the maps couldn't be created")]
    Table(#[source] StateError),
}

impl MapError {
    /// A failure of `source` in a map call: a lock timeout; the database
    /// unavailable, for a connection that failed or a session the server
    /// ended; or else a statement the database refused.
    pub(crate) fn database(source: tokio_postgres::Error) -> Self {
        match source.code() {
            Some(code) if *code == SqlState::LOCK_NOT_AVAILABLE => Self::LockTimeout(source),
            Some(code) if !ends_the_session(code) => Self::Statement(source),
            _ => Self::Unavailable(source),
        }
    }

    /// The failure of a request whose steps don't end the transaction, as
    /// [`database`](Self::database) has it, whether its failing step was
    /// sent or not: nothing lands before the transaction's commit either way.
    pub(super) fn request(failure: Failure) -> Self {
        match failure {
            Failure::Unsent(source) | Failure::Sent(source) => Self::database(source),
        }
    }

    /// The failure of a request that ends with the transaction's `COMMIT`: a
    /// statement that failed before it was sent, so that the `COMMIT`
    /// wasn't sent either; a statement the database refused, so the
    /// transaction rolled back; or else the connection failed, or the server
    /// ended the session, which may have come after the `COMMIT` ran, and the
    /// outcome is unknown.
    pub(super) fn committing(failure: Failure) -> Self {
        let source = match failure {
            Failure::Unsent(source) => return Self::CommitUnsent(source),
            Failure::Sent(source) => source,
        };

        match source.code() {
            Some(code) if !ends_the_session(code) => Self::database(source),
            _ => Self::OutcomeUnknown(source),
        }
    }
}

/// Whether the error with SQLSTATE `code` ends a session or keeps one from
/// starting: a connection failing, the server out of resources or shutting
/// down, or a session ended for sitting idle in its transaction.
fn ends_the_session(code: &SqlState) -> bool { ["08", "53", "57P", "25P03"].iter().any(|ending| code.code().starts_with(ending)) }

/// A storage transaction's failed map call, as core's error: a deadlock that
/// PostgreSQL detected, which two transactions whose lock names' keys collide
/// can meet, is a write conflict to retry, and every other failure is the
/// engine's own.
pub(crate) fn in_transaction(error: MapError) -> MutationError {
    match &error {
        MapError::Statement(source) if source.code() == Some(&SqlState::T_R_DEADLOCK_DETECTED) => MutationError::WriteConflict,
        _ => MutationError::General(Box::new(error)),
    }
}

impl From<MapError> for RetrievalError {
    fn from(error: MapError) -> Self { RetrievalError::storage(error) }
}
