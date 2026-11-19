//! A storage transaction's map calls, in the native transaction its first
//! call begins on a connection it leases, with the engine's writer turn, and
//! holds to its end.

use std::sync::Arc;

use ankurah_core::error::MutationError;
use ankurah_core::storage::{MapRead, MapRows, MapWrite};
use rusqlite::Connection;

use super::connections::Lease;
use super::{statements, Maps};
use crate::error::SqliteError;

/// Answer `reads`, in order, in the native transaction on the connection
/// `lease` holds.
pub(in crate::engine) async fn read(lease: &mut Option<Lease>, maps: &Maps, reads: Vec<MapRead>) -> Result<Vec<MapRows>, MutationError> {
    Ok(run(lease, maps, move |connection| reads.iter().map(|read| statements::read(connection, read)).collect()).await?)
}

/// Make `writes`, in order, in the native transaction on the connection
/// `lease` holds.
pub(in crate::engine) async fn write(lease: &mut Option<Lease>, maps: &Maps, writes: Vec<MapWrite>) -> Result<(), MutationError> {
    Ok(run(lease, maps, move |connection| writes.iter().try_for_each(|write| statements::write(connection, write))).await?)
}

/// Run `call` in the native transaction on the connection `lease` holds. The
/// transaction's first call takes the engine's writer turn, leases a
/// connection from `maps` and begins the transaction there with `BEGIN
/// IMMEDIATE`.
async fn run<T: Send + 'static>(
    lease: &mut Option<Lease>,
    maps: &Maps,
    call: impl FnOnce(&Connection) -> Result<T, SqliteError> + Send + 'static,
) -> Result<T, SqliteError> {
    let begin = lease.is_none();
    let lease = match &mut *lease {
        Some(held) => held,
        None => {
            let turn = Arc::clone(&maps.turn).lock_owned().await;
            let mut leased = maps.lease().await?;
            leased.hold(turn).await;
            lease.insert(leased)
        }
    };

    lease
        .run(move |connection| {
            if begin {
                connection.execute_batch("BEGIN IMMEDIATE")?;
            }

            call(connection)
        })
        .await
}
