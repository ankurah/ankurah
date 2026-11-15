//! A storage transaction's map calls: each a request on the connection the
//! transaction leases from the maps' pool at its first call and holds to its
//! end, with the native transaction begun there; the writes it holds for its
//! next request or its commit; and the end of that native transaction, its
//! `COMMIT` carrying the pending writes, or its `ROLLBACK`.

use std::sync::Arc;

use ankurah_core::error::MutationError;
use ankurah_core::storage::pending_writes::PendingWrites;
use ankurah_core::storage::{LockName, MapRead, MapRows, MapWrite};

use super::connection::{Lease, Pool};
use super::error::in_transaction;
use super::request::Request;
use super::MapError;

/// Take `locks`, then answer `reads`, in one request on the connection
/// `lease` holds: behind the native transaction's `BEGIN` at the
/// transaction's first map call, which leases the connection from `pool`,
/// and behind `pending`, the writes held for this request.
pub(crate) async fn lock_and_read(
    lease: &mut Option<Lease>,
    pending: &mut PendingWrites,
    pool: &Arc<Pool>,
    locks: &[LockName],
    reads: Vec<MapRead>,
) -> Result<Vec<MapRows>, MutationError> {
    let lease = match &mut *lease {
        Some(held) => held,
        None => lease.insert(pool.lease().await.map_err(in_transaction)?),
    };

    let mut request = Request::on(lease.connection());
    if !lease.in_transaction() {
        request.then(lease.begin(pool.begin_map_transaction()));
    }

    request.write(pending);
    request.lock(locks);
    request.read(reads);
    request.send().await.map_err(MapError::request).map_err(in_transaction)
}

/// Hold `writes` in `pending` for the transaction's next request. A first map
/// call begins the native transaction now, in a request of its own on a
/// connection leased from `pool`, so that the transaction shares the reset
/// lock once the call returns.
pub(crate) async fn write(
    lease: &mut Option<Lease>,
    pending: &mut PendingWrites,
    pool: &Arc<Pool>,
    writes: Vec<MapWrite>,
) -> Result<(), MutationError> {
    if lease.is_none() {
        lock_and_read(lease, pending, pool, &[], Vec::new()).await?;
    }

    pending.add(writes);
    Ok(())
}

/// Send `pending` and the `COMMIT`, in one request on the connection `lease`
/// holds, unless the session has already ended, landing nothing.
pub(crate) async fn commit(lease: Lease, mut pending: PendingWrites) -> Result<(), MutationError> {
    if lease.connection().client.is_closed() {
        return Err(in_transaction(MapError::SessionEnded));
    }

    let mut request = Request::on(lease.connection());
    request.write(&mut pending);
    request.then(lease.end("COMMIT"));
    request.send().await.map_err(MapError::committing).map_err(in_transaction)?;

    Ok(())
}

/// Roll the native transaction on the connection `lease` holds back, landing
/// none of its writes.
pub(crate) async fn roll_back(lease: Lease) -> Result<(), MutationError> {
    lease.end("ROLLBACK").await.map_err(MapError::database).map_err(in_transaction)
}
