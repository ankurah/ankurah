//! A storage transaction's part in the maps: the names it holds in the
//! engine, what it noted of the database's state, its readonly transactions,
//! and the writes it holds until its commit.

use ankurah_core::error::MutationError;
use ankurah_core::storage::pending_writes::PendingWrites;
use ankurah_core::storage::{HeldLocks, LockName, LockTable, MapRead, MapRows, MapWrite};

use super::reading::Reading;
use super::MapLanding;
use crate::database::Database;

/// One storage transaction's part in the maps: what its map calls hold
/// until it lands or ends.
pub(in crate::engine) struct TransactionMaps<'e> {
    database: &'e Database,
    held: HeldLocks<'e>,
    reading: Reading,
    pending: PendingWrites,
}

impl<'e> TransactionMaps<'e> {
    /// No map calls yet, on `database`, whose names the engine's `locks`
    /// keep.
    pub(in crate::engine) fn new(database: &'e Database, locks: &'e LockTable) -> Self {
        Self { database, held: HeldLocks::of(locks), reading: Reading::default(), pending: PendingWrites::default() }
    }

    /// Whether the transaction made a map call, which noted the database's
    /// reset epoch.
    pub(in crate::engine) fn made_map_calls(&self) -> bool { self.reading.has_noted() }

    /// Hold `locks` in the engine, each once no other transaction holds it,
    /// then note their versions and answer `reads` as the pending writes
    /// leave what the database holds, every read of it in one readonly
    /// transaction.
    pub(in crate::engine) async fn lock_and_read(
        &mut self,
        locks: &[LockName],
        reads: Vec<MapRead>,
    ) -> Result<Vec<MapRows>, MutationError> {
        for name in locks {
            self.held.take(name).await;
        }

        let (stored, overlay) = self.pending.read(reads);
        Ok(overlay.answer(self.reading.read(self.database, locks, &stored).await?))
    }

    /// Hold `writes` until the commit. A first map call notes the reset
    /// epoch, so that a reset before the commit fails it.
    pub(in crate::engine) async fn write(&mut self, writes: Vec<MapWrite>) -> Result<(), MutationError> {
        if !self.made_map_calls() {
            self.reading.read(self.database, &[], &[]).await?;
        }

        self.pending.add(writes);
        Ok(())
    }

    /// What the commit lands, if the transaction made map calls.
    pub(in crate::engine) fn landing(self) -> Option<MapLanding<'e>> {
        let noted = self.reading.into_noted()?;
        Some(MapLanding { noted, pending: self.pending, _held: self.held })
    }
}
