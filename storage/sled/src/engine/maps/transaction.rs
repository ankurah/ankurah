//! A storage transaction's part in the maps: the locks it holds, and the
//! writes it has yet to land.

use ankurah_core::error::MutationError;
use ankurah_core::storage::pending_writes::PendingWrites;
use ankurah_core::storage::{LockName, MapRead, MapRows, MapWrite};
use tokio::sync::OwnedRwLockReadGuard;

use super::landing::MapLanding;
use super::locks::Writing;
use super::read;
use crate::SledStorageEngine;

/// One storage transaction's part in the maps: what its map calls hold
/// until it lands or ends.
#[derive(Default)]
pub(crate) struct TransactionMaps {
    /// The reset lock, shared from the first map call.
    reset: Option<OwnedRwLockReadGuard<()>>,

    /// Each name's writers' lock, in the order taken.
    writing: Vec<Writing>,

    pending: PendingWrites,
}

impl TransactionMaps {
    /// Whether the transaction made a map call.
    pub(crate) fn made_map_calls(&self) -> bool { self.reset.is_some() }

    /// Hold the writers' lock of each of `locks`, in their order, waiting for
    /// any that another transaction holds; then answer `reads` as the pending
    /// writes leave what the engine has stored, in one read of what it has
    /// stored for those the pending writes alone don't answer.
    pub(crate) async fn lock_and_read(
        &mut self,
        engine: &SledStorageEngine,
        locks: &[LockName],
        reads: Vec<MapRead>,
    ) -> Result<Vec<MapRows>, MutationError> {
        self.share_reset(engine).await;
        for name in locks {
            self.writing.push(engine.map_locks.write_under(name).await);
        }

        let (stored, overlay) = self.pending.read(reads);
        let maps = engine.database.lock().unwrap().maps_tree.clone();
        Ok(overlay.answer(read::stored(&maps, stored).await?))
    }

    /// Hold `writes` for the commit, after those before them.
    pub(crate) async fn write(&mut self, engine: &SledStorageEngine, writes: Vec<MapWrite>) {
        self.share_reset(engine).await;
        self.pending.add(writes);
    }

    /// What the transaction's commit lands in the maps, with every lock it
    /// holds and, where there are map writes, those their landing takes.
    pub(crate) async fn landing(mut self, engine: &SledStorageEngine) -> MapLanding {
        // Even an entity/event-only commit may outlive its awaiter once
        // dispatched to a blocking thread. Reset waits for that work too.
        self.share_reset(engine).await;
        let changes = self.pending.take();
        let landing_locks = match changes.is_empty() {
            true => None,
            false => Some(engine.map_locks.land_under(&self.writing).await),
        };

        MapLanding { changes, _landing_locks: landing_locks, _writing: self.writing, _reset: self.reset }
    }

    /// Share the reset lock, unless an earlier map call has.
    async fn share_reset(&mut self, engine: &SledStorageEngine) {
        if self.reset.is_none() {
            self.reset = Some(engine.map_locks.share_reset().await);
        }
    }
}
