//! What a storage transaction's commit lands in the maps: the entries it
//! writes in the maps' sled tree, and the locks it holds until they have
//! landed.

use std::collections::BTreeMap;

use ankurah_core::storage::pending_writes::MapChanges;
use ankurah_core::storage::MapId;
use sled::transaction::{TransactionalTree, UnabortableTransactionError};
use tokio::sync::OwnedRwLockReadGuard;

use super::keys::MapPrefix;
use super::locks::{LandingLocks, Writing};

/// A commit's map writes, with every lock its transaction holds.
///
/// The locks go wherever the commit's sled transaction runs, and are
/// released only once it has landed or failed, even if the commit's caller
/// has gone: released sooner, they would let another transaction read under
/// a name while this one's writes under it are still landing.
pub(crate) struct MapLanding {
    /// Each map's pending writes.
    pub(super) changes: BTreeMap<MapId, MapChanges>,

    /// Held only where there are map writes.
    pub(super) _landing_locks: Option<LandingLocks>,

    pub(super) _writing: Vec<Writing>,
    pub(super) _reset: Option<OwnedRwLockReadGuard<()>>,
}

impl MapLanding {
    /// Whether the commit writes nothing in the maps.
    pub(crate) fn is_empty(&self) -> bool { self.changes.is_empty() }

    /// The entries the commit writes in `maps`, the maps' sled tree, taking
    /// its map writes: `None` where it has none. It blocks on sled.
    ///
    /// The commit's turn to land keeps the entries it lists for a clear from
    /// changing.
    pub(crate) fn entries(&mut self, maps: &sled::Tree) -> Result<Option<MapEntries>, sled::Error> {
        if self.changes.is_empty() {
            return Ok(None);
        }

        let mut entries = BTreeMap::new();
        for (map, changes) in std::mem::take(&mut self.changes) {
            let prefix = MapPrefix::of(&map);
            if changes.cleared {
                for entry in maps.scan_prefix(prefix.as_bytes()).keys() {
                    entries.insert(entry?.to_vec(), None);
                }
            }

            for (key, value) in changes.keys {
                entries.insert(prefix.entry(&key), value);
            }
        }

        Ok(Some(MapEntries(entries)))
    }
}

/// The entries a commit writes in the maps' sled tree, by their sled keys:
/// `Some` puts the value, `None` removes the entry.
pub(crate) struct MapEntries(BTreeMap<Vec<u8>, Option<Vec<u8>>>);

impl MapEntries {
    /// Write each entry in `maps`, the maps' sled tree within the commit's
    /// sled transaction.
    pub(crate) fn land(&self, maps: &TransactionalTree) -> Result<(), UnabortableTransactionError> {
        for (entry, value) in &self.0 {
            match value {
                Some(value) => maps.insert(entry.as_slice(), value.as_slice())?,
                None => maps.remove(entry.as_slice())?,
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use ankurah_core::storage::{LockName, MapId, MapRead, MapRows, MapSnapshot, MapWrite, StorageEngine, StorageTransaction};
    use ankurah_core::test_helpers::map_conformance::fixtures::{entity, HANG};
    use ankurah_proto::Clock;
    use futures::FutureExt;
    use tokio::time::timeout;

    use crate::SledStorageEngine;

    #[tokio::test]
    async fn a_commit_with_map_writes_dropped_while_another_holds_the_turn_to_land_lands_nothing() -> anyhow::Result<()> {
        let engine = SledStorageEngine::new_test()?;
        let (name, map) = (LockName::new("a"), MapId::new("a"));

        let mut trx = engine.transaction();
        trx.map_lock_and_read(std::slice::from_ref(&name), vec![]).await?;
        trx.map_write(vec![MapWrite::Put { map: map.clone(), key: b"k".to_vec(), value: b"v".to_vec() }]).await?;

        let turn = engine.map_locks.land_under(&[]).await;
        {
            let mut committing = pin!(trx.commit());
            assert!(committing.as_mut().now_or_never().is_none(), "the commit waits for its turn to land");
        }

        drop(turn);
        // A landing already under way would hold the name until it had landed.
        let _writing = engine.map_locks.write_under(&name).await;

        let read = MapRead::Get { map, keys: vec![b"k".to_vec()] };
        assert_eq!(engine.map_snapshot(&[]).await?.map_read(vec![read]).await?, vec![MapRows::Got(vec![None])]);
        Ok(())
    }

    /// Entity-only commits share reset protection but take no map landing turn.
    #[tokio::test]
    async fn a_commit_without_map_calls_waits_for_reset_but_not_the_map_landing_turn() -> anyhow::Result<()> {
        let engine = SledStorageEngine::new_test()?;
        let (state, event) = entity!(1);
        let _turn = engine.map_locks.land_under(&[]).await;
        let reset = engine.map_locks.hold_reset().await;

        let mut trx = engine.transaction();
        trx.add_events(&[event]).await?;
        trx.set_state(&Clock::default(), &state).await?;
        let mut commit = pin!(trx.commit());
        assert!(commit.as_mut().now_or_never().is_none(), "the commit waits for reset");
        drop(reset);
        timeout(HANG, commit).await??.committed()?;
        Ok(())
    }
}
