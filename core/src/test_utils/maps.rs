//! The test engine's ordered maps: the committed maps, which snapshots share,
//! and each transaction's part in them, the names it holds and its pending
//! writes, laid over the committed maps until it commits.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{MutationError, RetrievalError};
use crate::storage::pending_writes::{holds_nothing, MapChanges, PendingWrites};
use crate::storage::{HeldLocks, LockName, LockTable, MapId, MapRead, MapRows, MapSnapshot, MapWrite, ScanDirection};

use super::{TestData, TestStorage};

/// Every map's committed entries. A map that holds none is absent.
pub(super) type Maps = BTreeMap<MapId, Entries>;

/// One map's entries, by key.
type Entries = BTreeMap<Vec<u8>, Vec<u8>>;

/// The map reads answered while the storage records them, which its
/// snapshots share: `None` while it doesn't.
pub(super) type MapReads = Arc<Mutex<Option<Vec<MapRead>>>>;

/// Note `reads` among those answered, if the storage records them.
pub(super) fn record(recorded: &MapReads, reads: &[MapRead]) {
    if let Some(recorded) = recorded.lock().unwrap().as_mut() {
        recorded.extend_from_slice(reads);
    }
}

/// One storage transaction's part in the maps: the names it holds, its
/// writes, and the resets it has seen.
pub(super) struct TransactionMaps<'a> {
    locks: HeldLocks<'a>,
    writes: PendingWrites,

    /// The resets the storage had seen at this transaction's first map call:
    /// a reset since fails its later map calls and its commit.
    resets_seen: Option<u64>,
}

impl<'a> TransactionMaps<'a> {
    pub(super) fn new(locks: &'a LockTable) -> Self {
        Self { locks: HeldLocks::of(locks), writes: PendingWrites::default(), resets_seen: None }
    }

    /// Whether the transaction made a map call, which noted the resets.
    pub(super) fn made_map_calls(&self) -> bool { self.resets_seen.is_some() }

    pub(super) async fn lock_and_read(
        &mut self,
        storage: &TestStorage,
        locks: &[LockName],
        reads: Vec<MapRead>,
    ) -> Result<Vec<MapRows>, MutationError> {
        self.admit(&storage.data.lock().unwrap())?;

        for name in locks {
            self.locks.take(name).await;
        }

        let data = storage.data.lock().unwrap();
        self.admit(&data)?;

        let (stored, overlay) = self.writes.read(reads);
        Ok(overlay.answer(stored.into_iter().map(|read| committed(&data.maps, read)).collect()))
    }

    /// Lay `writes` over the committed maps, meeting the storage's map write
    /// hooks once the first is laid.
    pub(super) async fn write(&mut self, storage: &TestStorage, writes: Vec<MapWrite>) -> Result<(), MutationError> {
        self.admit(&storage.data.lock().unwrap())?;

        for (index, write) in writes.into_iter().enumerate() {
            self.writes.add(vec![write]);
            if index == 0 {
                storage.after_first_map_write().await?;
            }
        }

        Ok(())
    }

    /// Fail a map call or a commit on a transaction that a reset overtook
    /// after its first map call.
    pub(super) fn check(&self, data: &TestData) -> Result<(), MutationError> {
        match self.resets_seen {
            Some(seen) if seen != data.resets => Err(MutationError::WriteConflict),
            _ => Ok(()),
        }
    }

    /// Land this transaction's writes in the committed maps.
    pub(super) fn land(&mut self, maps: &mut Arc<Maps>) { land(maps, self.writes.take()) }

    /// Check a map call, noting the resets at the first.
    fn admit(&mut self, data: &TestData) -> Result<(), MutationError> {
        self.check(data)?;
        self.resets_seen.get_or_insert(data.resets);
        Ok(())
    }
}

impl TestStorage {
    /// The hook every map call of a transaction meets before it is answered:
    /// the pause of the call it counts down to. It is met before
    /// [`transaction_call`](crate::storage::transaction_call) admits the call, so a call
    /// dropped here poisons nothing itself: what dropped it must, as the
    /// storage adapter's guard does.
    pub(super) async fn before_map_call(&self) {
        let hold = {
            let mut hold = self.hold_map_call.lock().unwrap();
            match hold.as_mut() {
                Some((0, _)) => hold.take().map(|(_, pause)| pause),
                Some((calls, _)) => {
                    *calls -= 1;
                    None
                }
                None => None,
            }
        };

        if let Some((entered, release)) = hold {
            entered.send(()).unwrap();
            release.await.unwrap();
        }
    }

    /// The hooks a map write meets once its first write is laid: a pause,
    /// then a failure.
    async fn after_first_map_write(&self) -> Result<(), MutationError> {
        let hold = self.hold_map_write.lock().unwrap().take();
        if let Some((entered, release)) = hold {
            entered.send(()).unwrap();
            release.await.unwrap();
        }

        if self.fail_next_map_write.swap(false, Ordering::SeqCst) {
            return Err(MutationError::General("test storage map write failed".into()));
        }

        Ok(())
    }
}

/// A read of the maps as one commit left them: the committed maps when it
/// began, which a later commit replaces rather than changes.
pub(crate) struct TestMapSnapshot {
    pub(super) maps: Arc<Maps>,

    /// The storage's switch that fails the next snapshot read.
    pub(super) fail_next_read: Arc<AtomicBool>,

    /// The storage's record of the map reads answered.
    pub(super) reads: MapReads,

    pub(super) _open: OpenSnapshot,
}

/// A snapshot counted among those open on the storage, until it is dropped.
pub(super) struct OpenSnapshot {
    open: Arc<AtomicUsize>,
}

impl OpenSnapshot {
    /// Count a snapshot beginning among the `open` ones: it, and how many
    /// are open with it.
    pub(super) fn begin(open: &Arc<AtomicUsize>) -> (Self, usize) {
        let now_open = open.fetch_add(1, Ordering::SeqCst) + 1;
        (Self { open: Arc::clone(open) }, now_open)
    }
}

impl Drop for OpenSnapshot {
    fn drop(&mut self) { self.open.fetch_sub(1, Ordering::SeqCst); }
}

#[async_trait::async_trait]
impl MapSnapshot for TestMapSnapshot {
    async fn map_read(&mut self, reads: Vec<MapRead>) -> Result<Vec<MapRows>, RetrievalError> {
        if self.fail_next_read.swap(false, Ordering::SeqCst) {
            return Err(MutationError::WriteConflict.into());
        }

        record(&self.reads, &reads);
        Ok(reads.into_iter().map(|read| committed(&self.maps, read)).collect())
    }
}

/// The answer to `read` from the committed `maps` alone.
fn committed(maps: &Maps, read: MapRead) -> MapRows {
    match read {
        MapRead::Get { map, keys } => {
            let entries = maps.get(&map);
            MapRows::Got(keys.iter().map(|key| entries.and_then(|entries| entries.get(key)).cloned()).collect())
        }

        MapRead::Range { map, start, end, direction, limit } => {
            if holds_nothing(start.as_ref(), end.as_ref()) {
                return MapRows::Ranged(Vec::new());
            }

            let within = maps.get(&map).into_iter().flat_map(|entries| entries.range::<Vec<u8>, _>((start.as_ref(), end.as_ref())));
            let owned = |(key, value): (&Vec<u8>, &Vec<u8>)| (key.clone(), value.clone());

            MapRows::Ranged(match direction {
                ScanDirection::Forward => within.take(limit).map(owned).collect(),
                ScanDirection::Reverse => within.rev().take(limit).map(owned).collect(),
            })
        }
    }
}

/// Apply `changes`, a transaction's pending writes, to the committed `maps`,
/// replacing them rather than changing them while a snapshot holds them.
fn land(maps: &mut Arc<Maps>, changes: BTreeMap<MapId, MapChanges>) {
    if changes.is_empty() {
        return;
    }

    let maps = Arc::make_mut(maps);
    for (map, changes) in changes {
        let mut entries = maps.remove(&map).filter(|_| !changes.cleared).unwrap_or_default();

        for (key, value) in changes.keys {
            match value {
                Some(value) => entries.insert(key, value),
                None => entries.remove(&key),
            };
        }

        if !entries.is_empty() {
            maps.insert(map, entries);
        }
    }
}
