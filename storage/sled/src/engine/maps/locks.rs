//! The engine's locks over its maps are tokio's, which wake their waiters in
//! the order they came, so that no stream of snapshots keeps a commit from
//! its readers' lock, and no stream of writers keeps a transaction from a
//! name.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use ankurah_core::storage::LockName;
use tokio::sync::{Mutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, RwLockWriteGuard};

/// The locks every storage transaction and snapshot of one engine's maps
/// takes.
#[derive(Default)]
pub(crate) struct MapLocks {
    /// Under each name, held by the transaction that takes the name.
    writers: ByName<Mutex<()>>,

    /// Under each name, shared by the snapshots under it, and exclusive to a
    /// commit under it as it lands.
    readers: ByName<RwLock<()>>,

    /// Shared by map transactions, snapshots and native writes, exclusive to a reset.
    reset: Arc<RwLock<()>>,

    /// Held by each commit with map writes while it lands.
    turn_to_land: Arc<Mutex<()>>,
}

/// A transaction's hold on a name's writers' lock.
pub(super) type Writing = Held<Mutex<()>, OwnedMutexGuard<()>>;

/// A snapshot's shared hold on a name's readers' lock.
pub(super) type Reading = Held<RwLock<()>, OwnedRwLockReadGuard<()>>;

/// What a commit with map writes holds while it lands: the readers' lock of
/// each name it holds, exclusively, and the engine's turn to land.
pub(super) struct LandingLocks {
    _readers: Vec<Held<RwLock<()>, OwnedRwLockWriteGuard<()>>>,
    _turn: OwnedMutexGuard<()>,
}

impl MapLocks {
    /// Wait until no other transaction holds `name`'s writers' lock, then
    /// hold it.
    pub(super) async fn write_under(&self, name: &LockName) -> Writing {
        let lock = self.writers.lock_of(name);
        let guard = Arc::clone(&lock.lock).lock_owned().await;

        Held { _guard: guard, lock }
    }

    /// Wait until no commit under any of `names` is landing, then hold each
    /// name's readers' lock shared. They are taken once each, in ascending
    /// order, as a commit takes them, so that no two wait on each other.
    pub(super) async fn read_under(&self, names: &[LockName]) -> Vec<Reading> {
        let names: BTreeSet<&LockName> = names.iter().collect();

        let mut reading = Vec::with_capacity(names.len());
        for name in names {
            let lock = self.readers.lock_of(name);
            let guard = Arc::clone(&lock.lock).read_owned().await;
            reading.push(Held { _guard: guard, lock });
        }

        reading
    }

    /// Wait until no snapshot under any of the names `writing` holds is open,
    /// then hold each name's readers' lock exclusively, in the order taken,
    /// which ascends; then wait for the engine's turn to land, and hold it.
    pub(super) async fn land_under(&self, writing: &[Writing]) -> LandingLocks {
        let mut readers = Vec::with_capacity(writing.len());
        for held in writing {
            let lock = self.readers.lock_of(held.name());
            let guard = Arc::clone(&lock.lock).write_owned().await;
            readers.push(Held { _guard: guard, lock });
        }

        let turn = Arc::clone(&self.turn_to_land).lock_owned().await;
        LandingLocks { _readers: readers, _turn: turn }
    }

    /// Wait until no reset is under way, or waiting, then share the reset
    /// lock.
    pub(in crate::engine) async fn share_reset(&self) -> OwnedRwLockReadGuard<()> { Arc::clone(&self.reset).read_owned().await }

    /// The stable gate for individual query calls made by a retained state snapshot.
    pub(in crate::engine) fn reset_lock(&self) -> Arc<RwLock<()>> { self.reset.clone() }

    /// Wait until every transaction, snapshot and native write sharing the
    /// reset lock has ended, then hold it exclusively.
    pub(crate) async fn hold_reset(&self) -> RwLockWriteGuard<'_, ()> { self.reset.write().await }
}

/// One kind of lock under each name: a name that some transaction or
/// snapshot holds or waits for has its lock, and one that none does has
/// none, so that the locks number no more than the names in use.
struct ByName<L> {
    names: Names<L>,
}

/// Each name in use, with its lock: shared by every handle on one of them.
type Names<L> = Arc<std::sync::Mutex<HashMap<LockName, InUse<L>>>>;

impl<L> Default for ByName<L> {
    fn default() -> Self { Self { names: Arc::default() } }
}

/// A name's lock, and how many hold or wait for it.
struct InUse<L> {
    lock: Arc<L>,
    users: usize,
}

impl<L: Default> ByName<L> {
    /// The lock under `name`, made if no one uses it, and used until the
    /// handle is dropped.
    fn lock_of(&self, name: &LockName) -> NameLock<L> {
        let mut names = self.names.lock().unwrap();
        let in_use = names.entry(name.clone()).or_insert_with(|| InUse { lock: Arc::default(), users: 0 });
        in_use.users += 1;

        NameLock { names: Arc::clone(&self.names), name: name.clone(), lock: Arc::clone(&in_use.lock) }
    }
}

/// One user's handle on the lock under a name, which the name keeps while
/// any handle on it lives.
struct NameLock<L> {
    names: Names<L>,
    name: LockName,
    lock: Arc<L>,
}

impl<L> Drop for NameLock<L> {
    fn drop(&mut self) {
        let mut names = self.names.lock().unwrap();
        let in_use = names.get_mut(&self.name).expect("a name keeps its lock while a handle on it lives");

        in_use.users -= 1;
        if in_use.users == 0 {
            names.remove(&self.name);
        }
    }
}

/// A hold on the lock under a name, through `G`, its guard.
pub(super) struct Held<L, G> {
    /// Declared first, so that the lock is released before the handle goes:
    /// a name whose last handle went makes a new lock for its next user,
    /// which must find no guard still holding the old one.
    _guard: G,

    lock: NameLock<L>,
}

impl<L, G> Held<L, G> {
    /// The name whose lock this holds.
    fn name(&self) -> &LockName { &self.lock.name }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures::FutureExt;

    use super::*;

    /// How many names hold a lock of this kind.
    fn names_with_locks<L>(locks: &ByName<L>) -> usize { locks.names.lock().unwrap().len() }

    #[tokio::test]
    async fn a_names_locks_go_once_nothing_holds_or_waits_for_them() {
        let locks = MapLocks::default();
        let (a, b) = (LockName::new("a"), LockName::new("b"));

        let writing = locks.write_under(&a).await;
        {
            let mut waiting = pin!(locks.write_under(&a));
            assert!(waiting.as_mut().now_or_never().is_none(), "a second writer waits");
        }

        let landing_locks = locks.land_under(std::slice::from_ref(&writing)).await;
        assert_eq!((names_with_locks(&locks.writers), names_with_locks(&locks.readers)), (1, 1));

        drop(landing_locks);
        drop(writing);
        let reading = locks.read_under(&[b.clone(), a, b]).await;
        assert_eq!((names_with_locks(&locks.writers), names_with_locks(&locks.readers)), (0, 2));

        drop(reading);
        assert_eq!((names_with_locks(&locks.writers), names_with_locks(&locks.readers)), (0, 0));
    }
}
