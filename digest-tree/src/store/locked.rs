//! A store with no other owner: an ordered map behind the locks that give its
//! reads and transactions what a tree relies on.

use std::future::Future;
use std::ops::Bound;
use std::sync::Arc;

use tokio::sync::{Mutex, OwnedMutexGuard, OwnedRwLockReadGuard, RwLock};

use super::overlay::Overlay;
use super::{ReadTrx, Store, StoreTrx, WriteItem};
use crate::{ScanDirection, StoreError};

/// One tree's rows in key order, with a batch write that lands whole: what a
/// [`Locked`] store keeps its rows in. The map keeps no order between its
/// callers. The `Locked` around it does: it applies one batch at a time, and
/// holds every read off while it does.
pub trait OrderedMap: Send + Sync + 'static {
    /// The keys the map orders its rows by.
    type Key: Ord + Clone + Send + Sync + 'static;

    /// The rows.
    type Value: Clone + Send + Sync + 'static;

    /// The row under `key`.
    fn get(&self, key: &Self::Key) -> impl Future<Output = Result<Option<Self::Value>, StoreError>> + Send;

    /// The first `limit` rows whose keys lie between the bounds, in key order
    /// or reversed: fewer only where fewer lie between them.
    fn range(
        &self,
        start: Bound<&Self::Key>,
        end: Bound<&Self::Key>,
        direction: ScanDirection,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<(Self::Key, Self::Value)>, StoreError>> + Send;

    /// Put and delete these rows together: all of them, or none.
    fn apply(&self, batch: Vec<WriteItem<Self::Key, Self::Value>>) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// A store with no other owner: an [`OrderedMap`], and the locks that give its
/// reads and transactions what a tree relies on ([`Store`]). Clones share the
/// map and the locks.
///
/// A transaction holds the store's one writer's turn from
/// [`begin`](Self::begin) until it commits or is dropped, so transactions on
/// one store run one at a time. Its writes wait in the transaction, laid over
/// the map's rows for its own reads, until [`commit`](LockedTrx::commit)
/// applies them in one batch. Each read holds the readers' lock shared while it
/// is open, and a commit holds it exclusively only while it applies its batch:
/// a read sees the map before a commit or after it, never between, and never
/// waits for a transaction that is open but not yet committing.
///
/// Wrap each map in one `Locked`, and clone that: two `Locked`s over one map
/// keep separate locks, so neither's transactions would wait for the other's.
#[derive(Debug)]
pub struct Locked<M> {
    map: Arc<M>,

    /// Held by each transaction from `begin` until it commits or is dropped.
    writers: Arc<Mutex<()>>,

    /// Held shared by each read while it is open, and exclusively by a commit
    /// while it applies its batch.
    readers: Arc<RwLock<()>>,
}

impl<M> Locked<M> {
    /// The store over `map`, which no other `Locked` wraps.
    pub fn over(map: M) -> Self { Self { map: Arc::new(map), writers: Arc::default(), readers: Arc::default() } }
}

impl<M> Clone for Locked<M> {
    fn clone(&self) -> Self { Self { map: Arc::clone(&self.map), writers: Arc::clone(&self.writers), readers: Arc::clone(&self.readers) } }
}

impl<M: OrderedMap> Locked<M> {
    /// Begin a transaction on the store, once every transaction begun before
    /// it has committed or been dropped. It can't fail: a store with no other
    /// owner waits for nothing but its own writer's turn.
    pub async fn begin(&self) -> LockedTrx<M> {
        let turn = Arc::clone(&self.writers).lock_owned().await;
        LockedTrx { store: self.clone(), writes: Overlay::new(), _turn: turn }
    }
}

impl<M: OrderedMap> Store<M::Key, M::Value> for Locked<M> {
    type Read = LockedRead<M>;
    type Trx<'a> = &'a mut LockedTrx<M>;
    type Pending = ();

    async fn begin_read(&self) -> Result<LockedRead<M>, StoreError> {
        let reading = Arc::clone(&self.readers).read_owned().await;
        Ok(LockedRead { map: Arc::clone(&self.map), _reading: reading })
    }

    fn owns(&self, trx: &Self::Trx<'_>) -> bool { Arc::ptr_eq(&self.map, &trx.store.map) }

    /// A commit applies its batch to the map itself, so nothing waits for its
    /// landing.
    fn landed(&self, _: ()) {}
}

/// A read of a [`Locked`] store: it sees the map as the last commit before it
/// left it, and holds off the next commit's batch until it is dropped.
pub struct LockedRead<M> {
    map: Arc<M>,
    _reading: OwnedRwLockReadGuard<()>,
}

impl<M: OrderedMap> ReadTrx<M::Key, M::Value> for LockedRead<M> {
    fn get(&mut self, key: &M::Key) -> impl Future<Output = Result<Option<M::Value>, StoreError>> + Send { self.map.get(key) }

    async fn get_many(&mut self, keys: &[M::Key]) -> Result<Vec<Option<M::Value>>, StoreError> { get_each(self, keys).await }

    fn range(
        &mut self,
        start: Bound<&M::Key>,
        end: Bound<&M::Key>,
        direction: ScanDirection,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<(M::Key, M::Value)>, StoreError>> + Send {
        self.map.range(start, end, direction, limit)
    }
}

/// A transaction on a [`Locked`] store: it holds the store's writer's turn
/// until it commits or is dropped, and keeps its writes, laid over the map's
/// rows for its own reads, until it commits. Dropped uncommitted, it lands
/// nothing.
pub struct LockedTrx<M: OrderedMap> {
    store: Locked<M>,
    writes: Overlay<M::Key, M::Value>,
    _turn: OwnedMutexGuard<()>,
}

impl<M: OrderedMap> LockedTrx<M> {
    /// Apply the transaction's writes to the map in one batch, all of them or
    /// none, and end it: a failure to apply them ends it too, with nothing
    /// landed. Reads of the store wait only while the batch is applied.
    pub async fn commit(self) -> Result<(), StoreError> {
        let batch = self.writes.into_writes();
        if batch.is_empty() {
            return Ok(());
        }

        let _applying = self.store.readers.write().await;
        self.store.map.apply(batch).await
    }
}

impl<M: OrderedMap> ReadTrx<M::Key, M::Value> for LockedTrx<M> {
    async fn get(&mut self, key: &M::Key) -> Result<Option<M::Value>, StoreError> {
        self.writes.get(&mut Unlocked(&*self.store.map), key).await
    }

    async fn get_many(&mut self, keys: &[M::Key]) -> Result<Vec<Option<M::Value>>, StoreError> { get_each(self, keys).await }

    async fn range(
        &mut self,
        start: Bound<&M::Key>,
        end: Bound<&M::Key>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(M::Key, M::Value)>, StoreError> {
        self.writes.range(&mut Unlocked(&*self.store.map), start, end, direction, limit).await
    }
}

impl<M: OrderedMap> StoreTrx<M::Key, M::Value> for LockedTrx<M> {
    type Pending = ();

    async fn write(&mut self, batch: Vec<WriteItem<M::Key, M::Value>>) -> Result<(), StoreError> {
        self.writes.extend(batch);
        Ok(())
    }
}

/// A map's rows as they stand, which a [`LockedTrx`] reads beneath its own
/// writes. It takes no lock: the transaction holds the writer's turn, and only
/// the writer's commit changes the map.
struct Unlocked<'a, M>(&'a M);

impl<M: OrderedMap> ReadTrx<M::Key, M::Value> for Unlocked<'_, M> {
    fn get(&mut self, key: &M::Key) -> impl Future<Output = Result<Option<M::Value>, StoreError>> + Send { self.0.get(key) }

    async fn get_many(&mut self, keys: &[M::Key]) -> Result<Vec<Option<M::Value>>, StoreError> { get_each(self, keys).await }

    fn range(
        &mut self,
        start: Bound<&M::Key>,
        end: Bound<&M::Key>,
        direction: ScanDirection,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<(M::Key, M::Value)>, StoreError>> + Send {
        self.0.range(start, end, direction, limit)
    }
}

/// The row under each of `keys`, in order, read one at a time: an
/// [`OrderedMap`] has no read of many, and its rows are in this process, so
/// each read is a lookup rather than a round trip.
async fn get_each<K, V>(rows: &mut impl ReadTrx<K, V>, keys: &[K]) -> Result<Vec<Option<V>>, StoreError> {
    let mut found = Vec::with_capacity(keys.len());
    for key in keys {
        found.push(rows.get(key).await?);
    }

    Ok(found)
}
