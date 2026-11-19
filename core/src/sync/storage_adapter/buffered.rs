//! A managed tree's unflushed records, tied to an exact durable generation.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use web_time::Instant;

use ankurah_digest_tree::{DigestGroup, Encode, Row, RowKey, Store, StoreError, WriteItem};

use super::layout::{
    entry::{self, TreeEntry},
    log,
    names::trees_map,
};
use super::{EngineAdapter, Inner, StorageEngineSnapshot, StorageEngineTrx, TreeLanding};
use crate::error::{MutationError, RetrievalError};
use crate::storage::pending_writes::PendingWrites;
use crate::storage::{MapId, MapRead, MapRows, MapSnapshot, MapWrite, StateCursor, StorageEngine, StorageTransaction};
use crate::sync::error::{open_failure, snapshot_store_failure};
use crate::sync::Error;

#[derive(Clone)]
pub(in crate::sync) struct Buffered {
    pub(super) base: TreeEntry,
    pub(super) writes: PendingWrites,
    pub(in crate::sync) cursor: Option<StateCursor>,
    pub(in crate::sync) epoch: u64,
    pub(in crate::sync) changes: usize,
    pub(in crate::sync) dirty_since: Option<Instant>,
}

impl Buffered {
    pub(in crate::sync) fn has_writes(&self) -> bool { !self.writes.is_empty() }

    pub(in crate::sync) fn is_building(&self) -> bool { self.base.status == entry::Status::Building }

    fn empty(base: TreeEntry, cursor: Option<StateCursor>, epoch: u64) -> Self {
        Self { base, writes: PendingWrites::default(), cursor, epoch, changes: 0, dirty_since: None }
    }

    fn members(&self) -> MapId { self.base.incarnation.members() }
}

impl<SE, K, L, G> EngineAdapter<SE, K, L, G>
where
    SE: StorageEngine + 'static,
    K: Encode + Clone + Send + Sync + 'static,
    L: Encode + Clone + Send + Sync + 'static,
    G: DigestGroup,
{
    /// Reload the durable base after another manager flushed it. Existing views
    /// keep this adapter; only its current buffered version is replaced.
    pub(in crate::sync) async fn load_buffered(&self, publication: Arc<AtomicU64>) -> Result<(), RetrievalError> {
        let mut read = self.inner.storage.map_snapshot(&[self.inner.lock.clone()]).await?;
        let found = read.map_read(vec![entry::read([&self.inner.id])]).await?;
        // The manager has just opened or rebuilt this tree. Losing that base
        // before loading it is competing maintenance, not a permanent failure.
        let base = entry::found_one(&self.inner.id, found)?
            .filter(|base| base.status != entry::Status::Invalid)
            .ok_or(MutationError::WriteConflict)?;
        // Loading a durable base does not establish that it has caught up.
        let mut buffered = Buffered::empty(base, None, publication.load(Ordering::Acquire).wrapping_sub(1));
        let answers = read.map_read(vec![MapRead::Get { map: buffered.members(), keys: vec![Vec::new()] }]).await?;
        let [MapRows::Got(values)]: [_; 1] = answers.try_into().map_err(|_| Error::AnswerMisfit)? else {
            return Err(Error::AnswerMisfit.into());
        };
        let [cursor]: [_; 1] = values.try_into().map_err(|_| Error::AnswerMisfit)?;
        buffered.cursor = cursor.map(|bytes| bincode::deserialize(&bytes)).transpose()?;
        *self.inner.publication.lock().unwrap() = Some(publication);
        *self.inner.buffered.lock().unwrap() = Some(Arc::new(buffered));
        Ok(())
    }

    pub(in crate::sync) fn buffered(&self) -> Arc<Buffered> {
        self.inner.buffered.lock().unwrap().as_ref().expect("a managed tree has a buffered version").clone()
    }

    /// A private working copy. It reads the same durable base but publishes no
    /// partial scan to callers, and is not subject to the owner's dirty flag.
    pub(in crate::sync) fn working(&self) -> Self {
        let inner = Inner::new(self.inner.storage.clone(), self.inner.cache.clone(), self.inner.id.clone(), self.inner.incarnation);
        *inner.buffered.lock().unwrap() = Some(self.buffered());
        Self::new(inner)
    }

    pub(in crate::sync) fn publish(&self, version: Arc<Buffered>) { *self.inner.buffered.lock().unwrap() = Some(version); }

    pub(in crate::sync) fn advance(&self, cursor: StateCursor, epoch: u64, changes: usize) {
        let mut buffered = self.inner.buffered.lock().unwrap();
        let buffered = Arc::make_mut(buffered.as_mut().expect("managed store"));
        if changes > 0 || buffered.cursor.is_none() {
            buffered.dirty_since.get_or_insert_with(Instant::now);
        }
        buffered.cursor = Some(cursor);
        buffered.epoch = epoch;
        buffered.changes = buffered.changes.saturating_add(changes);
    }

    pub(in crate::sync) fn clear_buffered(&self) {
        let mut buffered = self.inner.buffered.lock().unwrap();
        let buffered = Arc::make_mut(buffered.as_mut().expect("managed store"));
        buffered.writes.add(vec![MapWrite::Clear { map: buffered.base.incarnation.rows() }, MapWrite::Clear { map: buffered.members() }]);
        buffered.cursor = None;
    }

    pub(super) async fn read_buffered(&self, buffered: Arc<Buffered>) -> Result<StorageEngineSnapshot<SE, K, L, G>, StoreError> {
        if self.inner.publication.lock().unwrap().as_ref().is_some_and(|epoch| epoch.load(Ordering::Acquire) != buffered.epoch) {
            return Err(snapshot_store_failure(MutationError::WriteConflict.into()));
        }
        let rows = buffered.base.incarnation.rows();
        let cached = self.inner.cache.generation(&rows);
        let mut snapshot = self.inner.storage.map_snapshot(&[self.inner.lock.clone()]).await.map_err(snapshot_store_failure)?;
        let mut reads = vec![entry::read([&self.inner.id])];
        reads.extend(cached.map(|from| log::read_after(buffered.base.incarnation.log(), from)));
        let mut answers = snapshot.map_read(reads).await.map_err(snapshot_store_failure)?.into_iter();
        let found = entry::found(&[&self.inner.id], answers.next())
            .map_err(StoreError::permanent)?
            .pop()
            .expect("one entry was requested")
            .map_err(StoreError::permanent)?;
        if found != Some(buffered.base) {
            return Err(snapshot_store_failure(MutationError::WriteConflict.into()));
        }
        if let Some(from) = cached.filter(|from| *from != buffered.base.generation) {
            let to = buffered.base.generation;
            let changed = match answers.next() {
                Some(MapRows::Ranged(entries)) if from < to => log::changes(&self.inner.id, from, to, entries),
                Some(MapRows::Ranged(_)) => None,
                _ => return Err(StoreError::permanent(Error::AnswerMisfit)),
            };
            match changed {
                Some(changed) => self.inner.cache.catch_up(&rows, from, to, changed),
                None => self.inner.cache.forget(&rows, from),
            }
        }
        let mut read = StorageEngineSnapshot::new(self.clone(), snapshot, buffered.base.generation);
        read.buffered = Some(buffered);
        Ok(read)
    }

    pub(in crate::sync) async fn buffered_transaction(&self) -> Result<StorageEngineTrx<'_, SE, K, L, G>, StoreError> {
        let read = self.begin_read().await?;
        Ok(StorageEngineTrx::buffered(self.clone(), read))
    }

    /// The owner's member lookup shares the tree's buffered and durable version.
    pub(in crate::sync) async fn member_rows(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, RetrievalError> {
        let mut snapshot = self.begin_read().await.map_err(|error| open_failure(error.into()))?;
        let read = MapRead::Get { map: snapshot.buffered.as_ref().expect("managed read").members(), keys: keys.to_vec() };
        let answers = snapshot.map_read(vec![read]).await.map_err(|error| open_failure(error.into()))?;
        let [MapRows::Got(values)]: [_; 1] = answers.try_into().map_err(|_| Error::AnswerMisfit)? else {
            return Err(Error::AnswerMisfit.into());
        };
        if values.len() != keys.len() {
            return Err(Error::AnswerMisfit.into());
        }
        Ok(values)
    }

    pub(in crate::sync) fn set_member_rows(&self, entries: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        let mut buffered = self.inner.buffered.lock().unwrap();
        let buffered = Arc::make_mut(buffered.as_mut().expect("managed store"));
        let map = buffered.members();
        let writes = entries
            .into_iter()
            .map(|(key, value)| match value {
                Some(value) => MapWrite::Put { map: map.clone(), key, value },
                None => MapWrite::Delete { map: map.clone(), key },
            })
            .collect();
        buffered.writes.add(writes);
    }

    /// Persist precisely this buffered version. The named lock protects the
    /// comparison and write together; a newer durable base requires reconciliation.
    pub(in crate::sync) async fn flush(&self) -> Result<(), RetrievalError> {
        let version = self.buffered();
        let mut trx = self.inner.storage.transaction();
        let found = trx.map_lock_and_read(&[self.inner.lock.clone()], vec![entry::read([&self.inner.id])]).await?;
        if entry::found_one(&self.inner.id, found)? != Some(version.base) {
            return Err(MutationError::WriteConflict.into());
        }
        let generation =
            version.base.generation.checked_add(1).ok_or_else(|| RetrievalError::InvalidState("tree generation exhausted".into()))?;
        // Initial builds persist resumable batches. Only manager-owned adapters
        // may read them; a complete cursor makes the tree available to ordinary
        // openers. Another builder continues these rows instead of erasing them.
        let base = TreeEntry {
            generation,
            status: if version.cursor.is_some() { entry::Status::Ready } else { entry::Status::Building },
            ..version.base
        };
        let mut pending = version.writes.clone();
        let mut writes = Vec::new();
        let mut changed = Vec::new();
        let rows = version.base.incarnation.rows();
        let mut cleared = false;
        for (map, changes) in pending.take() {
            if changes.cleared {
                writes.push(MapWrite::Clear { map: map.clone() });
                cleared |= map == rows;
            }
            for (key, value) in changes.keys {
                if map == rows && super::cache::admits(&RowKey::decode(&key).map_err(|error| open_failure(error.into()))?) {
                    changed.push((key.clone(), value.clone()));
                }
                writes.push(match value {
                    Some(value) => MapWrite::Put { map: map.clone(), key, value },
                    None => MapWrite::Delete { map: map.clone(), key },
                });
            }
        }
        if let Some(cursor) = &version.cursor {
            writes.push(MapWrite::Put { map: version.members(), key: Vec::new(), value: bincode::serialize(cursor)? });
        }
        writes.push(MapWrite::Put { map: trees_map(), key: self.inner.id.entry_key(), value: base.encode() });
        writes.push(MapWrite::Put {
            map: version.base.incarnation.log(),
            key: log::key(generation),
            value: if cleared { log::whole_tree() } else { log::entry(&changed) },
        });
        if let Some(unreached) = generation.checked_sub(log::LENGTH) {
            writes.push(MapWrite::Delete { map: version.base.incarnation.log(), key: log::key(unreached) });
        }
        trx.map_write(writes).await?;
        trx.commit().await?.committed()?;
        if cleared {
            self.inner.cache.forget(&rows, version.base.generation);
        }
        self.inner.cache.land(&rows, generation, changed);
        // The manager serializes flush with publication. A cancelled/uncertain
        // commit leaves the old base, so the next read detects and reconciles it.
        let clean = Buffered::empty(base, version.cursor.clone(), version.epoch);
        *self.inner.buffered.lock().unwrap() = Some(Arc::new(clean));
        Ok(())
    }
}

impl TreeLanding {
    pub(super) fn buffered(writes: Vec<MapWrite>) -> Self { Self { written: None, buffered: writes } }
}

pub(super) fn encode_batch<K: Encode, L: Encode, G: DigestGroup>(map: MapId, batch: Vec<WriteItem<RowKey, Row<K, L, G>>>) -> Vec<MapWrite> {
    batch
        .into_iter()
        .map(|item| match item {
            WriteItem::Put(key, value) => MapWrite::Put { map: map.clone(), key: key.encode(), value: value.encode() },
            WriteItem::Delete(key) => MapWrite::Delete { map: map.clone(), key: key.encode() },
        })
        .collect()
}
