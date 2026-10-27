//! The disk-backed reference store, over raw sled.

mod cache;

use std::ops::Bound;
use std::sync::{Arc, Mutex, MutexGuard};

use self::cache::Cache;
use super::{Encode, RowKey, Store, WriteItem};
use crate::span::holds_nothing;
use crate::{ScanDirection, StoreError};

/// How many rows a store keeps while recently used, unless it is told
/// otherwise: a few megabytes of leaves in the larger digest group.
const RECENT_CAPACITY: usize = 4096;

/// A reference store that keeps one tree's rows in a sled tree on disk, and
/// keeps rows in memory as their keys' [`Residency`](super::Residency) asks:
/// some always, the rest while recently used. Clones share the rows and the
/// cache.
///
/// Keys cross to sled as their [`Encode`] bytes, which sort as the keys do,
/// and rows as theirs. sled's calls are synchronous and run inline, blocking
/// the task that awaits them; that suits a reference store. An I/O failure is
/// retryable; bytes that don't decode, and sled's other failures, are
/// permanent.
pub struct SledStore<V> {
    tree: sled::Tree,
    cache: Arc<Mutex<Cache<V>>>,
}

impl<V: Clone> SledStore<V> {
    /// A store over `tree`, keeping a few thousand rows in memory while
    /// recently used. `tree` must hold this digest tree's rows and no others,
    /// and only this store may write it: the rows kept in memory change only
    /// with writes made through the store.
    pub fn new(tree: sled::Tree) -> Self { Self::with_recent_capacity(tree, RECENT_CAPACITY) }

    /// A store over `tree` that keeps at most `capacity` rows in memory while
    /// recently used, besides the rows it keeps always.
    pub fn with_recent_capacity(tree: sled::Tree, capacity: usize) -> Self {
        Self { tree, cache: Arc::new(Mutex::new(Cache::new(capacity))) }
    }

    fn cache(&self) -> Result<MutexGuard<'_, Cache<V>>, StoreError> {
        self.cache.lock().map_err(|_| StoreError::permanent("a panic while the sled store's cache was locked left it undefined"))
    }
}

impl<V> Clone for SledStore<V> {
    fn clone(&self) -> Self { Self { tree: self.tree.clone(), cache: Arc::clone(&self.cache) } }
}

impl<V> Store<RowKey, V> for SledStore<V>
where V: Encode + Clone + Send + Sync + 'static
{
    async fn get(&self, key: &RowKey) -> Result<Option<V>, StoreError> {
        let writes = {
            let mut cache = self.cache()?;
            if let Some(row) = cache.get(key) {
                return Ok(Some(row));
            }

            cache.writes()
        };

        let Some(bytes) = self.tree.get(key.encode()).map_err(failure)? else { return Ok(None) };
        let row = V::decode(&bytes)?;
        self.cache()?.fill(writes, [(key.clone(), row.clone())]);

        Ok(Some(row))
    }

    /// The rows the cache holds come from memory rather than being decoded
    /// again, as long as no write landed during the scan; the rest are
    /// decoded from what sled returned.
    async fn range(
        &self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, V)>, StoreError> {
        let writes = self.cache()?.writes();
        let (start, end) = (start.map(Encode::encode), end.map(Encode::encode));

        // sled documents no behavior for bounds that hold nothing.
        if holds_nothing(start.as_ref(), end.as_ref()) {
            return Ok(Vec::new());
        }

        let entries = self.tree.range::<Vec<u8>, _>((start, end));
        let entries: Vec<(sled::IVec, sled::IVec)> = match direction {
            ScanDirection::Forward => entries.take(limit).collect::<Result<_, _>>(),
            ScanDirection::Reverse => entries.rev().take(limit).collect::<Result<_, _>>(),
        }
        .map_err(failure)?;

        let keys = entries.iter().map(|(key, _)| RowKey::decode(key)).collect::<Result<Vec<_>, _>>()?;
        let cached: Vec<Option<V>> = {
            let cache = self.cache()?;
            match cache.writes() == writes {
                true => keys.iter().map(|key| cache.peek(key)).collect(),
                false => vec![None; keys.len()],
            }
        };

        let rows = keys
            .into_iter()
            .zip(cached)
            .zip(entries)
            .map(|((key, cached), (_, row))| {
                Ok((
                    key,
                    match cached {
                        Some(row) => row,
                        None => V::decode(&row)?,
                    },
                ))
            })
            .collect::<Result<Vec<_>, StoreError>>()?;

        self.cache()?.fill_scanned(writes, &rows);
        Ok(rows)
    }

    async fn write(&self, items: Vec<WriteItem<RowKey, V>>) -> Result<(), StoreError> {
        let mut batch = sled::Batch::default();
        for item in &items {
            match item {
                WriteItem::Put(key, row) => batch.insert(key.encode(), row.encode()),
                WriteItem::Delete(key) => batch.remove(key.encode()),
            }
        }

        // The cache stays locked while sled applies the batch, so no read
        // finds the cache older than the disk.
        let mut cache = self.cache()?;
        self.tree.apply_batch(batch).map_err(failure)?;
        cache.apply(items);

        Ok(())
    }
}

/// A failure of sled's: one of I/O may pass when tried again, while
/// corruption, an unsupported use and sled's own bugs won't.
fn failure(error: sled::Error) -> StoreError {
    match error {
        sled::Error::Io(_) => StoreError::retryable(error),
        _ => StoreError::permanent(error),
    }
}

#[cfg(test)]
mod tests;
