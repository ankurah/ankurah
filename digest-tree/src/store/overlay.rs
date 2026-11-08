//! Writes not yet applied, laid over the rows they will change, so that
//! whoever made them reads them back.

use std::collections::BTreeMap;
use std::iter::Peekable;
use std::ops::{Bound, RangeBounds};

use super::{ReadTrx, WriteItem};
use crate::span::holds_nothing;
use crate::{ScanDirection, StoreError};

/// Writes not yet applied to some rows, laid over them: a read through the
/// overlay sees the rows as the writes will leave them. A commit works its
/// rows out in one, and a reference store's transaction keeps its writes in
/// one until it commits.
pub(crate) struct Overlay<K, V> {
    /// Each key written: `Some` replaces the row beneath, `None` deletes it.
    writes: BTreeMap<K, Option<V>>,
}

impl<K: Ord + Clone, V: Clone> Overlay<K, V> {
    /// No writes yet: reads see the rows beneath as they are.
    pub(crate) fn new() -> Self { Self { writes: BTreeMap::new() } }

    /// Replace the row under `key`.
    pub(crate) fn put(&mut self, key: K, row: V) { self.writes.insert(key, Some(row)); }

    /// Remove any row under `key`.
    pub(crate) fn delete(&mut self, key: K) { self.writes.insert(key, None); }

    /// Lay `batch` over the writes before it: each put or delete replaces any
    /// earlier write under its key.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn extend(&mut self, batch: Vec<WriteItem<K, V>>) {
        for item in batch {
            match item {
                WriteItem::Put(key, row) => self.put(key, row),
                WriteItem::Delete(key) => self.delete(key),
            }
        }
    }

    /// Every write, as one batch, in key order.
    pub(crate) fn into_writes(self) -> Vec<WriteItem<K, V>> {
        self.writes
            .into_iter()
            .map(|(key, row)| match row {
                Some(row) => WriteItem::Put(key, row),
                None => WriteItem::Delete(key),
            })
            .collect()
    }

    /// The row under `key`, as the writes leave `rows`.
    pub(crate) async fn get<R: ReadTrx<K, V>>(&self, rows: &mut R, key: &K) -> Result<Option<V>, StoreError> {
        match self.writes.get(key) {
            Some(row) => Ok(row.clone()),
            None => rows.get(key).await,
        }
    }

    /// The row under each of `keys`, in order, as the writes leave `rows`:
    /// the keys the writes don't hold are read in one read of many.
    pub(crate) async fn get_many<R: ReadTrx<K, V>>(&self, rows: &mut R, keys: &[K]) -> Result<Vec<Option<V>>, StoreError> {
        let unwritten: Vec<K> = keys.iter().filter(|key| !self.writes.contains_key(key)).cloned().collect();
        let read = match unwritten.is_empty() {
            true => Vec::new(),
            false => rows.get_many(&unwritten).await?,
        };

        if read.len() != unwritten.len() {
            return Err(StoreError::permanent("a store answered a read of several keys with another number of rows"));
        }

        let mut read = read.into_iter();
        Ok(keys
            .iter()
            .map(|key| match self.writes.get(key) {
                Some(row) => row.clone(),
                None => read.next().expect("a row read for every key the writes don't hold"),
            })
            .collect())
    }

    /// The first `limit` rows between the bounds as the writes leave `rows`,
    /// in key order or reversed: fewer only where fewer lie between them.
    pub(crate) async fn range<R: ReadTrx<K, V>>(
        &self,
        rows: &mut R,
        start: Bound<&K>,
        end: Bound<&K>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(K, V)>, StoreError> {
        let mut read = Vec::new();
        if limit == 0 || holds_nothing(start, end) {
            return Ok(read);
        }

        // The part of the range still to read, which pages narrow from its
        // start, or from its end when reading in reverse.
        let (mut start, mut end) = (start.cloned(), end.cloned());
        loop {
            let asked = limit - read.len();
            let page = rows.range(start.as_ref(), end.as_ref(), direction, asked).await?;
            check_page(&page, asked, (&start, &end), direction)?;

            // A page shorter than asked holds every row left in the range, so
            // it covers the rest; a full one covers the keys up to its last.
            // The writes within what a page covers merge into it, deleted
            // rows dropping out, which is why a full page may still leave the
            // read short and the range is read on past it.
            if page.len() < asked {
                self.merge_writes(&mut read, page, (start, end), direction, limit);
                return Ok(read);
            }

            let (last, _) = page.last().expect("a full page holds a row, since every page is asked for one at least");
            let last = last.clone();

            let covered = match direction {
                ScanDirection::Forward => (start.clone(), Bound::Included(last.clone())),
                ScanDirection::Reverse => (Bound::Included(last.clone()), end.clone()),
            };

            self.merge_writes(&mut read, page, covered, direction, limit);

            if read.len() == limit {
                return Ok(read);
            }

            match direction {
                ScanDirection::Forward => start = Bound::Excluded(last),
                ScanDirection::Reverse => end = Bound::Excluded(last),
            }
        }
    }

    /// Append `page`, read in `direction`, merged with the writes within
    /// `covered`, until `read` holds `limit` rows.
    fn merge_writes(
        &self,
        read: &mut Vec<(K, V)>,
        page: Vec<(K, V)>,
        covered: (Bound<K>, Bound<K>),
        direction: ScanDirection,
        limit: usize,
    ) {
        let (start, end) = (covered.0.as_ref(), covered.1.as_ref());
        let page = page.into_iter().peekable();

        if holds_nothing(start, end) {
            return merge_ordered(read, page, std::iter::empty().peekable(), direction, limit);
        }

        let writes = self.writes.range((start, end));
        match direction {
            ScanDirection::Forward => merge_ordered(read, page, writes.peekable(), direction, limit),
            ScanDirection::Reverse => merge_ordered(read, page, writes.rev().peekable(), direction, limit),
        }
    }
}

/// Append the rows of `page` and `writes`, both in `direction`'s order, in
/// that order, until `read` holds `limit` rows: a write replaces or deletes
/// the row under its key, and a written row the page lacks joins the others.
/// Only the written rows appended are cloned.
fn merge_ordered<'w, K: Ord + Clone + 'w, V: Clone + 'w>(
    read: &mut Vec<(K, V)>,
    mut page: Peekable<impl Iterator<Item = (K, V)>>,
    mut writes: Peekable<impl Iterator<Item = (&'w K, &'w Option<V>)>>,
    direction: ScanDirection,
    limit: usize,
) {
    while read.len() < limit {
        let order = match (page.peek(), writes.peek()) {
            (None, None) => return,
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (Some((stored, _)), Some((written, _))) => match direction {
                ScanDirection::Forward => stored.cmp(*written),
                ScanDirection::Reverse => (*written).cmp(stored),
            },
        };

        if order.is_lt() {
            read.extend(page.next());
            continue;
        }

        if order.is_eq() {
            page.next();
        }

        let (key, row) = writes.next().expect("a write is next");
        if let Some(row) = row {
            read.push((key.clone(), row.clone()));
        }
    }
}

/// Check that a page holds at most the rows it was asked for, only keys
/// between the bounds it was asked for, strictly in the direction it was
/// asked for: a read that took a page breaking any of these could hold more
/// than its caller asked, and the paging that follows could repeat rows, or
/// never end.
fn check_page<K: Ord, V>(
    page: &[(K, V)],
    asked: usize,
    (start, end): (&Bound<K>, &Bound<K>),
    direction: ScanDirection,
) -> Result<(), StoreError> {
    let bounds = (start.as_ref(), end.as_ref());

    let ordered = page.windows(2).all(|pair| match direction {
        ScanDirection::Forward => pair[0].0 < pair[1].0,
        ScanDirection::Reverse => pair[0].0 > pair[1].0,
    });

    match page.len() <= asked && ordered && page.iter().all(|(key, _)| bounds.contains(key)) {
        true => Ok(()),
        false => {
            Err(StoreError::permanent("a store returned more rows than asked for, rows outside the range asked for, or rows out of order"))
        }
    }
}

#[cfg(test)]
mod tests;
