//! A tree's rows as one operation sees them: the store's rows, under the
//! changes the operation has made and not yet written.
//!
//! A read sees nothing pending, and reads the store as it stands. A commit
//! makes all its changes here, so that each change sees the ones before it,
//! and then writes them to the store in one batch.

use std::collections::BTreeMap;
use std::iter::Peekable;
use std::ops::{Bound, RangeBounds};

use crate::span::holds_nothing;
use crate::store::{LeafRow, Meta};
use crate::{Digest, DigestGroup, Error, NodePrefix, Payload, Row, RowKey, ScanDirection, Span, Store, StoreError, WriteItem};

/// The store's rows with one operation's staged changes on top: what the
/// operation reads, and, once it is done, the batch it writes.
pub(super) struct StagedRows<'s, K, L, G: DigestGroup, S> {
    store: &'s S,

    /// The rows the operation has changed: `Some` replaces the store's row,
    /// `None` deletes it.
    pending: BTreeMap<RowKey, Option<Row<K, L, G>>>,
}

impl<'s, K, L, G, S> StagedRows<'s, K, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    pub(super) fn new(store: &'s S) -> Self { Self { store, pending: BTreeMap::new() } }

    /// Replace the row under `key`.
    pub(super) fn put(&mut self, key: RowKey, row: Row<K, L, G>) { self.pending.insert(key, Some(row)); }

    /// Remove any row under `key`.
    pub(super) fn delete(&mut self, key: RowKey) { self.pending.insert(key, None); }

    /// Every change, as one batch for the store.
    pub(super) fn into_writes(self) -> Vec<WriteItem<RowKey, Row<K, L, G>>> {
        self.pending
            .into_iter()
            .map(|(key, row)| match row {
                Some(row) => WriteItem::Put(key, row),
                None => WriteItem::Delete(key),
            })
            .collect()
    }

    async fn get(&self, key: &RowKey) -> Result<Option<Row<K, L, G>>, Error> {
        match self.pending.get(key) {
            Some(row) => Ok(row.clone()),
            None => Ok(self.store.get(key).await?),
        }
    }

    /// At most `limit` rows between the bounds, in key order or reversed.
    pub(super) async fn range(
        &self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, Row<K, L, G>)>, Error> {
        let mut rows = Vec::new();
        if limit == 0 || holds_nothing(start, end) {
            return Ok(rows);
        }

        // The part of the range still to read, which pages narrow from its
        // start, or from its end when reading in reverse.
        let (mut start, mut end) = (start.cloned(), end.cloned());
        loop {
            let page = self.store.range(start.as_ref(), end.as_ref(), direction, limit - rows.len()).await?;
            check_page(&page, (&start, &end), direction)?;

            // A store may return fewer rows than asked while more remain, so
            // only an empty page ends the range. A page covers the keys up to
            // its last; the empty one covers the rest. Pending changes in what
            // a page covers merge into it, deleted rows dropping out.
            let Some(last) = page.last().map(|(key, _)| key.clone()) else {
                self.merge_pending(&mut rows, page, (start, end), direction);
                rows.truncate(limit);
                return Ok(rows);
            };

            let covered = match direction {
                ScanDirection::Forward => (start.clone(), Bound::Included(last.clone())),
                ScanDirection::Reverse => (Bound::Included(last.clone()), end.clone()),
            };

            self.merge_pending(&mut rows, page, covered, direction);

            if rows.len() >= limit {
                rows.truncate(limit);
                return Ok(rows);
            }

            match direction {
                ScanDirection::Forward => start = Bound::Excluded(last),
                ScanDirection::Reverse => end = Bound::Excluded(last),
            }
        }
    }

    /// Append `page`, which the store read in `direction`, merged with the
    /// pending changes within `covered`.
    fn merge_pending(
        &self,
        rows: &mut Vec<(RowKey, Row<K, L, G>)>,
        page: Vec<(RowKey, Row<K, L, G>)>,
        covered: (Bound<RowKey>, Bound<RowKey>),
        direction: ScanDirection,
    ) {
        let (start, end) = (covered.0.as_ref(), covered.1.as_ref());
        let page = page.into_iter().peekable();

        if holds_nothing(start, end) {
            return merge_ordered(rows, page, std::iter::empty().peekable(), direction);
        }

        let changes = self.pending.range((start, end));
        match direction {
            ScanDirection::Forward => merge_ordered(rows, page, changes.peekable(), direction),
            ScanDirection::Reverse => merge_ordered(rows, page, changes.rev().peekable(), direction),
        }
    }

    /// The tree's metadata.
    pub(super) async fn meta(&self) -> Result<Option<Meta<K>>, Error> {
        match self.get(&RowKey::Meta).await? {
            None => Ok(None),
            Some(Row::Meta(meta)) => Ok(Some(meta)),
            Some(_) => Err(wrong_kind()),
        }
    }

    /// The digest of the node at `prefix`, if the tree has one.
    pub(super) async fn node(&self, prefix: &NodePrefix) -> Result<Option<Digest<G>>, Error> {
        match self.get(&RowKey::Node(prefix.clone())).await? {
            None => Ok(None),
            Some(Row::Node(digest)) => Ok(Some(digest)),
            Some(_) => Err(wrong_kind()),
        }
    }

    /// The member at `address`.
    pub(super) async fn leaf(&self, address: &[u8]) -> Result<Option<LeafRow<L, G>>, Error> {
        match self.get(&RowKey::Leaf(address.to_vec())).await? {
            None => Ok(None),
            Some(Row::Leaf(leaf)) => Ok(Some(leaf)),
            Some(_) => Err(wrong_kind()),
        }
    }

    /// At most `limit` members within `span`, in address order, or reversed.
    pub(super) async fn leaves(&self, span: &Span, direction: ScanDirection, limit: usize) -> Result<Vec<(Vec<u8>, LeafRow<L, G>)>, Error> {
        // Leaf keys run from the empty address to the end of the rows.
        let start = match &span.start {
            Bound::Unbounded => Bound::Included(RowKey::Leaf(Vec::new())),
            Bound::Included(address) => Bound::Included(RowKey::Leaf(address.clone())),
            Bound::Excluded(address) => Bound::Excluded(RowKey::Leaf(address.clone())),
        };

        let end = match &span.end {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(address) => Bound::Included(RowKey::Leaf(address.clone())),
            Bound::Excluded(address) => Bound::Excluded(RowKey::Leaf(address.clone())),
        };

        self.range(start.as_ref(), end.as_ref(), direction, limit)
            .await?
            .into_iter()
            .map(|row| match row {
                (RowKey::Leaf(address), Row::Leaf(leaf)) => Ok((address, leaf)),
                _ => Err(wrong_kind()),
            })
            .collect()
    }
}

/// Append the rows of `page` and `pending`, both in `direction`'s order, in
/// that order: a pending change replaces or deletes the store's row under
/// its key, and a pending row the store lacks joins the others.
fn merge_ordered<'p, R: Clone + 'p>(
    rows: &mut Vec<(RowKey, R)>,
    mut page: Peekable<impl Iterator<Item = (RowKey, R)>>,
    mut pending: Peekable<impl Iterator<Item = (&'p RowKey, &'p Option<R>)>>,
    direction: ScanDirection,
) {
    loop {
        let order = match (page.peek(), pending.peek()) {
            (None, None) => return,
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (Some((stored, _)), Some((changed, _))) => match direction {
                ScanDirection::Forward => stored.cmp(*changed),
                ScanDirection::Reverse => (*changed).cmp(stored),
            },
        };

        if order.is_lt() {
            rows.extend(page.next());
            continue;
        }

        if order.is_eq() {
            page.next();
        }

        let (key, row) = pending.next().expect("a pending change is next");
        if let Some(row) = row {
            rows.push((key.clone(), row.clone()));
        }
    }
}

/// Check that a store's page holds only keys between the bounds it was asked
/// for, strictly in the direction it was asked for: the paging that follows a
/// page that breaks either could repeat rows, or never end.
fn check_page<R>(page: &[(RowKey, R)], (start, end): (&Bound<RowKey>, &Bound<RowKey>), direction: ScanDirection) -> Result<(), Error> {
    let bounds = (start.as_ref(), end.as_ref());

    let ordered = page.windows(2).all(|pair| match direction {
        ScanDirection::Forward => pair[0].0 < pair[1].0,
        ScanDirection::Reverse => pair[0].0 > pair[1].0,
    });

    match ordered && page.iter().all(|(key, _)| bounds.contains(key)) {
        true => Ok(()),
        false => Err(Error::Store(StoreError::permanent("a store returned rows outside the range asked for, or out of order"))),
    }
}

/// A row of another kind than its key names: the store's rows are damaged.
pub(super) fn wrong_kind() -> Error { Error::Inconsistent("a row is of another kind than its key") }

#[cfg(test)]
mod tests;
