//! A read of a tree's rows by key, as both of the tree's readers make it:
//! each key answered from what is at hand where it can be, and the rest by
//! one read of the tree's rows map.

use std::collections::BTreeMap;

use ankurah_digest_tree::{DigestGroup, Encode, Row, RowKey, StoreError};

use super::rows::{decode_row, got, row_or_damage};
use super::Inner;
use crate::storage::{MapRead, MapRows, StorageEngine};
use crate::sync::storage_adapter::cache::admits;
use crate::sync::TreeId;

/// One read of a tree's rows by key.
pub(super) struct ByKey<'k, K, L, G: DigestGroup> {
    keys: &'k [RowKey],

    /// Each key's row, `None` until the key is answered.
    answers: Vec<Option<Option<Row<K, L, G>>>>,
}

impl<'k, K, L, G> ByKey<'k, K, L, G>
where
    K: Encode + Clone + Send + Sync + 'static,
    L: Encode + Clone + Send + Sync + 'static,
    G: DigestGroup,
{
    pub(super) fn new(keys: &'k [RowKey]) -> Self { Self { keys, answers: keys.iter().map(|_| None).collect() } }

    /// Answer the keys among `found`, the bytes of rows a read with the
    /// tree's lock found.
    pub(super) fn fill_from_prefetch(&mut self, tree: &TreeId, found: &BTreeMap<RowKey, Option<Vec<u8>>>) -> Result<(), StoreError> {
        for (key, answer) in self.unanswered() {
            if let Some(bytes) = found.get(key) {
                *answer = Some(bytes.as_deref().map(|bytes| decode_row(tree, bytes)).transpose()?);
            }
        }

        Ok(())
    }

    /// Answer the keys whose rows the cache keeps of the tree at exactly
    /// `generation`.
    pub(super) fn fill_from_cache<SE: StorageEngine>(&mut self, tree: &Inner<SE>, generation: u64) {
        let unanswered: Vec<_> = self.unanswered().into_iter().filter(|(key, _)| admits(key)).collect();
        let rows = tree
            .cache
            .get_many(&tree.rows, generation, unanswered.iter().map(|(key, _)| key.encode()), |bytes| row_or_damage(&tree.id, bytes));
        for ((_, answer), row) in unanswered.into_iter().zip(rows) {
            if let Some(row) = row {
                *answer = Some(Some(row));
            }
        }
    }

    /// The read of the keys not yet answered, or `None` when every one is.
    pub(super) fn unread<SE: StorageEngine>(&self, tree: &Inner<SE>) -> Option<MapRead> {
        let keys: Vec<Vec<u8>> =
            self.keys.iter().zip(&self.answers).filter(|(_, answer)| answer.is_none()).map(|(key, _)| key.encode()).collect();
        (!keys.is_empty()).then(|| MapRead::Get { map: tree.rows.clone(), keys })
    }

    /// Answer the keys not yet answered from `answers`, the answers to their
    /// [read](Self::unread). Where that read saw the tree at `generation`, as
    /// the last landed commit left it, the cache takes in the rows it keeps.
    pub(super) fn fill_from_storage<SE: StorageEngine>(
        &mut self,
        tree: &Inner<SE>,
        answers: Vec<MapRows>,
        generation: Option<u64>,
    ) -> Result<(), StoreError> {
        let unanswered = self.unanswered();
        let values = got(answers, unanswered.len())?;
        let rows = values
            .iter()
            .map(|value| value.as_deref().map(|bytes| decode_row(&tree.id, bytes)).transpose())
            .collect::<Result<Vec<_>, _>>()?;

        if let Some(generation) = generation {
            let fills = unanswered.iter().zip(&rows).zip(&values).filter_map(|(((key, _), row), value)| {
                if let (true, Some(row), Some(bytes)) = (admits(key), row, value) {
                    Some((key.encode(), row.clone(), bytes.len()))
                } else {
                    None
                }
            });
            tree.cache.fill_many(&tree.rows, generation, fills);
        }

        for ((_, answer), row) in unanswered.into_iter().zip(rows) {
            *answer = Some(row);
        }

        Ok(())
    }

    /// Every key's row, once each is answered.
    pub(super) fn rows(self) -> Vec<Option<Row<K, L, G>>> {
        self.answers.into_iter().map(|answer| answer.expect("every key is answered before the rows are taken")).collect()
    }

    /// The keys not yet answered, each with its place for an answer.
    fn unanswered(&mut self) -> Vec<(&'k RowKey, &mut Option<Option<Row<K, L, G>>>)> {
        self.keys.iter().zip(self.answers.iter_mut()).filter(|(_, answer)| answer.is_none()).collect()
    }
}
