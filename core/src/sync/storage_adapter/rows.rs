//! A tree's rows as its map holds them: each row key and row as the crate
//! encodes it, and the answers to the reads that fetch them.

use std::ops::Bound;

use ankurah_digest_tree::{DigestGroup, Encode, Row, RowKey, StoreError};

use crate::storage::MapRows;
use crate::sync::{Error, TreeId, TreePart};

/// The row these bytes hold.
pub(super) fn decode_row<K: Encode, L: Encode, G: DigestGroup>(tree: &TreeId, bytes: &[u8]) -> Result<Row<K, L, G>, StoreError> {
    row_or_damage(tree, bytes).map_err(StoreError::permanent)
}

/// The row these bytes hold, or the tree's damage itself, not a store's
/// failure: the record cache logs it, and reads the row from storage instead.
pub(super) fn row_or_damage<K: Encode, L: Encode, G: DigestGroup>(tree: &TreeId, bytes: &[u8]) -> Result<Row<K, L, G>, Error> {
    Row::decode(bytes).map_err(|cause| Error::Damaged { tree: tree.clone(), part: TreePart::Row, cause: Some(cause) })
}

/// The row key and row of one entry a range read gave.
pub(super) fn decode_entry<K: Encode, L: Encode, G: DigestGroup>(
    tree: &TreeId,
    (key, row): (Vec<u8>, Vec<u8>),
) -> Result<(RowKey, Row<K, L, G>), StoreError> {
    let key = RowKey::decode(&key).map_err(|cause| Error::damaged(tree, TreePart::RowKey, Some(cause)))?;
    Ok((key, decode_row(tree, &row)?))
}

/// A range's bound on row keys, as a bound on their bytes, which sort as the
/// keys do.
pub(super) fn encode_bound(bound: Bound<&RowKey>) -> Bound<Vec<u8>> { bound.map(RowKey::encode) }

/// Extract a single get response, requiring one value slot per requested key.
pub(super) fn got(answers: Vec<MapRows>, keys: usize) -> Result<Vec<Option<Vec<u8>>>, StoreError> {
    match one(answers)? {
        MapRows::Got(values) if values.len() == keys => Ok(values),
        _ => Err(answer_misfit()),
    }
}

/// Extract the entries from a single range response.
pub(super) fn ranged(answers: Vec<MapRows>) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
    match one(answers)? {
        MapRows::Ranged(entries) => Ok(entries),
        MapRows::Got(_) => Err(answer_misfit()),
    }
}

fn one(answers: Vec<MapRows>) -> Result<MapRows, StoreError> {
    let [answer]: [MapRows; 1] = answers.try_into().map_err(|_| answer_misfit())?;
    Ok(answer)
}

fn answer_misfit() -> StoreError { StoreError::permanent(Error::AnswerMisfit) }
