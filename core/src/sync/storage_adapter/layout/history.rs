//! Bounded before-images for a captured tree version. Each write records the
//! previous values of every tree row it changes, including leaves, in the same
//! transaction as the rows and generation. Reads undo those writes over a short
//! current snapshot; they never hold an engine lock while waiting on a peer.
//!
//! This is separate from the cache's after-image log. Missing history is an
//! expired capture, not permission to answer from a newer version. A replacement,
//! an oversized write or damaged history ends the retained interval.
//!
//! Retention is bounded by 64 generations, 16,384 before-images and 8 MiB of
//! encoded keys, payloads and metadata per tree. Generation keys are big-endian
//! u64s. A complete entry is a tag, a u32 row count, and sorted key/value pairs;
//! the row count detects truncation even at a pair boundary. The empty key holds
//! each retained generation's row count and encoded size for bounded pruning.

use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;

use ankurah_digest_tree::{DigestGroup, Encode, Row};

use super::names::Incarnation;
use crate::error::{MutationError, RetrievalError};
use crate::storage::{pending_writes::PendingWrites, MapId, MapRead, MapRows, MapWrite, ScanDirection, StorageTransaction};
use crate::sync::{Error, StorageCache};

pub(in crate::sync::storage_adapter) const GENERATIONS: usize = 64;
pub(in crate::sync::storage_adapter) const MAX_ROWS: usize = 16_384;
pub(in crate::sync::storage_adapter) const MAX_BYTES: usize = 8 * 1024 * 1024;

const COMPLETE: u8 = 0;
const EXPIRED: u8 = 1;
const ABSENT: u8 = 0;
const PRESENT: u8 = 1;
// An entry's generation key and its generation/row-count/byte-count metadata.
const ENTRY_OVERHEAD: usize = 8 + 24;

/// Retained payload sizes, so pruning reads at most one small metadata record.
/// The empty key cannot collide with an eight-byte generation key.
#[derive(Clone, Copy)]
struct Retained {
    generation: u64,
    rows: usize,
    bytes: usize,
}

/// Reuse cached rows only at the exact generation whose before-images we need.
/// A transaction that has already written must read its own rows instead.
pub(in crate::sync::storage_adapter) fn cached<K, L, G>(
    cache: &StorageCache,
    incarnation: Incarnation,
    generation: Option<u64>,
    keys: &[Vec<u8>],
) -> BTreeMap<Vec<u8>, Option<Vec<u8>>>
where
    K: Encode + Clone + Send + Sync + 'static,
    L: Encode + Clone + Send + Sync + 'static,
    G: DigestGroup,
{
    let Some(generation) = generation else { return BTreeMap::new() };
    let values = cache.get_many(&incarnation.rows(), generation, keys.iter().cloned(), Row::<K, L, G>::decode);
    keys.iter().zip(values).filter_map(|(key, row)| row.map(|row| (key.clone(), Some(row.encode())))).collect()
}

/// Stage history before the tree rows are written. The caller already holds the
/// tree's named lock. Repeated keys retain their value before the whole batch.
/// `known` and `metadata`, when supplied, were read at that same generation.
pub(in crate::sync::storage_adapter) async fn record(
    trx: &mut dyn StorageTransaction,
    incarnation: Incarnation,
    generation: u64,
    keys: impl IntoIterator<Item = Vec<u8>>,
    mut known: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    metadata: Option<Option<Vec<u8>>>,
    cleared: bool,
) -> Result<Vec<MapWrite>, MutationError> {
    let map = incarnation.history();
    // Refuse oversized key sets before allocating or reading their before-images.
    let mut unique = BTreeMap::new();
    let mut key_bytes = 5 + ENTRY_OVERHEAD;
    let mut expired = cleared;
    if !expired {
        for key in keys {
            if unique.contains_key(&key) {
                continue;
            }
            key_bytes = key_bytes.saturating_add(4 + key.len() + 1);
            if unique.len() == MAX_ROWS || key_bytes > MAX_BYTES {
                expired = true;
                break;
            }
            unique.insert(key, ());
        }
    }
    let keys: Vec<_> = if expired { Vec::new() } else { unique.into_keys().collect() };
    let unread: Vec<_> = keys.iter().filter(|key| !known.contains_key(*key)).cloned().collect();
    let mut reads = if metadata.is_some() { Vec::new() } else { vec![metadata_read(incarnation)] };
    if !unread.is_empty() {
        reads.push(MapRead::Get { map: incarnation.rows(), keys: unread.clone() });
    }
    let mut answers = if reads.is_empty() { Vec::new() } else { trx.map_read(reads).await? }.into_iter();
    let metadata = match metadata {
        Some(metadata) => metadata,
        None => metadata_found(answers.next())?,
    };
    let first = generation == 1 && metadata.is_none();
    let retained = metadata.and_then(|bytes| decode_retained(&bytes));
    let mut writes = Vec::new();
    let mut retained = match retained {
        Some(retained) if retained.back().is_some_and(|last| last.generation.checked_add(1) == Some(generation)) => retained,
        // Old writers may not journal. Never bridge a gap or trust damaged sizes.
        _ => {
            if !first {
                writes.push(MapWrite::Clear { map: map.clone() });
            }
            VecDeque::new()
        }
    };
    let (bytes, count) = if expired {
        (vec![EXPIRED], 0)
    } else {
        if !unread.is_empty() {
            let Some(MapRows::Got(values)) = answers.next() else { return Err(Error::AnswerMisfit.into()) };
            if values.len() != unread.len() {
                return Err(Error::AnswerMisfit.into());
            }
            known.extend(unread.into_iter().zip(values));
        }
        encode(keys.into_iter().map(|key| {
            let value = known.remove(&key).expect("every before-image was read or cached");
            (key, value)
        }))
    };
    // An oversized write is an explicit boundary. Earlier history is useless
    // across it, so release its storage immediately.
    if bytes == [EXPIRED] {
        writes.push(MapWrite::Clear { map: map.clone() });
        retained.clear();
    }
    retained.push_back(Retained { generation, rows: count, bytes: bytes.len() });
    let (mut rows, mut total): (usize, usize) =
        retained.iter().fold((0, 0), |(rows, bytes), entry| (rows + entry.rows, bytes + entry.bytes + ENTRY_OVERHEAD));
    while retained.len() > GENERATIONS || rows > MAX_ROWS || total > MAX_BYTES {
        let old = retained.pop_front().expect("a retained entry exceeds the budget");
        rows -= old.rows;
        total -= old.bytes + ENTRY_OVERHEAD;
        writes.push(MapWrite::Delete { map: map.clone(), key: old.generation.to_be_bytes().to_vec() });
    }
    writes.push(MapWrite::Put { map: map.clone(), key: generation.to_be_bytes().to_vec(), value: bytes });
    writes.push(MapWrite::Put { map, key: Vec::new(), value: encode_retained(&retained) });
    Ok(writes)
}

/// Prefetch this with the tree's initial locked reads to avoid a write-only
/// round trip when the cache and checked leaves supply every before-image.
pub(in crate::sync::storage_adapter) fn metadata_read(incarnation: Incarnation) -> MapRead {
    MapRead::Get { map: incarnation.history(), keys: vec![Vec::new()] }
}

pub(in crate::sync::storage_adapter) fn metadata_found(answer: Option<MapRows>) -> Result<Option<Vec<u8>>, Error> {
    match answer {
        Some(MapRows::Got(mut values)) if values.len() == 1 => Ok(values.pop().flatten()),
        _ => Err(Error::AnswerMisfit),
    }
}

pub(in crate::sync::storage_adapter) fn read_after(map: MapId, generation: u64) -> MapRead {
    MapRead::Range {
        map,
        start: Bound::Excluded(generation.to_be_bytes().to_vec()),
        end: Bound::Unbounded,
        direction: ScanDirection::Forward,
        limit: GENERATIONS,
    }
}

/// Undo every intervening generation. The oldest before-image of a key wins.
/// Check the whole interval before exposing any reconstructed rows.
pub(in crate::sync::storage_adapter) fn rewind(
    rows: MapId,
    from: u64,
    to: u64,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<PendingWrites, RetrievalError> {
    let expired = || RetrievalError::from(MutationError::WriteConflict);
    let distance = to.checked_sub(from).filter(|distance| *distance <= GENERATIONS as u64).ok_or_else(expired)?;
    let mut seen = 0;
    let mut total_bytes = 0usize;
    let mut total_rows = 0usize;
    let mut oldest = BTreeMap::new();
    for (key, bytes) in entries {
        let generation = key.try_into().ok().map(u64::from_be_bytes).ok_or_else(expired)?;
        if generation > to {
            break;
        }
        seen += 1;
        if from.checked_add(seen) != Some(generation) {
            return Err(expired());
        }
        total_bytes = total_bytes.checked_add(bytes.len() + ENTRY_OVERHEAD).filter(|sum| *sum <= MAX_BYTES).ok_or_else(expired)?;
        let values = decode(&bytes).ok_or_else(expired)?;
        total_rows = total_rows.checked_add(values.len()).filter(|sum| *sum <= MAX_ROWS).ok_or_else(expired)?;
        for (key, value) in values {
            oldest.entry(key).or_insert(value);
        }
    }
    if seen != distance {
        return Err(expired());
    }
    let mut reverse = PendingWrites::default();
    reverse.add(
        oldest
            .into_iter()
            .map(|(key, value)| match value {
                Some(value) => MapWrite::Put { map: rows.clone(), key, value },
                None => MapWrite::Delete { map: rows.clone(), key },
            })
            .collect(),
    );
    Ok(reverse)
}

fn encode(values: impl IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>) -> (Vec<u8>, usize) {
    let mut bytes = vec![COMPLETE, 0, 0, 0, 0];
    let mut count = 0;
    for (key, value) in values {
        let added = 4usize
            .saturating_add(key.len())
            .saturating_add(1)
            .saturating_add(value.as_ref().map_or(0, |value| 4usize.saturating_add(value.len())));
        if count == MAX_ROWS || bytes.len().saturating_add(added).saturating_add(ENTRY_OVERHEAD) > MAX_BYTES {
            return (vec![EXPIRED], 0);
        }
        field(&mut bytes, &key);
        match value {
            Some(value) => {
                bytes.push(PRESENT);
                field(&mut bytes, &value);
            }
            None => bytes.push(ABSENT),
        }
        count += 1;
    }
    bytes[1..5].copy_from_slice(&(count as u32).to_be_bytes());
    (bytes, count)
}

fn decode(bytes: &[u8]) -> Option<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
    if bytes.len() > MAX_BYTES - ENTRY_OVERHEAD {
        return None;
    }
    let (&COMPLETE, mut rest) = bytes.split_first()? else { return None };
    let (count, tail) = rest.split_first_chunk::<4>()?;
    let count = u32::from_be_bytes(*count) as usize;
    if count > MAX_ROWS {
        return None;
    }
    rest = tail;
    let mut values = Vec::new();
    let mut previous: Option<Vec<u8>> = None;
    while !rest.is_empty() {
        if values.len() == MAX_ROWS {
            return None;
        }
        let key = read_field(&mut rest)?;
        if previous.as_ref().is_some_and(|previous| *previous >= key) {
            return None;
        }
        previous = Some(key.clone());
        let (&tag, tail) = rest.split_first()?;
        rest = tail;
        let value = match tag {
            ABSENT => None,
            PRESENT => Some(read_field(&mut rest)?),
            _ => return None,
        };
        values.push((key, value));
    }
    (values.len() == count).then_some(values)
}

fn encode_retained(entries: &VecDeque<Retained>) -> Vec<u8> {
    entries
        .iter()
        .flat_map(|entry| [entry.generation, entry.rows as u64, entry.bytes as u64].into_iter().flat_map(u64::to_be_bytes))
        .collect()
}

fn decode_retained(bytes: &[u8]) -> Option<VecDeque<Retained>> {
    if bytes.is_empty() || bytes.len() % 24 != 0 || bytes.len() > GENERATIONS * 24 {
        return None;
    }
    let mut entries: VecDeque<Retained> = VecDeque::new();
    let (mut total_rows, mut total_bytes) = (0usize, 0usize);
    for entry in bytes.chunks_exact(24) {
        let generation = u64::from_be_bytes(entry[..8].try_into().ok()?);
        let rows: usize = u64::from_be_bytes(entry[8..16].try_into().ok()?).try_into().ok()?;
        let bytes: usize = u64::from_be_bytes(entry[16..].try_into().ok()?).try_into().ok()?;
        if bytes == 0 || entries.back().is_some_and(|last| last.generation.checked_add(1) != Some(generation)) {
            return None;
        }
        total_rows = total_rows.checked_add(rows).filter(|sum| *sum <= MAX_ROWS)?;
        total_bytes = total_bytes.checked_add(bytes.checked_add(ENTRY_OVERHEAD)?).filter(|sum| *sum <= MAX_BYTES)?;
        entries.push_back(Retained { generation, rows, bytes });
    }
    Some(entries)
}

fn field(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
    bytes.extend_from_slice(value);
}

fn read_field(bytes: &mut &[u8]) -> Option<Vec<u8>> {
    let (length, rest) = bytes.split_first_chunk::<4>()?;
    let (value, rest) = rest.split_at_checked(u32::from_be_bytes(*length) as usize)?;
    *bytes = rest;
    Some(value.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MapSnapshot, StorageEngine};
    use crate::test_utils::TestStorage;

    async fn retained(engine: &TestStorage, incarnation: Incarnation) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let answer = engine
            .map_snapshot(&[])
            .await?
            .map_read(vec![MapRead::Range {
                map: incarnation.history(),
                start: Bound::Unbounded,
                end: Bound::Unbounded,
                direction: ScanDirection::Forward,
                limit: usize::MAX,
            }])
            .await?;
        let [MapRows::Ranged(entries)]: [_; 1] = answer.try_into().unwrap() else { panic!("history range") };
        Ok(entries)
    }

    async fn append(engine: &TestStorage, incarnation: Incarnation, generation: u64, keys: Vec<Vec<u8>>) -> anyhow::Result<()> {
        let mut transaction = engine.transaction();
        let writes = record(&mut transaction, incarnation, generation, keys, BTreeMap::new(), None, false).await?;
        transaction.map_write(writes).await?;
        transaction.commit().await?.committed()?;
        Ok(())
    }

    #[tokio::test]
    async fn retained_before_images_obey_both_the_row_and_byte_budgets() -> anyhow::Result<()> {
        for (count, value_size) in [(MAX_ROWS / 2 + 1, 0), (1, MAX_BYTES / 2 + 1)] {
            let engine = TestStorage::default();
            let incarnation = Incarnation::mint();
            let keys: Vec<_> = (0..count).map(|key| (key as u64).to_be_bytes().to_vec()).collect();
            let mut transaction = engine.transaction();
            transaction
                .map_write(
                    keys.iter()
                        .map(|key| MapWrite::Put { map: incarnation.rows(), key: key.clone(), value: vec![7; value_size] })
                        .collect(),
                )
                .await?;
            transaction.commit().await?.committed()?;
            append(&engine, incarnation, 1, keys.clone()).await?;
            append(&engine, incarnation, 2, keys).await?;
            let entries = retained(&engine, incarnation).await?;
            assert_eq!(entries.len(), 2, "only metadata and the newest generation fit");
            assert_eq!(entries[1].0, 2u64.to_be_bytes());
            assert!(entries[1].1.len() <= MAX_BYTES);
            assert!(rewind(incarnation.rows(), 0, 2, entries[1..].to_vec()).is_err());
            assert!(rewind(incarnation.rows(), 1, 2, entries[1..].to_vec()).is_ok());
        }
        Ok(())
    }

    #[tokio::test]
    async fn an_oversized_write_expires_old_captures_but_does_not_prevent_future_history() -> anyhow::Result<()> {
        for oversized_value in [false, true] {
            let engine = TestStorage::default();
            let incarnation = Incarnation::mint();
            append(&engine, incarnation, 1, vec![vec![1]]).await?;
            let keys = if oversized_value {
                let mut transaction = engine.transaction();
                transaction.map_write(vec![MapWrite::Put { map: incarnation.rows(), key: vec![1], value: vec![0; MAX_BYTES] }]).await?;
                transaction.commit().await?.committed()?;
                vec![vec![1]]
            } else {
                (0..=MAX_ROWS).map(|key| (key as u64).to_be_bytes().to_vec()).collect()
            };
            append(&engine, incarnation, 2, keys).await?;
            let entries = retained(&engine, incarnation).await?;
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[1], (2u64.to_be_bytes().to_vec(), vec![EXPIRED]));
            assert!(rewind(incarnation.rows(), 1, 2, entries[1..].to_vec()).is_err());
            append(&engine, incarnation, 3, vec![vec![2]]).await?;
            let entries = retained(&engine, incarnation).await?;
            assert!(rewind(incarnation.rows(), 2, 3, entries[2..].to_vec()).is_ok());
        }
        Ok(())
    }

    #[test]
    fn malformed_or_noncontiguous_history_is_a_conflict() {
        let map = Incarnation::mint().rows();
        let entry = encode([(vec![1], Some(vec![2])), (vec![3], None)]).0;
        for length in 0..entry.len() {
            assert!(rewind(map.clone(), 0, 1, vec![(1u64.to_be_bytes().to_vec(), entry[..length].to_vec())]).is_err());
        }
        for malformed in [vec![EXPIRED], vec![255]] {
            assert!(rewind(map.clone(), 0, 1, vec![(1u64.to_be_bytes().to_vec(), malformed)]).is_err());
        }
        assert!(rewind(map.clone(), 0, 2, vec![(2u64.to_be_bytes().to_vec(), entry.clone())]).is_err());
        assert!(rewind(map.clone(), 1, 0, Vec::new()).is_err());
        assert!(rewind(map, 0, 1, vec![(vec![1], entry)]).is_err());
    }

    #[tokio::test]
    async fn damaged_metadata_starts_a_new_bounded_history_interval() -> anyhow::Result<()> {
        let engine = TestStorage::default();
        let incarnation = Incarnation::mint();
        append(&engine, incarnation, 1, vec![vec![1]]).await?;
        let mut transaction = engine.transaction();
        transaction.map_write(vec![MapWrite::Put { map: incarnation.history(), key: Vec::new(), value: vec![255] }]).await?;
        transaction.commit().await?.committed()?;
        append(&engine, incarnation, 2, vec![vec![2]]).await?;
        let entries = retained(&engine, incarnation).await?;
        assert_eq!(entries.len(), 2, "untrusted old entries are released");
        assert!(rewind(incarnation.rows(), 0, 2, entries[1..].to_vec()).is_err());
        assert!(rewind(incarnation.rows(), 1, 2, entries[1..].to_vec()).is_ok());
        Ok(())
    }
}
