//! A tree's change log: for each generation after the tree's creation, the
//! rows the cache keeps that the generation's write put or deleted. Several
//! nodes commit to one tree in turn, and with the log each node's cache keeps
//! the rows it holds of the tree across the others' commits, bringing them up
//! to date from the log instead of reading them again.
//!
//! An entry's key is its generation, eight bytes big-endian, so that entries
//! sort by generation. Its value is a byte, then a body:
//!
//! - `0`, then each row: its key, and then `1` and the row for a row put, or
//!   `0` for a row deleted; a key or a row is its length, four bytes
//!   big-endian, and then its bytes;
//! - `1`, alone: the write changed more rows than an entry lists, and the
//!   whole tree is to be taken as changed.
//!
//! Every generation after a tree's creation has exactly one entry, though it
//! may list no rows, so a reader knows by counting whether the entries it
//! read reach from one generation to another. The log is kept as far back as
//! a reader catches up from: each write deletes the entry [`LENGTH`]
//! generations back.
//!
//! The log serves only the cache. Entries that don't reach, an entry that
//! doesn't decode, and a row that doesn't decode inside an entry that does,
//! cost a reader the rows it has cached of the tree, which it then reads
//! through storage, and nothing else.

use std::ops::Bound;

use super::names::TreeId;
use crate::storage::{MapId, MapRead, ScanDirection};

/// How many generations back the log reaches: a cache further behind than
/// this forgets the tree's rows instead of catching up.
pub(in crate::sync::storage_adapter) const LENGTH: u64 = 64;

/// The most rows an entry lists. A write that changes more, such as a tree
/// laid out anew, is logged as changing the whole tree.
pub(in crate::sync::storage_adapter) const MOST_ROWS: usize = 256;

const ROWS: u8 = 0;
const WHOLE_TREE: u8 = 1;

const DELETED: u8 = 0;
const PUT: u8 = 1;

/// The rows one write changed, by their keys' bytes: each row's bytes for a
/// row put, or `None` for a row deleted.
pub(in crate::sync::storage_adapter) type Changed = Vec<(Vec<u8>, Option<Vec<u8>>)>;

pub(in crate::sync::storage_adapter) fn key(generation: u64) -> Vec<u8> { generation.to_be_bytes().to_vec() }

/// The entry for a write that changed `changed`.
pub(in crate::sync::storage_adapter) fn entry(changed: &Changed) -> Vec<u8> {
    if changed.len() > MOST_ROWS {
        return vec![WHOLE_TREE];
    }

    let mut bytes = vec![ROWS];
    for (key, row) in changed {
        field(&mut bytes, key);

        match row {
            Some(row) => {
                bytes.push(PUT);
                field(&mut bytes, row);
            }

            None => bytes.push(DELETED),
        }
    }

    bytes
}

/// The read of the entries `log` holds after `generation`, as many as the
/// log keeps. The reader reads the tree's entry in the same request, so it
/// can't yet know how many it needs; a log holding more is damaged.
pub(in crate::sync::storage_adapter) fn read_after(log: MapId, generation: u64) -> MapRead {
    MapRead::Range {
        map: log,
        start: Bound::Excluded(key(generation)),
        end: Bound::Unbounded,
        direction: ScanDirection::Forward,
        limit: LENGTH as usize,
    }
}

/// What `tree`'s log says changed from generation `from` to `to`, from
/// `entries`, the entries a [`read_after`] `from` found: every write's rows
/// in turn, or `None` where the entries don't reach `to`, one says the whole
/// tree changed, or one doesn't decode, which is logged.
pub(in crate::sync::storage_adapter) fn changes(tree: &TreeId, from: u64, to: u64, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Option<Changed> {
    let mut changed = Vec::new();
    let mut reached = from;

    for (key, entry) in entries {
        let Some(generation) = key.try_into().ok().map(u64::from_be_bytes) else { return damaged(tree, "key") };
        if generation > to {
            break;
        }

        if reached.checked_add(1) != Some(generation) {
            return None;
        }

        match decode(&entry) {
            Some(Some(rows)) => changed.extend(rows),
            Some(None) => return None,
            None => return damaged(tree, "rows"),
        }

        reached = generation;
    }

    (reached == to).then_some(changed)
}

/// An entry of `tree`'s log whose `part` doesn't decode: logged, and taken
/// as a log that doesn't reach.
fn damaged(tree: &TreeId, part: &str) -> Option<Changed> {
    tracing::warn!("an entry of tree {tree}'s change log doesn't decode, by its {part}: the cache forgets the rows it kept of the tree");
    None
}

/// The rows an entry lists, or `None` for an entry that says the whole tree
/// changed. `None` outside for bytes that are no entry, among them an entry
/// listing more rows than [`MOST_ROWS`].
fn decode(bytes: &[u8]) -> Option<Option<Changed>> {
    let (&kind, mut body) = bytes.split_first()?;
    match kind {
        WHOLE_TREE if body.is_empty() => return Some(None),
        ROWS => {}
        _ => return None,
    }

    let mut rows = Vec::new();
    while !body.is_empty() {
        if rows.len() == MOST_ROWS {
            return None;
        }

        let key = read_field(&mut body)?;

        let (&kind, rest) = body.split_first()?;
        body = rest;

        let row = match kind {
            PUT => Some(read_field(&mut body)?),
            DELETED => None,
            _ => return None,
        };

        rows.push((key, row));
    }

    Some(Some(rows))
}

fn field(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend_from_slice(&u32::try_from(value.len()).expect("a row of fewer than 2^32 bytes").to_be_bytes());
    bytes.extend_from_slice(value);
}

/// The field `body` begins with, which it then moves past.
fn read_field(body: &mut &[u8]) -> Option<Vec<u8>> {
    let (length, rest) = body.split_first_chunk::<4>()?;
    let (value, rest) = rest.split_at_checked(u32::from_be_bytes(*length) as usize)?;
    *body = rest;

    Some(value.to_vec())
}

/// `changed![b"a" => b"1", b"b" => None]`: rows put and deleted, as a write
/// changes them.
#[cfg(test)]
macro_rules! changed {
    ($($key:expr => $row:expr),* $(,)?) => {{
        let changed: $crate::sync::storage_adapter::layout::log::Changed = vec![$(($key.to_vec(), Option::<&[u8]>::from($row).map(<[u8]>::to_vec))),*];
        changed
    }};
}

#[cfg(test)]
pub(in crate::sync::storage_adapter) use changed;

#[cfg(test)]
mod tests {
    use super::*;

    fn tree() -> TreeId { TreeId::new("tree") }

    /// Entries as a read of the log gives them, each for its generation.
    fn logged(entries: &[(u64, &Changed)]) -> Vec<(Vec<u8>, Vec<u8>)> {
        entries.iter().map(|(generation, changed)| (key(*generation), entry(changed))).collect()
    }

    #[test]
    fn entries_that_reach_from_one_generation_to_another_give_every_writes_rows_in_turn() {
        let (second, third) = (changed![b"a" => &b"2"[..], b"b" => None], changed![b"a" => &b"3"[..]]);
        let entries = logged(&[(2, &second), (3, &changed![]), (4, &third), (5, &changed![b"c" => &b"5"[..]])]);

        let changes = changes(&tree(), 1, 4, entries);
        assert_eq!(changes, Some(changed![b"a" => &b"2"[..], b"b" => None, b"a" => &b"3"[..]]));
    }

    #[test]
    fn entries_that_reach_the_last_generation_there_is_give_its_rows() {
        let rows = changed![b"a" => &b"1"[..]];

        assert_eq!(changes(&tree(), u64::MAX - 1, u64::MAX, logged(&[(u64::MAX, &rows)])), Some(rows));
    }

    #[test]
    fn entries_that_miss_a_generation_or_change_the_whole_tree_give_nothing() {
        let rows = changed![b"a" => &b"1"[..]];
        let whole: Changed = (0..=MOST_ROWS as u32).map(|n| (n.to_be_bytes().to_vec(), None)).collect();

        assert_eq!(changes(&tree(), 1, 3, logged(&[(3, &rows)])), None, "the first entry missing");
        assert_eq!(changes(&tree(), 1, 3, logged(&[(2, &rows)])), None, "the last entry missing");
        assert_eq!(changes(&tree(), 1, 3, logged(&[(2, &rows), (3, &whole)])), None, "the whole tree changed");
        assert_eq!(entry(&whole), [WHOLE_TREE]);
    }

    #[test]
    fn an_entry_that_doesnt_decode_gives_nothing() {
        let mut entries = logged(&[(2, &changed![b"a" => &b"1"[..]])]);
        entries[0].1.pop();

        assert_eq!(changes(&tree(), 1, 2, entries), None, "a truncated entry");
        assert_eq!(changes(&tree(), 1, 2, vec![(vec![2], entry(&changed![]))]), None, "a key that is no generation");
    }

    #[test]
    fn an_entry_listing_more_rows_than_an_entry_lists_gives_nothing() {
        let mut bytes = vec![ROWS];
        for n in 0..=MOST_ROWS as u32 {
            field(&mut bytes, &n.to_be_bytes());
            bytes.push(DELETED);
        }

        assert_eq!(changes(&tree(), 1, 2, vec![(key(2), bytes)]), None);
    }

    #[test]
    fn a_read_of_the_log_asks_for_no_more_entries_than_it_keeps() {
        let read = read_after(MapId::new("l"), 7);

        assert!(matches!(read, MapRead::Range { start: Bound::Excluded(start), limit, .. } if start == key(7) && limit == LENGTH as usize));
    }
}
