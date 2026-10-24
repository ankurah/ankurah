//! The memory-resident maps that hold a tree's rows, and the transactions over
//! them.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::{ByteBounds, Failure, MapAccess, MapName, MapTransaction};
use crate::error::TreeError;

/// A tree's three maps in memory, behind one lock, and the transactions over
/// them.
///
/// A transaction reads the maps as the last commit before it began left them,
/// overlaid with its own writes. Each conditional write is checked against that
/// view when it is made and buffered with what it expects; the commit takes the
/// lock, checks every expectation again against the maps as they stand, and
/// applies every write, or none when one fails. Appends take a counter as their
/// suffix.
///
/// An open transaction retains its original snapshot. A commit copies the maps
/// only when another transaction still holds the current snapshot; readers of
/// older snapshots do not require another copy.
#[derive(Debug, Default)]
pub struct MemoryMaps {
    shared: Arc<Mutex<Shared>>,
}

/// What every transaction over the maps shares, behind their lock.
#[derive(Debug, Default)]
struct Shared {
    /// The maps as the last commit left them.
    committed: Arc<Maps>,
    /// How many values have been appended: the next one's suffix.
    appended: u64,
    /// How many commits have written anything.
    revision: u64,
    /// How many commits have changed the topology.
    structure: u64,
}

/// The entries of the three maps, indexed by [`MapName`].
#[derive(Debug, Clone, Default)]
struct Maps([BTreeMap<Vec<u8>, Vec<u8>>; 3]);

impl MemoryMaps {
    /// Three empty maps.
    pub fn new() -> Self { Self::default() }

    /// Begin a transaction over the maps as they stand now. A commit copies
    /// all three maps if another transaction still holds the current version;
    /// transactions holding older versions do not require another copy.
    pub fn begin(&self) -> MemoryTransaction {
        let shared = self.shared.lock().unwrap();
        MemoryTransaction {
            shared: self.shared.clone(),
            began_with: shared.committed.clone(),
            writes: Default::default(),
            failure: None,
            revision: shared.revision,
            structure: shared.structure,
            watches_structure: false,
            changes_structure: false,
        }
    }
}

/// A transaction over [`MemoryMaps`].
#[derive(Debug)]
pub struct MemoryTransaction {
    shared: Arc<Mutex<Shared>>,
    /// The maps as the last commit before this transaction began left them.
    began_with: Arc<Maps>,
    /// The transaction's writes, by map and key, for its commit to apply.
    writes: [BTreeMap<Vec<u8>, Write>; 3],
    /// The first failure the transaction met, a failed condition or a failed
    /// tree operation, which prevents its commit.
    failure: Option<Failure>,
    /// The maps' revision when the transaction began.
    revision: u64,
    /// The maps' topology revision when the transaction began.
    structure: u64,
    /// Whether the transaction relies on the topology it read, so that a
    /// commit that changed the topology since fails it.
    watches_structure: bool,
    /// Whether the transaction changes the topology, so that any commit that
    /// wrote since fails it.
    changes_structure: bool,
}

/// A buffered write: the value to store, `None` to remove the key, and the
/// value the commit expects to find there, `None` for absent, which is what
/// the transaction saw before its first write to the key.
#[derive(Debug)]
struct Write {
    value: Option<Vec<u8>>,
    expected: Option<Vec<u8>>,
}

impl MemoryTransaction {
    /// The value the transaction sees under `key`: its own write there, or
    /// else the value it began with.
    fn seen(&self, map: MapName, key: &[u8]) -> Option<&[u8]> {
        match self.writes[map as usize].get(key) {
            Some(write) => write.value.as_deref(),
            None => self.began_with.0[map as usize].get(key).map(Vec::as_slice),
        }
    }

    /// Buffer `value` under `key`, or the key's removal for `None`, if the
    /// transaction sees `expected` there: [`TreeError::Conflict`] otherwise,
    /// which fails the commit too.
    fn write(&mut self, map: MapName, key: &[u8], expected: Option<&[u8]>, value: Option<&[u8]>) -> Result<(), TreeError> {
        if self.seen(map, key) != expected {
            self.poison(Failure::Conflict);
            return Err(TreeError::Conflict);
        }
        let value = value.map(<[u8]>::to_vec);
        match self.writes[map as usize].entry(key.to_vec()) {
            // The first write's expectation, the value the transaction began
            // with, is the one the commit checks.
            Entry::Occupied(mut write) => write.get_mut().value = value,
            Entry::Vacant(slot) => {
                slot.insert(Write { value, expected: expected.map(<[u8]>::to_vec) });
            }
        }
        Ok(())
    }
}

#[async_trait]
impl MapAccess for MemoryTransaction {
    fn poison(&mut self, failure: Failure) { self.failure.get_or_insert(failure); }

    fn watch_structure(&mut self) { self.watches_structure = true; }

    fn change_structure(&mut self) { self.changes_structure = true; }

    async fn get(&mut self, map: MapName, key: &[u8]) -> Result<Option<Vec<u8>>, TreeError> { Ok(self.seen(map, key).map(<[u8]>::to_vec)) }

    async fn scan(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError> {
        if inverted(bounds) {
            return Ok(Vec::new());
        }
        let began_with = self.began_with.0[map as usize].range::<[u8], _>(bounds);
        let written = self.writes[map as usize].range::<[u8], _>(bounds);
        Ok(overlay(began_with, written, false).take(limit).map(|(key, value)| (key.clone(), value.clone())).collect())
    }

    async fn scan_reverse(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError> {
        if inverted(bounds) {
            return Ok(Vec::new());
        }
        let began_with = self.began_with.0[map as usize].range::<[u8], _>(bounds).rev();
        let written = self.writes[map as usize].range::<[u8], _>(bounds).rev();
        Ok(overlay(began_with, written, true).take(limit).map(|(key, value)| (key.clone(), value.clone())).collect())
    }

    async fn count_up_to(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<usize, TreeError> {
        if inverted(bounds) {
            return Ok(0);
        }
        let began_with = self.began_with.0[map as usize].range::<[u8], _>(bounds);
        let written = self.writes[map as usize].range::<[u8], _>(bounds);
        Ok(overlay(began_with, written, false).take(limit).count())
    }

    async fn insert(&mut self, map: MapName, key: &[u8], value: &[u8]) -> Result<(), TreeError> { self.write(map, key, None, Some(value)) }

    async fn put_if(&mut self, map: MapName, key: &[u8], expected: Option<&[u8]>, value: &[u8]) -> Result<(), TreeError> {
        self.write(map, key, expected, Some(value))
    }

    async fn delete_if(&mut self, map: MapName, key: &[u8], expected: &[u8]) -> Result<(), TreeError> {
        self.write(map, key, Some(expected), None)
    }

    async fn append(&mut self, map: MapName, key_prefix: &[u8], value: &[u8]) -> Result<(), TreeError> {
        let suffix = {
            let mut shared = self.shared.lock().unwrap();
            shared.appended += 1;
            shared.appended
        };
        self.write(map, &[key_prefix, &suffix.to_be_bytes()].concat(), None, Some(value))
    }
}

#[async_trait]
impl MapTransaction for MemoryTransaction {
    async fn commit(self) -> Result<(), TreeError> {
        let MemoryTransaction { shared, began_with, writes, failure, revision, structure, watches_structure, changes_structure } = self;
        if let Some(failure) = failure {
            return Err(failure.into());
        }
        // Letting go of the maps it began with lets a commit change them in
        // place when no other transaction holds them.
        drop(began_with);
        let mut shared = shared.lock().unwrap();
        if (watches_structure && structure != shared.structure) || (changes_structure && revision != shared.revision) {
            return Err(TreeError::Conflict);
        }
        let expectations_hold = shared.committed.0.iter().zip(&writes).all(|(committed, writes)| {
            writes.iter().all(|(key, write)| committed.get(key).map(Vec::as_slice) == write.expected.as_deref())
        });
        if !expectations_hold {
            return Err(TreeError::Conflict);
        }
        if writes.iter().all(BTreeMap::is_empty) {
            return Ok(());
        }
        shared.revision = shared.revision.checked_add(1).expect("transaction revision exhausted");
        if changes_structure {
            shared.structure = shared.structure.checked_add(1).expect("topology revision exhausted");
        }
        let committed = Arc::make_mut(&mut shared.committed);
        for (map, writes) in committed.0.iter_mut().zip(writes) {
            for (key, write) in writes {
                match write.value {
                    Some(value) => map.insert(key, value),
                    None => map.remove(&key),
                };
            }
        }
        Ok(())
    }
}

/// Whether `bounds` are inverted, holding nothing. `BTreeMap::range` panics on
/// such a range, so scans and counts return nothing for it instead.
fn inverted(bounds: ByteBounds<'_>) -> bool {
    match bounds {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Excluded(start), Bound::Excluded(end)) => start >= end,
        (Bound::Included(start) | Bound::Excluded(start), Bound::Included(end) | Bound::Excluded(end)) => start > end,
    }
}

/// The entries of a range as a transaction sees them, in key order: its own
/// writes over the values it began with, where a removal hides the key.
fn overlay<'a>(
    began_with: impl Iterator<Item = (&'a Vec<u8>, &'a Vec<u8>)>,
    written: impl Iterator<Item = (&'a Vec<u8>, &'a Write)>,
    reverse: bool,
) -> impl Iterator<Item = (&'a Vec<u8>, &'a Vec<u8>)> {
    let (mut began_with, mut written) = (began_with.peekable(), written.peekable());
    std::iter::from_fn(move || loop {
        let began_with_key = began_with.peek().map(|(key, _)| *key);
        let written_key = written.peek().map(|(key, _)| *key);
        match (began_with_key, written_key) {
            (None, None) => return None,
            (Some(old), new) if new.is_none_or(|new| if reverse { old > new } else { old < new }) => return began_with.next(),
            (old, _) => {
                if old == written_key {
                    began_with.next();
                }
                let (key, write) = written.next().expect("a written key was peeked");
                if let Some(value) = &write.value {
                    return Some((key, value));
                }
            }
        }
    })
}

#[cfg(test)]
mod tests;
