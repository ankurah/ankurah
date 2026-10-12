//! The memory-resident maps that hold a tree's rows, and the transactions over
//! them.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::error::TreeError;
use crate::map::{ByteBounds, MapName, MapTransaction};

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

#[derive(Debug, Default)]
struct Shared {
    /// The maps as the last commit left them.
    committed: Arc<Maps>,
    /// How many values have been appended: the next one's suffix.
    appended: u64,
    revision: u64,
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
            shared: self.shared.clone(), began_with: shared.committed.clone(), writes: Default::default(), failed: false,
            revision: shared.revision, structure: shared.structure, watches_structure: false, changes_structure: false,
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
    /// Whether a write's condition failed, which fails the commit.
    failed: bool,
    revision: u64,
    structure: u64,
    watches_structure: bool,
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
            self.failed = true;
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
impl MapTransaction for MemoryTransaction {
    fn watch_structure(&mut self) { self.watches_structure = true; }

    fn change_structure(&mut self) { self.changes_structure = true; }

    async fn get(&mut self, map: MapName, key: &[u8]) -> Result<Option<Vec<u8>>, TreeError> { Ok(self.seen(map, key).map(<[u8]>::to_vec)) }

    async fn scan(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError> {
        // `BTreeMap::range` panics on an inverted range; such a range holds nothing.
        let inverted = match bounds {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
            (Bound::Excluded(start), Bound::Excluded(end)) => start >= end,
            (Bound::Included(start) | Bound::Excluded(start), Bound::Included(end) | Bound::Excluded(end)) => start > end,
        };
        if inverted {
            return Ok(Vec::new());
        }
        let began_with = self.began_with.0[map as usize].range::<[u8], _>(bounds);
        let written = self.writes[map as usize].range::<[u8], _>(bounds);
        Ok(overlay(began_with, written, false).take(limit).map(|(key, value)| (key.clone(), value.clone())).collect())
    }

    async fn scan_reverse(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError> {
        let owned = crate::AddressRange::new(bounds.0.map(<[u8]>::to_vec), bounds.1.map(<[u8]>::to_vec));
        if crate::bounds::empty(&owned) { return Ok(Vec::new()); }
        let began_with = self.began_with.0[map as usize].range::<[u8], _>(bounds).rev();
        let written = self.writes[map as usize].range::<[u8], _>(bounds).rev();
        Ok(overlay(began_with, written, true).take(limit).map(|(key, value)| (key.clone(), value.clone())).collect())
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

    async fn commit(self) -> Result<(), TreeError> {
        let MemoryTransaction { shared, began_with, writes, failed, revision, structure, watches_structure, changes_structure } = self;
        if failed {
            return Err(TreeError::Conflict);
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
mod tests {
    use super::*;
    use std::ops::Bound::{Excluded, Included, Unbounded};
    use MapName::{Leaves, Lookup, Nodes};

    /// Some entries, as text.
    fn text(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<(String, String)> {
        entries.into_iter().map(|(key, value)| (String::from_utf8(key).unwrap(), String::from_utf8(value).unwrap())).collect()
    }

    /// The keys of some entries, as text.
    fn keys(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<String> { text(entries).into_iter().map(|(key, _)| key).collect() }

    #[tokio::test]
    async fn each_conditional_write_succeeds_on_its_condition_and_conflicts_otherwise() {
        let maps = MemoryMaps::new();
        let mut transaction = maps.begin();
        assert_eq!(transaction.get(Leaves, b"k").await.unwrap(), None);
        transaction.insert(Leaves, b"k", b"first").await.unwrap();
        assert_eq!(transaction.get(Leaves, b"k").await.unwrap(), Some(b"first".to_vec()), "its own write");
        transaction.put_if(Leaves, b"k", Some(b"first"), b"second").await.unwrap();
        transaction.put_if(Leaves, b"new", None, b"value").await.unwrap();
        transaction.put_if(Leaves, b"gone", None, b"value").await.unwrap();
        transaction.delete_if(Leaves, b"gone", b"value").await.unwrap();
        transaction.put_if(Leaves, b"", None, b"").await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = maps.begin();
        assert_eq!(transaction.get(Leaves, b"k").await.unwrap(), Some(b"second".to_vec()));
        assert_eq!(transaction.get(Leaves, b"new").await.unwrap(), Some(b"value".to_vec()));
        assert_eq!(transaction.get(Leaves, b"gone").await.unwrap(), None);
        assert_eq!(transaction.get(Leaves, b"").await.unwrap(), Some(Vec::new()), "the empty key and the empty value are values");
        assert_eq!(transaction.get(Lookup, b"k").await.unwrap(), None, "each map has keys of its own");

        // A condition that fails conflicts at once and fails the commit, so
        // nothing the transaction wrote lands.
        for condition in [
            "insert needs the key absent",
            "put_if of absent finds a value",
            "put_if finds another value",
            "put_if of a value finds none",
            "delete_if finds another value",
            "delete_if finds none",
        ] {
            let mut transaction = maps.begin();
            transaction.insert(Lookup, b"written first", b"x").await.unwrap();
            let failed = match condition {
                "insert needs the key absent" => transaction.insert(Leaves, b"k", b"again").await,
                "put_if of absent finds a value" => transaction.put_if(Leaves, b"k", None, b"x").await,
                "put_if finds another value" => transaction.put_if(Leaves, b"k", Some(b"other"), b"x").await,
                "put_if of a value finds none" => transaction.put_if(Leaves, b"absent", Some(b"x"), b"y").await,
                "delete_if finds another value" => transaction.delete_if(Leaves, b"k", b"other").await,
                _ => transaction.delete_if(Leaves, b"absent", b"x").await,
            };
            assert!(matches!(failed, Err(TreeError::Conflict)), "{condition}");
            assert!(matches!(transaction.commit().await, Err(TreeError::Conflict)), "{condition}");
            assert_eq!(maps.begin().get(Lookup, b"written first").await.unwrap(), None, "{condition}");
        }
        assert_eq!(maps.begin().get(Leaves, b"k").await.unwrap(), Some(b"second".to_vec()));
    }

    /// A transaction reads the maps it began with, whatever commits land
    /// meanwhile, and its own writes over them, in gets and scans alike.
    #[tokio::test]
    async fn a_transaction_reads_the_maps_it_began_with_and_its_own_writes() {
        let maps = MemoryMaps::new();
        let mut setup = maps.begin();
        setup.insert(Nodes, b"a", b"1").await.unwrap();
        setup.insert(Nodes, b"c", b"3").await.unwrap();
        setup.commit().await.unwrap();

        let mut reader = maps.begin();
        let mut writer = maps.begin();
        writer.put_if(Nodes, b"a", Some(b"1"), b"one").await.unwrap();
        writer.insert(Nodes, b"b", b"2").await.unwrap();
        writer.delete_if(Nodes, b"c", b"3").await.unwrap();
        writer.commit().await.unwrap();

        let all = (Unbounded, Unbounded);
        assert_eq!(reader.get(Nodes, b"a").await.unwrap(), Some(b"1".to_vec()));
        assert_eq!(text(reader.scan(Nodes, all, usize::MAX).await.unwrap()), [("a".into(), "1".into()), ("c".into(), "3".into())]);

        // A new key, a replaced value and a removal, over what it began with.
        reader.insert(Nodes, b"d", b"4").await.unwrap();
        reader.put_if(Nodes, b"a", Some(b"1"), b"uno").await.unwrap();
        reader.delete_if(Nodes, b"c", b"3").await.unwrap();
        assert_eq!(text(reader.scan(Nodes, all, usize::MAX).await.unwrap()), [("a".into(), "uno".into()), ("d".into(), "4".into())]);
        assert_eq!(keys(reader.scan(Nodes, all, 1).await.unwrap()), ["a"], "the limit counts what it sees");
        assert_eq!(reader.get(Nodes, b"c").await.unwrap(), None);

        // The writer changed what it expected, so none of its writes land.
        assert!(matches!(reader.commit().await, Err(TreeError::Conflict)));
        let after = text(maps.begin().scan(Nodes, all, usize::MAX).await.unwrap());
        assert_eq!(after, [("a".into(), "one".into()), ("b".into(), "2".into())]);
    }

    /// Two transactions that write one key, in different maps from their
    /// other writes: the first commit lands whole, and the second fails whole.
    #[tokio::test]
    async fn a_commit_applies_all_its_writes_across_the_maps_or_none() {
        let maps = MemoryMaps::new();
        let mut first = maps.begin();
        let mut second = maps.begin();
        first.insert(Leaves, b"first row", b"1").await.unwrap();
        first.put_if(Lookup, b"state", None, b"first").await.unwrap();
        second.insert(Leaves, b"second row", b"2").await.unwrap();
        second.put_if(Lookup, b"state", None, b"second").await.unwrap();
        first.commit().await.unwrap();
        assert!(matches!(second.commit().await, Err(TreeError::Conflict)), "it expected no state");

        let mut after = maps.begin();
        assert_eq!(after.get(Lookup, b"state").await.unwrap(), Some(b"first".to_vec()));
        assert_eq!(after.get(Leaves, b"first row").await.unwrap(), Some(b"1".to_vec()));
        assert_eq!(after.get(Leaves, b"second row").await.unwrap(), None, "its other write did not land either");
    }

    #[tokio::test]
    async fn appends_under_one_prefix_take_distinct_keys_within_its_range() {
        let maps = MemoryMaps::new();
        let (mut first, mut second) = (maps.begin(), maps.begin());
        first.append(Nodes, b"p/", b"one").await.unwrap();
        second.append(Nodes, b"p/", b"two").await.unwrap();
        first.append(Nodes, b"q/", b"other").await.unwrap();
        second.commit().await.unwrap();
        first.commit().await.unwrap();
        let under_p = maps.begin().scan(Nodes, (Included(b"p/"), Excluded(b"p0")), usize::MAX).await.unwrap();
        assert_eq!(under_p.len(), 2, "appends never conflict");
        assert!(under_p.iter().all(|(key, _)| key.starts_with(b"p/") && key.len() > 2));
        assert_ne!(under_p[0].0, under_p[1].0);
        let values: Vec<&[u8]> = under_p.iter().map(|(_, value)| value.as_slice()).collect();
        assert_eq!(values, [b"one".as_slice(), b"two"], "in the order appended");
    }

    #[tokio::test]
    async fn scans_follow_the_bounds_and_the_limit_in_key_order() {
        let maps = MemoryMaps::new();
        let mut setup = maps.begin();
        for key in [b"d", b"b", b"a", b"c"] {
            setup.insert(Leaves, key, key).await.unwrap();
        }
        setup.commit().await.unwrap();
        let mut map = maps.begin();
        assert_eq!(keys(map.scan(Leaves, (Unbounded, Unbounded), usize::MAX).await.unwrap()), ["a", "b", "c", "d"]);
        assert_eq!(keys(map.scan(Leaves, (Included(b"b"), Excluded(b"d")), usize::MAX).await.unwrap()), ["b", "c"]);
        assert_eq!(keys(map.scan(Leaves, (Excluded(b"b"), Included(b"d")), usize::MAX).await.unwrap()), ["c", "d"]);
        assert_eq!(keys(map.scan(Leaves, (Excluded(b"b"), Excluded(b"c")), usize::MAX).await.unwrap()), [] as [&str; 0]);
        assert_eq!(
            keys(map.scan(Leaves, (Included(b"bb"), Unbounded), usize::MAX).await.unwrap()),
            ["c", "d"],
            "a bound need not be a key"
        );

        assert_eq!(keys(map.scan(Leaves, (Unbounded, Unbounded), 2).await.unwrap()), ["a", "b"]);
        assert_eq!(keys(map.scan(Leaves, (Excluded(b"b"), Unbounded), 1).await.unwrap()), ["c"], "the next page");
        assert_eq!(keys(map.scan(Leaves, (Unbounded, Unbounded), 0).await.unwrap()), [] as [&str; 0]);

        // Inverted bounds hold nothing rather than panicking.
        assert_eq!(keys(map.scan(Leaves, (Included(b"d"), Excluded(b"b")), usize::MAX).await.unwrap()), [] as [&str; 0]);
        assert_eq!(keys(map.scan(Leaves, (Excluded(b"b"), Excluded(b"b")), usize::MAX).await.unwrap()), [] as [&str; 0]);
        assert_eq!(keys(map.scan(Leaves, (Included(b"b"), Included(b"b")), usize::MAX).await.unwrap()), ["b"]);
    }
}
