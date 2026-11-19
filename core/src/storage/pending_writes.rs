//! Buffered map writes and reads that include those writes.
//!
//! Sled stages writes for its synchronous native transaction closure, and
//! IndexedDB stages them because a browser transaction cannot remain open
//! across arbitrary async work. PostgreSQL uses the same buffer to send writes
//! with its next read request or commit, saving a network round trip. Those
//! engines submit all pending writes before their storage commit completes.
//! SQLite writes directly into native transactions without this buffer.
//! The digest-tree adapter also uses this overlay for deferred persistence of
//! rebuildable trees. The owner determines when writes become visible and durable;
//! this helper supplies neither a commit boundary nor a recovery checkpoint.
//!
//! Engines add writes here until they submit them to storage. To answer reads
//! before submission, call [`PendingWrites::read`], execute the returned reads
//! against storage, then pass their answers to [`Overlay::answer`]. The overlay
//! merges stored entries with pending writes and preserves the requested order.
//! This module performs no I/O.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::iter::Peekable;
use std::ops::Bound;

use super::{MapId, MapRead, MapRows, MapWrite, ScanDirection};

/// Map writes yet to be submitted to storage.
#[derive(Clone, Default)]
pub struct PendingWrites {
    maps: BTreeMap<MapId, MapChanges>,
}

/// What pending writes do to one map.
#[derive(Clone, Default)]
pub struct MapChanges {
    /// Whether a `Clear` came before the writes in `keys`, so that none of
    /// the entries the map held before remain.
    pub cleared: bool,

    /// Each key written since: `Some` puts the value, `None` deletes the key.
    pub keys: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl PendingWrites {
    pub(crate) fn is_empty(&self) -> bool { self.maps.is_empty() }

    /// A pending value, deletion, or clear for this key. The outer `None`
    /// means these writes leave the stored value untouched.
    pub(crate) fn get(&self, map: &MapId, key: &[u8]) -> Option<Option<&[u8]>> {
        self.maps.get(map).and_then(|changes| written(changes, key))
    }

    /// Fold `writes`, in order, into those pending: a write replaces any
    /// pending under its key, and a clear every pending write to its map.
    pub fn add(&mut self, writes: Vec<MapWrite>) {
        for write in writes {
            match write {
                MapWrite::Put { map, key, value } => {
                    self.maps.entry(map).or_default().keys.insert(key, Some(value));
                }

                MapWrite::Delete { map, key } => {
                    self.maps.entry(map).or_default().keys.insert(key, None);
                }

                MapWrite::Clear { map } => {
                    self.maps.insert(map, MapChanges { cleared: true, keys: BTreeMap::new() });
                }
            }
        }
    }

    /// Every map's changes, taking them: none are pending after.
    pub fn take(&mut self) -> BTreeMap<MapId, MapChanges> { std::mem::take(&mut self.maps) }

    /// Plan reads that include pending writes: puts replace stored values,
    /// deletes hide them, and a clear hides the map's previously stored entries.
    ///
    /// Execute the returned reads against storage, then pass their answers to
    /// the returned overlay. Reads answered entirely by pending writes need no I/O.
    pub fn read(&self, reads: Vec<MapRead>) -> (Vec<MapRead>, Overlay<'_>) {
        let (mut stored, mut answers) = (Vec::new(), Vec::with_capacity(reads.len()));
        for read in reads {
            let (answer, needs) = self.begin(read);
            stored.extend(needs);
            answers.push(answer);
        }

        (stored, Overlay { answers })
    }

    /// Answer from pending writes alone, pass an untouched map's read through,
    /// or prepare an overlay and the storage read it needs.
    fn begin(&self, read: MapRead) -> (Answer<'_>, Option<MapRead>) {
        match &read {
            MapRead::Get { keys, .. } if keys.is_empty() => return (Answer::Given(MapRows::Got(Vec::new())), None),
            MapRead::Range { start, end, limit, .. } if *limit == 0 || holds_nothing(start.as_ref(), end.as_ref()) => {
                return (Answer::Given(MapRows::Ranged(Vec::new())), None);
            }
            _ => {}
        }

        let Some(changes) = self.maps.get(read.map()) else {
            let expected = match &read {
                MapRead::Get { keys, .. } => Expected::Got(keys.len()),
                MapRead::Range { .. } => Expected::Ranged,
            };
            return (Answer::Stored(expected), Some(read));
        };

        match read {
            MapRead::Get { map, keys } => {
                let unwritten: Vec<Vec<u8>> = keys.iter().filter(|key| written(changes, key).is_none()).cloned().collect();

                match unwritten.is_empty() {
                    true => {
                        let values = keys.iter().map(|key| written(changes, key).flatten().map(<[u8]>::to_vec)).collect();
                        (Answer::Given(MapRows::Got(values)), None)
                    }

                    false => {
                        let overlay = ReadOverlay { changes, read: Overlaid::Get { keys } };
                        (Answer::Over(overlay), Some(MapRead::Get { map, keys: unwritten }))
                    }
                }
            }

            MapRead::Range { map, start, end, direction, limit } => {
                let stored = match changes.cleared {
                    // A clear hides every stored entry.
                    true => None,

                    // Each pending delete within the bounds hides at most one
                    // stored entry, so this many stored entries still leave
                    // `limit` once the deletes hide theirs, where the map
                    // holds that many: one stored read always suffices.
                    false => {
                        let deleted = deletes_within(changes, &start, &end);
                        Some(MapRead::Range {
                            map,
                            start: start.clone(),
                            end: end.clone(),
                            direction,
                            limit: limit.saturating_add(deleted),
                        })
                    }
                };

                let overlay = ReadOverlay { changes, read: Overlaid::Range { start, end, direction, limit } };
                match stored {
                    None => (Answer::Given(overlay.answer(MapRows::Ranged(Vec::new()))), None),
                    Some(stored) => (Answer::Over(overlay), Some(stored)),
                }
            }
        }
    }
}

/// Completes a batch from [`PendingWrites::read`] by merging pending writes
/// with the engine's stored entries.
pub struct Overlay<'p> {
    answers: Vec<Answer<'p>>,
}

/// One read of a batch, as the pending writes leave it to be answered.
enum Answer<'p> {
    /// The pending writes alone gave its answer.
    Given(MapRows),

    /// The pending writes touch nothing of its map: the engine's answer to
    /// the read itself, which came with it, is its answer.
    Stored(Expected),

    /// It waits for the answer to the read of what the engine has stored that
    /// came with it.
    Over(ReadOverlay<'p>),
}

/// What an engine's answer to a read passed through to it must be: of the
/// read's kind, and for a get of as many values as keys.
enum Expected {
    Got(usize),
    Ranged,
}

impl Expected {
    /// `stored`, once checked.
    fn check(self, stored: MapRows) -> MapRows {
        match (&self, &stored) {
            (Self::Got(keys), MapRows::Got(values)) => {
                assert_eq!(values.len(), *keys, "an engine answered a get of {keys} keys with {} values", values.len())
            }
            (Self::Ranged, MapRows::Ranged(_)) => {}
            (_, _) => panic!("an engine answered a map read with rows of another kind"),
        }

        stored
    }
}

impl Overlay<'_> {
    /// Merge `stored` with pending writes and return answers in the original
    /// batch's order. `stored` must answer the reads returned alongside this
    /// overlay, in their order.
    ///
    /// # Panics
    ///
    /// If the engine returns the wrong number or kind of answers, or a get
    /// answer contains a different number of values than requested keys.
    pub fn answer(self, stored: Vec<MapRows>) -> Vec<MapRows> {
        let asked = self.answers.iter().filter(|answer| matches!(answer, Answer::Stored(_) | Answer::Over(_))).count();
        assert_eq!(stored.len(), asked, "an engine answered {asked} reads of what it has stored with {} answers", stored.len());

        let mut stored = stored.into_iter();
        self.answers
            .into_iter()
            .map(|answer| match answer {
                Answer::Given(rows) => rows,
                Answer::Stored(expected) => expected.check(stored.next().expect("a stored answer for every read that needed one")),
                Answer::Over(overlay) => overlay.answer(stored.next().expect("a stored answer for every read that needed one")),
            })
            .collect()
    }
}

/// The pending writes to one map, ready to lay over the stored entries one
/// read needed.
struct ReadOverlay<'p> {
    changes: &'p MapChanges,
    read: Overlaid,
}

/// The read an overlay answers.
enum Overlaid {
    Get { keys: Vec<Vec<u8>> },
    Range { start: Bound<Vec<u8>>, end: Bound<Vec<u8>>, direction: ScanDirection, limit: usize },
}

impl ReadOverlay<'_> {
    /// The read's answer: `stored`, the engine's answer to the read of what
    /// it has stored that came with this overlay, with the pending writes
    /// laid over it.
    fn answer(self, stored: MapRows) -> MapRows {
        match (self.read, stored) {
            (Overlaid::Get { keys }, MapRows::Got(stored)) => {
                let asked = keys.iter().filter(|key| written(self.changes, key).is_none()).count();
                assert_eq!(stored.len(), asked, "an engine answered a get of {asked} keys with {} values", stored.len());

                let mut stored = stored.into_iter();
                MapRows::Got(
                    keys.iter()
                        .map(|key| match written(self.changes, key) {
                            Some(value) => value.map(<[u8]>::to_vec),
                            None => stored.next().expect("a stored value for every key the pending writes leave unwritten"),
                        })
                        .collect(),
                )
            }

            (Overlaid::Range { start, end, direction, limit }, MapRows::Ranged(stored)) => {
                let written = self.changes.keys.range::<Vec<u8>, _>((start.as_ref(), end.as_ref()));
                let merged = match direction {
                    ScanDirection::Forward => merge(stored.into_iter().peekable(), written.peekable(), direction, limit),
                    ScanDirection::Reverse => merge(stored.into_iter().peekable(), written.rev().peekable(), direction, limit),
                };

                MapRows::Ranged(merged)
            }

            (_, _) => panic!("an engine answered a map read with rows of another kind"),
        }
    }
}

/// Whether no key lies between the bounds: a start past the end, or on it
/// with either excluded. A range read over them reads nothing, and ordered
/// stores refuse them: `BTreeMap::range` panics on such bounds, and
/// IndexedDB's key ranges throw.
pub fn holds_nothing<K: Ord + ?Sized>(start: Bound<&K>, end: Bound<&K>) -> bool {
    match (start, end) {
        (Bound::Included(start), Bound::Included(end)) => start > end,
        (Bound::Included(start) | Bound::Excluded(start), Bound::Included(end) | Bound::Excluded(end)) => start >= end,
        _ => false,
    }
}

/// What the pending writes to a map, `changes`, leave under `key`, where they
/// decide it: the value a put left, or `None` where a delete or the map's
/// clear removed it. `None` where they leave the stored value as it is.
fn written<'p>(changes: &'p MapChanges, key: &[u8]) -> Option<Option<&'p [u8]>> {
    match changes.keys.get(key) {
        Some(value) => Some(value.as_deref()),
        None => changes.cleared.then_some(None),
    }
}

/// How many of `changes`' pending deletes lie between the bounds, which hold
/// some key.
fn deletes_within(changes: &MapChanges, start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> usize {
    changes.keys.range::<Vec<u8>, _>((start.as_ref(), end.as_ref())).filter(|(_, value)| value.is_none()).count()
}

/// The first `limit` of the stored entries and the written keys, both in
/// `direction`'s order, merged in that order: a written key replaces or
/// removes the stored entry under it.
fn merge<'p>(
    mut stored: Peekable<impl Iterator<Item = (Vec<u8>, Vec<u8>)>>,
    mut written: Peekable<impl Iterator<Item = (&'p Vec<u8>, &'p Option<Vec<u8>>)>>,
    direction: ScanDirection,
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut merged = Vec::new();
    while merged.len() < limit {
        let order = match (stored.peek(), written.peek()) {
            (None, None) => break,
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some((stored, _)), Some((written, _))) => match direction {
                ScanDirection::Forward => stored.cmp(written),
                ScanDirection::Reverse => (*written).cmp(stored),
            },
        };

        if order.is_lt() {
            merged.extend(stored.next());
            continue;
        }

        if order.is_eq() {
            stored.next();
        }

        let (key, value) = written.next().expect("a write is next");
        if let Some(value) = value {
            merged.push((key.clone(), value.clone()));
        }
    }

    merged
}

#[cfg(test)]
mod tests;
