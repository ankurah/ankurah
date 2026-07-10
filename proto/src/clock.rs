use serde::{Deserialize, Serialize};

use crate::{error::DecodeError, EventId};

/// Set of event ids which represents a head in a DAG of events
///
/// The inner vec is always sorted and deduplicated: `contains`/`insert`/
/// `remove` binary-search, so sortedness is a load-bearing invariant. Every
/// construction path (including deserialization of peer-supplied clocks)
/// normalizes rather than trusting input order.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
#[serde(from = "Vec<EventId>")]
pub struct Clock(pub(crate) Vec<EventId>);

fn normalized(mut ids: Vec<EventId>) -> Vec<EventId> {
    ids.sort();
    ids.dedup();
    ids
}

impl Clock {
    pub fn new(ids: impl Into<Vec<EventId>>) -> Self { Self(normalized(ids.into())) }

    pub fn as_slice(&self) -> &[EventId] { &self.0 }

    pub fn to_strings(&self) -> Vec<String> { self.0.iter().map(|id| id.to_base64()).collect() }

    pub fn to_base64_short(&self) -> String { format!("[{}]", self.0.iter().map(|id| id.to_base64_short()).collect::<Vec<_>>().join(",")) }

    pub fn to_base64(&self) -> String { format!("[{}]", self.0.iter().map(|id| id.to_base64()).collect::<Vec<_>>().join(",")) }

    pub fn from_strings(strings: Vec<String>) -> Result<Self, DecodeError> {
        let ids = strings.into_iter().map(|s| s.try_into()).collect::<Result<Vec<_>, _>>()?;
        Ok(Self(normalized(ids)))
    }

    pub fn contains(&self, id: &EventId) -> bool { self.0.binary_search(id).is_ok() }

    pub fn insert(&mut self, id: EventId) {
        // binary search for the insertion point, and don't insert if it's already present
        let index = self.0.binary_search(&id).unwrap_or_else(|i| i);
        if index == self.0.len() || self.0[index] != id {
            self.0.insert(index, id);
        }
    }

    /// Remove an event ID from the clock if present.
    /// Returns true if the ID was present and removed.
    pub fn remove(&mut self, id: &EventId) -> bool {
        if let Ok(index) = self.0.binary_search(id) {
            self.0.remove(index);
            true
        } else {
            false
        }
    }

    /// Creates a clone of the clock with the given event inserted
    pub fn with_event(&self, id: EventId) -> Self {
        let mut n = self.clone();
        n.insert(id);
        n
    }

    pub fn len(&self) -> usize { self.0.len() }

    pub fn is_empty(&self) -> bool { self.0.is_empty() }

    pub fn iter(&self) -> impl Iterator<Item = &EventId> { self.0.iter() }

    pub fn to_vec(&self) -> Vec<EventId> { self.0.clone() }
}

impl From<Vec<EventId>> for Clock {
    fn from(ids: Vec<EventId>) -> Self { Self(normalized(ids)) }
}
impl TryInto<Clock> for Vec<Vec<u8>> {
    type Error = DecodeError;

    fn try_into(self) -> Result<Clock, Self::Error> {
        let mut ids: Vec<EventId> = Vec::new();
        for id_bytes in self {
            let bytes: [u8; 32] = id_bytes.try_into().map_err(|_| DecodeError::InvalidLength)?;
            let id = EventId::from_bytes(bytes);
            ids.push(id);
        }
        Ok(Clock(normalized(ids)))
    }
}

impl From<&Clock> for Vec<EventId> {
    fn from(clock: &Clock) -> Self { clock.0.to_vec() }
}
impl From<EventId> for Clock {
    fn from(id: EventId) -> Self { Self(vec![id]) }
}

impl std::fmt::Display for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            write!(f, "{}", self.to_base64_short())
        } else {
            write!(f, "{}", self.to_base64())
        }
    }
}

/// A materialized head annotated with each tip's event GENERATION (D2 plan
/// REV 5 section K): every entry pairs one head tip's id with the generation
/// its event payload carries, pinned from the admission-verified stamp when
/// the tip joined the head and dropped when the tip is superseded. The head
/// never regresses, so entries never need revision; maintenance is exact,
/// O(1) per head change, and read-free.
///
/// `GClock` exists ALONGSIDE the unchanged [`Clock`] and lives in exactly
/// three homes: (1) [`crate::State`] (and therefore the wire
/// `StateFragment`), (2) the resident entity's in-memory state, and (3) each
/// storage engine's persisted entity record, so rehydration reconstitutes it
/// without reading events. `Event.parent` keeps the plain `Clock` (no
/// id-formula change). Anywhere else, generation values may exist only as
/// transient locals inside a comparison walk (the registry ban, REV 5
/// section A, with this materialization as its sole carve-out).
///
/// CANONICAL ENTRY ORDER (pinned by the M4 brief; serde stability and
/// set-comparison against plain `Clock` heads require one): entries are
/// sorted ascending by the `(u32, EventId)` tuple's DERIVED order, i.e.
/// generation-major with EventId tiebreak, matching the comparison
/// frontier's deterministic ordering (max-generation-first, EventId
/// tiebreak), and deduplicated by id (a head is a set of tips; on
/// duplicate-id input the smallest tuple deterministically wins). The
/// canonical order makes `max_generation` the last entry. Every construction
/// path, deserialization of peer-supplied values included, normalizes rather
/// than trusting input order.
///
/// TRUST: entries carry their values unconditionally (stamping needs the max
/// over all tips regardless, and a wrong inherited value self-defeats at the
/// next durable admission); acceleration ELIGIBILITY is answered at
/// consumption time by the node's unverified-events id set, so no per-entry
/// flag exists here. A durable node never adopts a wire-carried entry that
/// contradicts an event payload it holds (validated at application); an
/// ephemeral node adopts entries inside the same trust envelope as the state
/// itself.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
#[serde(from = "Vec<(u32, EventId)>")]
pub struct GClock(pub(crate) Vec<(u32, EventId)>);

fn gclock_normalized(mut entries: Vec<(u32, EventId)>) -> Vec<(u32, EventId)> {
    entries.sort();
    let mut seen = std::collections::HashSet::new();
    entries.retain(|(_, id)| seen.insert(id.clone()));
    entries
}

impl GClock {
    pub fn new(entries: impl Into<Vec<(u32, EventId)>>) -> Self { Self(gclock_normalized(entries.into())) }

    pub fn as_slice(&self) -> &[(u32, EventId)] { &self.0 }

    pub fn len(&self) -> usize { self.0.len() }

    pub fn is_empty(&self) -> bool { self.0.is_empty() }

    pub fn iter(&self) -> impl Iterator<Item = &(u32, EventId)> { self.0.iter() }

    /// The maximum generation over all entries: the stamping operand
    /// (`1 + max`) and the P1 precheck operand. O(1) by the canonical
    /// generation-major order (the last entry carries the max).
    pub fn max_generation(&self) -> Option<u32> { self.0.last().map(|(g, _)| *g) }

    /// The materialized generation for one tip id, if present. Heads are
    /// tiny (almost always one entry), so the linear scan is the right tool.
    pub fn generation_of(&self, id: &EventId) -> Option<u32> { self.0.iter().find(|(_, eid)| eid == id).map(|(g, _)| *g) }

    /// Pin an entry for a tip joining the head. Any existing entry for the
    /// same id is replaced (an id appears at most once).
    pub fn insert(&mut self, generation: u32, id: EventId) {
        self.remove(&id);
        let entry = (generation, id);
        let index = self.0.binary_search(&entry).unwrap_or_else(|i| i);
        self.0.insert(index, entry);
    }

    /// Drop a superseded tip's entry. Returns true if an entry was removed.
    pub fn remove(&mut self, id: &EventId) -> bool {
        if let Some(index) = self.0.iter().position(|(_, eid)| eid == id) {
            self.0.remove(index);
            true
        } else {
            false
        }
    }

    /// Set-comparison against a plain `Clock` head: true iff the entries
    /// annotate exactly the head's tips (same ids, each exactly once). The
    /// load-bearing structural invariant everywhere a GClock travels next to
    /// a head.
    pub fn matches_head(&self, head: &Clock) -> bool { self.0.len() == head.len() && self.0.iter().all(|(_, id)| head.contains(id)) }
}

impl From<Vec<(u32, EventId)>> for GClock {
    fn from(entries: Vec<(u32, EventId)>) -> Self { Self(gclock_normalized(entries)) }
}

impl From<(u32, EventId)> for GClock {
    fn from(entry: (u32, EventId)) -> Self { Self(vec![entry]) }
}

impl std::fmt::Display for GClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", self.0.iter().map(|(g, id)| format!("g{} {}", g, id.to_base64_short())).collect::<Vec<_>>().join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(b: u8) -> EventId { EventId::from_bytes([b; 32]) }

    #[test]
    fn constructors_normalize() {
        let unsorted = vec![id(3), id(1), id(2), id(1)];
        for clock in [Clock::new(unsorted.clone()), Clock::from(unsorted.clone())] {
            assert_eq!(clock.as_slice(), &[id(1), id(2), id(3)], "sorted and deduplicated");
        }

        let raw: Vec<Vec<u8>> = unsorted.iter().map(|i| i.as_bytes().to_vec()).collect();
        let clock: Clock = raw.try_into().unwrap();
        assert_eq!(clock.as_slice(), &[id(1), id(2), id(3)]);

        let strings: Vec<String> = unsorted.iter().map(|i| i.to_base64()).collect();
        let clock = Clock::from_strings(strings).unwrap();
        assert_eq!(clock.as_slice(), &[id(1), id(2), id(3)]);
    }

    /// A peer-supplied unsorted clock must not defeat binary-search
    /// membership: deserialization normalizes rather than trusting wire
    /// order. The raw vec on the wire stands in for a clock produced by a
    /// buggy or malicious peer.
    #[test]
    fn deserialization_normalizes_unsorted_wire_clock() {
        let unsorted = vec![id(3), id(1), id(2)];
        let bytes = bincode::serialize(&unsorted).unwrap();
        let mut clock: Clock = bincode::deserialize(&bytes).unwrap();

        assert_eq!(clock.as_slice(), &[id(1), id(2), id(3)]);
        assert!(clock.contains(&id(1)));
        assert!(clock.contains(&id(3)));
        assert!(clock.remove(&id(2)));
        assert_eq!(clock.as_slice(), &[id(1), id(3)]);

        // Round-trip stays normalized.
        let bytes = bincode::serialize(&clock).unwrap();
        let clock: Clock = bincode::deserialize(&bytes).unwrap();
        assert_eq!(clock.as_slice(), &[id(1), id(3)]);
    }

    /// Head-maintenance regression: with an unsorted head [C, B, E], removing
    /// the meet ancestor B binary-searched the wrong region and missed,
    /// leaving a redundant non-antichain tip in the head.
    #[test]
    fn head_maintenance_remove_does_not_miss() {
        let (b, c, e) = (id(2), id(3), id(5));
        // Given in the unsorted order [C, B, E].
        let mut head = Clock::new(vec![c.clone(), b.clone(), e.clone()]);
        assert!(head.remove(&b), "meet ancestor must be found and removed");
        assert!(!head.contains(&b));
        assert_eq!(head.as_slice(), &[c, e]);
    }

    /// GClock's pinned canonical entry order (M4 brief): ascending by the
    /// (u32, EventId) tuple's derived order, generation-major with EventId
    /// tiebreak. Every construction path normalizes to it.
    #[test]
    fn gclock_canonical_order_is_generation_major_with_id_tiebreak() {
        let shuffled = vec![(3, id(1)), (2, id(9)), (2, id(4)), (1, id(7))];
        let canonical = vec![(1, id(7)), (2, id(4)), (2, id(9)), (3, id(1))];
        assert_eq!(GClock::new(shuffled.clone()).as_slice(), canonical.as_slice());
        assert_eq!(GClock::from(shuffled).as_slice(), canonical.as_slice());
        // insert maintains the order and max_generation is the last entry.
        let mut g = GClock::default();
        for (gen, eid) in &canonical {
            g.insert(*gen, eid.clone());
        }
        assert_eq!(g.as_slice(), canonical.as_slice());
        assert_eq!(g.max_generation(), Some(3));
        assert_eq!(g.generation_of(&id(4)), Some(2));
        assert!(g.remove(&id(4)) && !g.remove(&id(4)));
    }

    /// An id appears at most once (a head is a set of tips): duplicate-id
    /// input deduplicates deterministically, keeping the smallest tuple, and
    /// insert replaces an existing entry for the same id.
    #[test]
    fn gclock_deduplicates_by_id() {
        let g = GClock::new(vec![(5, id(2)), (3, id(2)), (4, id(1))]);
        assert_eq!(g.as_slice(), &[(3, id(2)), (4, id(1))]);

        let mut g = GClock::from((2, id(8)));
        g.insert(7, id(8));
        assert_eq!(g.as_slice(), &[(7, id(8))], "insert replaces the entry for an existing id");
    }

    /// Serde stability and determinism: a canonical GClock round-trips
    /// byte-identically through bincode, and a peer-supplied UNSORTED wire
    /// value normalizes on deserialization rather than trusting wire order
    /// (mirroring Clock's deserialization invariant).
    #[test]
    fn gclock_serde_is_stable_and_normalizes_wire_input() {
        let canonical = GClock::new(vec![(1, id(7)), (2, id(4)), (2, id(9))]);
        let bytes = bincode::serialize(&canonical).unwrap();
        let reread: GClock = bincode::deserialize(&bytes).unwrap();
        assert_eq!(reread, canonical);
        assert_eq!(bincode::serialize(&reread).unwrap(), bytes, "canonical form re-serializes byte-identically");

        // Raw unsorted (and duplicated) pairs on the wire stand in for a
        // buggy or malicious peer; deserialization must normalize.
        let raw: Vec<(u32, EventId)> = vec![(2, id(9)), (1, id(7)), (2, id(4)), (9, id(7))];
        let wire = bincode::serialize(&raw).unwrap();
        let gclock: GClock = bincode::deserialize(&wire).unwrap();
        assert_eq!(gclock, canonical);
        assert_eq!(bincode::serialize(&gclock).unwrap(), bytes, "normalized wire input reaches the same canonical bytes");
    }

    /// matches_head is exact set comparison against a plain Clock head.
    #[test]
    fn gclock_matches_head_is_exact() {
        let g = GClock::new(vec![(2, id(4)), (1, id(7))]);
        assert!(g.matches_head(&Clock::new(vec![id(4), id(7)])));
        assert!(!g.matches_head(&Clock::new(vec![id(4)])), "extra entry must not match");
        assert!(!g.matches_head(&Clock::new(vec![id(4), id(7), id(9)])), "missing entry must not match");
        assert!(!g.matches_head(&Clock::new(vec![id(4), id(9)])), "wrong id must not match");
        assert!(GClock::default().matches_head(&Clock::default()), "empty annotates empty");
    }
}
