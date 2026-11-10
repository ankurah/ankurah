use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::{Event, EventId};

/// A causal frontier, retaining the generation of each event it names.
///
/// Entries are ordered by event id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Clock {
    entries: Vec<(u32, EventId)>,
}

/// Why entries do not form a clock.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClockError {
    #[error("a clock entry of generation 0; generations start at 1")]
    ZeroGeneration,
    #[error("two clock entries name the same event")]
    RepeatedEvent,
    /// Only bytes hold entries out of order: a constructor sorts them.
    #[error("clock entries out of ascending order of event id")]
    OutOfOrder,
}

impl Clock {
    /// A clock of `entries` in any order; an entry given twice counts once.
    pub fn new(entries: impl Into<Vec<(u32, EventId)>>) -> Result<Self, ClockError> {
        let mut entries = entries.into();
        entries.sort_by(|a, b| a.1.cmp(&b.1));
        entries.dedup();
        Self::from_ascending(entries)
    }

    /// A clock of `entries` as its bytes hold them: in strictly ascending order of event id.
    fn from_ascending(entries: Vec<(u32, EventId)>) -> Result<Self, ClockError> {
        if entries.iter().any(|(generation, _)| *generation == 0) {
            return Err(ClockError::ZeroGeneration);
        }
        for pair in entries.windows(2) {
            match pair[0].1.cmp(&pair[1].1) {
                Ordering::Less => {}
                Ordering::Equal => return Err(ClockError::RepeatedEvent),
                Ordering::Greater => return Err(ClockError::OutOfOrder),
            }
        }
        Ok(Self { entries })
    }

    pub fn from_events<'a>(events: impl IntoIterator<Item = &'a Event>) -> Self {
        Self::new(events.into_iter().map(|event| (event.generation(), event.id())).collect::<Vec<_>>())
            .expect("event IDs determine their generations")
    }

    pub fn singleton(event: &Event) -> Self { Self { entries: vec![(event.generation(), event.id())] } }

    /// The head right after a genesis applies: generation 1, with the genesis as its one tip.
    pub fn genesis(id: EventId) -> Self { Self { entries: vec![(1, id)] } }

    /// Event IDs in lexicographic order.
    pub fn ids(&self) -> impl Iterator<Item = &EventId> { self.entries.iter().map(|(_, id)| id) }

    pub fn to_base64_short(&self) -> String { format!("{self:#}") }

    pub fn len(&self) -> usize { self.entries.len() }

    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    pub fn contains(&self, id: &EventId) -> bool { self.entries.binary_search_by(|(_, tip)| tip.cmp(id)).is_ok() }

    pub fn entries(&self) -> &[(u32, EventId)] { &self.entries }

    pub fn generation_of(&self, id: &EventId) -> Option<u32> {
        self.entries.binary_search_by(|(_, tip)| tip.cmp(id)).ok().map(|index| self.entries[index].0)
    }

    pub fn max_generation(&self) -> Option<u32> { self.entries.iter().map(|(generation, _)| *generation).max() }

    /// One above the greatest tip generation, saturating at `u32::MAX`.
    pub fn child_generation(&self) -> u32 { self.max_generation().unwrap_or(0).saturating_add(1) }

    /// Make `event` a tip. The caller first removes the tips it supersedes.
    pub fn join(&mut self, event: &Event) {
        // The event supplies its own generation; replace any snapshot annotation for this id.
        let entry = (event.generation(), event.id());
        match self.entries.binary_search_by(|(_, id)| id.cmp(&entry.1)) {
            Ok(index) => self.entries[index] = entry,
            Err(index) => self.entries.insert(index, entry),
        }
    }

    /// Remove a tip and its generation together.
    pub fn remove(&mut self, id: &EventId) -> bool {
        if let Ok(index) = self.entries.binary_search_by(|(_, tip)| tip.cmp(id)) {
            self.entries.remove(index);
            true
        } else {
            false
        }
    }
}

impl<'a> IntoIterator for &'a Clock {
    type Item = &'a (u32, EventId);
    type IntoIter = std::slice::Iter<'a, (u32, EventId)>;

    fn into_iter(self) -> Self::IntoIter { self.entries.iter() }
}

impl Serialize for Clock {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> { self.entries.serialize(serializer) }
}

/// Strict, since an update's id covers its parent clock: bytes from a peer or a store must hold
/// the one encoding of their entries.
impl<'de> Deserialize<'de> for Clock {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::from_ascending(Vec::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl std::fmt::Display for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for (i, (generation, id)) in self.entries.iter().enumerate() {
            if i != 0 {
                write!(f, ",")?;
            }
            if f.alternate() {
                write!(f, "{generation}:{id:#}")?;
            } else {
                write!(f, "{generation}:{id}")?;
            }
        }
        write!(f, "]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::{decode_exact, encode};
    use crate::{Identity, OperationSet};

    fn id(b: u8) -> EventId { EventId::from_bytes([b; 32]) }

    #[test]
    fn a_constructed_clock_sorts_its_entries_and_counts_an_entry_given_twice_once() {
        let head = Clock::new(vec![(2u32, id(3)), (7, id(1)), (7, id(1))]).unwrap();
        let expected = vec![(7, id(1)), (2, id(3))];
        assert_eq!(head.entries(), &expected);
        assert!(head.contains(&id(1)));
        assert_eq!(head.generation_of(&id(3)), Some(2));
        assert_eq!(head.max_generation(), Some(7));
        assert_eq!(head.child_generation(), 8);
        assert_eq!(encode(&head), encode(&expected));
    }

    #[test]
    fn a_constructed_clock_refuses_generation_zero_and_two_generations_for_one_event() {
        assert_eq!(Clock::new(vec![(0, id(1))]), Err(ClockError::ZeroGeneration));
        assert_eq!(Clock::new(vec![(2, id(1)), (7, id(1))]), Err(ClockError::RepeatedEvent));
    }

    /// Clock bytes that are not the one encoding of their entries are refused by every decoder.
    #[test]
    fn a_decoded_clock_holds_its_entries_in_strictly_ascending_order_or_is_refused() {
        let ascending = vec![(7u32, id(1)), (2, id(3))];
        assert_eq!(decode_exact::<Clock>(&encode(&ascending)).unwrap(), Clock::new(ascending.clone()).unwrap());
        assert_eq!(serde_json::from_str::<Clock>(&serde_json::to_string(&ascending).unwrap()).unwrap(), Clock::new(ascending).unwrap());
        for (entries, refusal) in [
            (vec![(2, id(3)), (7, id(1))], ClockError::OutOfOrder),
            (vec![(7, id(1)), (7, id(1))], ClockError::RepeatedEvent),
            (vec![(2, id(1)), (7, id(1))], ClockError::RepeatedEvent),
            (vec![(0, id(1))], ClockError::ZeroGeneration),
        ] {
            let bincode_error = decode_exact::<Clock>(&encode(&entries)).unwrap_err();
            assert_eq!(bincode_error.to_string(), refusal.to_string(), "{entries:?}");
            let json_error = serde_json::from_str::<Clock>(&serde_json::to_string(&entries).unwrap()).unwrap_err();
            assert!(json_error.to_string().starts_with(&refusal.to_string()), "{entries:?}: {json_error}");
        }
    }

    #[test]
    fn event_constructors_preserve_generations() {
        let a = Event::genesis(Identity::Ed25519([1; 32]), OperationSet::default());
        let b = Event::update(a.entity_id, Clock::singleton(&a), OperationSet::default());
        let c = Event::update(a.entity_id, Clock::singleton(&b), OperationSet::default());
        let d = Event::update(a.entity_id, Clock::singleton(&a), OperationSet::default());
        assert_eq!(Clock::genesis(a.id()), Clock::singleton(&a));
        let mut head = Clock::singleton(&b);
        assert!(head.remove(&b.id()));
        head.join(&c);
        head.join(&d);
        assert_eq!(head, Clock::from_events([&c, &d]));
        assert!(head.ids().is_sorted());
        assert_eq!(head.child_generation(), 4);
        let mut stale = Clock::new(vec![(99, c.id()), (2, d.id())]).unwrap();
        stale.join(&c);
        assert_eq!(stale, head);
        assert!(head.remove(&c.id()));
        assert_eq!(head, Clock::singleton(&d));
        assert_eq!(head.generation_of(&c.id()), None);
        assert!(!head.remove(&c.id()));
    }

    #[test]
    fn equality_includes_annotations_and_child_generation_saturates() {
        assert_ne!(Clock::new(vec![(2, id(1))]).unwrap(), Clock::new(vec![(3, id(1))]).unwrap());
        assert_eq!(Clock::new(vec![(u32::MAX, id(1))]).unwrap().child_generation(), u32::MAX);
        assert_eq!(Clock::default().child_generation(), 1);
    }
}
