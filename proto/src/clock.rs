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
}
