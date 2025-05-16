use serde::{Deserialize, Serialize};

use crate::{error::DecodeError, EventId};

/// S set of event ids which create a dag of events
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct Clock(pub(crate) Vec<EventId>);

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ClockOrdering {
    Parent,
    Child,
    Sibling,
    Unrelated,
}

impl Clock {
    pub fn new(ids: impl Into<Vec<EventId>>) -> Self { Self(ids.into()) }

    pub fn as_slice(&self) -> &[EventId] { &self.0 }

    pub fn to_strings(&self) -> Vec<String> { self.0.iter().map(|id| id.to_base64()).collect() }

    pub fn to_base64_short(&self) -> String { format!("[{}]", self.0.iter().map(|id| id.to_base64_short()).collect::<Vec<_>>().join(",")) }

    pub fn from_strings(strings: Vec<String>) -> Result<Self, DecodeError> {
        let mut ids = strings.into_iter().map(|s| s.try_into()).collect::<Result<Vec<_>, _>>()?;
        ids.sort();
        Ok(Self(ids))
    }

    pub fn contains(&self, id: &EventId) -> bool { self.0.binary_search(id).is_ok() }

    pub fn insert(&mut self, id: EventId) {
        // binary search for the insertion point, and don't insert if it's already present
        let index = self.0.binary_search(&id).unwrap_or_else(|i| i);
        if index == self.0.len() || self.0[index] != id {
            self.0.insert(index, id);
        }
    }

    pub fn len(&self) -> usize { self.0.len() }

    pub fn is_empty(&self) -> bool { self.0.is_empty() }

    pub fn iter(&self) -> impl Iterator<Item = &EventId> { self.0.iter() }
}

impl From<Vec<EventId>> for Clock {
    fn from(ids: Vec<EventId>) -> Self { Self(ids.into_iter().collect()) }
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
        Ok(Clock(ids.into_iter().collect()))
    }
}

impl From<&Clock> for Vec<EventId> {
    fn from(clock: &Clock) -> Self { clock.0.to_vec() }
}
impl From<EventId> for Clock {
    fn from(id: EventId) -> Self { Self(vec![id]) }
}

impl std::fmt::Display for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "[{}]", self.to_base64_short()) }
}
