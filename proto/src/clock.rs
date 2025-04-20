use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use wasm_bindgen::JsValue;

use crate::{error::DecodeError, EventID};

/// S set of event ids which create a dag of events
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct Clock(BTreeSet<EventID>);

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ClockOrdering {
    Parent,
    Child,
    Sibling,
    Unrelated,
}

impl Clock {
    pub fn new(ids: impl Into<BTreeSet<EventID>>) -> Self { Self(ids.into()) }

    pub fn as_slice(&self) -> &BTreeSet<EventID> { &self.0 }

    pub fn to_strings(&self) -> Vec<String> { self.0.iter().map(|id| id.to_string()).collect() }

    pub fn from_strings(strings: Vec<String>) -> Result<Self, DecodeError> {
        let ids = strings.into_iter().map(|s| s.try_into()).collect::<Result<BTreeSet<_>, _>>()?;
        Ok(Self(ids))
    }

    pub fn insert(&mut self, id: EventID) { self.0.insert(id); }

    pub fn len(&self) -> usize { self.0.len() }

    pub fn is_empty(&self) -> bool { self.0.is_empty() }

    pub fn to_bytes_vec(&self) -> Vec<[u8; 32]> { self.0.iter().map(|id| id.clone().to_bytes()).collect() }

    pub fn from_bytes_vec(bytes: Vec<[u8; 32]>) -> Self {
        let ids = bytes.into_iter().map(|b| EventID::from_bytes(b)).collect();
        Self(ids)
    }

    pub fn iter(&self) -> impl Iterator<Item = &EventID> { self.0.iter() }
}

impl TryFrom<JsValue> for Clock {
    type Error = DecodeError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        if value.is_undefined() || value.is_null() {
            return Ok(Clock::default());
        }
        let ids: Vec<String> =
            serde_wasm_bindgen::from_value(value).map_err(|e| DecodeError::Other(anyhow::anyhow!("Failed to parse clock: {}", e)))?;
        Self::from_strings(ids)
    }
}

impl From<&Clock> for JsValue {
    fn from(val: &Clock) -> Self {
        let strings = val.to_strings();
        // This should not be able to fail
        serde_wasm_bindgen::to_value(&strings).expect("Failed to serialize clock")
    }
}

impl From<Vec<EventID>> for Clock {
    fn from(ids: Vec<EventID>) -> Self { Self(ids.into_iter().collect()) }
}

impl From<&Clock> for Vec<EventID> {
    fn from(clock: &Clock) -> Self { clock.0.iter().cloned().collect() }
}

impl std::fmt::Display for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "[{}]", self.to_strings().join(", ")) }
}
