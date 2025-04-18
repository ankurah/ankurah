use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use ulid::Ulid;
use uuid::Uuid;
use wasm_bindgen::JsValue;

use crate::{error::DecodeError, id::ID};

/// S set of event ids which create a dag of events
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct Clock(BTreeSet<ID>);

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ClockOrdering {
    Parent,
    Child,
    Sibling,
    Unrelated,
}

impl Clock {
    pub fn new(ids: impl Into<BTreeSet<ID>>) -> Self { Self(ids.into()) }

    pub fn as_slice(&self) -> &BTreeSet<ID> { &self.0 }

    pub fn to_strings(&self) -> Vec<String> { self.0.iter().map(|id| id.to_string()).collect() }

    pub fn from_strings(strings: Vec<String>) -> Result<Self, DecodeError> {
        let ids = strings.into_iter().map(|s| s.try_into()).collect::<Result<BTreeSet<_>, _>>()?;
        Ok(Self(ids))
    }

    pub fn insert(&mut self, id: ID) { self.0.insert(id); }

    pub fn len(&self) -> usize { self.0.len() }

    pub fn is_empty(&self) -> bool { self.0.is_empty() }
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

impl From<Vec<Uuid>> for Clock {
    fn from(uuids: Vec<Uuid>) -> Self {
        let ids = uuids
            .into_iter()
            .map(|uuid| {
                let ulid = Ulid::from(uuid);
                ID::from_ulid(ulid)
            })
            .collect();
        Self(ids)
    }
}

impl From<&Clock> for Vec<Uuid> {
    fn from(clock: &Clock) -> Self {
        clock
            .0
            .iter()
            .map(|id| {
                let ulid: Ulid = (*id).into();
                ulid.into()
            })
            .collect()
    }
}

impl std::fmt::Display for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "[{}]", self.to_strings().join(", ")) }
}
