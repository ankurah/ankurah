use std::{
    any::Any,
    collections::{btree_map::Entry, BTreeMap},
    fmt::Debug,
    sync::{Arc, RwLock},
};

use ankurah_proto::{EntityId, Operation};
use serde::{Deserialize, Serialize};

use crate::{
    error::{MutationError, StateError},
    property::{backend::PropertyBackend, PropertyName, PropertyValue},
};

const REF_DIFF_VERSION: u8 = 1;

#[derive(Clone, Debug)]
pub struct RefBackend {
    values: Arc<RwLock<BTreeMap<PropertyName, Option<EntityId>>>>,
}

#[derive(Serialize, Deserialize)]
pub struct RefDiff {
    version: u8,
    data: Vec<u8>,
}

impl Default for RefBackend {
    fn default() -> Self { Self::new() }
}

impl RefBackend {
    pub fn new() -> RefBackend { Self { values: Arc::new(RwLock::new(BTreeMap::default())) } }

    pub fn set(&self, property_name: PropertyName, value: Option<EntityId>) {
        let mut values = self.values.write().unwrap();
        match values.entry(property_name) {
            Entry::Occupied(mut entry) => {
                entry.insert(value);
            }
            Entry::Vacant(entry) => {
                entry.insert(value);
            }
        }
    }

    pub fn get(&self, property_name: &PropertyName) -> Option<EntityId> {
        let values = self.values.read().unwrap();
        values.get(property_name).cloned().flatten()
    }
}

impl PropertyBackend for RefBackend {
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> { self as Arc<dyn Any + Send + Sync + 'static> }

    fn as_debug(&self) -> &dyn Debug { self as &dyn Debug }

    fn fork(&self) -> Box<dyn PropertyBackend> {
        let values = self.values.read().unwrap();
        let cloned = (*values).clone();
        drop(values);

        Box::new(Self { values: Arc::new(RwLock::new(cloned)) })
    }

    fn properties(&self) -> Vec<PropertyName> {
        let values = self.values.read().unwrap();
        values.keys().cloned().collect::<Vec<PropertyName>>()
    }

    fn property_value(&self, property_name: &PropertyName) -> Option<PropertyValue> {
        self.get(property_name).map(|id| PropertyValue::EntityId(id))
    }

    fn property_values(&self) -> BTreeMap<PropertyName, Option<PropertyValue>> {
        let ids = self.values.read().unwrap();

        let mut values = BTreeMap::new();
        for (property_name, id) in ids.iter() {
            let value = id.map(|id| PropertyValue::EntityId(id));
            values.insert(property_name.clone(), value);
        }

        values
    }

    fn property_backend_name() -> String { "ref".to_owned() }

    fn to_state_buffer(&self) -> Result<Vec<u8>, StateError> {
        let ids = self.values.read().unwrap();
        let state_buffer = bincode::serialize(&*ids)?;
        Ok(state_buffer)
    }

    fn from_state_buffer(state_buffer: &Vec<u8>) -> std::result::Result<Self, crate::error::RetrievalError>
    where Self: Sized {
        let map = bincode::deserialize::<BTreeMap<PropertyName, Option<EntityId>>>(state_buffer)?;
        Ok(Self { values: Arc::new(RwLock::new(map)) })
    }

    fn to_operations(&self) -> Result<Vec<Operation>, MutationError> {
        let values = self.values.read().unwrap();
        let serialized_diff = bincode::serialize(&RefDiff { version: REF_DIFF_VERSION, data: bincode::serialize(&*values)? })?;
        Ok(vec![Operation { diff: serialized_diff }])
    }

    fn apply_operations(&self, operations: &Vec<Operation>) -> Result<(), MutationError> {
        for operation in operations {
            let RefDiff { version, data } = bincode::deserialize(&operation.diff)?;
            match version {
                1 => {
                    let map: BTreeMap<PropertyName, Option<EntityId>> = bincode::deserialize(&data)?;
                    let mut values = self.values.write().unwrap();
                    for (property_name, new_value) in map {
                        values.insert(property_name, new_value);
                    }
                }
                version => return Err(MutationError::UpdateFailed(anyhow::anyhow!("Unknown Ref operation version: {:?}", version).into())),
            }
        }
        Ok(())
    }
}

// Need ID based happens-before determination to resolve conflicts

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn property_backend() {
        let backend = RefBackend::new();
        let prop1 = "Property 1".to_owned();
        let prop2 = "Property 2".to_owned();
        let id1 = EntityId::new();
        let id2 = EntityId::new();
        backend.set(prop1.clone(), Some(id1));
        backend.set(prop2.clone(), Some(id2));
        assert_eq!(backend.get(&prop1), Some(id1));
        assert_eq!(backend.property_value(&prop1), Some(PropertyValue::EntityId(id1)));
        assert_eq!(backend.properties(), vec![prop1, prop2]);

        let state_buffer = backend.to_state_buffer().unwrap();
        let from_state_buffer = RefBackend::from_state_buffer(&state_buffer).unwrap();
        assert_eq!(backend.property_values(), from_state_buffer.property_values());

        let new_backend = RefBackend::new();
        let operations = backend.to_operations().unwrap();
        new_backend.apply_operations(&operations).unwrap();
        assert_eq!(backend.property_values(), new_backend.property_values());

        // TODO: More robust event tests:

        // TODO: Older update (Parent) shouldn't change the values.

        // TODO: Newer update (Child) should change the values.

        // TODO: Sibling update should do... what? ULID based clocks
        // mean this shouldn't really happen, but later on prob some
        // clock + the precursors to figure this out?
    }
}
