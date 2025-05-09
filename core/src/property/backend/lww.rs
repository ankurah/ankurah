use std::{
    any::Any,
    collections::{btree_map::Entry, BTreeMap},
    fmt::Debug,
    sync::{Arc, RwLock},
};

use ankurah_proto::Operation;
use serde::{Deserialize, Serialize};

use crate::{
    error::{MutationError, StateError},
    property::{backend::PropertyBackend, PropertyName, PropertyValue},
};

const LWW_DIFF_VERSION: u8 = 1;

#[derive(Clone, Debug)]
pub struct LWWBackend {
    values: Arc<RwLock<BTreeMap<PropertyName, Option<PropertyValue>>>>,
}

#[derive(Serialize, Deserialize)]
pub struct LWWDiff {
    version: u8,
    data: Vec<u8>,
}

impl Default for LWWBackend {
    fn default() -> Self { Self::new() }
}

impl LWWBackend {
    pub fn new() -> LWWBackend { Self { values: Arc::new(RwLock::new(BTreeMap::default())) } }

    pub fn set(&self, property_name: PropertyName, value: Option<PropertyValue>) {
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

    pub fn get(&self, property_name: &PropertyName) -> Option<PropertyValue> {
        let values = self.values.read().unwrap();
        values.get(property_name).cloned().flatten()
    }
}

impl PropertyBackend for LWWBackend {
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

    fn property_value(&self, property_name: &PropertyName) -> Option<PropertyValue> { self.get(property_name) }

    fn property_values(&self) -> BTreeMap<PropertyName, Option<PropertyValue>> {
        let values = self.values.read().unwrap();
        values.clone()
    }

    fn property_backend_name() -> String { "lww".to_owned() }

    // This is identical to [`to_operations`] for [`LWWBackend`].
    fn to_state_buffer(&self) -> Result<Vec<u8>, StateError> {
        let property_values = self.property_values();
        let state_buffer = bincode::serialize(&property_values)?;
        Ok(state_buffer)
    }

    fn from_state_buffer(state_buffer: &Vec<u8>) -> std::result::Result<Self, crate::error::RetrievalError>
    where Self: Sized {
        let map = bincode::deserialize::<BTreeMap<PropertyName, Option<PropertyValue>>>(state_buffer)?;
        Ok(Self { values: Arc::new(RwLock::new(map)) })
    }

    fn to_operations(&self) -> Result<Vec<Operation>, MutationError> {
        let property_values = self.property_values();
        let serialized_diff = bincode::serialize(&LWWDiff { version: LWW_DIFF_VERSION, data: bincode::serialize(&property_values)? })?;
        Ok(vec![Operation { diff: serialized_diff }])
    }

    fn apply_operations(&self, operations: &Vec<Operation>) -> Result<(), MutationError> {
        for operation in operations {
            let LWWDiff { version, data } = bincode::deserialize(&operation.diff)?;
            match version {
                1 => {
                    let map: BTreeMap<PropertyName, Option<PropertyValue>> = bincode::deserialize(&data)?;

                    let mut values = self.values.write().unwrap();
                    for (property_name, new_value) in map {
                        values.insert(property_name, new_value);
                    }
                }
                version => return Err(MutationError::UpdateFailed(anyhow::anyhow!("Unknown LWW operation version: {:?}", version).into())),
            }
        }
        Ok(())
    }
}

// Need ID based happens-before determination to resolve conflicts
