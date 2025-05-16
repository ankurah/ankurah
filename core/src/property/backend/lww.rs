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
struct ValueEntry {
    value: Option<PropertyValue>,
    committed: bool,
}

#[derive(Clone, Debug)]
pub struct LWWBackend {
    values: Arc<RwLock<BTreeMap<PropertyName, ValueEntry>>>,
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
        values.insert(property_name, ValueEntry { value, committed: false });
    }

    pub fn get(&self, property_name: &PropertyName) -> Option<PropertyValue> {
        let values = self.values.read().unwrap();
        values.get(property_name).map(|entry| entry.value.clone()).flatten()
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
        values.iter().map(|(k, v)| (k.clone(), v.value.clone())).collect()
    }

    fn property_backend_name() -> String { "lww".to_owned() }

    fn to_state_buffer(&self) -> Result<Vec<u8>, StateError> {
        let property_values = self.property_values();
        let state_buffer = bincode::serialize(&property_values)?;
        Ok(state_buffer)
    }

    fn from_state_buffer(state_buffer: &Vec<u8>) -> std::result::Result<Self, crate::error::RetrievalError>
    where Self: Sized {
        let raw_map = bincode::deserialize::<BTreeMap<PropertyName, Option<PropertyValue>>>(state_buffer)?;
        let map = raw_map.into_iter().map(|(k, v)| (k, ValueEntry { value: v, committed: true })).collect();
        Ok(Self { values: Arc::new(RwLock::new(map)) })
    }

    fn to_operations(&self) -> Result<Vec<Operation>, MutationError> {
        let mut values = self.values.write().unwrap();
        let mut changed_values = BTreeMap::new();

        for (name, entry) in values.iter_mut() {
            if !entry.committed {
                changed_values.insert(name.clone(), entry.value.clone());
                entry.committed = true;
            }
        }

        if changed_values.is_empty() {
            return Ok(vec![]);
        }

        Ok(vec![Operation {
            diff: bincode::serialize(&LWWDiff { version: LWW_DIFF_VERSION, data: bincode::serialize(&changed_values)? })?,
        }])
    }

    fn apply_operations(&self, operations: &Vec<Operation>) -> Result<(), MutationError> {
        for operation in operations {
            let LWWDiff { version, data } = bincode::deserialize(&operation.diff)?;
            match version {
                1 => {
                    let changes: BTreeMap<PropertyName, Option<PropertyValue>> = bincode::deserialize(&data)?;

                    let mut values = self.values.write().unwrap();
                    for (property_name, new_value) in changes {
                        // Insert as committed entry since this came from an operation
                        values.insert(property_name, ValueEntry { value: new_value, committed: true });
                    }
                }
                version => return Err(MutationError::UpdateFailed(anyhow::anyhow!("Unknown LWW operation version: {:?}", version).into())),
            }
        }
        Ok(())
    }
}

// Need ID based happens-before determination to resolve conflicts
