use std::{
    any::Any,
    collections::{btree_map::Entry, BTreeMap},
    fmt::Debug,
    ops::Deref,
    sync::{Arc, Mutex, RwLock},
};

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    property::{backend::PropertyBackend, PropertyName},
    storage::Materialized,
};

#[derive(Clone, Debug)]
pub struct LWWBackend {
    // TODO: store a timestamp/precursor id at time of setting value (or maybe when we commit?).
    values: Arc<RwLock<BTreeMap<PropertyName, Vec<u8>>>>,
}

impl LWWBackend {
    pub fn new() -> LWWBackend {
        Self {
            values: Arc::new(RwLock::new(BTreeMap::default())),
        }
    }

    pub fn set(&mut self, property_name: PropertyName, value: Vec<u8>) {
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

    pub fn get(&self, property_name: PropertyName) -> Option<Vec<u8>> {
        let values = self.values.read().unwrap();
        values.get(&property_name).cloned()
    }
}

impl PropertyBackend for LWWBackend {
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> {
        self as Arc<dyn Any + Send + Sync + 'static>
    }

    fn as_debug(&self) -> &dyn Debug {
        self as &dyn Debug
    }

    fn fork(&self) -> Box<dyn PropertyBackend> {
        let values = self.values.read().unwrap();
        let cloned = (*values).clone();
        drop(values);

        Box::new(Self {
            values: Arc::new(RwLock::new(cloned)),
        })
    }

    fn properties(&self) -> Vec<String> {
        let values = self.values.read().unwrap();
        values.keys().cloned().collect::<Vec<String>>()
    }

    fn materialized(&self) -> BTreeMap<PropertyName, Materialized> {
        unimplemented!()
    }

    fn property_backend_name() -> String {
        "lww".to_owned()
    }

    fn to_state_buffer(&self) -> anyhow::Result<Vec<u8>> {
        let values = self.values.read().unwrap();
        let state_buffer = bincode::serialize(&*values)?;
        Ok(state_buffer)
    }

    fn from_state_buffer(
        state_buffer: &Vec<u8>,
    ) -> std::result::Result<Self, crate::error::RetrievalError>
    where
        Self: Sized,
    {
        let map = bincode::deserialize::<BTreeMap<PropertyName, Vec<u8>>>(state_buffer)?;
        Ok(Self {
            values: Arc::new(RwLock::new(map)),
        })
    }

    fn to_operations(&self /*precursor: ULID*/) -> anyhow::Result<Vec<super::Operation>> {
        todo!()
    }

    fn apply_operations(&self, operations: &Vec<super::Operation>) -> anyhow::Result<()> {
        todo!()
    }
}

// Need ID based happens-before determination to resolve conflicts
