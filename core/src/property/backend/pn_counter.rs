use std::{
    any::Any,
    collections::BTreeMap,
    fmt::Debug,
    ops::DerefMut,
    sync::{Arc, RwLock},
};

use crate::{
    property::{
        backend::{Operation, PropertyBackend},
        PropertyName,
    },
    storage::Materialized,
};

#[derive(Debug)]
pub struct PNBackend {
    values: Arc<RwLock<BTreeMap<PropertyName, PNValue>>>,
}

#[derive(Debug, Default)]
pub struct PNValue {
    // TODO: Maybe use something aside from i64 for the base?
    pub value: i64,
    // TODO: replace with precursor record?
    pub previous_value: i64,
}

impl PNValue {
    pub fn snapshot(&self) -> Self {
        Self {
            value: self.value,
            previous_value: self.value,
        }
    }

    pub fn diff(&self) -> i64 {
        self.value - self.previous_value
    }
}

impl Default for PNBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl PNBackend {
    pub fn new() -> PNBackend {
        Self {
            values: Arc::new(RwLock::new(BTreeMap::default())),
        }
    }

    pub fn get(&self, property_name: PropertyName) -> i64 {
        let values = self.values.read().unwrap();
        values
            .get(&property_name)
            .map(|pnvalue| pnvalue.value)
            .unwrap_or(0)
    }

    pub fn add(&self, property_name: PropertyName, amount: i64) {
        let values = self.values.write().unwrap();
        Self::add_raw(values, property_name, amount);
    }

    pub fn add_raw(
        mut values: impl DerefMut<Target = BTreeMap<PropertyName, PNValue>>,
        property_name: PropertyName,
        amount: i64,
    ) {
        let value = values.deref_mut().entry(property_name).or_default();
        value.value += amount;
    }
}

impl PropertyBackend for PNBackend {
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> {
        self as Arc<dyn Any + Send + Sync + 'static>
    }

    fn as_debug(&self) -> &dyn Debug {
        self as &dyn Debug
    }

    fn fork(&self) -> Box<dyn PropertyBackend> {
        let values = self.values.read().unwrap();
        let snapshotted = values
            .iter()
            .map(|(key, value)| (key.to_owned(), value.snapshot()))
            .collect::<BTreeMap<_, _>>();
        Box::new(Self {
            values: Arc::new(RwLock::new(snapshotted)),
        })
    }

    fn properties(&self) -> Vec<String> {
        let values = self.values.read().unwrap();
        values.keys().cloned().collect::<Vec<String>>()
    }

    fn materialized(&self) -> BTreeMap<PropertyName, Materialized> {
        let values = self.values.read().unwrap();
        let mut map = BTreeMap::new();
        for (property, data) in values.iter() {
            map.insert(property.clone(), Materialized::Number(data.value));
        }

        map
    }

    fn property_backend_name() -> String {
        "pn".to_owned()
    }

    fn to_state_buffer(&self) -> anyhow::Result<Vec<u8>> {
        let values = self.values.read().unwrap();
        let serializable = values
            .iter()
            .map(|(key, value)| (key, value.value))
            .collect::<BTreeMap<_, _>>();
        let serialized = bincode::serialize(&serializable)?;
        Ok(serialized)
    }

    fn from_state_buffer(
        state_buffer: &Vec<u8>,
    ) -> std::result::Result<Self, crate::error::RetrievalError> {
        let values = bincode::deserialize::<BTreeMap<PropertyName, i64>>(state_buffer)?;
        let values = values
            .into_iter()
            .map(|(key, value)| {
                (
                    key,
                    PNValue {
                        value,
                        previous_value: value,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        Ok(Self {
            values: Arc::new(RwLock::new(values)),
        })
    }

    fn to_operations(&self /*precursor: ULID*/) -> anyhow::Result<Vec<Operation>> {
        let values = self.values.read().unwrap();
        let diffs = values
            .iter()
            .map(|(key, value)| (key, value.diff()))
            .collect::<BTreeMap<_, _>>();

        let serialized_diffs = bincode::serialize(&diffs)?;
        Ok(vec![Operation {
            diff: serialized_diffs,
        }])
    }

    fn apply_operations(&self, operations: &Vec<Operation>) -> anyhow::Result<()> {
        for operation in operations {
            let diffs = bincode::deserialize::<BTreeMap<PropertyName, i64>>(&operation.diff)?;

            let mut values = self.values.write().unwrap();
            for (property, diff) in diffs {
                Self::add_raw(&mut *values, property, diff);
            }
        }

        Ok(())
    }

    fn get_property_value_string(&self, property_name: &str) -> Option<String> {
        self.values
            .read()
            .unwrap()
            .get(property_name)
            .map(|v| v.value.to_string())
    }
}
