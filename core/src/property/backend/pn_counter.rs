use std::{
    any::Any,
    collections::BTreeMap,
    fmt::Debug,
    ops::DerefMut,
    sync::{Arc, RwLock},
};

use ankurah_proto::Clock;

use crate::{
    property::{
        backend::{Operation, PropertyBackend},
        PropertyName, PropertyValue,
    },
    Node,
};

use serde::{Serialize, Deserialize};

#[derive(Debug)]
pub struct PNBackend {
    values: Arc<RwLock<BTreeMap<PropertyName, PNValue>>>,
}

impl Default for PNBackend {
    fn default() -> Self { Self::new() }
}

impl PNBackend {
    pub fn new() -> PNBackend { Self { values: Arc::new(RwLock::new(BTreeMap::default())) } }

    pub fn get(&self, property_name: PropertyName) -> Option<PNValue> {
        let values = self.values.read().unwrap();
        values.get(&property_name).cloned()
    }

    pub fn add(&self, property_name: PropertyName, amount: PNValue) {
        let values = self.values.write().unwrap();
        Self::add_raw(values, property_name, amount);
    }

    pub fn add_raw(mut values: impl DerefMut<Target = BTreeMap<PropertyName, PNValue>>, property_name: PropertyName, amount: PNValue) {
        let value = values.deref_mut().entry(property_name).or_default();
        value.value += amount;
    }
}

impl PropertyBackend for PNBackend {
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> { self as Arc<dyn Any + Send + Sync + 'static> }

    fn as_debug(&self) -> &dyn Debug { self as &dyn Debug }

    fn fork(&self) -> Box<dyn PropertyBackend> {
        let values = self.values.read().unwrap();
        let snapshotted = values.iter().map(|(key, value)| (key.to_owned(), value.snapshot())).collect::<BTreeMap<_, _>>();
        Box::new(Self { values: Arc::new(RwLock::new(snapshotted)) })
    }

    fn properties(&self) -> Vec<String> {
        let values = self.values.read().unwrap();
        values.keys().cloned().collect::<Vec<String>>()
    }

    fn property_values(&self) -> BTreeMap<PropertyName, PropertyValue> {
        let values = self.values.read().unwrap();
        let mut map = BTreeMap::new();
        for (property, data) in values.iter() {
            map.insert(property.clone(), PropertyValue::Number(data.value));
        }

        map
    }

    fn property_backend_name() -> String { "pn".to_owned() }

    fn to_state_buffer(&self) -> anyhow::Result<Vec<u8>> {
        let values = self.values.read().unwrap();
        let serializable = values.iter().map(|(key, value)| (key, value.value)).collect::<BTreeMap<_, _>>();
        let serialized = bincode::serialize(&serializable)?;
        Ok(serialized)
    }

    fn from_state_buffer(state_buffer: &Vec<u8>) -> std::result::Result<Self, crate::error::RetrievalError> {
        let values = bincode::deserialize::<BTreeMap<PropertyName, PNValue>>(state_buffer)?;
        Ok(Self { values: Arc::new(RwLock::new(values)) })
    }

    fn to_operations(&self) -> anyhow::Result<Vec<Operation>> {
        let values = self.values.read().unwrap();
        let diffs = values.iter().map(|(key, value)| (key, value.diff())).collect::<BTreeMap<_, _>>();

        let serialized_diffs = bincode::serialize(&diffs)?;
        Ok(vec![Operation { diff: serialized_diffs }])
    }

    fn apply_operations(
        &self,
        operations: &Vec<Operation>,
        _current_head: &Clock,
        _event_head: &Clock,
        // _context: &Box<dyn TContext>,
    ) -> anyhow::Result<()> {
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
        //self.values.read().unwrap().get(property_name).map(|v| v.value.to_string())
        unimplemented!()
    }
}


macro_rules! pn_value {
    ($($integer:ty => $variant:ident),*) => {
        $(
            impl Into<PNValue> for $integer {
                fn into(self) -> PNValue {
                    PNValue::$variant { value: self, prev: self }
                }
            }

            impl From<PNValue> for $integer {
                fn from(value: PNValue) -> Self {
                    if let PNValue::$variant { value, .. } = value {
                        value
                    } else {
                        panic!("yeah this is stupid, blame me later and yell at me to fix it");
                    }
                }
            }
        )*

        #[derive(Serialize, Deserialize, Debug, Copy, Clone)]
        pub enum PNValue {
            $(
                $variant { value: $integer, prev: $integer },
            )*
        }

        impl PNValue {
            pub fn as_zero(&self) -> Self {
                match self {
                    $(
                        PNValue::$variant { .. } => PNValue::$variant { value: 0, prev: 0 },
                    )*
                }
            }
        }

        impl<'a> Into<PropertyValue> for &'a PNValue {
            fn into(self) -> PropertyValue {
                match self {
                    $(
                        PNValue::$variant { value, .. } => PropertyValue::$variant(*value),
                    )*
                }
            }
        }

        #[derive(Serialize, Deserialize, Debug, Copy, Clone)]
        pub enum PNDiff {
            $(
                $variant($integer),
            )*
        }

        impl PNValue {
            pub fn snapshot(&self) -> Self {
                match self {
                    $(
                        PNValue::$variant { value, .. } => PNValue::$variant { value: *value, prev: *value },
                    )*
                }
            }

            pub fn diff(&self) -> PNDiff {
                match self {
                    $(
                        PNValue::$variant { value, prev } => PNDiff::$variant(value - prev),
                    )*
                }
            }
        }
    };
}

pn_value!(
    i8  => I8,  u8  => U8,
    i16 => I16, u16 => U16,
    i32 => I32, u32 => U32,
    i64 => I64, u64 => U64
);