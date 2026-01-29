use std::{
    any::Any,
    collections::BTreeMap,
    fmt::Debug,
    sync::{Arc, Mutex, RwLock},
};

use ankurah_proto::Operation;
use ankurah_signals::signal::Listener;
use serde::{Deserialize, Serialize};

use crate::{
    error::{AnyhowWrapper, InternalError, MutationError, StateError, StorageError},
    property::{backend::PropertyBackend, PropertyName, Value},
};
use error_stack::Report;

/// Convert bincode error to MutationError
fn bincode_to_mutation(e: bincode::Error) -> MutationError {
    MutationError::Failure(Report::new(AnyhowWrapper::from(format!("bincode error: {}", e))).change_context(InternalError))
}

const LWW_DIFF_VERSION: u8 = 1;

#[derive(Clone, Debug)]
struct ValueEntry {
    value: Option<Value>,
    committed: bool,
}

#[derive(Debug)]
pub struct LWWBackend {
    // TODO - can this be safely combined with the values map?
    values: RwLock<BTreeMap<PropertyName, ValueEntry>>,
    field_broadcasts: Mutex<BTreeMap<PropertyName, ankurah_signals::broadcast::Broadcast>>,
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
    pub fn new() -> LWWBackend { Self { values: RwLock::new(BTreeMap::default()), field_broadcasts: Mutex::new(BTreeMap::new()) } }

    pub fn set(&self, property_name: PropertyName, value: Option<Value>) {
        let mut values = self.values.write().unwrap();
        values.insert(property_name, ValueEntry { value, committed: false });
    }

    pub fn get(&self, property_name: &PropertyName) -> Option<Value> {
        let values = self.values.read().unwrap();
        values.get(property_name).and_then(|entry| entry.value.clone())
    }
}

impl PropertyBackend for LWWBackend {
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> { self as Arc<dyn Any + Send + Sync + 'static> }

    fn as_debug(&self) -> &dyn Debug { self as &dyn Debug }

    fn fork(&self) -> Arc<dyn PropertyBackend> {
        let values = self.values.read().unwrap();
        let cloned = (*values).clone();
        drop(values);

        Arc::new(Self {
            values: RwLock::new(cloned),
            // Create fresh broadcasts (don't clone the existing ones for transaction isolation)
            field_broadcasts: Mutex::new(BTreeMap::new()),
        })
    }

    fn properties(&self) -> Vec<PropertyName> {
        let values = self.values.read().unwrap();
        values.keys().cloned().collect::<Vec<PropertyName>>()
    }

    fn property_value(&self, property_name: &PropertyName) -> Option<Value> { self.get(property_name) }

    fn property_values(&self) -> BTreeMap<PropertyName, Option<Value>> {
        let values = self.values.read().unwrap();
        values.iter().map(|(k, v)| (k.clone(), v.value.clone())).collect()
    }

    fn property_backend_name() -> String { "lww".to_owned() }

    fn to_state_buffer(&self) -> Result<Vec<u8>, StateError> {
        let property_values = self.property_values();
        let state_buffer = bincode::serialize(&property_values)?;
        Ok(state_buffer)
    }

    fn from_state_buffer(state_buffer: &Vec<u8>) -> std::result::Result<Self, StorageError>
    where Self: Sized {
        let raw_map = bincode::deserialize::<BTreeMap<PropertyName, Option<Value>>>(state_buffer)?;
        let map = raw_map.into_iter().map(|(k, v)| (k, ValueEntry { value: v, committed: true })).collect();
        Ok(Self { values: RwLock::new(map), field_broadcasts: Mutex::new(BTreeMap::new()) })
    }

    fn to_operations(&self) -> Result<Option<Vec<Operation>>, MutationError> {
        let mut values = self.values.write().unwrap();
        let mut changed_values = BTreeMap::new();

        for (name, entry) in values.iter_mut() {
            if !entry.committed {
                changed_values.insert(name.clone(), entry.value.clone());
                entry.committed = true;
            }
        }

        if changed_values.is_empty() {
            return Ok(None);
        }

        Ok(Some(vec![Operation {
            diff: bincode::serialize(&LWWDiff { version: LWW_DIFF_VERSION, data: bincode::serialize(&changed_values).map_err(bincode_to_mutation)? }).map_err(bincode_to_mutation)?,
        }]))
    }

    fn apply_operations(&self, operations: &Vec<Operation>) -> Result<(), MutationError> {
        let mut changed_fields = Vec::new();

        for operation in operations {
            let LWWDiff { version, data } = bincode::deserialize(&operation.diff).map_err(bincode_to_mutation)?;
            match version {
                1 => {
                    let changes: BTreeMap<PropertyName, Option<Value>> = bincode::deserialize(&data).map_err(bincode_to_mutation)?;

                    let mut values = self.values.write().unwrap();
                    for (property_name, new_value) in changes {
                        // Insert as committed entry since this came from an operation
                        values.insert(property_name.clone(), ValueEntry { value: new_value, committed: true });
                        changed_fields.push(property_name);
                    }
                }
                version => return Err(MutationError::InvalidUpdate(format!("Unknown LWW operation version: {:?}", version))),
            }
        }

        // Notify field subscribers for changed fields only
        let field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        for field_name in changed_fields {
            if let Some(broadcast) = field_broadcasts.get(&field_name) {
                broadcast.send(());
            }
        }

        Ok(())
    }

    fn listen_field(&self, field_name: &PropertyName, listener: Listener) -> ankurah_signals::signal::ListenerGuard {
        // Get or create the broadcast for this field
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(field_name.clone()).or_default();

        // Subscribe to the broadcast and return the guard
        broadcast.reference().listen(listener).into()
    }
}

impl LWWBackend {
    /// Get the broadcast ID for a specific field, creating the broadcast if necessary
    pub fn field_broadcast_id(&self, field_name: &PropertyName) -> ankurah_signals::broadcast::BroadcastId {
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(field_name.clone()).or_default();
        broadcast.id()
    }
}

// Need ID based happens-before determination to resolve conflicts
