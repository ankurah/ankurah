use std::{
    any::Any,
    collections::BTreeMap,
    fmt::Debug,
    sync::{Arc, Mutex, RwLock},
};

use ankurah_proto::{Event, EventId, Operation};
use ankurah_signals::signal::Listener;
use serde::{Deserialize, Serialize};

use crate::{
    error::{MutationError, StateError},
    event_dag::{CausalRelation, EventLayer},
    property::{backend::PropertyBackend, PropertyName, Value},
};

const LWW_DIFF_VERSION: u8 = 1;

#[derive(Clone, Debug)]
struct ValueEntry {
    value: Option<Value>,
    committed: bool,
    /// The event that wrote this value (None for uncommitted local changes)
    event_id: Option<EventId>,
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
        values.insert(property_name, ValueEntry { value, committed: false, event_id: None });
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

    fn property_backend_name() -> &'static str { "lww" }

    fn to_state_buffer(&self) -> Result<Vec<u8>, StateError> {
        // Serialize with required event_id for per-property conflict resolution after loading.
        let values = self.values.read().unwrap();
        let mut serializable: BTreeMap<PropertyName, (Option<Value>, EventId)> = BTreeMap::new();
        for (name, entry) in values.iter() {
            let Some(event_id) = entry.event_id.clone() else {
                return Err(StateError::SerializationError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("LWW state requires event_id for property {}", name),
                ))));
            };
            serializable.insert(name.clone(), (entry.value.clone(), event_id));
        }
        let state_buffer = bincode::serialize(&serializable)?;
        Ok(state_buffer)
    }

    fn from_state_buffer(state_buffer: &Vec<u8>) -> std::result::Result<Self, crate::error::RetrievalError>
    where Self: Sized {
        let raw_map = bincode::deserialize::<BTreeMap<PropertyName, (Option<Value>, EventId)>>(state_buffer)?;
        let map = raw_map.into_iter().map(|(k, (v, eid))| (k, ValueEntry { value: v, committed: true, event_id: Some(eid) })).collect();
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
            diff: bincode::serialize(&LWWDiff { version: LWW_DIFF_VERSION, data: bincode::serialize(&changed_values)? })?,
        }]))
    }

    fn apply_operations(&self, operations: &[Operation]) -> Result<(), MutationError> {
        // No event tracking - used when loading from state buffer
        self.apply_operations_internal(operations, None)
    }

    fn apply_operations_with_event(&self, operations: &[Operation], event_id: EventId) -> Result<(), MutationError> {
        // Track which event set each property - used for all event applications
        self.apply_operations_internal(operations, Some(event_id))
    }

    fn apply_layer(&self, layer: &EventLayer<EventId, Event>) -> Result<(), MutationError> {
        #[derive(Clone)]
        struct Candidate {
            value: Option<Value>,
            event_id: EventId,
            from_to_apply: bool,
        }

        let mut winners: BTreeMap<PropertyName, Candidate> = BTreeMap::new();

        // Seed with stored last-write candidates (required event_id).
        {
            let values = self.values.read().unwrap();
            for (prop, entry) in values.iter() {
                let Some(event_id) = entry.event_id.clone() else {
                    return Err(MutationError::UpdateFailed(
                        anyhow::anyhow!("LWW candidate missing event_id for property {}", prop).into(),
                    ));
                };
                winners.insert(
                    prop.clone(),
                    Candidate {
                        value: entry.value.clone(),
                        event_id,
                        from_to_apply: false,
                    },
                );
            }
        }

        // Add candidates from events in this layer.
        for (event, from_to_apply) in layer
            .already_applied
            .iter()
            .map(|e| (e, false))
            .chain(layer.to_apply.iter().map(|e| (e, true)))
        {
            if let Some(operations) = event.operations.get(&Self::property_backend_name().to_string()) {
                for operation in operations {
                    let LWWDiff { version, data } = bincode::deserialize(&operation.diff)?;
                    match version {
                        1 => {
                            let changes: BTreeMap<PropertyName, Option<Value>> = bincode::deserialize(&data)?;
                            for (prop, value) in changes {
                                let candidate = Candidate { value, event_id: event.id(), from_to_apply };
                                if let Some(current) = winners.get_mut(&prop) {
                                    let relation = layer.compare(&candidate.event_id, &current.event_id).map_err(|err| {
                                        MutationError::UpdateFailed(Box::new(err))
                                    })?;
                                    match relation {
                                        CausalRelation::Descends => {
                                            *current = candidate;
                                        }
                                        CausalRelation::Ascends => {}
                                        CausalRelation::Concurrent => {
                                            if candidate.event_id > current.event_id {
                                                *current = candidate;
                                            }
                                        }
                                    }
                                } else {
                                    winners.insert(prop, candidate);
                                }
                            }
                        }
                        version => {
                            return Err(MutationError::UpdateFailed(anyhow::anyhow!("Unknown LWW operation version: {:?}", version).into()))
                        }
                    }
                }
            }
        }

        // Apply winning values that come from to_apply events.
        let mut changed_fields = Vec::new();
        {
            let mut values = self.values.write().unwrap();
            for (prop, candidate) in winners {
                if candidate.from_to_apply {
                    values.insert(
                        prop.clone(),
                        ValueEntry { value: candidate.value, committed: true, event_id: Some(candidate.event_id) },
                    );
                    changed_fields.push(prop);
                }
            }
        }

        // Notify subscribers for changed fields
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

    /// Get the event_id that last wrote a property value (if tracked).
    pub fn get_event_id(&self, property_name: &PropertyName) -> Option<EventId> {
        let values = self.values.read().unwrap();
        values.get(property_name).and_then(|entry| entry.event_id.clone())
    }
    /// Internal implementation that handles both tracked and untracked operations.
    fn apply_operations_internal(&self, operations: &[Operation], event_id: Option<EventId>) -> Result<(), MutationError> {
        let mut changed_fields = Vec::new();

        for operation in operations {
            let LWWDiff { version, data } = bincode::deserialize(&operation.diff)?;
            match version {
                1 => {
                    let changes: BTreeMap<PropertyName, Option<Value>> = bincode::deserialize(&data)?;

                    let mut values = self.values.write().unwrap();
                    for (property_name, new_value) in changes {
                        // Insert as committed entry since this came from an operation
                        values.insert(property_name.clone(), ValueEntry { value: new_value, committed: true, event_id: event_id.clone() });
                        changed_fields.push(property_name);
                    }
                }
                version => return Err(MutationError::UpdateFailed(anyhow::anyhow!("Unknown LWW operation version: {:?}", version).into())),
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
}

// Need ID based happens-before determination to resolve conflicts
