use std::{
    any::Any,
    collections::BTreeMap,
    fmt::Debug,
    sync::{Arc, Mutex, RwLock},
};

use ankurah_proto::{EventId, Operation};
use serde::{Deserialize, Serialize};

use crate::{
    causal_dag::forward_view::ReadySet,
    error::{MutationError, StateError},
    property::{
        backend::{PropertyBackend, PropertyTransaction},
        PropertyName, Value,
    },
};

const LWW_DIFF_VERSION: u8 = 1;

/// In memory value register - not to be serialized
#[derive(Clone, Debug)]
struct ValueRegister {
    #[allow(dead_code)] // Will be used in step 5 for causal LWW tiebreaking
    last_writer_event_id: Option<EventId>,
    value: Option<Value>,
    committed: bool, // NOTE: this is used for Mutables which will emit events, not for event application
}

// This is what we serialize/deserialize
#[derive(Serialize, Deserialize)]
pub struct CausalLWWState {
    // a list of event ids pertinent to the current register values in this state. Max length is 2**16
    events: Vec<EventId>,
    // Registers
    registers: BTreeMap<PropertyName, ValueRegisterWire>,
}
#[derive(Serialize, Deserialize)]
pub struct ValueRegisterWire {
    entity_id_offset: u16, // offset in `events` list
    value: Option<Value>,  // tombstone
}

#[derive(Debug)]
pub struct LWWBackend {
    // TODO - can this be safely combined with the values map?
    values: RwLock<BTreeMap<PropertyName, ValueRegister>>,
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
        values.insert(property_name, ValueRegister { last_writer_event_id: None, value, committed: false });
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
        // TODO stop using property_values and start using
        let property_values = self.property_values();
        let state_buffer = bincode::serialize(&property_values)?;
        Ok(state_buffer)
    }

    fn from_state_buffer(state_buffer: &Vec<u8>) -> std::result::Result<Self, crate::error::RetrievalError>
    where Self: Sized {
        let raw_map = bincode::deserialize::<BTreeMap<PropertyName, Option<Value>>>(state_buffer)?;
        let map = raw_map.into_iter().map(|(k, v)| (k, ValueRegister { last_writer_event_id: None, value: v, committed: true })).collect();
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

    fn apply_operations(&self, operations: &Vec<Operation>) -> Result<(), MutationError> {
        let mut changed_fields = Vec::new();

        for operation in operations {
            let LWWDiff { version, data } = bincode::deserialize(&operation.diff)?;
            match version {
                1 => {
                    let changes: BTreeMap<PropertyName, Option<Value>> = bincode::deserialize(&data)?;

                    let mut values = self.values.write().unwrap();
                    for (property_name, new_value) in changes {
                        // Insert as committed entry since this came from an operation
                        values
                            .insert(property_name.clone(), ValueRegister { last_writer_event_id: None, value: new_value, committed: true });
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

    fn listen_field(
        &self,
        field_name: &PropertyName,
        listener: ankurah_signals::broadcast::Listener,
    ) -> ankurah_signals::broadcast::ListenerGuard {
        // Get or create the broadcast for this field
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(field_name.clone()).or_default();

        // Subscribe to the broadcast and return the guard
        broadcast.reference().listen(listener)
    }

    fn begin(&self) -> Result<Box<dyn PropertyTransaction<ankurah_proto::Event>>, MutationError> {
        let values = self.values.read().unwrap();
        let base_registers = values.clone();

        Ok(Box::new(LWWTransaction { base_registers, pending_winners: BTreeMap::new() }))
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

/// Transaction for applying concurrent updates to LWW backend
pub struct LWWTransaction {
    /// Snapshot of registers at transaction start (reflects Primary head)
    #[allow(dead_code)] // Will be used in step 5 for causal LWW tiebreaking
    base_registers: BTreeMap<PropertyName, ValueRegister>,
    /// Proposed winners after ReadySet processing: (event_id, value)
    #[allow(dead_code)] // Will be used in step 5 for causal LWW tiebreaking
    pending_winners: BTreeMap<PropertyName, (EventId, Option<Value>)>,
}

impl PropertyTransaction<ankurah_proto::Event> for LWWTransaction {
    fn apply_ready_set(&mut self, _ready_set: &ReadySet<ankurah_proto::Event>) -> Result<(), MutationError> {
        // TODO: Implement causal LWW tiebreaking
        // Per ReadySet procedure:
        // 1. Determine Primary event ID for this layer (if any) - do NOT apply it
        // 2. For each property mentioned by any event in {Primary ∪ Concurrency}:
        //    - Get current_winner_id from base_registers (if exists)
        //    - Compute best_id using lineage + lexicographic tiebreak
        //    - If best_id != current_winner_id, update pending_winners
        // 3. Repeat for each ReadySet
        Ok(())
    }

    fn commit(&mut self) -> Result<(), MutationError> {
        // TODO: Implement commit
        // 1. Compute diffs: pending_winners vs base_registers
        // 2. Build events: Vec<EventId> and entity_id_offset from changed winners
        // 3. Merge winners into backend's ValueRegisters
        // 4. to_state_buffer() serializes only changed registers (diff)
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), MutationError> {
        // Drop pending state
        Ok(())
    }
}

// Need ID based happens-before determination to resolve conflicts
