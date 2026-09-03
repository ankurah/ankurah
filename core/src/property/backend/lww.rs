use std::{
    any::Any,
    collections::BTreeMap,
    fmt::Debug,
    sync::{Arc, Mutex, RwLock},
};

use ankurah_proto::{BackendOperation as Operation, EventId, PropertyId};
use ankurah_signals::signal::Listener;
use serde::{Deserialize, Serialize};

use crate::{
    error::{MutationError, StateError},
    event_dag::{CausalRelation, EventLayer},
    property::{backend::PropertyBackend, Value},
};

const LWW_DIFF_VERSION: u8 = 2;

// 0xA1 was name-keyed; 0xA2 is PropertyId-keyed. Older formats are refused.
const LWW_STATE_VERSION_BASE: u8 = 0xA0;
const LWW_STATE_VERSION_1: u8 = LWW_STATE_VERSION_BASE + 1;
const LWW_STATE_VERSION_2: u8 = LWW_STATE_VERSION_BASE + 2;

#[derive(Clone, Debug)]
enum ValueEntry {
    Uncommitted { value: Option<Value> },
    Pending { value: Option<Value> },
    Committed { value: Option<Value>, event_id: EventId },
}

impl ValueEntry {
    fn value(&self) -> Option<Value> {
        match self {
            ValueEntry::Uncommitted { value } => value.clone(),
            ValueEntry::Pending { value } => value.clone(),
            ValueEntry::Committed { value, .. } => value.clone(),
        }
    }

    fn event_id(&self) -> Option<EventId> {
        match self {
            ValueEntry::Committed { event_id, .. } => Some(event_id.clone()),
            ValueEntry::Uncommitted { .. } | ValueEntry::Pending { .. } => None,
        }
    }
}

/// A last-write-wins store keyed by durable property identity.
#[derive(Debug)]
pub struct LWWBackend {
    // TODO - can this be safely combined with the values map?
    values: RwLock<BTreeMap<PropertyId, ValueEntry>>,
    field_broadcasts: Mutex<BTreeMap<PropertyId, ankurah_signals::broadcast::Broadcast>>,
}

#[derive(Serialize, Deserialize)]
pub struct LWWDiff {
    version: u8,
    data: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct CommittedEntry {
    value: Option<Value>,
    event_id: EventId,
}

fn decode_changes(operation: &Operation) -> Result<BTreeMap<PropertyId, Option<Value>>, MutationError> {
    let LWWDiff { version, data } = bincode::deserialize(&operation.diff)?;
    if version != LWW_DIFF_VERSION {
        return Err(MutationError::UpdateFailed(anyhow::anyhow!("Unknown LWW operation version: {:?}", version).into()));
    }
    let mut changes = bincode::deserialize::<BTreeMap<PropertyId, Option<Value>>>(&data)?;
    if changes.remove(&PropertyId::Id).is_some() {
        tracing::warn!("skipping id pseudo-property key in LWW diff");
    }
    Ok(changes)
}

impl Default for LWWBackend {
    fn default() -> Self { Self::new() }
}

impl LWWBackend {
    pub fn new() -> LWWBackend { Self { values: RwLock::new(BTreeMap::default()), field_broadcasts: Mutex::new(BTreeMap::new()) } }

    pub fn set(&self, property: PropertyId, value: Option<Value>) {
        if property == PropertyId::Id {
            tracing::warn!("skipping id pseudo-property write to LWW backend");
            return;
        }
        let mut values = self.values.write().unwrap();
        values.insert(property, ValueEntry::Uncommitted { value });
    }

    pub fn get(&self, property: &PropertyId) -> Option<Value> {
        let values = self.values.read().unwrap();
        values.get(property).and_then(|entry| entry.value())
    }
}

impl PropertyBackend for LWWBackend {
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> { self as Arc<dyn Any + Send + Sync + 'static> }

    fn as_debug(&self) -> &dyn Debug { self as &dyn Debug }

    fn fork(&self) -> Arc<dyn PropertyBackend> {
        let values = self.values.read().unwrap();
        let cloned = (*values).clone();
        drop(values);

        Arc::new(Self { values: RwLock::new(cloned), field_broadcasts: Mutex::new(BTreeMap::new()) })
    }

    fn properties(&self) -> Vec<PropertyId> {
        let values = self.values.read().unwrap();
        values.keys().cloned().collect::<Vec<PropertyId>>()
    }

    fn property_value(&self, property: &PropertyId) -> Option<Value> { self.get(property) }

    fn property_values(&self) -> BTreeMap<PropertyId, Option<Value>> {
        let values = self.values.read().unwrap();
        values.iter().map(|(k, v)| (k.clone(), v.value())).collect()
    }

    fn property_backend_name() -> &'static str { "lww" }

    fn to_state_buffer(&self) -> Result<Vec<u8>, StateError> {
        let values = self.values.read().unwrap();
        let mut serializable: BTreeMap<PropertyId, CommittedEntry> = BTreeMap::new();
        for (property, entry) in values.iter() {
            let Some(event_id) = entry.event_id() else {
                return Err(StateError::SerializationError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("LWW state requires event_id for property {}", property),
                ))));
            };
            serializable.insert(property.clone(), CommittedEntry { value: entry.value(), event_id });
        }
        let mut state_buffer = vec![LWW_STATE_VERSION_2];
        bincode::serialize_into(&mut state_buffer, &serializable)?;
        Ok(state_buffer)
    }

    fn from_state_buffer(state_buffer: &Vec<u8>) -> std::result::Result<Self, crate::error::RetrievalError>
    where Self: Sized {
        let (version, payload) = match state_buffer.split_first() {
            Some((version, payload)) => (*version, payload),
            None => return Err(crate::error::RetrievalError::Other("empty LWW state buffer".to_string())),
        };
        if version < LWW_STATE_VERSION_BASE {
            return Err(crate::error::RetrievalError::Other(
                "unversioned pre-0.9 LWW state buffer is name-keyed and not readable by this binary".to_string(),
            ));
        }
        if version == LWW_STATE_VERSION_1 {
            return Err(crate::error::RetrievalError::Other(format!(
                "LWW state buffer version {LWW_STATE_VERSION_1:#04x} is the name-keyed 0.9 encoding; this binary reads only {LWW_STATE_VERSION_2:#04x}"
            )));
        }
        if version != LWW_STATE_VERSION_2 {
            return Err(crate::error::RetrievalError::Other(format!(
                "unknown LWW state buffer version {version:#04x} (this binary supports {LWW_STATE_VERSION_2:#04x})"
            )));
        }
        let raw_map = bincode::deserialize::<BTreeMap<PropertyId, CommittedEntry>>(payload)?;
        // `id` is computed from the entity, never stored.
        let map = raw_map
            .into_iter()
            .filter(|(k, _)| {
                if matches!(k, PropertyId::Id) {
                    tracing::warn!("skipping id pseudo-property key in LWW state buffer");
                    return false;
                }
                true
            })
            .map(|(k, entry)| (k, ValueEntry::Committed { value: entry.value, event_id: entry.event_id }))
            .collect();
        Ok(Self { values: RwLock::new(map), field_broadcasts: Mutex::new(BTreeMap::new()) })
    }

    fn to_operations(&self) -> Result<Option<Vec<Operation>>, MutationError> {
        let mut values = self.values.write().unwrap();
        let mut changed_values = BTreeMap::new();

        for (property, entry) in values.iter_mut() {
            let ValueEntry::Uncommitted { value } = entry else {
                continue;
            };
            let value = value.clone();
            changed_values.insert(property.clone(), value.clone());
            *entry = ValueEntry::Pending { value };
        }

        if changed_values.is_empty() {
            return Ok(None);
        }

        Ok(Some(vec![Operation {
            diff: bincode::serialize(&LWWDiff { version: LWW_DIFF_VERSION, data: bincode::serialize(&changed_values)? })?,
        }]))
    }

    fn apply_operations(&self, operations: &[Operation]) -> Result<(), MutationError> { self.apply_operations_internal(operations, None) }

    fn apply_operations_with_event(&self, operations: &[Operation], event_id: EventId) -> Result<(), MutationError> {
        self.apply_operations_internal(operations, Some(event_id))
    }

    fn apply_layer(&self, layer: &EventLayer) -> Result<(), MutationError> {
        #[derive(Clone)]
        struct Candidate {
            value: Option<Value>,
            event_id: EventId,
            from_to_apply: bool,
            older_than_meet: bool,
        }

        let mut winners: BTreeMap<PropertyId, Candidate> = BTreeMap::new();

        // Stored values outside the accumulated DAG precede the meet, so any
        // layer candidate for that property wins.
        {
            let values = self.values.read().unwrap();
            for (prop, entry) in values.iter() {
                let Some(event_id) = entry.event_id() else {
                    return Err(MutationError::UpdateFailed(
                        anyhow::anyhow!("LWW candidate missing event_id for property {}", prop).into(),
                    ));
                };

                let known_in_dag = layer.dag_contains(&event_id);
                winners.insert(*prop, Candidate { value: entry.value(), event_id, from_to_apply: false, older_than_meet: !known_in_dag });
            }
        }

        for (event, from_to_apply) in layer.already_applied.iter().map(|e| (e, false)).chain(layer.to_apply.iter().map(|e| (e, true))) {
            for operation in event.operations().backend_operations(Self::property_backend_name()) {
                for (prop, value) in decode_changes(operation)? {
                    let candidate = Candidate { value, event_id: event.id(), from_to_apply, older_than_meet: false };
                    if let Some(current) = winners.get_mut(&prop) {
                        if current.older_than_meet {
                            *current = candidate;
                        } else {
                            match layer.compare(&candidate.event_id, &current.event_id) {
                                CausalRelation::Descends => *current = candidate,
                                CausalRelation::Ascends => {}
                                CausalRelation::Concurrent if candidate.event_id > current.event_id => *current = candidate,
                                CausalRelation::Concurrent => {}
                            }
                        }
                    } else {
                        winners.insert(prop, candidate);
                    }
                }
            }
        }

        let mut changed_fields = Vec::new();
        {
            let mut values = self.values.write().unwrap();
            for (prop, candidate) in winners {
                if candidate.from_to_apply {
                    values.insert(prop, ValueEntry::Committed { value: candidate.value, event_id: candidate.event_id });
                    changed_fields.push(prop);
                }
            }
        }

        super::notify_changed_fields(&self.field_broadcasts, changed_fields.iter());

        Ok(())
    }

    fn listen_field(&self, property: &PropertyId, listener: Listener) -> ankurah_signals::signal::ListenerGuard {
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(*property).or_default();
        broadcast.reference().listen(listener).into()
    }
}

impl LWWBackend {
    /// Get the broadcast ID for a specific field, creating the broadcast if necessary
    pub fn field_broadcast_id(&self, property: &PropertyId) -> ankurah_signals::broadcast::BroadcastId {
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(property.clone()).or_default();
        broadcast.id()
    }

    /// Get the event_id that last wrote a property value (if tracked).
    pub fn get_event_id(&self, property: &PropertyId) -> Option<EventId> {
        let values = self.values.read().unwrap();
        values.get(property).and_then(|entry| entry.event_id())
    }
    /// Internal implementation that handles both tracked and untracked operations.
    fn apply_operations_internal(&self, operations: &[Operation], event_id: Option<EventId>) -> Result<(), MutationError> {
        let mut changed_fields = Vec::new();

        for operation in operations {
            let changes = decode_changes(operation)?;
            let mut values = self.values.write().unwrap();
            for (property, new_value) in changes {
                let entry = match event_id.clone() {
                    Some(event_id) => ValueEntry::Committed { value: new_value, event_id },
                    None => ValueEntry::Pending { value: new_value },
                };
                values.insert(property.clone(), entry);
                changed_fields.push(property);
            }
        }

        super::notify_changed_fields(&self.field_broadcasts, changed_fields.iter());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_proto::{EntityId, SystemProperty};

    fn title() -> PropertyId {
        let mut bytes = [0u8; EntityId::BYTE_LEN];
        bytes[0] = 0x71;
        PropertyId::EntityId(EntityId::from_bytes(bytes))
    }

    fn committed_backend(event_id: EventId) -> LWWBackend {
        let backend = LWWBackend::new();
        backend.set(title(), Some(Value::String("alpha".into())));
        let ops = backend.to_operations().unwrap().expect("pending write should yield operations");
        backend.apply_operations_with_event(&ops, event_id).unwrap();
        backend
    }

    #[test]
    fn state_buffer_round_trips_with_version_header() {
        let event_id = EventId::from_bytes([7; 32]);
        let backend = committed_backend(event_id.clone());

        let buffer = backend.to_state_buffer().unwrap();
        assert_eq!(buffer[0], LWW_STATE_VERSION_2, "first byte is the state version header");

        let restored = LWWBackend::from_state_buffer(&buffer).unwrap();
        assert_eq!(restored.get(&title()), Some(Value::String("alpha".into())));
        assert_eq!(restored.get_event_id(&title()), Some(event_id));
    }

    #[test]
    fn stored_property_id_arms_round_trip() {
        let event_id = EventId::from_bytes([9; 32]);
        let backend = LWWBackend::new();
        backend.set(title(), Some(Value::String("registered".into())));
        backend.set(PropertyId::System(SystemProperty::Label), Some(Value::String("system".into())));
        let ops = backend.to_operations().unwrap().expect("pending writes should yield operations");
        backend.apply_operations_with_event(&ops, event_id).unwrap();

        let restored = LWWBackend::from_state_buffer(&backend.to_state_buffer().unwrap()).unwrap();
        assert_eq!(restored.get(&title()), Some(Value::String("registered".into())));
        assert_eq!(restored.get(&PropertyId::System(SystemProperty::Label)), Some(Value::String("system".into())));
    }

    #[test]
    fn operation_decoder_skips_id() {
        let data = bincode::serialize(&BTreeMap::from([
            (PropertyId::Id, Some(Value::EntityId(EntityId::from_bytes([1; 32])))),
            (title(), Some(Value::String("kept".into()))),
        ]))
        .unwrap();
        let operation = Operation { diff: bincode::serialize(&LWWDiff { version: LWW_DIFF_VERSION, data }).unwrap() };

        assert_eq!(decode_changes(&operation).unwrap(), BTreeMap::from([(title(), Some(Value::String("kept".into())))]));
    }

    #[test]
    fn set_skips_id() {
        let backend = LWWBackend::new();
        backend.set(PropertyId::Id, Some(Value::EntityId(EntityId::from_bytes([1; 32]))));

        assert_eq!(backend.get(&PropertyId::Id), None);
        assert!(backend.to_operations().unwrap().is_none());
    }

    #[test]
    fn name_keyed_09_buffer_is_refused() {
        let buffer = vec![LWW_STATE_VERSION_1, 0xde, 0xad];
        let err = LWWBackend::from_state_buffer(&buffer).unwrap_err();
        assert!(err.to_string().contains("name-keyed 0.9 encoding"), "unexpected error: {err}");
    }

    #[test]
    fn unversioned_pre_09_buffer_is_refused() {
        let legacy: BTreeMap<String, Option<Value>> = [("title".to_string(), Some(Value::String("alpha".into())))].into_iter().collect();
        let legacy_buffer = bincode::serialize(&legacy).unwrap();
        assert!(legacy_buffer[0] < LWW_STATE_VERSION_BASE);

        let err = LWWBackend::from_state_buffer(&legacy_buffer).unwrap_err();
        assert!(err.to_string().contains("pre-0.9"), "unexpected error: {err}");
    }

    #[test]
    fn unknown_future_version_is_refused() {
        let backend = committed_backend(EventId::from_bytes([7; 32]));
        let mut buffer = backend.to_state_buffer().unwrap();
        buffer[0] = LWW_STATE_VERSION_BASE + 9;

        let err = LWWBackend::from_state_buffer(&buffer).unwrap_err();
        assert!(err.to_string().contains("unknown LWW state buffer version"), "unexpected error: {err}");
    }

    #[test]
    fn empty_buffer_is_refused() {
        let err = LWWBackend::from_state_buffer(&Vec::new()).unwrap_err();
        assert!(err.to_string().contains("empty LWW state buffer"), "unexpected error: {err}");
    }
}
