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

const LWW_STATE_VERSION: u8 = 0xA2;

/// Order writes by generation, then event ID. Field order is comparison order.
///
/// [Shapiro et al. (2011), section 3.2.1][1] describe LWW using totally ordered
/// timestamps that increase along causality, keeping the greatest timestamp.
/// Here the timestamp is (generation, event ID): generation grows along parent
/// links, and event ID breaks ties. The winner is independent of delivery order
/// and needs no ancestry lookup between candidates.
///
/// [Zawirski et al. (2016), sections 2–4][2] separate causal overwrite from a configurable
/// resolver over concurrent writes. Our fixed rank already respects causality: taking
/// the greatest rank over all writes gives the same result as first excluding
/// superseded writes. We store one value/rank per property; their implementation also
/// carries a register-wide version vector. Entity-level history comparison still
/// determines which events to apply and checks known generation claims.
///
/// These properties assume generations have not saturated. At u32::MAX a descendant
/// can tie its ancestor's generation, so the fallback below uses causal comparison.
///
/// [1]: https://www.lip6.fr/Marc.Shapiro/papers/2011/Comprehensive-CRDTs-RR7506-2011-01.pdf
///      "A comprehensive study of Convergent and Commutative Replicated Data Types"
/// [2]: https://doi.org/10.1145/2911151.2911157
///      "Eventually Consistent Register Revisited"
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct WriteRank {
    generation: u32,
    event_id: EventId,
}

impl WriteRank {
    fn replaces_in_layer(&self, current: &Self, layer: &EventLayer) -> bool {
        if self.generation != u32::MAX || current.generation != u32::MAX {
            return self >= current;
        }
        // Preserve causal overwrite after saturation. Comparison must retain
        // the full ancestry here; generation alone cannot order these writes.
        if !layer.dag_contains(&current.event_id) {
            return true;
        }
        match layer.compare(&self.event_id, &current.event_id) {
            CausalRelation::Descends => true,
            CausalRelation::Ascends => false,
            CausalRelation::Concurrent => self.event_id > current.event_id,
        }
    }
}

#[derive(Clone, Debug)]
enum ValueEntry {
    Uncommitted { value: Option<Value> },
    Pending { value: Option<Value> },
    Committed { value: Option<Value>, rank: WriteRank },
}

impl ValueEntry {
    fn value(&self) -> Option<Value> {
        match self {
            ValueEntry::Uncommitted { value } => value.clone(),
            ValueEntry::Pending { value } => value.clone(),
            ValueEntry::Committed { value, .. } => value.clone(),
        }
    }

    fn rank(&self) -> Option<&WriteRank> {
        match self {
            ValueEntry::Committed { rank, .. } => Some(rank),
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
    rank: WriteRank,
}

fn decode_changes(operation: &Operation) -> Result<BTreeMap<PropertyId, Option<Value>>, MutationError> {
    let LWWDiff { version, data } = bincode::deserialize(&operation.diff)?;
    if version != LWW_DIFF_VERSION {
        return Err(MutationError::UpdateFailed(anyhow::anyhow!("Unknown LWW operation version: {:?}", version).into()));
    }
    let changes = bincode::deserialize::<BTreeMap<PropertyId, Option<Value>>>(&data)?;
    if changes.contains_key(&PropertyId::Id) {
        return Err(MutationError::UpdateFailed(anyhow::anyhow!("LWW operations cannot store the id pseudo-property").into()));
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
            let Some(rank) = entry.rank() else {
                return Err(StateError::SerializationError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("LWW state requires write rank for property {}", property),
                ))));
            };
            serializable.insert(property.clone(), CommittedEntry { value: entry.value(), rank: rank.clone() });
        }
        let mut state_buffer = vec![LWW_STATE_VERSION];
        bincode::serialize_into(&mut state_buffer, &serializable)?;
        Ok(state_buffer)
    }

    fn from_state_buffer(state_buffer: &Vec<u8>) -> std::result::Result<Self, crate::error::RetrievalError>
    where Self: Sized {
        let (version, payload) = match state_buffer.split_first() {
            Some((version, payload)) => (*version, payload),
            None => return Err(crate::error::RetrievalError::InvalidState("empty LWW state buffer".to_string())),
        };
        if version != LWW_STATE_VERSION {
            return Err(crate::error::RetrievalError::InvalidState(format!(
                "unsupported LWW state buffer version {version:#04x} (expected {LWW_STATE_VERSION:#04x})"
            )));
        }
        let raw_map = bincode::deserialize::<BTreeMap<PropertyId, CommittedEntry>>(payload)?;
        if raw_map.contains_key(&PropertyId::Id) {
            return Err(crate::error::RetrievalError::InvalidState("LWW state cannot store the id pseudo-property".into()));
        }
        let map = raw_map.into_iter().map(|(k, entry)| (k, ValueEntry::Committed { value: entry.value, rank: entry.rank })).collect();
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

    fn apply_layer(&self, layer: &EventLayer) -> Result<(), MutationError> {
        struct Candidate {
            value: Option<Value>,
            rank: WriteRank,
            from_to_apply: bool,
        }

        let mut winners: BTreeMap<PropertyId, Candidate> = BTreeMap::new();

        {
            let values = self.values.read().unwrap();
            for (event, from_to_apply) in layer.already_applied.iter().map(|e| (e, false)).chain(layer.to_apply.iter().map(|e| (e, true))) {
                let rank = WriteRank { generation: event.generation(), event_id: event.id() };
                for operation in event.operations().backend_operations(Self::property_backend_name()) {
                    for (prop, value) in decode_changes(operation)? {
                        let current =
                            winners.get(&prop).map(|candidate| &candidate.rank).or_else(|| values.get(&prop).and_then(ValueEntry::rank));
                        // A tie is the same event, so its later write takes the property.
                        if current.is_some_and(|current| !rank.replaces_in_layer(current, layer)) {
                            continue;
                        }
                        winners.insert(prop, Candidate { value, rank: rank.clone(), from_to_apply });
                    }
                }
            }
        }

        let mut changed_fields = Vec::new();
        {
            let mut values = self.values.write().unwrap();
            for (prop, candidate) in winners {
                if candidate.from_to_apply {
                    values.insert(prop, ValueEntry::Committed { value: candidate.value, rank: candidate.rank });
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
        values.get(property).and_then(|entry| entry.rank()).map(|rank| rank.event_id.clone())
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

    fn committed_backend() -> LWWBackend {
        let backend = LWWBackend::new();
        backend.set(title(), Some(Value::String("alpha".into())));
        crate::test_helpers::commit_backend(&backend, 7).unwrap();
        backend
    }

    #[test]
    fn state_buffer_round_trips_with_version_header() {
        let backend = committed_backend();
        let buffer = backend.to_state_buffer().unwrap();
        assert_eq!(buffer[0], LWW_STATE_VERSION, "first byte is the state version header");
        let restored = LWWBackend::from_state_buffer(&buffer).unwrap();
        assert_eq!(restored.get(&title()), Some(Value::String("alpha".into())));
        assert_eq!(restored.values.read().unwrap()[&title()].rank(), backend.values.read().unwrap()[&title()].rank());
    }

    #[test]
    fn layers_keep_the_greater_rank_in_either_order() {
        use ankurah_proto::{AuthorId, Clock, Event, OperationSet};
        let scratch = LWWBackend::new();
        scratch.set(title(), Some(Value::String("shallower".into())));
        let shallower = crate::test_helpers::commit_backend(&scratch, 9).unwrap();
        scratch.set(title(), Some(Value::String("deeper".into())));
        let operations = OperationSet::from_backends(BTreeMap::from([("lww".into(), scratch.to_operations().unwrap().unwrap())]));
        let deeper = (0..200)
            .find_map(|seed| {
                let event = Event {
                    entity_id: shallower.entity_id,
                    parent: Clock::singleton(&shallower),
                    body: ankurah_proto::EventBody::Update {
                        nonce: [seed; 32],
                        timestamp: 0,
                        author: AuthorId::Unknown,
                        operations: operations.clone(),
                    },
                };
                (event.id() < shallower.id()).then_some(event)
            })
            .expect("a deeper write whose ID sorts below the shallower one");
        for order in [[&deeper, &shallower], [&shallower, &deeper]] {
            let backend = LWWBackend::new();
            for event in order {
                crate::test_helpers::apply_backend_event(&backend, event).unwrap();
            }
            assert_eq!(backend.get(&title()), Some(Value::String("deeper".into())));
        }
    }

    #[test]
    fn stored_property_id_arms_round_trip() {
        let backend = LWWBackend::new();
        backend.set(title(), Some(Value::String("registered".into())));
        backend.set(PropertyId::System(SystemProperty::Label), Some(Value::String("system".into())));
        crate::test_helpers::commit_backend(&backend, 9).unwrap();
        let restored = LWWBackend::from_state_buffer(&backend.to_state_buffer().unwrap()).unwrap();
        assert_eq!(restored.get(&title()), Some(Value::String("registered".into())));
        assert_eq!(restored.get(&PropertyId::System(SystemProperty::Label)), Some(Value::String("system".into())));
    }

    #[test]
    fn operation_decoder_rejects_id() {
        let data = bincode::serialize(&BTreeMap::from([
            (PropertyId::Id, Some(Value::EntityId(EntityId::from_bytes([1; 32])))),
            (title(), Some(Value::String("kept".into()))),
        ]))
        .unwrap();
        let operation = Operation { diff: bincode::serialize(&LWWDiff { version: LWW_DIFF_VERSION, data }).unwrap() };

        assert!(decode_changes(&operation).unwrap_err().to_string().contains("cannot store the id pseudo-property"));
    }

    #[test]
    fn set_skips_id() {
        let backend = LWWBackend::new();
        backend.set(PropertyId::Id, Some(Value::EntityId(EntityId::from_bytes([1; 32]))));

        assert_eq!(backend.get(&PropertyId::Id), None);
        assert!(backend.to_operations().unwrap().is_none());
    }

    #[test]
    fn other_state_versions_are_refused() {
        let backend = committed_backend();
        let mut buffer = backend.to_state_buffer().unwrap();
        for version in [0x00, 0xA1, 0xFF] {
            buffer[0] = version;
            let err = LWWBackend::from_state_buffer(&buffer).unwrap_err();
            assert!(err.to_string().contains("unsupported LWW state buffer version"), "unexpected error: {err}");
        }
    }

    #[test]
    fn state_decoder_rejects_id() {
        let entry = CommittedEntry {
            value: Some(Value::EntityId(EntityId::from_bytes([1; 32]))),
            rank: WriteRank { generation: 1, event_id: EventId::from_bytes([2; 32]) },
        };
        let mut buffer = vec![LWW_STATE_VERSION];
        bincode::serialize_into(&mut buffer, &BTreeMap::from([(PropertyId::Id, entry)])).unwrap();
        assert!(LWWBackend::from_state_buffer(&buffer).unwrap_err().to_string().contains("cannot store the id pseudo-property"));
    }

    #[test]
    fn empty_buffer_is_refused() {
        let err = LWWBackend::from_state_buffer(&Vec::new()).unwrap_err();
        assert!(err.to_string().contains("empty LWW state buffer"), "unexpected error: {err}");
    }
    #[test]
    fn layer_only_considers_properties_its_operations_touch() {
        let backend = committed_backend();
        let original_writer = backend.get_event_id(&title());
        let unrelated = PropertyId::System(SystemProperty::Label);
        backend.set(unrelated, Some(Value::String("still local".into())));
        let scratch = LWWBackend::new();
        scratch.set(title(), Some(Value::String("updated".into())));
        let event = crate::test_helpers::commit_backend(&scratch, 8).unwrap();
        crate::test_helpers::apply_backend_event(&backend, &event).unwrap();
        assert_eq!(backend.get_event_id(&title()), Some(original_writer.unwrap().max(event.id())));
        assert_eq!(backend.get(&unrelated), Some(Value::String("still local".into())));
        assert_eq!(backend.get_event_id(&unrelated), None);
        assert!(backend.to_state_buffer().is_err(), "the unrelated local edit remains uncommitted");
    }
}
