use std::{
    any::Any,
    collections::BTreeMap,
    fmt::Debug,
    sync::{Arc, Mutex, RwLock},
};

use ankurah_proto::{EventId, Operation};
use ankurah_signals::signal::Listener;
use serde::{Deserialize, Serialize};

use crate::{
    error::{MutationError, StateError},
    event_dag::{CausalRelation, EventLayer},
    property::{backend::PropertyBackend, Value},
};
use ankql::ast::PropertyId;

/// Diff version for the id-keyed encoding this build emits: the payload is a
/// single [`PropertyId`]-tagged map (rfc 5.5 in
/// specs/model-property-metadata/rfc.md). The pre-identity-model name-keyed
/// v1 encoding is no longer decoded: the wire/on-disk format is deliberately
/// broken across this refactor, and a dev database is reset, not migrated.
const LWW_DIFF_VERSION_2: u8 = 2;

/// Version header for serialized LWW state buffers: the first byte of every
/// buffer identifies its encoding, and deserialization refuses buffers whose
/// version it does not know rather than guessing.
///
/// Versions are offset high so a corrupt or foreign buffer's small leading
/// byte (e.g. a stray bincode length prefix) can never collide with a real
/// version tag.
const LWW_STATE_VERSION_BASE: u8 = 0xA0;
/// State header for the id-keyed encoding this build emits: a single
/// [`PropertyId`]-tagged map of committed entries.
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

/// A dumb, [`PropertyId`]-keyed last-write-wins store. Every value is keyed by
/// its durable `PropertyId` (a registered user field's catalog id, or a
/// system/catalog field's name). The backend holds NO schema state: name
/// resolution happens once, upstream, at accessor construction (rfc.md 5.5),
/// so this type never sees the catalog, a binding, or a display-name hint.
#[derive(Debug)]
pub struct LWWBackend {
    values: RwLock<BTreeMap<PropertyId, ValueEntry>>,
    /// Field-change broadcasts keyed by [`PropertyId`]: a listener registers
    /// under the key its accessor resolved to, and a change fires under the
    /// key it wrote, so a change always reaches a listener resolved to the
    /// same key.
    field_broadcasts: Mutex<BTreeMap<PropertyId, ankurah_signals::broadcast::Broadcast>>,
}

#[derive(Serialize, Deserialize)]
pub struct LWWDiff {
    version: u8,
    data: Vec<u8>,
}

/// A committed entry in a state buffer: value plus the provenance event that
/// last wrote it. No display name: materialization needs are not the
/// backend's concern.
#[derive(Serialize, Deserialize, Clone)]
struct CommittedEntry {
    value: Option<Value>,
    event_id: EventId,
}

impl Default for LWWBackend {
    fn default() -> Self { Self::new() }
}

impl LWWBackend {
    pub fn new() -> LWWBackend { Self { values: RwLock::new(BTreeMap::default()), field_broadcasts: Mutex::new(BTreeMap::new()) } }

    /// Stage an uncommitted write under `key`. The caller has already
    /// resolved `key` (at accessor construction, rfc.md 5.5); this backend
    /// never resolves a name.
    pub fn set(&self, key: PropertyId, value: Option<Value>) {
        let mut values = self.values.write().unwrap();
        values.insert(key, ValueEntry::Uncommitted { value });
    }

    /// Read a key's stored value (present, whatever its commit state). This is
    /// a raw single-key read; see [`Self::entry`] for the presence-distinguishing
    /// primitive the generic read dispatch runs on.
    pub fn get(&self, key: &PropertyId) -> Option<Value> { self.values.read().unwrap().get(key).and_then(|e| e.value()) }

    /// Presence-distinguishing read of a single key: `None` = key absent,
    /// `Some(None)` = key present but cleared (a tombstone), `Some(Some(v))` =
    /// a live value. This is the primitive the generic read dispatch
    /// (`crate::property::read_by_id`, via [`PropertyBackend::entry`]) runs
    /// on -- a distinction the `Option`-valued [`Self::get`] cannot express.
    pub fn entry(&self, key: &PropertyId) -> Option<Option<Value>> { self.values.read().unwrap().get(key).map(|e| e.value()) }

    /// The event_id that last wrote a key's value (if tracked). Raw
    /// single-key lookup, same keying as [`Self::get`].
    pub fn get_event_id(&self, key: &PropertyId) -> Option<EventId> { self.values.read().unwrap().get(key).and_then(|e| e.event_id()) }

    /// Get the broadcast ID for a specific key, creating the broadcast if necessary.
    pub fn field_broadcast_id(&self, key: &PropertyId) -> ankurah_signals::broadcast::BroadcastId {
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(key.clone()).or_default();
        broadcast.id()
    }

    /// Build a decoded backend from a key map. Decode carries no schema
    /// state; keys are stored exactly as the buffer tagged them.
    fn from_decoded(map: BTreeMap<PropertyId, ValueEntry>) -> Self {
        Self { values: RwLock::new(map), field_broadcasts: Mutex::new(BTreeMap::new()) }
    }

    /// Internal apply for both tracked (committed) and untracked (pending) operations.
    fn apply_operations_internal(&self, operations: &[Operation], event_id: Option<EventId>) -> Result<(), MutationError> {
        let mut changed_fields = Vec::new();

        for operation in operations {
            let changes = decode_diff(&operation.diff)?;

            let mut values = self.values.write().unwrap();
            for (key, new_value) in changes {
                let entry = match event_id.clone() {
                    Some(event_id) => ValueEntry::Committed { value: new_value, event_id },
                    None => ValueEntry::Pending { value: new_value },
                };
                // Fire the broadcast under the key that changed; a listener
                // that resolved to the same key hears it (see field_broadcasts).
                changed_fields.push(key.clone());
                values.insert(key, entry);
            }
        }

        super::notify_changed_fields(&self.field_broadcasts, changed_fields.iter());
        Ok(())
    }
}

/// Decode an LWW diff into `(PropertyId, value)` changes: the single
/// [`PropertyId`]-tagged map this build emits (v2 / 0xA2).
fn decode_diff(diff: &[u8]) -> Result<Vec<(PropertyId, Option<Value>)>, MutationError> {
    let LWWDiff { version, data } = bincode::deserialize(diff)?;
    match version {
        LWW_DIFF_VERSION_2 => Ok(bincode::deserialize::<BTreeMap<PropertyId, Option<Value>>>(&data)?.into_iter().collect()),
        version => Err(MutationError::UpdateFailed(anyhow::anyhow!("Unknown LWW operation version: {:?}", version).into())),
    }
}

impl PropertyBackend for LWWBackend {
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> { self as Arc<dyn Any + Send + Sync + 'static> }

    fn as_debug(&self) -> &dyn Debug { self as &dyn Debug }

    fn fork(&self) -> Arc<dyn PropertyBackend> {
        let values = self.values.read().unwrap().clone();
        // Fresh broadcasts (transaction isolation): don't clone the existing ones.
        Arc::new(Self { values: RwLock::new(values), field_broadcasts: Mutex::new(BTreeMap::new()) })
    }

    fn properties(&self) -> Vec<PropertyId> { self.values.read().unwrap().keys().cloned().collect() }

    fn property_value(&self, key: &PropertyId) -> Option<Value> { self.get(key) }

    fn property_values(&self) -> BTreeMap<PropertyId, Option<Value>> {
        self.values.read().unwrap().iter().map(|(k, v)| (k.clone(), v.value())).collect()
    }

    fn entry(&self, key: &PropertyId) -> Option<Option<Value>> { LWWBackend::entry(self, key) }

    fn restage(&self, key: &PropertyId, value: Option<Value>) { self.set(key.clone(), value); }

    fn property_backend_name() -> &'static str { "lww" }

    fn to_state_buffer(&self) -> Result<Vec<u8>, StateError> {
        // Serialize with the required event_id for per-property conflict
        // resolution after loading. Always the id-keyed (0xA2) tagged form.
        let values = self.values.read().unwrap();
        let mut serializable: BTreeMap<PropertyId, CommittedEntry> = BTreeMap::new();
        for (key, entry) in values.iter() {
            let event_id = entry.event_id().ok_or_else(|| {
                StateError::SerializationError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("LWW state requires event_id for property {:?}", key),
                )))
            })?;
            serializable.insert(key.clone(), CommittedEntry { value: entry.value(), event_id });
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
        match version {
            LWW_STATE_VERSION_2 => {
                // The only decodable payload: one PropertyId-tagged map. Keys
                // land exactly as tagged (EntityId for registered user data,
                // System for system/catalog fields).
                let raw_map = bincode::deserialize::<BTreeMap<PropertyId, CommittedEntry>>(payload)?;
                let map = raw_map
                    .into_iter()
                    .map(|(k, entry)| (k, ValueEntry::Committed { value: entry.value, event_id: entry.event_id }))
                    .collect();
                Ok(Self::from_decoded(map))
            }
            // Every other version -- including the retired pre-identity-model
            // encodings -- is refused loudly rather than misread. The
            // wire/on-disk format is deliberately broken across this
            // refactor; a dev database is reset, not migrated.
            _ => Err(crate::error::RetrievalError::Other(format!(
                "unknown LWW state buffer version {version:#04x} (this binary supports {LWW_STATE_VERSION_2:#04x})"
            ))),
        }
    }

    fn to_operations(&self) -> Result<Option<Vec<Operation>>, MutationError> {
        let mut values = self.values.write().unwrap();

        // Collect the keys+values that transitioned Uncommitted -> Pending.
        let mut changed: BTreeMap<PropertyId, Option<Value>> = BTreeMap::new();
        for (key, entry) in values.iter_mut() {
            let ValueEntry::Uncommitted { value } = entry else {
                continue;
            };
            let value = value.clone();
            changed.insert(key.clone(), value.clone());
            *entry = ValueEntry::Pending { value };
        }

        if changed.is_empty() {
            return Ok(None);
        }

        let diff = bincode::serialize(&LWWDiff { version: LWW_DIFF_VERSION_2, data: bincode::serialize(&changed)? })?;
        Ok(Some(vec![Operation { diff }]))
    }

    fn apply_operations(&self, operations: &[Operation]) -> Result<(), MutationError> {
        // No event tracking - used when loading from state buffer
        self.apply_operations_internal(operations, None)
    }

    fn apply_operations_with_event(&self, operations: &[Operation], event_id: EventId) -> Result<(), MutationError> {
        // Track which event set each property - used for all event applications
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

        // Keyed by PropertyId: EntityId-keyed and System-keyed candidates
        // elect independently (no binding folds one onto the other). The
        // per-key LWW logic is identical for both.
        let mut winners: BTreeMap<PropertyId, Candidate> = BTreeMap::new();

        // Seed with stored last-write candidates (required event_id).
        {
            let values = self.values.read().unwrap();
            for (prop, entry) in values.iter() {
                let Some(event_id) = entry.event_id() else {
                    return Err(MutationError::UpdateFailed(
                        anyhow::anyhow!("LWW candidate missing event_id for property {:?}", prop).into(),
                    ));
                };

                // KEY RULE: If stored event_id is NOT in the accumulated DAG,
                // it is strictly older than the meet. Any layer candidate wins.
                // We still seed it so it participates if no layer event touches
                // this property, but mark it as auto-losable.
                let known_in_dag = layer.dag_contains(&event_id);
                winners.insert(
                    prop.clone(),
                    Candidate { value: entry.value(), event_id, from_to_apply: false, older_than_meet: !known_in_dag },
                );
            }
        }

        // Add candidates from events in this layer.
        for (event, from_to_apply) in layer.already_applied.iter().map(|e| (e, false)).chain(layer.to_apply.iter().map(|e| (e, true))) {
            if let Some(operations) = event.operations.get(&Self::property_backend_name().to_string()) {
                for operation in operations {
                    // Both wire versions normalize to (PropertyId, value);
                    // election below is version-agnostic.
                    for (prop, value) in decode_diff(&operation.diff)? {
                        let candidate = Candidate { value, event_id: event.id(), from_to_apply, older_than_meet: false };
                        if let Some(current) = winners.get_mut(&prop) {
                            if current.older_than_meet {
                                // Stored value is below meet -- any layer candidate wins
                                *current = candidate;
                            } else {
                                // Both in accumulated set -- use causal comparison (infallible)
                                let relation = layer.compare(&candidate.event_id, &current.event_id);
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
                            }
                        } else {
                            winners.insert(prop, candidate);
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
                    changed_fields.push(prop.clone());
                    values.insert(prop, ValueEntry::Committed { value: candidate.value, event_id: candidate.event_id });
                }
            }
        }

        // Notify subscribers for changed fields
        super::notify_changed_fields(&self.field_broadcasts, changed_fields.iter());

        Ok(())
    }

    fn listen_field(&self, key: &PropertyId, listener: Listener) -> ankurah_signals::signal::ListenerGuard {
        // Get or create the broadcast for this key
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(key.clone()).or_default();

        // Subscribe to the broadcast and return the guard
        broadcast.reference().listen(listener).into()
    }

    fn uncommitted_keys(&self) -> Vec<PropertyId> {
        self.values
            .read()
            .unwrap()
            .iter()
            .filter_map(|(key, entry)| matches!(entry, ValueEntry::Uncommitted { .. }).then(|| key.clone()))
            .collect()
    }
}

// Need ID based happens-before determination to resolve conflicts

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_proto::EntityId;

    fn id(byte: u8) -> EntityId {
        let mut bytes = [0u8; 16];
        bytes[0] = byte;
        EntityId::from_bytes(bytes)
    }

    /// A registered-property key.
    fn eid(byte: u8) -> PropertyId { PropertyId::EntityId(id(byte)) }

    /// A system/catalog-collection key.
    fn sys(property: ankql::ast::SystemProperty) -> PropertyId { PropertyId::System(property) }

    /// Commit a single system-keyed write and return the backend (a
    /// system-table shape: System key, committed provenance).
    fn committed_backend(event_id: EventId) -> LWWBackend {
        let backend = LWWBackend::new();
        backend.set(sys(ankql::ast::SystemProperty::Name), Some(Value::String("alpha".into())));
        let ops = backend.to_operations().unwrap().expect("pending write should yield operations");
        backend.apply_operations_with_event(&ops, event_id).unwrap();
        backend
    }

    #[test]
    fn state_buffer_round_trips_id_and_system_under_one_tag() {
        let event_id = EventId::from_bytes([7; 32]);
        let backend = LWWBackend::new();
        backend.set(eid(0x11), Some(Value::String("alpha".into())));
        backend.set(sys(ankql::ast::SystemProperty::Label), Some(Value::I64(7)));
        let ops = backend.to_operations().unwrap().unwrap();
        backend.apply_operations_with_event(&ops, event_id.clone()).unwrap();

        let buffer = backend.to_state_buffer().unwrap();
        assert_eq!(buffer[0], LWW_STATE_VERSION_2, "always emits the id-keyed 0xA2 header");
        // The payload is one PropertyId-tagged map: EntityId and System coexist.
        let decoded: BTreeMap<PropertyId, CommittedEntry> = bincode::deserialize(&buffer[1..]).unwrap();
        assert_eq!(decoded.get(&eid(0x11)).map(|e| e.value.clone()), Some(Some(Value::String("alpha".into()))));
        assert_eq!(decoded.get(&sys(ankql::ast::SystemProperty::Label)).map(|e| e.value.clone()), Some(Some(Value::I64(7))));

        let restored = LWWBackend::from_state_buffer(&buffer).unwrap();
        assert_eq!(restored.get(&eid(0x11)), Some(Value::String("alpha".into())));
        assert_eq!(restored.get(&sys(ankql::ast::SystemProperty::Label)), Some(Value::I64(7)));
        assert_eq!(restored.get_event_id(&eid(0x11)), Some(event_id));
    }

    #[test]
    fn diff_round_trips_tagged_property_id() {
        let source = LWWBackend::new();
        source.set(eid(0x11), Some(Value::String("alpha".into())));
        source.set(sys(ankql::ast::SystemProperty::Label), Some(Value::I64(7)));
        let ops = source.to_operations().unwrap().expect("writes yield operations");

        let LWWDiff { version, data } = bincode::deserialize(&ops[0].diff).unwrap();
        assert_eq!(version, LWW_DIFF_VERSION_2);
        let changes: BTreeMap<PropertyId, Option<Value>> = bincode::deserialize(&data).unwrap();
        assert_eq!(changes.get(&eid(0x11)), Some(&Some(Value::String("alpha".into()))));
        assert_eq!(changes.get(&sys(ankql::ast::SystemProperty::Label)), Some(&Some(Value::I64(7))));

        let sink = LWWBackend::new();
        sink.apply_operations_with_event(&ops, EventId::from_bytes([9; 32])).unwrap();
        assert_eq!(sink.get(&eid(0x11)), Some(Value::String("alpha".into())));
        assert_eq!(sink.get(&sys(ankql::ast::SystemProperty::Label)), Some(Value::I64(7)));
    }

    #[test]
    fn unknown_future_version_is_refused() {
        let backend = committed_backend(EventId::from_bytes([7; 32]));
        let mut buffer = backend.to_state_buffer().unwrap();
        buffer[0] = LWW_STATE_VERSION_BASE + 9;
        let err = LWWBackend::from_state_buffer(&buffer).unwrap_err();
        assert!(err.to_string().contains("unknown LWW state buffer version"), "unexpected error: {err}");
        // The supported-version text names the one decodable version.
        assert!(err.to_string().contains("0xa2"), "supported version should be named: {err}");
    }

    #[test]
    fn empty_buffer_is_refused() {
        let err = LWWBackend::from_state_buffer(&Vec::new()).unwrap_err();
        assert!(err.to_string().contains("empty LWW state buffer"), "unexpected error: {err}");
    }

    #[test]
    fn unknown_diff_version_is_refused() {
        let backend = committed_backend(EventId::from_bytes([7; 32]));
        let bad_diff =
            bincode::serialize(&LWWDiff { version: 3, data: bincode::serialize(&BTreeMap::<PropertyId, Option<Value>>::new()).unwrap() })
                .unwrap();
        let err = backend.apply_operations(&[Operation { diff: bad_diff }]).unwrap_err();
        assert!(err.to_string().contains("Unknown LWW operation version"), "unexpected error: {err}");
    }

    // ---- entry(): the presence primitive the read dispatch builds on -------
    // (The read dispatch itself lives OUTSIDE the backend; its tests are with
    // it, `property::read_dispatch_tests`.)

    #[test]
    fn entry_distinguishes_absent_tombstone_and_value() {
        let backend = LWWBackend::new();
        assert_eq!(backend.entry(&sys(ankql::ast::SystemProperty::Name)), None, "never written: absent");
        backend.set(sys(ankql::ast::SystemProperty::Name), None);
        assert_eq!(backend.entry(&sys(ankql::ast::SystemProperty::Name)), Some(None), "cleared: present tombstone");
        backend.set(sys(ankql::ast::SystemProperty::Name), Some(Value::String("v".into())));
        assert_eq!(backend.entry(&sys(ankql::ast::SystemProperty::Name)), Some(Some(Value::String("v".into()))));
        // Commit state does not affect presence semantics.
        let ops = backend.to_operations().unwrap().unwrap();
        backend.apply_operations_with_event(&ops, EventId::from_bytes([4; 32])).unwrap();
        assert_eq!(backend.entry(&sys(ankql::ast::SystemProperty::Name)), Some(Some(Value::String("v".into()))));
    }

    // ---- election + reactivity -------------------------------------------

    #[test]
    fn id_keyed_change_fires_broadcast_for_id_listener() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let backend = LWWBackend::new();
        let fired = Arc::new(AtomicUsize::new(0));
        let fired_clone = fired.clone();
        // A listener registered under the resolved id (as the accessor does
        // once it resolves the field name).
        let _guard = backend.listen_field(
            &eid(0x11),
            Arc::new(move |_| {
                fired_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        backend.set(eid(0x11), Some(Value::String("alpha".into())));
        let ops = backend.to_operations().unwrap().unwrap();
        backend.apply_operations_with_event(&ops, EventId::from_bytes([1; 32])).unwrap();

        assert_eq!(fired.load(Ordering::SeqCst), 1, "an id-keyed change fires the id-registered broadcast");
    }

    #[test]
    fn apply_layer_is_idempotent_for_single_writer() {
        // A committed backend re-applying its own single event is a no-op on
        // the value (sanity that PropertyId-keyed election round-trips).
        let backend = LWWBackend::new();
        backend.set(eid(0x11), Some(Value::String("alpha".into())));
        let ops = backend.to_operations().unwrap().expect("pending write should yield operations");
        backend.apply_operations_with_event(&ops, EventId::from_bytes([1; 32])).unwrap();
        assert_eq!(backend.get(&eid(0x11)), Some(Value::String("alpha".into())));
    }
}
