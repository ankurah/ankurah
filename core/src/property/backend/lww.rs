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
    property::{backend::PropertyBackend, PropertyKey, PropertyName, Value},
};

/// Diff version for the legacy name-keyed (pre-epoch, 0.9) encoding. Still
/// DECODED (real 0.9 events carry it); never emitted.
const LWW_DIFF_VERSION_1: u8 = 1;
/// Diff version for the id-keyed encoding this build emits. The payload is a
/// single [`PropertyKey`]-tagged map: each key self-identifies as `Id` or
/// `Name` (the ratified PropertyKey amendment on #289, RFC 5.5 in
/// specs/model-property-metadata/rfc.md).
const LWW_DIFF_VERSION_2: u8 = 2;

/// Version header for serialized LWW state buffers: the first byte of every
/// buffer identifies its encoding, and deserialization refuses buffers whose
/// version it does not know rather than guessing.
///
/// Versions are offset high because unversioned pre-0.9 buffers were raw
/// bincode maps whose first byte is the low byte of the property count -- a
/// small number. Keeping versions at 0xA1+ makes the two ranges disjoint, so
/// one byte classifies any buffer without parse-probing. (A legacy entity
/// would need 160+ properties to be misclassified, and would then fail
/// loudly during deserialization, never silently misparse.)
const LWW_STATE_VERSION_BASE: u8 = 0xA0;
/// State header for the legacy name-keyed (0.9) encoding. Still DECODED; never
/// emitted.
const LWW_STATE_VERSION_1: u8 = LWW_STATE_VERSION_BASE + 1;
/// State header for the id-keyed encoding this build emits: a single
/// [`PropertyKey`]-tagged map of committed entries (the ratified PropertyKey
/// amendment, #289).
const LWW_STATE_VERSION_2: u8 = LWW_STATE_VERSION_BASE + 2;

/// Provenance stamp for values loaded from unversioned (pre-0.9) state
/// buffers, which recorded no per-property event id. All-zeros is not a
/// reachable content hash, so it is never found in an accumulated DAG and
/// merge resolution treats such values as older-than-meet: any event that
/// writes the property wins. For pre-0.9 stores this reproduces the true
/// outcome exactly -- their histories are linear, so a real per-property
/// stamp would also lose to every later write -- and unlike stamping with
/// the local head it yields the same election result on every replica,
/// including replicas that rebuilt provenance by replaying events.
const LEGACY_EVENT_ID: [u8; 32] = [0; 32];

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

/// A dumb, PropertyKey-keyed last-write-wins store. Every value is keyed by a
/// [`PropertyKey`] that self-identifies as an id (registered user data) or a
/// name (system/catalog collections, or legacy pre-epoch data). The backend
/// holds NO schema state: name-to-id resolution happens on the catalog-aware
/// write/read paths (the ratified PropertyKey amendment, #289), so this type
/// never sees the catalog, a binding, or a display-name hint.
#[derive(Debug)]
pub struct LWWBackend {
    values: RwLock<BTreeMap<PropertyKey, ValueEntry>>,
    /// Field-change broadcasts keyed by [`PropertyKey`]: a listener registers
    /// under the key its accessor resolved to (id when the catalog knows the
    /// field, else name), and a change fires under the key it wrote, so
    /// id-keyed changes reach id-registered listeners. A change whose key the
    /// accessor could not resolve at subscribe time is the accepted signals
    /// known-limitation (the PropertyKey amendment, #289).
    field_broadcasts: Mutex<BTreeMap<PropertyKey, ankurah_signals::broadcast::Broadcast>>,
}

#[derive(Serialize, Deserialize)]
pub struct LWWDiff {
    version: u8,
    data: Vec<u8>,
}

/// A committed entry in a state buffer: value plus the provenance event that
/// last wrote it. No display name: materialization needs are not the backend's
/// concern (the PropertyKey amendment withdrew the 0xA2 display-name hint).
/// The same shape decodes both the legacy 0xA1 (name-keyed)
/// payload and the current 0xA2 ([`PropertyKey`]-keyed) payload.
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

    /// Stage an uncommitted write under the accessor's key. Ordinary sync
    /// accessors pass [`PropertyKey::Name`] and the async commit path re-keys
    /// it before the event; an explicit-id accessor already passes
    /// [`PropertyKey::Id`]. System and catalog collections pass `Name` and
    /// stay name-keyed (the PropertyKey amendment, #289).
    pub fn set(&self, key: PropertyKey, value: Option<Value>) {
        let mut values = self.values.write().unwrap();
        values.insert(key, ValueEntry::Uncommitted { value });
    }

    /// Read a key's stored value (present, whatever its commit state). This is
    /// a raw single-key read; the Id-then-Name legacy fallback lives on the
    /// catalog-aware caller (see [`entry`]).
    pub fn get(&self, key: &PropertyKey) -> Option<Value> { self.values.read().unwrap().get(key).and_then(|e| e.value()) }

    /// Presence-distinguishing read of a single key: `None` = key absent,
    /// `Some(None)` = key present but cleared (a tombstone), `Some(Some(v))` =
    /// a live value. This is the primitive the generic read dispatch
    /// (`crate::property::read_resolved`, via `PropertyBackend::entry`) builds
    /// the legacy Id-then-Name fallback on: a PRESENT id entry (even a
    /// tombstone) is authoritative and never falls back to a stale legacy
    /// `Name` residue -- a distinction the `Option`-valued [`get`] cannot
    /// express. The backend itself holds no name-to-id knowledge (the
    /// PropertyKey amendment, #289); the dispatch lives entirely on the
    /// caller.
    pub fn entry(&self, key: &PropertyKey) -> Option<Option<Value>> { self.values.read().unwrap().get(key).map(|e| e.value()) }

    /// Presence-distinguishing read restricted to a transaction-local staged
    /// write. This lets an ordinary name-addressed accessor read its own
    /// write before commit without giving committed legacy `Name` residue
    /// precedence over the authoritative id-keyed entry.
    pub(crate) fn uncommitted_entry(&self, key: &PropertyKey) -> Option<Option<Value>> {
        self.values.read().unwrap().get(key).and_then(|entry| match entry {
            ValueEntry::Uncommitted { value } => Some(value.clone()),
            ValueEntry::Pending { .. } | ValueEntry::Committed { .. } => None,
        })
    }

    /// The event_id that last wrote a key's value (if tracked). Raw single-key
    /// lookup, same keying as [`get`].
    pub fn get_event_id(&self, key: &PropertyKey) -> Option<EventId> { self.values.read().unwrap().get(key).and_then(|e| e.event_id()) }

    /// Get the broadcast ID for a specific key, creating the broadcast if necessary.
    pub fn field_broadcast_id(&self, key: &PropertyKey) -> ankurah_signals::broadcast::BroadcastId {
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(key.clone()).or_default();
        broadcast.id()
    }

    /// Build a decoded backend from a key map. Decode carries no schema state;
    /// keys are stored exactly as the buffer named them (legacy/0xA1 -> `Name`,
    /// 0xA2 -> whatever the buffer tagged).
    fn from_decoded(map: BTreeMap<PropertyKey, ValueEntry>) -> Self {
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

/// Decode an LWW diff into `(PropertyKey, value)` changes. Legacy version 1
/// carries a name-keyed map (real 0.9 events); version 2 carries the
/// [`PropertyKey`]-tagged map this build emits.
fn decode_diff(diff: &[u8]) -> Result<Vec<(PropertyKey, Option<Value>)>, MutationError> {
    let LWWDiff { version, data } = bincode::deserialize(diff)?;
    match version {
        LWW_DIFF_VERSION_1 => Ok(bincode::deserialize::<BTreeMap<PropertyName, Option<Value>>>(&data)?
            .into_iter()
            .map(|(name, value)| (PropertyKey::Name(name), value))
            .collect()),
        LWW_DIFF_VERSION_2 => Ok(bincode::deserialize::<BTreeMap<PropertyKey, Option<Value>>>(&data)?.into_iter().collect()),
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

    fn properties(&self) -> Vec<PropertyKey> { self.values.read().unwrap().keys().cloned().collect() }

    fn property_value(&self, key: &PropertyKey) -> Option<Value> { self.get(key) }

    fn property_values(&self) -> BTreeMap<PropertyKey, Option<Value>> {
        self.values.read().unwrap().iter().map(|(k, v)| (k.clone(), v.value())).collect()
    }

    fn entry(&self, key: &PropertyKey) -> Option<Option<Value>> { LWWBackend::entry(self, key) }

    fn restage(&self, key: &PropertyKey, value: Option<Value>) { self.set(key.clone(), value); }

    fn property_backend_name() -> &'static str { "lww" }

    fn to_state_buffer(&self) -> Result<Vec<u8>, StateError> {
        // Serialize with the required event_id for per-property conflict
        // resolution after loading. Always the id-keyed (0xA2) tagged form.
        let values = self.values.read().unwrap();
        let mut serializable: BTreeMap<PropertyKey, CommittedEntry> = BTreeMap::new();
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
        if version < LWW_STATE_VERSION_BASE {
            // Unversioned pre-0.9 buffer: raw bincode of property -> value, no
            // header and no per-property provenance. The version ranges are
            // disjoint by construction, so this is a dispatch on the same
            // single byte, not a parse probe. Loaded values carry
            // LEGACY_EVENT_ID; the next to_state_buffer rewrites them 0xA2.
            let legacy_map = bincode::deserialize::<BTreeMap<PropertyName, Option<Value>>>(state_buffer)
                .map_err(|e| crate::error::RetrievalError::Other(format!("failed to parse pre-0.9 legacy LWW state buffer: {e}")))?;
            let map = legacy_map
                .into_iter()
                .map(|(k, value)| (PropertyKey::Name(k), ValueEntry::Committed { value, event_id: EventId::from_bytes(LEGACY_EVENT_ID) }))
                .collect();
            return Ok(Self::from_decoded(map));
        }
        match version {
            LWW_STATE_VERSION_1 => {
                // Legacy name-keyed (0.9) payload: still emitted by real 0.9
                // stores. Decodes to Name keys; the next save rewrites 0xA2.
                let raw_map = bincode::deserialize::<BTreeMap<PropertyName, CommittedEntry>>(payload)?;
                let map = raw_map
                    .into_iter()
                    .map(|(k, entry)| (PropertyKey::Name(k), ValueEntry::Committed { value: entry.value, event_id: entry.event_id }))
                    .collect();
                Ok(Self::from_decoded(map))
            }
            LWW_STATE_VERSION_2 => {
                // Current id-keyed payload: one PropertyKey-tagged map. Keys
                // land exactly as tagged (Id for user data, Name for system).
                let raw_map = bincode::deserialize::<BTreeMap<PropertyKey, CommittedEntry>>(payload)?;
                let map = raw_map
                    .into_iter()
                    .map(|(k, entry)| (k, ValueEntry::Committed { value: entry.value, event_id: entry.event_id }))
                    .collect();
                Ok(Self::from_decoded(map))
            }
            // Unknown versions are refused loudly rather than misread. A 0.9
            // binary refuses 0xA2 (and anything above 0xA1) via this identical
            // arm it shipped with; #294's version negotiation turns that
            // refusal into a negotiated capability rather than an error.
            _ => Err(crate::error::RetrievalError::Other(format!(
                "unknown LWW state buffer version {version:#04x} (this binary supports {LWW_STATE_VERSION_1:#04x} and {LWW_STATE_VERSION_2:#04x})"
            ))),
        }
    }

    fn to_operations(&self) -> Result<Option<Vec<Operation>>, MutationError> {
        let mut values = self.values.write().unwrap();

        // Collect the keys+values that transitioned Uncommitted -> Pending.
        let mut changed: BTreeMap<PropertyKey, Option<Value>> = BTreeMap::new();
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

        // Keyed by PropertyKey: id-keyed and name-keyed candidates elect
        // independently (no binding folds a name onto an id -- a legacy name
        // entry is simply shadowed by its id entry on read, the accepted
        // stale-name erratum). The per-key LWW logic is identical for both.
        let mut winners: BTreeMap<PropertyKey, Candidate> = BTreeMap::new();

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
            if let Some(operations) = event.operations().get(Self::property_backend_name()) {
                for operation in operations {
                    // Both wire versions normalize to (PropertyKey, value);
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

    fn listen_field(&self, key: &PropertyKey, listener: Listener) -> ankurah_signals::signal::ListenerGuard {
        // Get or create the broadcast for this key
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(key.clone()).or_default();

        // Subscribe to the broadcast and return the guard
        broadcast.reference().listen(listener).into()
    }

    fn uncommitted_keys(&self) -> Vec<PropertyKey> {
        self.values
            .read()
            .unwrap()
            .iter()
            .filter_map(|(key, entry)| matches!(entry, ValueEntry::Uncommitted { .. }).then(|| key.clone()))
            .collect()
    }

    fn rekey(&self, from: &PropertyKey, to: PropertyKey) {
        let mut values = self.values.write().unwrap();
        if let Some(entry) = values.remove(from) {
            values.insert(to, entry);
        }
    }
}

// Need ID based happens-before determination to resolve conflicts

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_proto::EntityId;

    fn id(byte: u8) -> EntityId {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        EntityId::from_bytes(bytes)
    }

    /// Commit a single name-keyed write and return the backend (a system-table
    /// shape: Name key, committed provenance).
    fn committed_backend(event_id: EventId) -> LWWBackend {
        let backend = LWWBackend::new();
        backend.set(PropertyKey::name("title"), Some(Value::String("alpha".into())));
        let ops = backend.to_operations().unwrap().expect("pending write should yield operations");
        backend.apply_operations_with_event(&ops, event_id).unwrap();
        backend
    }

    #[test]
    fn state_buffer_round_trips_id_and_name_under_one_tag() {
        let event_id = EventId::from_bytes([7; 32]);
        let backend = LWWBackend::new();
        backend.set(PropertyKey::Id(id(0x11)), Some(Value::String("alpha".into())));
        backend.set(PropertyKey::name("legacy"), Some(Value::I64(7)));
        let ops = backend.to_operations().unwrap().unwrap();
        backend.apply_operations_with_event(&ops, event_id.clone()).unwrap();

        let buffer = backend.to_state_buffer().unwrap();
        assert_eq!(buffer[0], LWW_STATE_VERSION_2, "always emits the id-keyed 0xA2 header");
        // The payload is one PropertyKey-tagged map: Id and Name coexist.
        let decoded: BTreeMap<PropertyKey, CommittedEntry> = bincode::deserialize(&buffer[1..]).unwrap();
        assert_eq!(decoded.get(&PropertyKey::Id(id(0x11))).map(|e| e.value.clone()), Some(Some(Value::String("alpha".into()))));
        assert_eq!(decoded.get(&PropertyKey::Name("legacy".into())).map(|e| e.value.clone()), Some(Some(Value::I64(7))));

        let restored = LWWBackend::from_state_buffer(&buffer).unwrap();
        assert_eq!(restored.get(&PropertyKey::Id(id(0x11))), Some(Value::String("alpha".into())));
        assert_eq!(restored.get(&PropertyKey::name("legacy")), Some(Value::I64(7)));
        assert_eq!(restored.get_event_id(&PropertyKey::Id(id(0x11))), Some(event_id));
    }

    #[test]
    fn diff_round_trips_tagged_propertykey() {
        let source = LWWBackend::new();
        source.set(PropertyKey::Id(id(0x11)), Some(Value::String("alpha".into())));
        source.set(PropertyKey::name("legacy"), Some(Value::I64(7)));
        let ops = source.to_operations().unwrap().expect("writes yield operations");

        let LWWDiff { version, data } = bincode::deserialize(&ops[0].diff).unwrap();
        assert_eq!(version, LWW_DIFF_VERSION_2);
        let changes: BTreeMap<PropertyKey, Option<Value>> = bincode::deserialize(&data).unwrap();
        assert_eq!(changes.get(&PropertyKey::Id(id(0x11))), Some(&Some(Value::String("alpha".into()))));
        assert_eq!(changes.get(&PropertyKey::Name("legacy".into())), Some(&Some(Value::I64(7))));

        let sink = LWWBackend::new();
        sink.apply_operations_with_event(&ops, EventId::from_bytes([9; 32])).unwrap();
        assert_eq!(sink.get(&PropertyKey::Id(id(0x11))), Some(Value::String("alpha".into())));
        assert_eq!(sink.get(&PropertyKey::name("legacy")), Some(Value::I64(7)));
    }

    #[test]
    fn unversioned_pre_09_buffer_loads_via_fallback_and_upgrades_on_save() {
        // Pre-0.9 buffer: raw bincode of property -> Option<Value>, no header.
        let legacy: BTreeMap<PropertyName, Option<Value>> =
            [("title".to_string(), Some(Value::String("alpha".into())))].into_iter().collect();
        let legacy_buffer = bincode::serialize(&legacy).unwrap();
        assert!(legacy_buffer[0] < LWW_STATE_VERSION_BASE);

        let restored = LWWBackend::from_state_buffer(&legacy_buffer).unwrap();
        assert_eq!(restored.get(&PropertyKey::name("title")), Some(Value::String("alpha".into())));
        assert_eq!(restored.get_event_id(&PropertyKey::name("title")), Some(EventId::from_bytes(LEGACY_EVENT_ID)));

        // Re-serialization writes the current 0xA2 format (lazy upgrade), still
        // name-keyed (nothing resolved the legacy names to ids).
        let upgraded = restored.to_state_buffer().unwrap();
        assert_eq!(upgraded[0], LWW_STATE_VERSION_2);
        let round_tripped = LWWBackend::from_state_buffer(&upgraded).unwrap();
        assert_eq!(round_tripped.get(&PropertyKey::name("title")), Some(Value::String("alpha".into())));
        assert_eq!(round_tripped.get_event_id(&PropertyKey::name("title")), Some(EventId::from_bytes(LEGACY_EVENT_ID)));
    }

    #[test]
    fn legacy_0xa1_buffer_decodes_to_name_keys() {
        // A real 0.9 store emits 0xA1 with a name-keyed CommittedEntry map.
        let event_id = EventId::from_bytes([5; 32]);
        let raw: BTreeMap<PropertyName, CommittedEntry> =
            [("title".to_string(), CommittedEntry { value: Some(Value::String("alpha".into())), event_id: event_id.clone() })]
                .into_iter()
                .collect();
        let mut buffer = vec![LWW_STATE_VERSION_1];
        bincode::serialize_into(&mut buffer, &raw).unwrap();

        let restored = LWWBackend::from_state_buffer(&buffer).unwrap();
        assert_eq!(restored.get(&PropertyKey::name("title")), Some(Value::String("alpha".into())));
        assert_eq!(restored.get_event_id(&PropertyKey::name("title")), Some(event_id));
        // And it upgrades to 0xA2 on save.
        assert_eq!(restored.to_state_buffer().unwrap()[0], LWW_STATE_VERSION_2);
    }

    #[test]
    fn empty_legacy_buffer_loads_as_empty() {
        let legacy: BTreeMap<PropertyName, Option<Value>> = BTreeMap::new();
        let legacy_buffer = bincode::serialize(&legacy).unwrap();
        assert_eq!(legacy_buffer[0], 0x00);
        let restored = LWWBackend::from_state_buffer(&legacy_buffer).unwrap();
        assert!(restored.properties().is_empty());
    }

    #[test]
    fn corrupt_legacy_buffer_errors_clearly() {
        let garbage = vec![0x03, 0xde, 0xad, 0xbe, 0xef];
        let err = LWWBackend::from_state_buffer(&garbage).unwrap_err();
        assert!(err.to_string().contains("legacy LWW state buffer"), "unexpected error: {err}");
    }

    #[test]
    fn unknown_future_version_is_refused() {
        let backend = committed_backend(EventId::from_bytes([7; 32]));
        let mut buffer = backend.to_state_buffer().unwrap();
        buffer[0] = LWW_STATE_VERSION_BASE + 9;
        let err = LWWBackend::from_state_buffer(&buffer).unwrap_err();
        assert!(err.to_string().contains("unknown LWW state buffer version"), "unexpected error: {err}");
        // The supported-version text names both decodable versions.
        assert!(err.to_string().contains("0xa1") && err.to_string().contains("0xa2"), "supported list should name both: {err}");
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
            bincode::serialize(&LWWDiff { version: 3, data: bincode::serialize(&BTreeMap::<PropertyKey, Option<Value>>::new()).unwrap() })
                .unwrap();
        let err = backend.apply_operations(&[Operation { diff: bad_diff }]).unwrap_err();
        assert!(err.to_string().contains("Unknown LWW operation version"), "unexpected error: {err}");
    }

    // ---- entry(): the presence primitive the read dispatch builds on -------
    // (The Id-then-Name dispatch itself lives OUTSIDE the backend; its tests
    // are with it, `property::read_dispatch_tests`.)

    #[test]
    fn entry_distinguishes_absent_tombstone_and_value() {
        let backend = LWWBackend::new();
        assert_eq!(backend.entry(&PropertyKey::name("title")), None, "never written: absent");
        backend.set(PropertyKey::name("title"), None);
        assert_eq!(backend.entry(&PropertyKey::name("title")), Some(None), "cleared: present tombstone");
        backend.set(PropertyKey::name("title"), Some(Value::String("v".into())));
        assert_eq!(backend.entry(&PropertyKey::name("title")), Some(Some(Value::String("v".into()))));
        // Commit state does not affect presence semantics.
        let ops = backend.to_operations().unwrap().unwrap();
        backend.apply_operations_with_event(&ops, EventId::from_bytes([4; 32])).unwrap();
        assert_eq!(backend.entry(&PropertyKey::name("title")), Some(Some(Value::String("v".into()))));
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
            &PropertyKey::Id(id(0x11)),
            Arc::new(move |_| {
                fired_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        backend.set(PropertyKey::Id(id(0x11)), Some(Value::String("alpha".into())));
        let ops = backend.to_operations().unwrap().unwrap();
        backend.apply_operations_with_event(&ops, EventId::from_bytes([1; 32])).unwrap();

        assert_eq!(fired.load(Ordering::SeqCst), 1, "an id-keyed change fires the id-registered broadcast");
    }

    #[test]
    fn apply_layer_is_idempotent_for_single_writer() {
        // A committed backend re-applying its own single event is a no-op on
        // the value (sanity that PropertyKey-keyed election round-trips).
        let backend = LWWBackend::new();
        backend.set(PropertyKey::Id(id(0x11)), Some(Value::String("alpha".into())));
        let ops = backend.to_operations().unwrap().expect("pending write should yield operations");
        backend.apply_operations_with_event(&ops, EventId::from_bytes([1; 32])).unwrap();
        assert_eq!(backend.get(&PropertyKey::Id(id(0x11))), Some(Value::String("alpha".into())));
    }
}
