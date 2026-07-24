use std::{
    any::Any,
    collections::BTreeMap,
    fmt::Debug,
    sync::{Arc, Mutex},
};

use yrs::{
    updates::{decoder::Decode, encoder::Encode},
    GetString, Observable, ReadTxn, StateVector, Text, Transact,
};
use yrs::{Update, WriteTxn};

use crate::{
    error::{MutationError, StateError},
    property::{
        backend::{Operation, PropertyBackend},
        Value,
    },
};
use ankql::ast::PropertyId;

/// The Yjs Doc root name a [`PropertyId`] stores under: a stable JSON
/// encoding of the id itself. Deterministic and losslessly invertible (see
/// [`key_from_root`]), so [`YrsBackend::properties`] recovers the original
/// `PropertyId` straight from the Doc's root names, with no side metadata and
/// no name-based fallback.
fn root_name(key: &PropertyId) -> String { serde_json::to_string(key).expect("PropertyId serialization is infallible") }

/// Recover a [`PropertyId`] from a Yjs Doc root name written by
/// [`root_name`]. Every root in a healthy Doc was created by that function,
/// so a decode failure means the buffer or update holds a root this backend
/// never wrote -- corruption or a foreign encoding, not a legacy format to
/// fall back on. Ingest ([`YrsBackend::from_state_buffer`] and
/// [`YrsBackend::apply_update`]) refuses such roots fallibly, so a live Doc
/// never carries one.
fn key_from_root(root: &str) -> Result<PropertyId, serde_json::Error> { serde_json::from_str(root) }

/// The first Doc root that does not decode as a [`PropertyId`], if any.
/// Ingest calls this after applying a buffer or update so a malformed root is
/// refused at the boundary instead of surfacing later in `properties()` and
/// friends.
fn first_undecodable_root<T: ReadTxn>(txn: &T) -> Option<String> {
    txn.root_refs().map(|(name, _)| name).find(|name| key_from_root(name).is_err()).map(str::to_string)
}

/// Stores one or more properties of an entity
#[derive(Debug)]
pub struct YrsBackend {
    pub(crate) doc: yrs::Doc,
    previous_state: Mutex<StateVector>,
    field_broadcasts: Mutex<BTreeMap<PropertyId, ankurah_signals::broadcast::Broadcast>>,
}

impl Default for YrsBackend {
    fn default() -> Self { Self::new() }
}

impl YrsBackend {
    pub fn new() -> Self {
        let doc = yrs::Doc::new();
        let starting_state = doc.transact().state_vector();
        Self { doc, previous_state: Mutex::new(starting_state), field_broadcasts: Mutex::new(BTreeMap::new()) }
    }

    pub fn get_string(&self, key: &PropertyId) -> Option<String> {
        let txn = self.doc.transact();
        let text = txn.get_text(root_name(key).as_str());
        text.map(|t| t.get_string(&txn))
    }

    pub fn insert(&self, key: &PropertyId, index: u32, value: &str) -> Result<(), MutationError> {
        let text = self.doc.get_or_insert_text(root_name(key).as_str());
        let mut ytx = self.doc.transact_mut();
        text.insert(&mut ytx, index, value);
        Ok(())
    }

    pub fn delete(&self, key: &PropertyId, index: u32, length: u32) -> Result<(), MutationError> {
        let text = self.doc.get_or_insert_text(root_name(key).as_str());
        let mut ytx = self.doc.transact_mut();
        text.remove_range(&mut ytx, index, length);
        Ok(())
    }

    /// Pre-flight an incoming update on a scratch Doc before the live Doc
    /// sees it. Refused ingest must leave the live state unchanged (the LWW
    /// backend's decode failure merges nothing), but yrs offers no public way
    /// to inspect the roots an un-integrated `Update` would create, and a
    /// `TransactionMut` cannot roll back. So the update is integrated into a
    /// scratch Doc seeded with the live Doc's full state -- the integrated
    /// blocks AND any pending ones, so an update that completes previously
    /// missing dependencies materializes the same roots the live Doc would --
    /// and the root check runs there. An update that fails to decode,
    /// integrate, or keep every root PropertyId-keyed is refused with the
    /// live Doc untouched.
    fn check_update(&self, update: &[u8]) -> Result<(), MutationError> {
        let scratch = yrs::Doc::new();
        let mut scratch_txn = scratch.transact_mut();
        {
            let live = self.doc.transact();
            let state = live.encode_state_as_update_v2(&StateVector::default());
            let state = Update::decode_v2(&state).map_err(|e| MutationError::UpdateFailed(Box::new(e)))?;
            scratch_txn.apply_update(state).map_err(|e| MutationError::UpdateFailed(Box::new(e)))?;
            if let Some(pending) = live.store().pending_update() {
                let pending = Update::decode_v2(&pending.update.encode_v2()).map_err(|e| MutationError::UpdateFailed(Box::new(e)))?;
                scratch_txn.apply_update(pending).map_err(|e| MutationError::UpdateFailed(Box::new(e)))?;
            }
        }
        let incoming = Update::decode_v2(update).map_err(|e| StateError::SerializationError(Box::new(e)))?;
        scratch_txn.apply_update(incoming).map_err(|e| MutationError::UpdateFailed(Box::new(e)))?;
        scratch_txn.commit();
        if let Some(root) = first_undecodable_root(&scratch_txn) {
            return Err(MutationError::UpdateFailed(
                format!("yrs update would create root {root:?}, which is not an encoded PropertyId").into(),
            ));
        }
        Ok(())
    }

    fn apply_update(&self, update: &[u8], changed_fields: &Arc<Mutex<std::collections::HashSet<PropertyId>>>) -> Result<(), MutationError> {
        // Refuse a malformed update BEFORE the live doc integrates anything.
        self.check_update(update)?;

        let mut txn = self.doc.transact_mut();

        // TODO: There's gotta be a better way to do this - but I don't see it at the time of this writing
        let _subs: Vec<yrs::Subscription> = self
            .field_broadcasts
            .lock()
            .unwrap()
            .keys()
            .map(|key| {
                let changed_fields = changed_fields.clone();
                let key = key.clone();
                let root = root_name(&key);
                txn.get_or_insert_text(root.as_str()).observe(move |_, _| {
                    let mut changed_fields = changed_fields.lock().unwrap();
                    changed_fields.insert(key.clone());
                })
            })
            .collect();

        let update = Update::decode_v2(update).map_err(|e| StateError::SerializationError(Box::new(e)))?;
        txn.apply_update(update).map_err(|e| MutationError::UpdateFailed(Box::new(e)))?;
        txn.commit();

        // Second line behind `check_update`'s scratch pre-flight: the
        // scratch mirrors the live doc exactly, so this can only fire if
        // that mirroring assumption is broken. A loud refusal here still
        // beats a foreign root riding silently into future state encodes.
        if let Some(root) = first_undecodable_root(&txn) {
            return Err(MutationError::UpdateFailed(
                format!("yrs update created root {root:?}, which is not an encoded PropertyId").into(),
            ));
        }

        Ok(())
    }

    fn get_property_string(&self, trx: &yrs::Transaction, key: &PropertyId) -> Option<Value> {
        let value = match trx.get_text(root_name(key).as_str()) {
            Some(text_ref) => {
                let text = text_ref.get_string(trx);
                Some(text)
            }
            None => None,
        };

        value.map(Value::String)
    }
}

impl PropertyBackend for YrsBackend {
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> { self as Arc<dyn Any + Send + Sync + 'static> }

    fn as_debug(&self) -> &dyn Debug { self as &dyn Debug }

    fn fork(&self) -> Arc<dyn PropertyBackend> {
        // TODO: Don't do all this just to sever the internal Yrs Arcs
        let state_buffer = self.to_state_buffer().unwrap();
        let backend = Self::from_state_buffer(&state_buffer).unwrap();
        Arc::new(backend)
    }

    fn properties(&self) -> Vec<PropertyId> {
        let trx = Transact::transact(&self.doc);
        let root_refs = trx.root_refs();
        // Ingest validation keeps undecodable roots out of a live Doc, so
        // this filter never drops anything in practice; if one appears
        // anyway (corruption), surface it loudly without panicking the
        // read path -- this signature has no error channel.
        root_refs
            .filter_map(|(name, _)| match key_from_root(name) {
                Ok(key) => Some(key),
                Err(e) => {
                    tracing::error!("yrs doc holds root {name:?} which is not an encoded PropertyId ({e}); ignoring it");
                    None
                }
            })
            .collect()
    }

    fn property_value(&self, key: &PropertyId) -> Option<Value> {
        let trx = Transact::transact(&self.doc);
        self.get_property_string(&trx, key)
    }

    fn property_values(&self) -> BTreeMap<PropertyId, Option<Value>> {
        let properties = self.properties();

        let mut values = BTreeMap::new();
        let trx = Transact::transact(&self.doc);
        for key in properties {
            let value = self.get_property_string(&trx, &key);
            values.insert(key, value);
        }

        values
    }

    fn property_backend_name() -> &'static str { "yrs" }

    fn to_state_buffer(&self) -> Result<Vec<u8>, StateError> {
        let txn = self.doc.transact();
        // The yrs docs aren't great about how to encode all state as an update.
        // the state vector is just a clock reading. It doesn't contain all updates
        let state_buffer = txn.encode_state_as_update_v2(&yrs::StateVector::default());
        Ok(state_buffer)
    }

    fn from_state_buffer(state_buffer: &Vec<u8>) -> std::result::Result<Self, crate::error::RetrievalError> {
        let doc = yrs::Doc::new();
        let mut txn = doc.transact_mut();
        let update = yrs::Update::decode_v2(state_buffer).map_err(|e| crate::error::RetrievalError::FailedUpdate(Box::new(e)))?;
        txn.apply_update(update).map_err(|e| crate::error::RetrievalError::FailedUpdate(Box::new(e)))?;
        txn.commit(); // I just don't trust `Drop` too much
        drop(txn);

        // Same refusal class the LWW backend uses for a malformed state
        // buffer: a root that does not decode as a PropertyId means the
        // buffer was never written by this backend, so the whole buffer is
        // refused (the wire/on-disk format is deliberately broken across
        // this refactor; a dev database is reset, not migrated).
        if let Some(root) = first_undecodable_root(&doc.transact()) {
            return Err(crate::error::RetrievalError::Other(format!(
                "malformed yrs state buffer: root {root:?} is not an encoded PropertyId"
            )));
        }
        let starting_state = doc.transact().state_vector();

        Ok(Self { doc, previous_state: Mutex::new(starting_state), field_broadcasts: Mutex::new(BTreeMap::new()) })
    }

    fn to_operations(&self) -> Result<Option<Vec<Operation>>, MutationError> {
        let mut previous_state = self.previous_state.lock().unwrap();

        let txn = self.doc.transact_mut();
        let diff = txn.encode_diff_v2(&previous_state);
        *previous_state = txn.state_vector();

        // Check if this is actually an empty update by comparing to the known empty pattern
        if diff == Update::EMPTY_V2 {
            Ok(None)
        } else {
            Ok(Some(vec![Operation { diff }]))
        }
    }

    fn apply_operations(&self, operations: &[Operation]) -> Result<(), MutationError> {
        let changed_fields = Arc::new(Mutex::new(std::collections::HashSet::new()));
        for operation in operations {
            self.apply_update(&operation.diff, &changed_fields)?;
        }
        //Only notify field subscribers for fields that actually changed
        super::notify_changed_fields(&self.field_broadcasts, changed_fields.lock().unwrap().iter());

        Ok(())
    }

    fn apply_layer(&self, layer: &crate::event_dag::EventLayer) -> Result<(), MutationError> {
        // Order within layer doesn't matter for CRDTs - they're commutative.
        // Just apply all operations from to_apply events.
        let changed_fields = Arc::new(Mutex::new(std::collections::HashSet::new()));

        for event in &layer.to_apply {
            // Extract Yrs operations from this event
            if let Some(operations) = event.operations.get(&Self::property_backend_name().to_string()) {
                for operation in operations {
                    self.apply_update(&operation.diff, &changed_fields)?;
                }
            }
        }

        // Notify field subscribers for fields that actually changed
        super::notify_changed_fields(&self.field_broadcasts, changed_fields.lock().unwrap().iter());

        Ok(())
    }

    fn listen_field(&self, key: &PropertyId, listener: ankurah_signals::signal::Listener) -> ankurah_signals::signal::ListenerGuard {
        // Get or create the broadcast for this key
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(key.clone()).or_default();

        // Subscribe to the broadcast and return the guard
        broadcast.reference().listen(listener).into()
    }
}

impl YrsBackend {
    /// Get the broadcast ID for a specific key, creating the broadcast if necessary
    pub fn field_broadcast_id(&self, key: &PropertyId) -> ankurah_signals::broadcast::BroadcastId {
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(key.clone()).or_default();
        broadcast.id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::property::backend::PropertyBackend;
    use ankql::ast::{PropertyId, SystemProperty};
    use ankurah_proto::EntityId;

    fn eid(b: u8) -> PropertyId {
        let mut x = [0u8; 16];
        x[0] = b;
        PropertyId::EntityId(EntityId::from_bytes(x))
    }

    /// Yrs Doc roots are named by the serialized PropertyId and recovered
    /// losslessly, for both the registered (EntityId) and system (System{name})
    /// arms, across a native serialize -> deserialize round-trip.
    #[test]
    fn root_keys_roundtrip_by_property_id() {
        let backend = YrsBackend::new();
        let reg = eid(0x11);
        let sys = PropertyId::System(SystemProperty::Name);
        backend.insert(&reg, 0, "hello").unwrap();
        backend.insert(&sys, 0, "world").unwrap();
        assert_eq!(backend.get_string(&reg), Some("hello".to_string()));
        assert_eq!(backend.get_string(&sys), Some("world".to_string()));

        let mut props = backend.properties();
        props.sort();
        let mut want = vec![reg.clone(), sys.clone()];
        want.sort();
        assert_eq!(props, want, "properties() must recover both PropertyIds from the doc roots");

        let buf = backend.to_state_buffer().unwrap();
        let restored = YrsBackend::from_state_buffer(&buf).unwrap();
        assert_eq!(restored.get_string(&reg), Some("hello".to_string()));
        assert_eq!(restored.get_string(&sys), Some("world".to_string()));
    }

    /// A Yrs update can name any root it likes -- an untrusted peer's update
    /// is applied without the local accessors that encode roots as
    /// PropertyIds -- so ingest must refuse a root that does not decode as
    /// one, cleanly, on both ingest paths. Before this refusal existed,
    /// `key_from_root` panicked in `property_values()`, which every storage
    /// engine calls while persisting state.
    #[test]
    fn undecodable_root_is_refused_at_ingest() {
        // Craft an update whose root is a plain display name, not an
        // encoded PropertyId (the shape any independent yrs writer would
        // produce).
        let doc = yrs::Doc::new();
        let text = doc.get_or_insert_text("title");
        {
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 0, "hello");
        }
        let update = doc.transact().encode_state_as_update_v2(&yrs::StateVector::default());

        // State-buffer ingest: same refusal class as a malformed LWW buffer.
        let err = YrsBackend::from_state_buffer(&update).unwrap_err();
        assert!(err.to_string().contains("not an encoded PropertyId"), "unexpected error: {err}");
        assert!(err.to_string().contains("title"), "the offending root should be named: {err}");

        // Operation ingest: the same update arriving as an event operation is
        // refused fallibly, and the readers stay panic-free afterwards.
        let backend = YrsBackend::new();
        backend.insert(&eid(0x22), 0, "kept").unwrap();
        let err = backend.apply_operations(&[Operation { diff: update }]).unwrap_err();
        assert!(err.to_string().contains("not an encoded PropertyId"), "unexpected error: {err}");
        let values = backend.property_values(); // must not panic
        assert!(!values.keys().any(|k| matches!(k, PropertyId::System(_))));

        // Refused ingest leaves the live doc unchanged: its full encoded
        // state still ingests cleanly (from_state_buffer refuses foreign
        // roots, so success proves absence) and the prior write survives.
        let state = backend.to_state_buffer().unwrap();
        let restored = YrsBackend::from_state_buffer(&state).expect("a refused update must not leak into the encoded state");
        assert_eq!(restored.get_string(&eid(0x22)), Some("kept".to_string()));
    }

    /// A root can also materialize when a LATER update completes earlier
    /// missing dependencies: the first update parks as pending, and the
    /// completing update is the one whose integration surfaces the foreign
    /// root. The scratch-doc pre-flight seeds the live doc's pending blocks
    /// too, so whichever apply surfaces the root is refused before the live
    /// doc mutates -- and no state encode ever carries the foreign root.
    #[test]
    fn foreign_root_via_pending_completion_never_escapes() {
        // A remote writer produces two causally chained updates: u1 touches
        // a legitimately keyed root, u2 (depending on u1's clocks) creates
        // the foreign root "title".
        let remote = yrs::Doc::new();
        let good = remote.get_or_insert_text(root_name(&eid(0x33)).as_str());
        {
            let mut txn = remote.transact_mut();
            good.insert(&mut txn, 0, "ok");
        }
        let u1 = remote.transact().encode_state_as_update_v2(&yrs::StateVector::default());
        let sv1 = remote.transact().state_vector();
        let foreign = remote.get_or_insert_text("title");
        {
            let mut txn = remote.transact_mut();
            foreign.insert(&mut txn, 0, "no");
        }
        let u2 = remote.transact().encode_state_as_update_v2(&sv1);

        // Deliver out of order: u2 first (parks on u1's missing clocks),
        // then u1 (whose integration completes u2).
        let backend = YrsBackend::new();
        let r2 = backend.apply_operations(&[Operation { diff: u2 }]);
        let r1 = backend.apply_operations(&[Operation { diff: u1 }]);
        assert!(r1.is_err() || r2.is_err(), "the foreign root must be refused at some ingest point (r1: {r1:?}, r2: {r2:?})");

        // Whatever was admitted still round-trips cleanly and carries no
        // foreign root (from_state_buffer refuses them, so success proves
        // absence).
        let state = backend.to_state_buffer().unwrap();
        YrsBackend::from_state_buffer(&state).expect("refused ingest must leave an encodable, ingestible state");
        let values = backend.property_values(); // and reads stay panic-free
        assert!(!values.keys().any(|k| matches!(k, PropertyId::System(_))));
    }
}
