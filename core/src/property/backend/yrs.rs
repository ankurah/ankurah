use std::{
    any::Any,
    collections::BTreeMap,
    fmt::Debug,
    sync::{Arc, Mutex},
};

use yrs::{updates::decoder::Decode, GetString, Observable, ReadTxn, StateVector, Text, Transact};
use yrs::{Update, WriteTxn};

use crate::{
    error::{MutationError, StateError},
    property::{backend::PropertyBackend, Value},
};
use ankurah_core_types::PropertyId;

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

    pub fn get_string(&self, property_name: &PropertyId) -> Option<String> {
        let txn = self.doc.transact();
        let text = txn.get_text(*property_name);
        text.map(|t| t.get_string(&txn))
    }

    pub fn insert(&self, property_name: &PropertyId, index: u32, value: &str) -> Result<(), MutationError> {
        let text = self.doc.get_or_insert_text(*property_name);
        let mut ytx = self.doc.transact_mut();
        text.insert(&mut ytx, index, value);
        Ok(())
    }

    pub fn delete(&self, property_name: &PropertyId, index: u32, length: u32) -> Result<(), MutationError> {
        let text = self.doc.get_or_insert_text(*property_name);
        let mut ytx = self.doc.transact_mut();
        text.remove_range(&mut ytx, index, length);
        Ok(())
    }

    fn apply_update(&self, update: &[u8], changed_fields: &Arc<Mutex<std::collections::HashSet<PropertyId>>>) -> Result<(), MutationError> {
        let mut txn = self.doc.transact_mut();

        // TODO: There's gotta be a better way to do this - but I don't see it at the time of this writing
        let _subs: Vec<yrs::Subscription> = self
            .field_broadcasts
            .lock()
            .unwrap()
            .keys()
            .map(|b| {
                let changed_fields = changed_fields.clone();
                let b = *b;
                txn.get_or_insert_text(b).observe(move |_, _| {
                    let mut changed_fields = changed_fields.lock().unwrap();
                    changed_fields.insert(b.clone());
                })
            })
            .collect();

        let update = Update::decode_v2(update).map_err(|e| StateError::SerializationError(Box::new(e)))?;
        txn.apply_update(update).map_err(|e| MutationError::UpdateFailed(Box::new(e)))?;
        txn.commit();

        Ok(())
    }

    fn get_property_string(&self, trx: &yrs::Transaction, property_name: &PropertyId) -> Option<Value> {
        let value = match trx.get_text(property_name.clone()) {
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
        trx.root_refs().filter_map(|(name, _)| name.parse().ok()).collect()
    }

    fn property_value(&self, property_name: &PropertyId) -> Option<Value> {
        let trx = Transact::transact(&self.doc);
        self.get_property_string(&trx, property_name)
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
        let starting_state = doc.transact().state_vector();

        Ok(Self { doc, previous_state: Mutex::new(starting_state), field_broadcasts: Mutex::new(BTreeMap::new()) })
    }

    fn to_operations(&self) -> Result<Option<Vec<ankurah_proto::BackendOperation>>, MutationError> {
        let mut previous_state = self.previous_state.lock().unwrap();

        let txn = self.doc.transact_mut();
        let diff = txn.encode_diff_v2(&previous_state);
        *previous_state = txn.state_vector();

        // Check if this is actually an empty update by comparing to the known empty pattern
        if diff == Update::EMPTY_V2 {
            Ok(None)
        } else {
            Ok(Some(vec![ankurah_proto::BackendOperation { diff }]))
        }
    }

    fn apply_operations(&self, operations: &[ankurah_proto::BackendOperation]) -> Result<(), MutationError> {
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
            for operation in event.operations.backend_operations(Self::property_backend_name()) {
                self.apply_update(&operation.diff, &changed_fields)?;
            }
        }

        // Notify field subscribers for fields that actually changed
        super::notify_changed_fields(&self.field_broadcasts, changed_fields.lock().unwrap().iter());

        Ok(())
    }

    fn listen_field(&self, field_name: &PropertyId, listener: ankurah_signals::signal::Listener) -> ankurah_signals::signal::ListenerGuard {
        // Get or create the broadcast for this field
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(*field_name).or_default();

        // Subscribe to the broadcast and return the guard
        broadcast.reference().listen(listener).into()
    }
}

impl YrsBackend {
    /// Get the broadcast ID for a specific key, creating the broadcast if necessary
    pub fn field_broadcast_id(&self, field_name: &PropertyId) -> ankurah_signals::broadcast::BroadcastId {
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(*field_name).or_default();
        broadcast.id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_core_types::{EntityId, SystemProperty};

    fn eid(b: u8) -> PropertyId {
        let mut x = [0u8; 16];
        x[0] = b;
        PropertyId::EntityId(EntityId::from_bytes(x))
    }

    /// Roots are named by PropertyId and recovered losslessly, for both the
    /// registered and system arms, across a state-buffer round trip.
    #[test]
    fn root_keys_roundtrip_by_property_id() {
        let backend = YrsBackend::new();
        let reg = eid(0x11);
        let sys = PropertyId::System(SystemProperty::Name);
        backend.insert(&reg, 0, "hello").unwrap();
        backend.insert(&sys, 0, "world").unwrap();

        let mut props = backend.properties();
        props.sort();
        let mut want = vec![reg, sys];
        want.sort();
        assert_eq!(props, want);

        let restored = YrsBackend::from_state_buffer(&backend.to_state_buffer().unwrap()).unwrap();
        assert_eq!(restored.get_string(&reg), Some("hello".to_string()));
        assert_eq!(restored.get_string(&sys), Some("world".to_string()));
    }

    /// A root that is not a PropertyId is dropped, and does not condemn the buffer.
    #[test]
    fn unparseable_root_is_dropped() {
        let doc = yrs::Doc::new();
        let title = doc.get_or_insert_text("title");
        let mut txn = doc.transact_mut();
        title.insert(&mut txn, 0, "nope");
        drop(txn);
        let buffer = doc.transact().encode_state_as_update_v2(&yrs::StateVector::default());

        let backend = YrsBackend::from_state_buffer(&buffer).unwrap();
        assert!(backend.properties().is_empty());
    }
}
