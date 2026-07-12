use std::{
    any::Any,
    collections::BTreeMap,
    fmt::Debug,
    sync::{Arc, Mutex},
};

use ankurah_proto::EntityId;
use yrs::{updates::decoder::Decode, GetString, Observable, ReadTxn, StateVector, Text, Transact};
use yrs::{Update, WriteTxn};

use crate::{
    error::{MutationError, StateError},
    property::{
        backend::{Operation, PropertyBackend},
        PropertyKey, Value,
    },
};

/// The Yjs Doc root name a [`PropertyKey`] stores under. An `Id` key uses a
/// `#`-prefixed base64 id (a leading `#` is illegal in a Rust field
/// identifier and in a system-table property name, so it can never collide
/// with a `Name` root); a `Name` key uses the bare name. yrs stores id-keyed
/// like every other backend (the PropertyKey amendment on #289 superseded the
/// earlier name-keyed-root design).
fn root_name(key: &PropertyKey) -> String {
    match key {
        PropertyKey::Id(id) => format!("#{}", id.to_base64()),
        PropertyKey::Name(name) => name.clone(),
    }
}

/// Classify a Yjs Doc root name back to its [`PropertyKey`]: a `#`-prefixed
/// root whose remainder decodes as an id is an `Id` key, everything else is a
/// `Name` key.
fn key_from_root(root: &str) -> PropertyKey {
    if let Some(encoded) = root.strip_prefix('#') {
        if let Ok(id) = EntityId::from_base64(encoded) {
            return PropertyKey::Id(id);
        }
    }
    PropertyKey::Name(root.to_string())
}

/// Stores one or more properties of an entity
#[derive(Debug)]
pub struct YrsBackend {
    pub(crate) doc: yrs::Doc,
    previous_state: Mutex<StateVector>,
    field_broadcasts: Mutex<BTreeMap<PropertyKey, ankurah_signals::broadcast::Broadcast>>,
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

    pub fn get_string(&self, key: &PropertyKey) -> Option<String> {
        let txn = self.doc.transact();
        let text = txn.get_text(root_name(key).as_str());
        text.map(|t| t.get_string(&txn))
    }

    pub fn insert(&self, key: &PropertyKey, index: u32, value: &str) -> Result<(), MutationError> {
        let text = self.doc.get_or_insert_text(root_name(key).as_str());
        let mut ytx = self.doc.transact_mut();
        text.insert(&mut ytx, index, value);
        Ok(())
    }

    pub fn delete(&self, key: &PropertyKey, index: u32, length: u32) -> Result<(), MutationError> {
        let text = self.doc.get_or_insert_text(root_name(key).as_str());
        let mut ytx = self.doc.transact_mut();
        text.remove_range(&mut ytx, index, length);
        Ok(())
    }

    fn apply_update(
        &self,
        update: &[u8],
        changed_fields: &Arc<Mutex<std::collections::HashSet<PropertyKey>>>,
    ) -> Result<(), MutationError> {
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

        Ok(())
    }

    fn get_property_string(&self, trx: &yrs::Transaction, key: &PropertyKey) -> Option<Value> {
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

    fn properties(&self) -> Vec<PropertyKey> {
        let trx = Transact::transact(&self.doc);
        let root_refs = trx.root_refs();
        root_refs.map(|(name, _)| key_from_root(name)).collect()
    }

    fn property_value(&self, key: &PropertyKey) -> Option<Value> {
        let trx = Transact::transact(&self.doc);
        self.get_property_string(&trx, key)
    }

    fn property_values(&self) -> BTreeMap<PropertyKey, Option<Value>> {
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

    fn listen_field(&self, key: &PropertyKey, listener: ankurah_signals::signal::Listener) -> ankurah_signals::signal::ListenerGuard {
        // Get or create the broadcast for this key
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(key.clone()).or_default();

        // Subscribe to the broadcast and return the guard
        broadcast.reference().listen(listener).into()
    }
}

impl YrsBackend {
    /// Get the broadcast ID for a specific key, creating the broadcast if necessary
    pub fn field_broadcast_id(&self, key: &PropertyKey) -> ankurah_signals::broadcast::BroadcastId {
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(key.clone()).or_default();
        broadcast.id()
    }
}
