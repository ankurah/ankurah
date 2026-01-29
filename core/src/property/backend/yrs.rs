use std::{
    any::Any,
    collections::BTreeMap,
    fmt::Debug,
    sync::{Arc, Mutex},
};

use yrs::{updates::decoder::Decode, GetString, Observable, ReadTxn, StateVector, Text, Transact};
use yrs::{Update, WriteTxn};

use crate::{
    error::{AnyhowWrapper, InternalError, MutationError, RetrievalError, StateError},
    property::{
        backend::{Operation, PropertyBackend},
        PropertyName, Value,
    },
};
use error_stack::Report;

/// Stores one or more properties of an entity
#[derive(Debug)]
pub struct YrsBackend {
    pub(crate) doc: yrs::Doc,
    previous_state: Mutex<StateVector>,
    field_broadcasts: Mutex<BTreeMap<PropertyName, ankurah_signals::broadcast::Broadcast>>,
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

    pub fn get_string(&self, property_name: impl AsRef<str>) -> Option<String> {
        let txn = self.doc.transact();
        let text = txn.get_text(property_name.as_ref()); // We only have one field in the yrs doc
        text.map(|t| t.get_string(&txn))
    }

    pub fn insert(&self, property_name: impl AsRef<str>, index: u32, value: &str) -> Result<(), MutationError> {
        let text = self.doc.get_or_insert_text(property_name.as_ref()); // We only have one field in the yrs doc
        let mut ytx = self.doc.transact_mut();
        text.insert(&mut ytx, index, value);
        Ok(())
    }

    pub fn delete(&self, property_name: impl AsRef<str>, index: u32, length: u32) -> Result<(), MutationError> {
        let text = self.doc.get_or_insert_text(property_name.as_ref()); // We only have one field in the yrs doc
        let mut ytx = self.doc.transact_mut();
        text.remove_range(&mut ytx, index, length);
        Ok(())
    }

    fn apply_update(&self, update: &[u8], changed_fields: &Arc<Mutex<std::collections::HashSet<String>>>) -> Result<(), MutationError> {
        let mut txn = self.doc.transact_mut();

        // TODO: There's gotta be a better way to do this - but I don't see it at the time of this writing
        let _subs: Vec<yrs::Subscription> = self
            .field_broadcasts
            .lock()
            .unwrap()
            .keys()
            .map(|b| {
                let changed_fields = changed_fields.clone();
                let b = b.to_string();
                txn.get_or_insert_text(b.as_str()).observe(move |_, _| {
                    let mut changed_fields = changed_fields.lock().unwrap();
                    changed_fields.insert(b.clone());
                })
            })
            .collect();

        let update = Update::decode_v2(update).map_err(|e| MutationError::InvalidUpdate(format!("yrs decode failed: {}", e)))?;
        txn.apply_update(update).map_err(|e| MutationError::InvalidUpdate(format!("yrs apply update failed: {}", e)))?;
        txn.commit();

        Ok(())
    }

    fn get_property_string(&self, trx: &yrs::Transaction, property_name: &PropertyName) -> Option<Value> {
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

    fn properties(&self) -> Vec<String> {
        let trx = Transact::transact(&self.doc);
        let root_refs = trx.root_refs();
        root_refs.map(|(name, _)| name.to_owned()).collect()
    }

    fn property_value(&self, property_name: &PropertyName) -> Option<Value> {
        let trx = Transact::transact(&self.doc);
        self.get_property_string(&trx, property_name)
    }

    fn property_values(&self) -> BTreeMap<PropertyName, Option<Value>> {
        let properties = self.properties();

        let mut values = BTreeMap::new();
        let trx = Transact::transact(&self.doc);
        for property_name in properties {
            let value = self.get_property_string(&trx, &property_name);
            values.insert(property_name, value);
        }

        values
    }

    fn property_backend_name() -> String { "yrs".to_owned() }

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
        let update = yrs::Update::decode_v2(state_buffer).map_err(|e| RetrievalError::Failure(Report::new(AnyhowWrapper::from(format!("yrs decode failed: {}", e))).change_context(InternalError)))?;
        txn.apply_update(update).map_err(|e| RetrievalError::Failure(Report::new(AnyhowWrapper::from(format!("yrs apply update failed: {}", e))).change_context(InternalError)))?;
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

    fn apply_operations(&self, operations: &Vec<Operation>) -> Result<(), MutationError> {
        let changed_fields = Arc::new(Mutex::new(std::collections::HashSet::new()));
        for operation in operations {
            self.apply_update(&operation.diff, &changed_fields)?;
        }
        //Only notify field subscribers for fields that actually changed
        let field_broadcasts = self.field_broadcasts.lock().expect("field_broadcasts lock is poisoned");
        for field_name in changed_fields.lock().unwrap().iter() {
            if let Some(broadcast) = field_broadcasts.get(field_name) {
                broadcast.send(());
            }
        }

        Ok(())
    }

    fn listen_field(
        &self,
        field_name: &PropertyName,
        listener: ankurah_signals::signal::Listener,
    ) -> ankurah_signals::signal::ListenerGuard {
        // Get or create the broadcast for this field
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(field_name.clone()).or_default();

        // Subscribe to the broadcast and return the guard
        broadcast.reference().listen(listener).into()
    }
}

impl YrsBackend {
    /// Get the broadcast ID for a specific field, creating the broadcast if necessary
    pub fn field_broadcast_id(&self, field_name: &PropertyName) -> ankurah_signals::broadcast::BroadcastId {
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(field_name.clone()).or_default();
        broadcast.id()
    }
}
