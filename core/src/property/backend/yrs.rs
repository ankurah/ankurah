use std::{
    any::Any,
    collections::BTreeMap,
    fmt::Debug,
    sync::{Arc, Mutex},
};

use yrs::{updates::decoder::Decode, GetString, Observable, ReadTxn, StateVector, Text, Transact};
use yrs::{Update, WriteTxn};

use crate::{
    causal_dag::forward_view::ReadySet,
    error::{MutationError, StateError},
    property::{
        backend::{Operation, PropertyBackend, PropertyTransaction},
        PropertyName, Value,
    },
};

/// Inner state for YrsBackend, wrapped in Arc for sharing with transactions
#[derive(Debug)]
struct YrsBackendInner {
    doc: yrs::Doc,
    previous_state: Mutex<StateVector>,
    field_broadcasts: Mutex<BTreeMap<PropertyName, ankurah_signals::broadcast::Broadcast>>,
}

/// Stores one or more properties of an entity (newtype over Arc<YrsBackendInner>)
#[derive(Debug, Clone)]
pub struct YrsBackend(Arc<YrsBackendInner>);

impl Default for YrsBackend {
    fn default() -> Self { Self::new() }
}

impl YrsBackend {
    pub fn new() -> Self {
        let doc = yrs::Doc::new();
        let starting_state = doc.transact().state_vector();
        Self(Arc::new(YrsBackendInner { doc, previous_state: Mutex::new(starting_state), field_broadcasts: Mutex::new(BTreeMap::new()) }))
    }

    pub fn get_string(&self, property_name: impl AsRef<str>) -> Option<String> {
        let txn = self.0.doc.transact();
        let text = txn.get_text(property_name.as_ref()); // We only have one field in the yrs doc
        text.map(|t| t.get_string(&txn))
    }

    pub fn insert(&self, property_name: impl AsRef<str>, index: u32, value: &str) -> Result<(), MutationError> {
        let text = self.0.doc.get_or_insert_text(property_name.as_ref()); // We only have one field in the yrs doc
        let mut ytx = self.0.doc.transact_mut();
        text.insert(&mut ytx, index, value);
        Ok(())
    }

    pub fn delete(&self, property_name: impl AsRef<str>, index: u32, length: u32) -> Result<(), MutationError> {
        let text = self.0.doc.get_or_insert_text(property_name.as_ref()); // We only have one field in the yrs doc
        let mut ytx = self.0.doc.transact_mut();
        text.remove_range(&mut ytx, index, length);
        Ok(())
    }

    fn apply_update(&self, update: &[u8], changed_fields: &Arc<Mutex<std::collections::HashSet<String>>>) -> Result<(), MutationError> {
        let mut txn = self.0.doc.transact_mut();

        // TODO: There's gotta be a better way to do this - but I don't see it at the time of this writing
        let _subs: Vec<yrs::Subscription> = self
            .0
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

        let update = Update::decode_v2(update).map_err(|e| StateError::SerializationError(Box::new(e)))?;
        txn.apply_update(update).map_err(|e| MutationError::UpdateFailed(Box::new(e)))?;
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
        let trx = Transact::transact(&self.0.doc);
        let root_refs = trx.root_refs();
        root_refs.map(|(name, _)| name.to_owned()).collect()
    }

    fn property_value(&self, property_name: &PropertyName) -> Option<Value> {
        let txn = Transact::transact(&self.0.doc);
        self.get_property_string(&txn, property_name)
    }

    fn property_values(&self) -> BTreeMap<PropertyName, Option<Value>> {
        let properties = self.properties();

        let mut values = BTreeMap::new();
        let trx = Transact::transact(&self.0.doc);
        for property_name in properties {
            let value = self.get_property_string(&trx, &property_name);
            values.insert(property_name, value);
        }

        values
    }

    fn property_backend_name() -> String { "yrs".to_owned() }

    fn to_state_buffer(&self) -> Result<Vec<u8>, StateError> {
        let txn = self.0.doc.transact();
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

        Ok(Self(Arc::new(YrsBackendInner {
            doc,
            previous_state: Mutex::new(starting_state),
            field_broadcasts: Mutex::new(BTreeMap::new()),
        })))
    }

    fn to_operations(&self) -> Result<Option<Vec<Operation>>, MutationError> {
        let mut previous_state = self.0.previous_state.lock().unwrap();

        let txn = self.0.doc.transact_mut();
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
        let field_broadcasts = self.0.field_broadcasts.lock().expect("field_broadcasts lock is poisoned");
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
        listener: ankurah_signals::broadcast::Listener,
    ) -> ankurah_signals::broadcast::ListenerGuard {
        // Get or create the broadcast for this field
        let mut field_broadcasts = self.0.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(field_name.clone()).or_default();

        // Subscribe to the broadcast and return the guard
        broadcast.reference().listen(listener)
    }

    fn begin(&self) -> Result<Box<dyn PropertyTransaction<ankurah_proto::Event>>, MutationError> {
        Ok(Box::new(YrsTransaction { backend: self.clone(), pending_operations: Vec::new() }))
    }
}

impl YrsBackend {
    /// Get the broadcast ID for a specific field, creating the broadcast if necessary
    pub fn field_broadcast_id(&self, field_name: &PropertyName) -> ankurah_signals::broadcast::BroadcastId {
        let mut field_broadcasts = self.0.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(field_name.clone()).or_default();
        broadcast.id()
    }
}

/// Transaction for applying concurrent updates to Yrs backend
pub struct YrsTransaction {
    /// Shared inner state (doc, field_broadcasts, etc.)
    backend: YrsBackend,
    /// Accumulated operations from concurrent events to apply on commit
    pending_operations: Vec<Vec<u8>>,
}

impl PropertyTransaction<ankurah_proto::Event> for YrsTransaction {
    fn apply_ready_set(&mut self, ready_set: &ReadySet<ankurah_proto::Event>) -> Result<(), MutationError> {
        // Accumulate operations from concurrent events
        // Primary events are already applied, so we only need Concurrency events
        for event in ready_set.concurrency_events() {
            // Only apply operations for this backend (yrs)
            if let Some(operations) = event.payload.operations.get("yrs") {
                for operation in operations {
                    self.pending_operations.push(operation.diff.clone());
                }
            }
        }
        Ok(())
    }

    fn commit(&mut self) -> Result<(), MutationError> {
        // Apply all accumulated operations using the backend's apply_operations
        // This handles change tracking, field notifications, and all yrs subscription logic
        if !self.pending_operations.is_empty() {
            let operations: Vec<Operation> =
                std::mem::take(&mut self.pending_operations).into_iter().map(|diff| Operation { diff }).collect();

            // Apply operations to the backend
            self.backend.apply_operations(&operations)?;
        }
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), MutationError> {
        // Clear pending operations
        self.pending_operations.clear();
        Ok(())
    }
}
