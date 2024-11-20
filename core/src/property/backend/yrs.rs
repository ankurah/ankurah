use std::sync::{Arc, Mutex};

use yrs::Update;
use yrs::{updates::decoder::Decode, GetString, ReadTxn, StateVector, Text, Transact};

use crate::{
    property::backend::{Operation, PropertyBackend},
    ID,
};

/// Stores one or more properties of a record
#[derive(Debug)]
pub struct YrsBackend {
    pub(crate) doc: yrs::Doc,
    previous_state: Arc<Mutex<StateVector>>,
}

impl YrsBackend {
    pub fn new() -> Self {
        let doc = yrs::Doc::new();
        let starting_state = doc.transact().state_vector();
        Self {
            doc: doc,
            previous_state: Arc::new(Mutex::new(starting_state)),
        }
    }

    pub fn get_string(&self, property_name: &'static str) -> String {
        let text = self.doc.get_or_insert_text(property_name); // We only have one field in the yrs doc
        text.get_string(&self.doc.transact())
    }

    pub fn insert(&self, property_name: &'static str, index: u32, value: &str) {
        let text = self.doc.get_or_insert_text(property_name); // We only have one field in the yrs doc
        let mut ytx = self.doc.transact_mut();
        text.insert(&mut ytx, index, value);
    }

    pub fn delete(&self, property_name: &'static str, index: u32, length: u32) {
        let text = self.doc.get_or_insert_text(property_name); // We only have one field in the yrs doc
        let mut ytx = self.doc.transact_mut();
        text.remove_range(&mut ytx, index, length);
    }

    fn apply_update(&self, update: &[u8]) -> anyhow::Result<()> {
        let mut txn = self.doc.transact_mut();
        let update = Update::decode_v2(update)?;
        txn.apply_update(update)?;
        Ok(())
    }
}

impl PropertyBackend for YrsBackend {
    fn property_backend_name() -> &'static str {
        "yrs"
    }

    fn to_state_buffer(&self) -> Vec<u8> {
        let txn = self.doc.transact();
        // The yrs docs aren't great about how to encode all state as an update.
        // the state vector is just a clock reading. It doesn't contain all updates
        let state_buffer = txn.encode_state_as_update_v2(&yrs::StateVector::default());
        println!("state_buffer: {:?}", state_buffer);
        state_buffer
    }

    fn from_state_buffer(
        state_buffer: &Vec<u8>,
    ) -> std::result::Result<Self, crate::error::RetrievalError> {
        let doc = yrs::Doc::new();
        let mut txn = doc.transact_mut();
        let update = yrs::Update::decode_v2(&state_buffer)
            .map_err(|e| crate::error::RetrievalError::FailedUpdate(Box::new(e)))?;
        txn.apply_update(update)
            .map_err(|e| crate::error::RetrievalError::FailedUpdate(Box::new(e)))?;
        txn.commit(); // I just don't trust `Drop` too much
        drop(txn);
        let starting_state = doc.transact().state_vector();

        Ok(Self {
            doc: doc,
            previous_state: Arc::new(Mutex::new(starting_state)),
        })
    }

    fn to_operations(&self /*precursor: ULID*/) -> Vec<Operation> {
        let mut operations = Vec::new();

        let mut previous_state = self.previous_state.lock().unwrap();

        let txn = self.doc.transact_mut();
        let diff = txn.encode_diff_v2(&previous_state);
        *previous_state = txn.state_vector();

        if !diff.is_empty() {
            operations.push(Operation { diff: diff })
        }

        operations
    }

    fn apply_operations(&self, operations: Vec<Operation>) -> anyhow::Result<()> {
        for operation in operations {
            self.apply_update(&operation.diff)?;
        }

        Ok(())
    }
}
