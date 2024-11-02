use std::sync::Arc;

use yrs::{updates::decoder::Decode, GetString, Text, Transact};
use anyhow::*;

use crate::model::RecordInner;

/// Stores one or more properties of a record
#[derive(Debug)]
pub struct Yrs {
    // TODO consolidate
    pub(crate) doc: yrs::Doc,
    record_inner: Arc<RecordInner>,
}

impl Yrs {
    pub fn new(record_inner: Arc<RecordInner>) -> Self {
        Self {
            doc: yrs::Doc::new(),
            record_inner,
        }
    }

    pub fn from_backend_state_buffer(record_inner: Arc<RecordInner>, state_buffer: Vec<u8>) -> Result<Self> {
        let doc = yrs::Doc::new();
        let mut txn = doc.transact_mut();
        let update = yrs::Update::decode_v2(&state_buffer)?;
        txn.apply_update(update)?;
        //let current_state = txn.state_vector();
        txn.commit(); // I just don't trust `Drop` too much
        drop(txn);
        Ok(Self {
            doc: doc,
            record_inner,
        })
    }

    pub fn get_string(&self, property_name: &'static str) -> String {
        let text = self.doc.get_or_insert_text(property_name); // We only have one field in the yrs doc
        text.get_string(&self.doc.transact())
    }

    pub fn insert(&self, property_name: &'static str, index: u32, value: &str) {
        let trx = self.record_inner.transaction_manager.handle();

        let text = self.doc.get_or_insert_text(property_name); // We only have one field in the yrs doc
        let mut ytx = self.doc.transact_mut();
        text.insert(&mut ytx, index, value);

        trx.add_operation(
            "yrs",
            self.record_inner.collection,
            self.record_inner.id,
            ytx.encode_update_v2(),
        );
    }

    pub fn delete(&self, property_name: &'static str, index: u32, length: u32) {
        let text = self.doc.get_or_insert_text(property_name); // We only have one field in the yrs doc
        let mut ytx = self.doc.transact_mut();
        text.remove_range(&mut ytx, index, length);

        let trx = self.record_inner.transaction_manager.handle();
        trx.add_operation(
            "yrs",
            self.record_inner.collection,
            self.record_inner.id,
            ytx.encode_update_v2(),
        );
    }
}
