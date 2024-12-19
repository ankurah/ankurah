use std::sync::Arc;

use crate::{
    error::RetrievalError,
    model::{RecordInner, ScopedRecord, ID},
    Model, Node,
};

use append_only_vec::AppendOnlyVec;

// Q. When do we want unified vs individual property storage for TypeEngine operations?
// A. When we start to care about differentiating possible recipients for different properties.

pub struct Transaction {
    pub(crate) node: Arc<Node>, // only here for committing records to storage engine

    records: AppendOnlyVec<RecordInner>,

    // markers
    implicit: bool,
    consumed: bool,
}

impl Transaction {
    pub fn new(node: Arc<Node>) -> Self {
        Self {
            node,
            records: AppendOnlyVec::new(),
            implicit: true,
            consumed: false,
        }
    }

    /// Fetch a record already in the transaction.
    pub fn fetch_record_from_transaction(
        &self,
        id: ID,
        bucket_name: &'static str,
    ) -> Option<&RecordInner> {
        self.records.iter().find(|&record| record.id() == id && record.bucket_name() == bucket_name)
    }

    /// Fetch a record.
    pub fn fetch_record(
        &self,
        id: ID,
        bucket_name: &'static str,
    ) -> Result<&RecordInner, RetrievalError> {
        if let Some(local) = self.fetch_record_from_transaction(id, bucket_name) {
            return Ok(local);
        }

        let record_inner = self.node.fetch_record_inner(id, bucket_name)?;
        let record_ref = self.add_record(record_inner.snapshot());
        Ok(record_ref)
    }

    pub fn add_record(&self, record: RecordInner) -> &RecordInner {
        let index = self.records.push(record);
        &self.records[index]
    }

    pub fn create<'rec, 'trx: 'rec, M: Model>(&'trx self, model: &M) -> M::ScopedRecord<'rec> {
        let id = self.node.next_id();
        let record_inner = model.to_record_inner(id);
        let record_ref = self.add_record(record_inner);
        <M::ScopedRecord<'rec> as ScopedRecord<'rec>>::from_record_inner(record_ref)
    }

    pub fn edit<'rec, 'trx: 'rec, M: Model>(
        &'trx self,
        id: impl Into<ID>,
    ) -> Result<M::ScopedRecord<'rec>, crate::error::RetrievalError> {
        let id = id.into();
        let record_ref = self.fetch_record(id, M::bucket_name())?;
        Ok(<M::ScopedRecord<'rec> as ScopedRecord<'rec>>::from_record_inner(record_ref))
    }

    #[must_use]
    pub fn commit(mut self) -> anyhow::Result<()> {
        self.commit_mut_ref()
    }

    #[must_use]
    // only because Drop is &mut self not mut self
    pub(crate) fn commit_mut_ref(&mut self) -> anyhow::Result<()> {
        println!("trx.commit_mut_ref");
        self.consumed = true;
        // this should probably be done in parallel, but microoptimizations
        let mut record_events = Vec::new();
        for record in self.records.iter() {
            if let Some(record_event) = record.get_record_event()? {
                record_events.push(record_event);
            }
        }

        println!("record_events: {:?}", record_events);

        self.node.commit_events(&record_events)
    }

    pub fn rollback(mut self) {
        self.consumed = true; // just do nothing on drop
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.implicit && !self.consumed {
            match self.commit_mut_ref() {
                Ok(()) => {}
                // Probably shouldn't panic here, but for testing purposes whatever.
                Err(err) => panic!("Failed to commit: {:?}", err),
            }
        }
    }
}
