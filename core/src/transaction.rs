use std::{
    sync::Arc,
};

use crate::{
    error::RetrievalError, model::{Record, RecordInner, ID, ScopedRecord}, Model, Node
};

use append_only_vec::AppendOnlyVec;

// Q. When do we want unified vs individual property storage for TypeEngine operations?
// A. When we start to care about differentiating possible recipients for different properties.

#[derive(Debug)]
pub struct Operation {
    engine: &'static str,
    payload: Vec<u8>,
}

pub struct Transaction {
    pub(crate) node: Arc<Node>, // only here for committing records to storage engine

    records: AppendOnlyVec<Box<dyn ScopedRecord>>,

    // markers
    implicit: bool,
    consumed: bool,
}

pub struct RecordAccessor<'trx, A> {
    pub transaction: &'trx Transaction,
    pub record: A,
}

impl Transaction {
    pub fn new(node: Arc<Node>) -> Self {
        Self {
            node: node,
            records: AppendOnlyVec::new(),
            implicit: true,
            consumed: false,
        }
    }

    pub fn edit<A: Model>(
        &self,
        id: ID,
    ) -> Result<&A::ScopedRecord, crate::error::RetrievalError> {
        let bucket = A::bucket_name();

        // check if its already in this transaction
        for record in self.records.iter() {
            // TODO: Optimize this.
            // It shouldn't be a problem until we have a LOT of records but still
            // Probably just a hashing pre-check and then we retrieve it the same way
            // Either that or use a different append-only structure that uses hashing.
            if record.id() == id && record.bucket_name() == bucket {
                let upcast = record.as_dyn_any();
                return Ok(upcast
                    .downcast_ref::<A::ScopedRecord>()
                    .expect("Expected correct downcast"));
            }
        }

        // we don't already have this record, fetch it from the node
        let raw_bucket = self.node.bucket(bucket);
        let record_state = raw_bucket.0.get(id)?;
        let base_record = <A::ScopedRecord>::from_record_state(id, &record_state)?;
        let index = self.records.push(Box::new(base_record));
        let upcast = self.records[index].as_dyn_any();
        Ok(upcast
            .downcast_ref::<A::ScopedRecord>()
            .expect("Expected correct downcast"))
    }

    #[must_use]
    pub fn commit(mut self) -> anyhow::Result<()> {
        self.commit_mut_ref()
    }

    #[must_use]
    // only because Drop is &mut self not mut self
    pub(crate) fn commit_mut_ref(&mut self) -> anyhow::Result<()> {
        self.consumed = true;
        // this should probably be done in parallel, but microoptimizations
        for record in self.records.iter() {
            record.commit_record(self.node.clone())?;
        }
        Ok(())
    }

    pub fn rollback(mut self) {
        self.consumed = true; // just do nothing on drop
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.implicit && !self.consumed {
            match self.commit_mut_ref() {
                Ok(()) => {},
                // Probably shouldn't panic here, but for testing purposes whatever.
                Err(err) => panic!("Failed to commit: {:?}", err),
            }
        }
    }
}
