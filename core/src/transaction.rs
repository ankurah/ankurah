use std::sync::Arc;

use crate::{
    error::RetrievalError, model::{ScopedRecord, ID}, Model, Node
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

    /// Fetch a record already in the transaction.
    pub fn fetch_record_from_transaction<A: Model>(
        &self,
        id: ID,
    ) -> Option<&A::ScopedRecord> {
        let bucket = A::bucket_name();

        for record in self.records.iter() {
            // TODO: Optimize this.
            // It shouldn't be a problem until we have a LOT of records but still
            // Probably just a hashing pre-check and then we retrieve it the same way
            // Either that or use a different append-only structure that uses hashing.
            if record.id() == id && record.bucket_name() == bucket {
                let upcast = record.as_dyn_any();
                return Some(upcast
                    .downcast_ref::<A::ScopedRecord>()
                    .expect("Expected correct downcast"));
            }
        }

        None
    }

    /// Fetch a record already in the transaction.
    pub fn fetch_record_from_storage<A: Model>(
        &self,
        id: ID,
    ) -> Result<A::ScopedRecord, RetrievalError> {
        let bucket = A::bucket_name();

        let raw_bucket = self.node.bucket(bucket);
        let record_state = raw_bucket.0.get(id)?;
        let scoped_record = <A::ScopedRecord>::from_record_state(id, &record_state)?;
        return Ok(scoped_record)
    }

    /// Fetch a record.
    pub fn fetch_record<A: Model>(
        &self,
        id: ID,
    ) -> Result<&A::ScopedRecord, RetrievalError> {
        if let Some(local) = self.fetch_record_from_transaction::<A>(id) {
            return Ok(local)
        }

        let scoped_record = self.fetch_record_from_storage::<A>(id)?;
        let index = self.records.push(Box::new(scoped_record));
        let upcast = self.records[index].as_dyn_any();
        Ok(upcast
            .downcast_ref::<A::ScopedRecord>()
            .expect("Expected correct downcast"))
    }

    /*pub fn create<A: Model>(
        &self,
        model: &A,
    ) -> anyhow::Result<A::Record, RetrievalError> {
        //A::Record::new
        Ok(())
    }*/

    pub fn edit<A: Model>(
        &self,
        id: ID,
    ) -> Result<&A::ScopedRecord, crate::error::RetrievalError> {
        // TODO: should we automatically create the record if it doesn't exist already?
        self.fetch_record::<A>(id)
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
