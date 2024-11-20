use std::sync::Arc;

use crate::{
    error::RetrievalError,
    model::{ScopedRecord, ID},
    Model, Node,
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
    pub fn fetch_record_from_transaction<A: Model>(&self, id: ID) -> Option<&A::ScopedRecord> {
        let bucket = A::bucket_name();

        for record in self.records.iter() {
            // TODO: Optimize this.
            // It shouldn't be a problem until we have a LOT of records but still
            // Probably just a hashing pre-check and then we retrieve it the same way
            // Either that or use a different append-only structure that uses hashing.
            if record.id() == id && record.bucket_name() == bucket {
                let upcast = record.as_dyn_any();
                return Some(
                    upcast
                        .downcast_ref::<A::ScopedRecord>()
                        .expect("Expected correct downcast"),
                );
            }
        }

        None
    }

    /// Fetch a record.
    pub fn fetch_record<A: Model>(&self, id: ID) -> Result<&A::ScopedRecord, RetrievalError> {
        if let Some(local) = self.fetch_record_from_transaction::<A>(id) {
            return Ok(local);
        }

        let erased_record = self.node.fetch_record(id, A::bucket_name())?;
        let scoped_record = erased_record.into_scoped_record::<A>();

        Ok(self.add_record::<A>(scoped_record))
    }

    pub fn add_record<M: Model>(&self, record: M::ScopedRecord) -> &M::ScopedRecord {
        let index = self.records.push(Box::new(record));
        let upcast = self.records[index].as_dyn_any();
        upcast
            .downcast_ref::<M::ScopedRecord>()
            .expect("Expected correct downcast")
    }

    pub fn create<M: Model>(&self, model: &M) -> &M::ScopedRecord {
        let id = self.node.next_id();
        let new_record = <M as Model>::new_scoped_record(id, model);
        let record_ref = self.add_record::<M>(new_record);
        record_ref
    }

    pub fn edit<M: Model>(
        &self,
        id: impl Into<ID>,
    ) -> Result<&M::ScopedRecord, crate::error::RetrievalError> {
        let id = id.into();
        self.fetch_record::<M>(id)
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
        let record_events = self
            .records
            .iter()
            .filter_map(|record| record.get_record_event())
            .collect::<Vec<_>>();

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
