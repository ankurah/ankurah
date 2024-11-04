use std::{
    any::Any,
    collections::BTreeMap,
    marker::PhantomData,
    mem::MaybeUninit,
    ops::Deref,
    sync::{Arc, Mutex},
};

use crate::{
    model::{Record, RecordInner, ID},
    Node,
};
use anyhow::Result;
use append_only_vec::AppendOnlyVec;

pub const PAGE_SIZE: usize = 16;

// Q. When do we want unified vs individual property storage for TypeEngine operations?
// A. When we start to care about differentiating possible recipients for different properties.

#[derive(Debug)]
pub struct Operation {
    engine: &'static str,
    payload: Vec<u8>,
}

pub struct Transaction {
    pub(crate) node: Arc<Node>, // only here for committing records to storage engine

    active_records: AppendOnlyVec<Box<dyn Record>>,

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
            active_records: AppendOnlyVec::new(),
            implicit: true,
            consumed: false,
        }
    }

    pub fn get<A: Record>(&self, bucket: &'static str, id: ID) -> Result<&A> {
        println!("mark 1");
        let raw_bucket = self.node.raw_bucket(bucket);
        let record_state = raw_bucket.bucket.get(id)?;
        let record_inner = RecordInner {
            id: id,
            collection: bucket,
        };
        println!("mark 2");
        let base_record = A::from_record_state(id, &record_state)?;
        println!("mark 3");
        let index = self.active_records.push(Box::new(base_record));
        let upcast = self.active_records[index].as_dyn_any();
        println!("mark 4");
        Ok(upcast
            .downcast_ref::<A>()
            .expect("Expected correct downcast"))
    }

    #[must_use]
    pub fn commit(mut self) -> Result<()> {
        self.commit_mut_ref()
    }

    #[must_use]
    // only because Drop is &mut self not mut self
    pub(crate) fn commit_mut_ref(&mut self) -> Result<()> {
        self.consumed = true;
        // this should probably be done in parallel, but microoptimizations
        for record in self.active_records.iter() {
            record.commit_record(self.node.clone());
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
            self.commit_mut_ref();
        }
    }
}
