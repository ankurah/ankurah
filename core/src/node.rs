use append_only_vec::AppendOnlyVec;
use bincode::config::WithOtherEndian;
use ulid::Ulid;

use crate::{
    error::RetrievalError,
    event::Operation,
    model::{Record, ScopedRecord, ID},
    property::backend::{Events, RecordEvent},
    storage::{Bucket, RecordState, StorageEngine},
    transaction::Transaction,
    Model,
};
use std::{
    any::Any,
    collections::BTreeMap,
    ops::Deref,
    sync::{mpsc, Arc, Mutex, RwLock, Weak},
};

/// Manager for all records and their properties on this client.
pub struct Node {
    /// Ground truth local state for records.
    ///
    /// Things like `postgres`, `sled`, `TKiV`.
    storage_engine: Box<dyn StorageEngine>,
    // Storage for each collection
    collections: RwLock<BTreeMap<&'static str, Bucket>>,

    // TODO: Something other than an a Mutex.
    // We mostly need the mutex to clean up records we don't care about anymore.
    records: Arc<Mutex<Vec<Weak<dyn ScopedRecord>>>>,
    // peer_connections: Vec<PeerConnection>,
}

impl Node {
    pub fn new(engine: Box<dyn StorageEngine>) -> Self {
        Self {
            storage_engine: engine,
            collections: RwLock::new(BTreeMap::new()),
            records: Arc::new(Mutex::new(Vec::new())),
            // peer_connections: Vec::new(),
        }
    }

    pub fn local_connect(&self, _peer: &Arc<Node>) {
        unimplemented!()
        // let (tx, rx) = mpsc::channel();
        // self.peer_connections.push(PeerConnection { channel: tx });
        // let peer = peer.clone();
        // tokio::spawn(async move {
        //     for operation in rx {
        //         peer.apply_operation(operation);
        //     }
        // });
    }

    pub fn collection<T: Record>(&self, name: &'static str) -> Collection<T> {
        Collection {
            bucket: self.bucket(name),
            phantom: std::marker::PhantomData,
        }
    }

    pub fn bucket(&self, name: &'static str) -> Bucket {
        let collections = self.collections.read().unwrap();
        if let Some(store) = collections.get(name) {
            return store.clone();
        }
        drop(collections);

        let mut collections = self.collections.write().unwrap();
        let collection = Bucket::new(self.storage_engine.bucket(name).unwrap());

        assert!(collections.insert(name, collection.clone()).is_none());

        collection
    }

    pub fn next_id(&self) -> ID {
        ID(Ulid::new())
    }

    /// Begin a transaction.
    ///
    /// This is the main way to edit records.
    pub fn begin(self: &Arc<Self>) -> Transaction {
        Transaction::new(self.clone())
    }

    /// Apply events to local state buffer and broadcast to subscribed peers.
    pub fn commit_events(&self, events: &Events) -> anyhow::Result<()> {
        for record_event in &events.record_events {
            //self.storage_engine.insert(record_event)
        }
        Ok(())
    }

    pub fn empty_record_slot(&self) -> Option<usize> {
        // Assuming that we should propagate panics to the whole program.
        let records = self.records.lock().unwrap();
        for (index, record) in records.iter().enumerate() {
            if record.strong_count() == 0 {
                return Some(index);
            }
        }

        None
    }

    pub fn apply_record_event(&self, event: RecordEvent) {}

    pub fn get_record_state<A: Model>(&self, id: ID) -> Result<RecordState, RetrievalError> {
        let bucket = A::bucket_name();
        let raw_bucket = self.bucket(bucket);
        raw_bucket.0.get_record_state(id)
    }

    // ----  Node record management ----
    pub fn add_record<M: Model>(
        &self,
        record: M::ScopedRecord,
    ) -> Result<Arc<M::ScopedRecord>, RetrievalError> {
        // Assuming that we should propagate panics to the whole program.
        let mut records = self.records.lock().unwrap();

        let mut empty_index = None;
        for (index, record) in records.iter().enumerate() {
            if record.strong_count() == 0 {
                empty_index = Some(index);
            }
        }

        let return_record = Arc::new(record);
        let scoped_record = return_record.clone() as Arc<dyn ScopedRecord>;
        match empty_index {
            Some(empty_index) => {
                records[empty_index] = Arc::downgrade(&scoped_record);
            }
            None => {
                records.push(Arc::downgrade(&scoped_record));
            }
        }

        Ok(return_record)
    }

    pub fn fetch_record_from_node<A: Model>(&self, id: ID) -> Option<Arc<A::ScopedRecord>> {
        let bucket = A::bucket_name();

        // Assuming that we should propagate panics to the whole program.
        let records = self.records.lock().unwrap();
        for record in records.iter() {
            if let Some(record) = record.upgrade() {
                if record.id() == id && record.bucket_name() == bucket {
                    let upcast = record.as_arc_dyn_any();
                    return Some(
                        upcast
                            .downcast::<A::ScopedRecord>()
                            .expect("Expected correct downcast"),
                    );
                }
            }
        }

        None
    }

    pub fn fetch_record_from_storage<A: Model>(
        &self,
        id: ID,
    ) -> Result<A::ScopedRecord, RetrievalError> {
        let record_state = self.get_record_state::<A>(id)?;
        let scoped_record = <A::ScopedRecord>::from_record_state(id, &record_state)?;
        Ok(scoped_record)
    }

    /// Fetch a record.
    pub fn fetch_record<A: Model>(&self, id: ID) -> Result<Arc<A::ScopedRecord>, RetrievalError> {
        if let Some(local) = self.fetch_record_from_node::<A>(id) {
            return Ok(local);
        }

        let scoped_record = self.fetch_record_from_storage::<A>(id)?;
        self.add_record::<A>(scoped_record)
    }
}

/// Handle to a collection of records of the same type
/// Able to construct records of type `T`
pub struct Collection<T: Record> {
    bucket: Bucket,
    phantom: std::marker::PhantomData<T>,
}

pub struct PeerConnection {
    channel: mpsc::Sender<Operation>,
}
