use ulid::Ulid;

use crate::{
    error::RetrievalError,
    event::Operation,
    model::{ErasedRecord, ScopedRecord, ID},
    property::backend::RecordEvent,
    storage::{Bucket, RecordState, StorageEngine},
    transaction::Transaction,
    Model,
};
use std::{
    collections::{btree_map::Entry, BTreeMap},
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

    // TODO: Something other than an a Mutex/Rwlock?.
    // We mostly need the mutable access to clean up records we don't care about anymore.
    //records: Arc<RwLock<BTreeMap<(ID, &'static str), Weak<dyn ScopedRecord>>>>,
    records: Arc<RwLock<BTreeMap<(ID, &'static str), Weak<ErasedRecord>>>>,
    // peer_connections: Vec<PeerConnection>,
}

impl Node {
    pub fn new(engine: Box<dyn StorageEngine>) -> Self {
        Self {
            storage_engine: engine,
            collections: RwLock::new(BTreeMap::new()),
            records: Arc::new(RwLock::new(BTreeMap::new())),
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
    pub fn commit_events(&self, events: &Vec<RecordEvent>) -> anyhow::Result<()> {
        for record_event in events {
            // Apply record events to the Node's global records first.
            self.apply_record_event(record_event);

            // Then we push the state buffers based on the global records.
            self.update_record_state_buffer(record_event.id(), record_event.bucket_name());

            // Then we push the record events to subscribed peers.
            self.broadcast_record_event(record_event);
        }

        //events.iter().map(|event| self.apply_record_event(event));

        Ok(())
    }

    /// Apply the record event to our Node's record.
    pub fn apply_record_event(&self, event: &RecordEvent) -> anyhow::Result<()> {
        // Create or grab an `Arc<Box<dyn ScopedRecord>>` for our record.

        // Thinking do I need a `&'static str -> Fn(|Arc<Box<dyn ScopedRecord>>| {})`
        // for this...?
        // I don't think that'll work since you'd need to register it beforehand or have
        // some piece of code run with an explicit model first.
        // Then again we don't really need to update it if it doesn't exist already.

        // Fetch the global shared record if it exists.
        // We need to lock the records for the duration of the applying otherwise we might
        // end up with a difference between our storage engine and the node's local record.

        let records = self.records.read().unwrap();
        let Some(record) = self.fetch_record_from_node_erased(event.id(), event.bucket_name()) else {
            return Ok(());
        };
        record.apply_record_event(event)
    }

    pub fn update_record_state_buffer(&self, id: ID, bucket_name: &'static str) {
        // TODO
    }

    pub fn broadcast_record_event(&self, event: &RecordEvent) {
        // TODO
    }

    pub fn get_record_state(&self, id: ID, bucket_name: &'static str) -> Result<RecordState, RetrievalError> {
        let raw_bucket = self.bucket(bucket_name);
        raw_bucket.0.get_record_state(id)
    }

    // ----  Node record management ----
    pub(crate) fn add_record<M: Model>(
        &self,
        record: &Arc<M::ScopedRecord>,
    ) -> Result<(), RetrievalError> {
        // Assuming that we should propagate panics to the whole program.
        let mut records = self.records.write().unwrap();

        match records.entry((record.id(), record.bucket_name())) {
            Entry::Occupied(_) => {
                panic!("Attempted to add a `ScopedRecord` that already existed in the node");
            }
            Entry::Vacant(entry) => {
                let store_record = record.clone() as Arc<dyn ScopedRecord>;
                entry.insert(Arc::downgrade(&store_record));
            }
        }

        Ok(())
    }

    pub fn fetch_record_from_node_erased(
        &self,
        id: ID,
        bucket_name: &'static str,
    ) -> Option<Arc<dyn ScopedRecord>> {
        let records = self.records.read().unwrap();
        if let Some(record) = records.get(&(id, bucket_name)) {
            record.upgrade()
        } else {
            None
        }
    }

    pub fn fetch_record_from_node<A: Model>(&self, id: ID) -> Option<Arc<A::ScopedRecord>> {
        // Assuming that we should propagate panics to the whole program.
        let records = self.records.read().unwrap();
        if let Some(record) = records.get(&(id, A::bucket_name())) {
            if let Some(record) = record.upgrade() {
                let upcast = record.as_arc_dyn_any();
                return Some(
                    upcast
                        .downcast::<A::ScopedRecord>()
                        .expect("Expected correct downcast"),
                );
            }
        }

        None
    }

    /// Grab the record state and create a new [`ScopedRecord`] based on that.
    pub fn fetch_record_from_storage<A: Model>(
        &self,
        id: ID,
    ) -> Result<A::ScopedRecord, RetrievalError> {
        let record_state = self.get_record_state(id, A::bucket_name())?;
        let scoped_record = <A::ScopedRecord>::from_record_state(id, &record_state)?;
        Ok(scoped_record)
    }

    /// Fetch a record.
    pub fn fetch_record<A: Model>(&self, id: ID) -> Result<Arc<A::ScopedRecord>, RetrievalError> {
        if let Some(local) = self.fetch_record_from_node::<A>(id) {
            return Ok(local);
        }

        let scoped_record = self.fetch_record_from_storage::<A>(id)?;
        let ref_record = Arc::new(scoped_record);
        self.add_record::<A>(&ref_record)?;
        Ok(ref_record)
    }
}

pub struct PeerConnection {
    channel: mpsc::Sender<Operation>,
}
