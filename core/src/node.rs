use ulid::Ulid;

use crate::{
    error::RetrievalError,
    event::Operation,
    model::{Record, RecordInner, ID},
    property::backend::RecordEvent,
    storage::{Bucket, RecordState, StorageEngine},
    transaction::Transaction,
};
use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::{mpsc, Arc, RwLock, Weak},
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
    records: Arc<RwLock<NodeRecords>>,
    // peer_connections: Vec<PeerConnection>,
}

type NodeRecords = BTreeMap<(ID, &'static str), Weak<RecordInner>>;

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
        println!("node.commit_events");
        for record_event in events {
            // Apply record events to the Node's global records first.
            let record = self.fetch_record_inner(record_event.id(), record_event.bucket_name())?;
            record.apply_record_event(record_event)?;

            let record_state = record.to_record_state()?;
            // Push the state buffers to storage.
            self.bucket(record_event.bucket_name())
                .set_record_state(record_event.id(), &record_state)?;

            // TODO: Push the record events to subscribed peers.
        }

        Ok(())
    }

    pub fn get_record_state(
        &self,
        id: ID,
        bucket_name: &'static str,
    ) -> Result<RecordState, RetrievalError> {
        let raw_bucket = self.bucket(bucket_name);
        raw_bucket.0.get_record_state(id)
    }

    // ----  Node record management ----
    /// Add record to the node's list of records, if this returns false, then the erased record was already present.
    #[must_use]
    pub(crate) fn add_record(&self, record: &Arc<RecordInner>) -> bool {
        println!("node.add_record");
        // Assuming that we should propagate panics to the whole program.
        let mut records = self.records.write().unwrap();

        match records.entry((record.id(), record.bucket_name())) {
            Entry::Occupied(mut entry) => {
                if entry.get().strong_count() == 0 {
                    entry.insert(Arc::downgrade(record));
                } else {
                    return true;
                    //panic!("Attempted to add a `ScopedRecord` that already existed in the node")
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(Arc::downgrade(record));
            }
        }

        false
    }

    pub(crate) fn fetch_record_from_node(
        &self,
        id: ID,
        bucket_name: &'static str,
    ) -> Option<Arc<RecordInner>> {
        let records = self.records.read().unwrap();
        if let Some(record) = records.get(&(id, bucket_name)) {
            record.upgrade()
        } else {
            None
        }
    }

    /// Grab the record state if it exists and create a new [`RecordInner`] based on that.
    pub(crate) fn fetch_record_from_storage(
        &self,
        id: ID,
        bucket_name: &'static str,
    ) -> Result<RecordInner, RetrievalError> {
        match self.get_record_state(id, bucket_name) {
            Ok(record_state) => {
                println!("fetched record state: {:?}", record_state);
                RecordInner::from_record_state(id, bucket_name, &record_state)
            }
            Err(RetrievalError::NotFound(id)) => {
                println!("ID not found, creating new record");
                Ok(RecordInner::new(id, bucket_name))
            }
            Err(err) => Err(err),
        }
    }

    /// Fetch a record.
    pub fn fetch_record_inner(
        &self,
        id: ID,
        bucket_name: &'static str,
    ) -> Result<Arc<RecordInner>, RetrievalError> {
        println!("fetch_record {:?}-{:?}", id, bucket_name);
        if let Some(local) = self.fetch_record_from_node(id, bucket_name) {
            println!("passing ref to existing record");
            return Ok(local);
        }

        let scoped_record = self.fetch_record_from_storage(id, bucket_name)?;
        let ref_record = Arc::new(scoped_record);
        let already_present = self.add_record(&ref_record);
        if already_present {
            // We hit a small edge case where the record was fetched twice
            // at the same time and the other fetch added the record first.
            // So try this function again.
            // TODO: lock only once to prevent this?
            return self.fetch_record_inner(id, bucket_name);
        }

        Ok(ref_record)
    }

    pub fn get_record<R: Record>(&self, id: ID) -> Result<R, RetrievalError> {
        use crate::model::Model;
        let collection_name = R::Model::bucket_name();
        let record_inner = self.fetch_record_inner(id, collection_name)?;
        Ok(R::from_record_inner(record_inner))
    }
}

pub struct PeerConnection {
    channel: mpsc::Sender<Operation>,
}
