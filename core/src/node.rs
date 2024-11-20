use append_only_vec::AppendOnlyVec;
use ulid::Ulid;

use crate::{
    event::Operation,
    model::{Record, ScopedRecord, ID},
    storage::{Bucket, StorageEngine},
    transaction::Transaction,
};
use std::{
    collections::BTreeMap, sync::{mpsc, Arc, RwLock, Weak}
};

/// Manager for all records and their properties on this client.
pub struct Node {
    /// Ground truth local state for records.
    ///
    /// Things like `postgres`, `sled`, `TKiV`.
    storage_engine: Box<dyn StorageEngine>,
    // Storage for each collection
    collections: RwLock<BTreeMap<&'static str, Bucket>>,
    records: AppendOnlyVec<Weak<Box<dyn ScopedRecord>>>
    // peer_connections: Vec<PeerConnection>,
}

impl Node {
    pub fn new(engine: Box<dyn StorageEngine>) -> Self {
        Self {
            storage_engine: engine,
            collections: RwLock::new(BTreeMap::new()),
            records: AppendOnlyVec::new(),
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
    pub fn begin(self: &Arc<Self>) -> Transaction {
        Transaction::new(self.clone())
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
