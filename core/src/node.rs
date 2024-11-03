use ulid::Ulid;

use crate::{
    event::Operation,
    model::{Model, ID},
    storage::{StorageEngine, RawBucket},
    transaction::{TransactionGuard, TransactionManager},
};
use anyhow::Result;
use std::{
    collections::BTreeMap,
    sync::{mpsc, Arc},
};

/// Manager for all records and their properties on this client.
pub struct Node {
    /// Ground truth local state for records.
    /// 
    /// Things like `postgres`, `sled`, `TKiV`.
    storage_engine: Box<dyn StorageEngine>,
    // Modified, potentially uncommitted changes to records.
    storage_buckets: BTreeMap<String, RawBucket>,
    pub transaction_manager: Arc<TransactionManager>,
    // peer_connections: Vec<PeerConnection>,
}

impl Node {
    pub fn new(engine: Box<dyn StorageEngine>) -> Self {
        Self {
            storage_engine: engine,
            storage_buckets: BTreeMap::new(),
            transaction_manager: Arc::new(TransactionManager::new()),
            // peer_connections: Vec::new(),
        }
    }

    pub fn register_model<M>(&mut self, name: &str) -> Result<()>
    where
        M: Model,
    {
        let bucket = self.storage_engine.bucket(name)?;
        self.storage_buckets
            .insert(name.to_owned(), RawBucket::new(bucket));
        Ok(())
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
    pub fn raw_bucket(&self, name: &str) -> &RawBucket {
        let raw = self
            .storage_buckets
            .get(name)
            .expect(&format!("Collection {} expected to exist", name));
        raw
    }
    pub fn next_id(&self) -> ID {
        ID(Ulid::new())
    }
    pub fn begin(&self) -> Result<TransactionGuard> {
        self.transaction_manager.begin()
    }
}

pub struct PeerConnection {
    channel: mpsc::Sender<Operation>,
}
