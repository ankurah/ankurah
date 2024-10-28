use ulid::Ulid;

use crate::{
    collection::{Collection, CollectionMut, RawCollection},
    event::Operation,
    model::Model,
    storage::StorageEngine,
    transaction::{TransactionGuard, TransactionManager},
    types::ID,
};
use anyhow::Result;
use std::{
    collections::BTreeMap,
    sync::{mpsc, Arc},
};

pub struct Node {
    // We don't know the collection type at compile time except via usage of the .collection() method
    collections: BTreeMap<String, RawCollection>,
    pub transaction_manager: Arc<TransactionManager>,
    // peer_connections: Vec<PeerConnection>,
}

impl Node {
    pub fn new(storage: impl StorageEngine) -> Arc<Self> {
        Arc::new(Self {
            collections: BTreeMap::new(),
            transaction_manager: Arc::new(TransactionManager::new()),
            // peer_connections: Vec::new(),
        })
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
    pub fn get_collection<'a, M: Model>(
        &'a self,
        name: &str,
    ) -> Option<Collection<'a, M>> {
        let raw = self.collections.get(name)?;
        Some(Collection::<'a, M>::new(name, raw))
    }
    pub fn collection<'a, M: Model>(&'a self, name: &str) -> Collection<'a, M> {
        let raw = self
            .collections
            .get(name)
            .expect(&format!("Collection {} expected to exist", name));
        Collection::<'a, M>::new(name, raw)
    }
    // TODO: Make it so you can get a bunch of collections separately and mutably.
    pub fn get_collection_mut<'a, M: Model>(
        &'a mut self,
        name: &str,
    ) -> Option<CollectionMut<'a, M>> {
        let raw = self.collections.get_mut(name)?;
        Some(CollectionMut::<'a, M>::new(name, raw))
    }
    pub fn collection_mut<'a, M: Model>(&'a mut self, name: &str) -> CollectionMut<'a, M> {
        let raw = self
            .collections
            .get_mut(name)
            .expect(&format!("Collection {} expected to exist", name));
        CollectionMut::<'a, M>::new(name, raw)
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
