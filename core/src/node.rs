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

pub struct Node<E: StorageEngine> {
    storage_engine: E,
    // We don't know the collection type at compile time except via usage of the .collection() method
    collections: BTreeMap<String, RawCollection>,
    pub transaction_manager: Arc<TransactionManager>,
    // peer_connections: Vec<PeerConnection>,
}

impl<E> Node<E>
where 
    E: StorageEngine,
    E::StorageBucket: 'static,
{
    pub fn new(engine: E) -> Self {
        Self {
            storage_engine: engine,
            collections: BTreeMap::new(),
            transaction_manager: Arc::new(TransactionManager::new()),
            // peer_connections: Vec::new(),
        }
    }

    pub fn register_model<M>(&mut self, name: &str) -> Result<()>
    where 
        M: Model,
    {
        let bucket = self.storage_engine.bucket(name)?;
        self.collections.insert(name.to_owned(), RawCollection::new(Box::new(bucket)));
        Ok(())
    }
    pub fn local_connect(&self, _peer: &Arc<Node<E>>) {
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
    pub fn get_collection<M: Model>(
        &self,
        name: &str,
    ) -> Option<Collection<M>> {
        let raw = self.collections.get(name)?;
        Some(Collection::<M>::new(name, raw))
    }
    pub fn collection<M: Model>(&self, name: &str) -> Collection<M> {
        let raw = self
            .collections
            .get(name)
            .expect(&format!("Collection {} expected to exist", name));
        Collection::<M>::new(name, raw)
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
