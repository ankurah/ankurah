use ulid::Ulid;

use crate::{
    collection::{CollectionHandle, RawCollection},
    event::Operation,
    model::Model,
    storage::StorageEngine, types::ID,
};
use std::{
    collections::BTreeMap,
    sync::{mpsc, Arc},
};

pub struct Node {
    // We don't know the collection type at compile time except via usage of the .collection() method
    collections: BTreeMap<String, RawCollection>,
    // peer_connections: Vec<PeerConnection>,
}

impl Node {
    pub fn new(storage: impl StorageEngine) -> Arc<Self> {
        Arc::new(Self {
            collections: BTreeMap::new(),
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
    pub fn collection<M: Model>(&self, name: &str) -> &CollectionHandle<M> {
        unimplemented!()
        // self.collections.get(name).unwrap()
    }
    pub fn next_id(&self) -> ID {
        ID(Ulid::new())
    }
}

pub struct PeerConnection {
    channel: mpsc::Sender<Operation>,
}
