use crate::{
    core::{Collection, Model},
    operation::Operation,
    types::ID,
};
use std::{
    collections::BTreeMap,
    sync::{mpsc, Arc},
};

pub struct Node {
    // We don't know the collection type at compile time except via usage of the .collection() method
    collections: BTreeMap<String, Box<dyn Collection>>,
    peer_connections: Vec<PeerConnection>,
}

impl Node {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            collections: BTreeMap::new(),
            peer_connections: Vec::new(),
        })
    }
    pub fn local_connect(&self, peer: &Arc<Node>) {
        let (tx, rx) = mpsc::channel();
        self.peer_connections.push(PeerConnection { channel: tx });
        let peer = peer.clone();
        tokio::spawn(async move {
            for operation in rx {
                peer.apply_operation(operation);
            }
        });
    }

    pub fn apply_operation(&self, operation: Operation) {
        let collection = self.collections.get_mut(&operation.collection).unwrap();
        collection.apply_operation(operation);
    }
    pub fn collection<M: Model>(&self, name: &str) -> &Collection<M> {
        self.collections.get(name).unwrap()
    }
}

pub struct PeerConnection {
    channel: mpsc::Sender<Operation>,
}
