use ulid::Ulid;

use crate::{
    error::RetrievalError,
    event::Operation,
    model::{Record, RecordInner, ID},
    property::backend::RecordEvent,
    resultset::ResultSet,
    storage::{Bucket, RecordState, StorageEngine},
    transaction::Transaction,
};
use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::{Arc, RwLock, Weak},
};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct NodeId(Ulid);

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct RequestId(Ulid);

#[derive(Debug)]
pub struct NodeRequest {
    id: RequestId,
    to: NodeId,
    from: NodeId,
    body: NodeRequestBody,
}

#[derive(Debug)]
pub struct NodeResponse {
    request_id: RequestId,
    from: NodeId,
    to: NodeId,
    result: anyhow::Result<NodeResponseBody>,
}

#[derive(Debug)]
enum NodeRequestBody {
    // Events to be committed on the remote node
    CommitEvents(Vec<RecordEvent>),
    // Request to fetch records matching a predicate
    FetchRecords {
        bucket_name: &'static str,
        predicate: String,
    },
}

#[derive(Debug)]
enum NodeResponseBody {
    // Response to CommitEvents
    CommitComplete(anyhow::Result<()>),
    // Response to FetchRecords
    Records(anyhow::Result<Vec<RecordState>>),
}

/// Manager for all records and their properties on this client.
pub struct Node {
    id: NodeId,

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
    peer_connections: RwLock<BTreeMap<NodeId, PeerConnection>>,
    pending_requests:
        RwLock<BTreeMap<RequestId, oneshot::Sender<anyhow::Result<NodeResponseBody>>>>,
}

type NodeRecords = BTreeMap<(ID, &'static str), Weak<RecordInner>>;

#[derive(Debug)]
enum PeerMessage {
    Request(NodeRequest),
    Response(NodeResponse),
}

#[derive(Clone)]
pub enum PeerConnection {
    Local(mpsc::Sender<PeerMessage>),
}

impl Node {
    pub fn new(engine: Box<dyn StorageEngine>) -> Self {
        Self {
            id: NodeId(Ulid::new()),
            storage_engine: engine,
            collections: RwLock::new(BTreeMap::new()),
            records: Arc::new(RwLock::new(BTreeMap::new())),
            peer_connections: RwLock::new(BTreeMap::new()),
            pending_requests: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn local_connect(self: &Arc<Self>, peer: &Arc<Node>) {
        let (my_tx, my_rx) = mpsc::channel(100);
        let (peer_tx, peer_rx) = mpsc::channel(100); // Buffer size of 100

        self.setup_local_handler(my_rx);
        peer.setup_local_handler(peer_rx);
        // Store the peer connection for the client
        self.peer_connections
            .write()
            .unwrap()
            .insert(peer.id.clone(), PeerConnection::Local(peer_tx));
        // Store the peer connection for the server
        peer.peer_connections
            .write()
            .unwrap()
            .insert(self.id.clone(), PeerConnection::Local(my_tx));
    }

    fn setup_local_handler(self: &Arc<Self>, mut rx: mpsc::Receiver<PeerMessage>) {
        let me = self.clone();
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                println!("Received message: {:?}", message);
                match message {
                    PeerMessage::Request(request) => {
                        // double check to make sure we have a connection to the peer based on the node id
                        let conn = {
                            let peer_connections = me.peer_connections.read().unwrap();
                            peer_connections.get(&request.from).cloned()
                        };

                        if let Some(tx) = conn {
                            let me = me.clone();
                            tokio::spawn(async move {
                                let from = request.from.clone();
                                let to = request.to.clone();
                                let id = request.id.clone();
                                let result = me.handle_request(request).await;
                                tx.send_message(PeerMessage::Response(NodeResponse {
                                    request_id: id,
                                    from: from,
                                    to: to,
                                    result,
                                }))
                                .await
                                .unwrap();
                            });
                        }
                    }
                    PeerMessage::Response(response) => {
                        if let Some(tx) = me
                            .pending_requests
                            .write()
                            .unwrap()
                            .remove(&response.request_id)
                        {
                            tx.send(response.result).unwrap();
                        }
                    }
                }
            }
        });
    }

    pub async fn request(
        &self,
        node_id: NodeId,
        request_body: NodeRequestBody,
    ) -> anyhow::Result<NodeResponseBody> {
        let (response_tx, response_rx) =
            oneshot::channel::<Result<NodeResponseBody, anyhow::Error>>();
        let request_id = RequestId(Ulid::new());

        // Store the response channel
        self.pending_requests
            .write()
            .unwrap()
            .insert(request_id.clone(), response_tx);

        let request = NodeRequest {
            id: request_id,
            to: node_id.clone(),
            from: self.id.clone(),
            body: request_body,
        };

        // Get the peer connection
        let peer_connections = self.peer_connections.read().unwrap();
        let connection = peer_connections
            .get(&node_id)
            .ok_or_else(|| anyhow::anyhow!("No connection to peer"))?;

        // Send the request
        connection
            .send_message(PeerMessage::Request(request))
            .await
            .map_err(|_| anyhow::anyhow!("Failed to send request"))?;

        // Wait for response
        let response = response_rx
            .await
            .map_err(|_| anyhow::anyhow!("Failed to receive response"))?;

        response
    }

    async fn handle_request(
        self: &Arc<Self>,
        request: NodeRequest,
    ) -> anyhow::Result<NodeResponseBody> {
        match request.body {
            NodeRequestBody::CommitEvents(events) => Ok(NodeResponseBody::CommitComplete(
                // TODO - relay to peers in a gossipy/resource-available manner, so as to improve propagation
                // With moderate potential for duplication, while not creating message loops
                // Doing so would be a secondary/tertiary/etc hop for this message
                self.commit_events_local(&events).await,
            )),
            NodeRequestBody::FetchRecords {
                bucket_name,
                predicate,
            } => {
                let predicate = ankql::parser::parse_selection(&predicate)
                    .map_err(|e| anyhow::anyhow!("Failed to parse predicate: {}", e))?;
                let states: Vec<_> = self
                    .storage_engine
                    .fetch_states(bucket_name, &predicate)
                    .await?
                    //             .await?
                    .into_iter()
                    .map(|(_, state)| state)
                    .collect();
                Ok(NodeResponseBody::Records(Ok(states)))
            }
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

    pub async fn commit_events_local(
        self: &Arc<Self>,
        events: &Vec<RecordEvent>,
    ) -> anyhow::Result<()> {
        // println!("node.commit_events_local");
        // First apply events locally
        for record_event in events {
            // Apply record events to the Node's global records first.
            let record = self
                .fetch_record_inner(record_event.id(), record_event.bucket_name())
                .await?;

            record.apply_record_event(record_event)?;

            let record_state = record.to_record_state()?;
            // Push the state buffers to storage.
            self.bucket(record_event.bucket_name())
                .set_record(record_event.id(), &record_state)
                .await?;
        }

        Ok(())
    }

    /// Apply events to local state buffer and broadcast to peers.
    pub async fn commit_events(self: &Arc<Self>, events: &Vec<RecordEvent>) -> anyhow::Result<()> {
        // println!("node.commit_events");

        self.commit_events_local(events).await?;

        // Then propagate to all peers
        let mut futures = Vec::new();
        let peer_ids = {
            self.peer_connections
                .read()
                .unwrap()
                .iter()
                .map(|(peer_id, _)| peer_id.clone())
                .collect::<Vec<_>>()
        };

        // LEFT OFF HERE - can't hold this across an await
        // Send commit request to all peers
        for peer_id in peer_ids {
            futures.push(self.request(
                peer_id.clone(),
                NodeRequestBody::CommitEvents(events.to_vec()),
            ));
        }

        // // Wait for all peers to confirm
        for future in futures {
            match future.await {
                Ok(NodeResponseBody::CommitComplete(result)) => result?,
                Ok(_) => return Err(anyhow::anyhow!("Unexpected response type")),
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    pub async fn get_record_state(
        &self,
        id: ID,
        bucket_name: &'static str,
    ) -> Result<RecordState, RetrievalError> {
        let raw_bucket = self.bucket(bucket_name);
        raw_bucket.0.get_record(id).await
    }

    // ----  Node record management ----
    /// Add record to the node's list of records, if this returns false, then the erased record was already present.
    #[must_use]
    pub(crate) fn add_record(&self, record: &Arc<RecordInner>) -> bool {
        // println!("node.add_record");
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
    pub(crate) async fn fetch_record_from_storage(
        &self,
        id: ID,
        bucket_name: &'static str,
    ) -> Result<RecordInner, RetrievalError> {
        match self.get_record_state(id, bucket_name).await {
            Ok(record_state) => {
                // println!("fetched record state: {:?}", record_state);
                RecordInner::from_record_state(id, bucket_name, &record_state)
            }
            Err(RetrievalError::NotFound(id)) => {
                // println!("ID not found, creating new record");
                Ok(RecordInner::new(id, bucket_name))
            }
            Err(err) => Err(err),
        }
    }

    /// Fetch a record.
    pub async fn fetch_record_inner(
        &self,
        id: ID,
        bucket_name: &'static str,
    ) -> Result<Arc<RecordInner>, RetrievalError> {
        // println!("fetch_record {:?}-{:?}", id, bucket_name);
        if let Some(local) = self.fetch_record_from_node(id, bucket_name) {
            println!("passing ref to existing record");
            return Ok(local);
        }

        let scoped_record = self.fetch_record_from_storage(id, bucket_name).await?;
        let ref_record = Arc::new(scoped_record);
        let already_present = self.add_record(&ref_record);
        if already_present {
            // We hit a small edge case where the record was fetched twice
            // at the same time and the other fetch added the record first.
            // So try this function again.
            // TODO: lock only once to prevent this?
            if let Some(local) = self.fetch_record_from_node(id, bucket_name) {
                return Ok(local);
            }
        }

        Ok(ref_record)
    }

    pub async fn get_record<R: Record>(&self, id: ID) -> Result<R, RetrievalError> {
        use crate::model::Model;
        let collection_name = R::Model::bucket_name();
        let record_inner = self.fetch_record_inner(id, collection_name).await?;
        Ok(R::from_record_inner(record_inner))
    }

    pub async fn fetch<R: Record>(&self, predicate_str: &str) -> anyhow::Result<ResultSet<R>> {
        use crate::model::Model;
        let collection_name = R::Model::bucket_name();

        // Parse the predicate
        let predicate = ankql::parser::parse_selection(predicate_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse predicate: {}", e))?;

        // Fetch raw states from storage
        let states = self
            .storage_engine
            .fetch_states(collection_name, &predicate)
            .await?;

        // Convert states to records
        let records = states
            .into_iter()
            .map(|(id, state)| {
                let record_inner = RecordInner::from_record_state(id, collection_name, &state)?;
                let ref_record = Arc::new(record_inner);
                // Only add the record if it doesn't exist yet in the node's state
                if self.fetch_record_from_node(id, collection_name).is_none() {
                    self.add_record(&ref_record);
                }
                Ok(R::from_record_inner(ref_record))
            })
            .collect::<anyhow::Result<Vec<R>>>()?;

        Ok(ResultSet { records })
    }
}

impl PeerConnection {
    async fn send_message(&self, message: PeerMessage) -> anyhow::Result<()> {
        match self {
            PeerConnection::Local(sender) => {
                sender
                    .send(message)
                    .await
                    .map_err(|_| anyhow::anyhow!("Failed to send message"))?;
                Ok(())
            }
        }
    }
}
