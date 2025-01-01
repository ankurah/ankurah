use ankurah_proto as proto;
use anyhow::{anyhow, Result};
use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::{Arc, Weak},
};
use tokio::sync::{oneshot, RwLock};

use crate::{
    changes::{ChangeSet, RecordChange, RecordChangeKind},
    connector::PeerSender,
    error::RetrievalError,
    model::{Record, RecordInner},
    reactor::Reactor,
    resultset::ResultSet,
    storage::{Bucket, StorageEngine},
    transaction::Transaction,
};
use tracing::{debug, info};
/// Manager for all records and their properties on this client.
pub struct Node {
    pub id: proto::NodeId,

    /// Ground truth local state for records.
    ///
    /// Things like `postgres`, `sled`, `TKiV`.
    storage_engine: Arc<dyn StorageEngine>,
    // Storage for each collection
    collections: RwLock<BTreeMap<String, Bucket>>,

    // TODO: Something other than an a Mutex/Rwlock?.
    // We mostly need the mutable access to clean up records we don't care about anymore.
    //records: Arc<RwLock<BTreeMap<(ID, &'static str), Weak<dyn ScopedRecord>>>>,
    records: Arc<RwLock<NodeRecords>>,
    // peer_connections: Vec<PeerConnection>,
    peer_connections: tokio::sync::RwLock<BTreeMap<proto::NodeId, Box<dyn PeerSender>>>,
    pending_requests:
        tokio::sync::RwLock<BTreeMap<proto::RequestId, oneshot::Sender<proto::NodeResponseBody>>>,

    /// The reactor for handling subscriptions
    reactor: Arc<Reactor>,
}

type NodeRecords = BTreeMap<(proto::ID, String), Weak<RecordInner>>;

impl Node {
    pub fn new(engine: Arc<dyn StorageEngine>) -> Arc<Self> {
        let reactor = Reactor::new(engine.clone());
        Arc::new(Self {
            id: proto::NodeId::new(),
            storage_engine: engine,
            collections: RwLock::new(BTreeMap::new()),
            records: Arc::new(RwLock::new(BTreeMap::new())),
            peer_connections: tokio::sync::RwLock::new(BTreeMap::new()),
            pending_requests: tokio::sync::RwLock::new(BTreeMap::new()),
            reactor,
        })
    }

    pub async fn register_peer_sender(&self, sender: Box<dyn PeerSender>) {
        info!("node.register_peer_sender 1");
        let mut peer_connections = self.peer_connections.write().await;
        peer_connections.insert(sender.node_id(), sender);
        info!("node.register_peer_sender 2");
        // TODO send hello message to the peer, including present head state for all relevant collections
    }
    pub async fn deregister_peer_sender(&self, node_id: proto::NodeId) {
        let mut peer_connections = self.peer_connections.write().await;
        peer_connections.remove(&node_id);
    }

    pub async fn request(
        &self,
        node_id: proto::NodeId,
        request_body: proto::NodeRequestBody,
    ) -> anyhow::Result<proto::NodeResponseBody> {
        let (response_tx, response_rx) = oneshot::channel::<proto::NodeResponseBody>();
        let request_id = proto::RequestId::new();

        // Store the response channel
        self.pending_requests
            .write()
            .await
            .insert(request_id.clone(), response_tx);

        let request = proto::NodeRequest {
            id: request_id,
            to: node_id.clone(),
            from: self.id.clone(),
            body: request_body,
        };

        {
            // Get the peer connection

            let peer_connections = self.peer_connections.read().await;
            let connection = peer_connections
                .get(&node_id)
                .ok_or_else(|| anyhow!("No connection to peer"))?
                .cloned();

            drop(peer_connections);

            // Send the request
            connection
                .send_message(proto::PeerMessage::Request(request))
                .await
                .map_err(|_| anyhow!("Failed to send request"))?;
        }

        // Wait for response
        let response = response_rx
            .await
            .map_err(|_| anyhow!("Failed to receive response"))?;

        Ok(response)
    }

    pub async fn handle_message(
        self: &Arc<Self>,
        message: proto::PeerMessage,
    ) -> anyhow::Result<()> {
        info!("Received message: {:?}", message);
        match message {
            proto::PeerMessage::Request(request) => {
                // TODO: Should we spawn a task here and make handle_message synchronous?
                // I think this depends on how we want to handle timeouts.
                // I think we want timeouts to be handled by the node, not the connector,
                // which would lend itself to spawning a task here and making this function synchronous.

                // double check to make sure we have a connection to the peer based on the node id
                if let Some(sender) = {
                    let peer_connections = self.peer_connections.read().await;
                    peer_connections.get(&request.from).map(|c| c.cloned())
                } {
                    let from = request.from.clone();
                    let to = request.to.clone();
                    let request_id = request.id.clone();

                    let body = match self.handle_request(request).await {
                        Ok(result) => result,
                        Err(e) => proto::NodeResponseBody::Error(e.to_string()),
                    };
                    let foo = sender
                        .send_message(proto::PeerMessage::Response(proto::NodeResponse {
                            request_id,
                            from,
                            to,
                            body,
                        }))
                        .await;
                }
            }
            proto::PeerMessage::Response(response) => {
                if let Some(tx) = self
                    .pending_requests
                    .write()
                    .await
                    .remove(&response.request_id)
                {
                    tx.send(response.body)
                        .map_err(|e| anyhow!("Failed to send response: {:?}", e))?;
                }
            }
        }
        Ok(())
    }

    async fn handle_request(
        self: &Arc<Self>,
        request: proto::NodeRequest,
    ) -> anyhow::Result<proto::NodeResponseBody> {
        match request.body {
            proto::NodeRequestBody::CommitEvents(events) => {
                // TODO - relay to peers in a gossipy/resource-available manner, so as to improve propagation
                // With moderate potential for duplication, while not creating message loops
                // Doing so would be a secondary/tertiary/etc hop for this message
                match self.commit_events_local(&events).await {
                    Ok(_) => Ok(proto::NodeResponseBody::CommitComplete),
                    Err(e) => Ok(proto::NodeResponseBody::Error(e.to_string())),
                }
            }
            proto::NodeRequestBody::FetchRecords {
                bucket_name,
                predicate,
            } => {
                let predicate = ankql::parser::parse_selection(&predicate)
                    .map_err(|e| anyhow!("Failed to parse predicate: {}", e))?;
                let states: Vec<_> = self
                    .storage_engine
                    .fetch_states(bucket_name, &predicate)
                    .await?
                    .into_iter()
                    .map(|(_, state)| state)
                    .collect();
                Ok(proto::NodeResponseBody::Records(states))
            }
        }
    }

    pub async fn bucket(&self, name: &str) -> Bucket {
        let collections = self.collections.read().await;
        if let Some(store) = collections.get(name) {
            return store.clone();
        }
        drop(collections);

        let collection = Bucket::new(self.storage_engine.bucket(name).await.unwrap());

        let mut collections = self.collections.write().await;
        assert!(collections
            .insert(name.to_string(), collection.clone())
            .is_none());
        drop(collections);

        collection
    }

    pub fn next_record_id(&self) -> proto::ID {
        proto::ID::new()
    }

    /// Begin a transaction.
    ///
    /// This is the main way to edit records.
    pub fn begin(self: &Arc<Self>) -> Transaction {
        Transaction::new(self.clone())
    }

    pub async fn commit_events_local(
        self: &Arc<Self>,
        events: &Vec<proto::RecordEvent>,
    ) -> anyhow::Result<()> {
        debug!("node.commit_events_local 1");

        let mut changes = Vec::new();

        // First apply events locally
        for record_event in events {
            debug!(
                "node.commit_events_local 2: record_event: {:?}",
                record_event
            );
            // Apply record events to the Node's global records first.
            let record = self
                .fetch_record_inner(record_event.id, &record_event.bucket_name)
                .await?;

            debug!("node.commit_events_local 3: record: {:?}", record);
            record.apply_record_event(record_event)?;
            debug!("node.commit_events_local 4: apply_record_event done");

            let record_state = record.to_record_state()?;
            debug!(
                "node.commit_events_local 5: record_state: {:?}",
                record_state
            );
            // Push the state buffers to storage.
            self.bucket(record_event.bucket_name())
                .await
                .set_record(record_event.id(), &record_state)
                .await?;
            debug!("node.commit_events_local 6: done");

            // Collect the change
            changes.push(RecordChange {
                record: record.clone(),
                updates: events.clone(),
                kind: RecordChangeKind::Edit,
            });
        }

        // Notify reactor with a single changeset containing all changes
        let changeset = ChangeSet { changes };
        self.reactor.notify_change(changeset);

        Ok(())
    }

    /// Create a new record and notify subscribers
    pub(crate) async fn create_record(
        self: &Arc<Self>,
        record: Arc<RecordInner>,
    ) -> anyhow::Result<()> {
        // Store the record
        let record_state = record.to_record_state()?;
        self.bucket(record.bucket_name())
            .await
            .set_record(record.id(), &record_state)
            .await?;

        // Add to node's records
        self.add_record(&record).await;

        // Notify subscribers
        let change = RecordChange {
            record: record.clone(),
            updates: vec![],
            kind: RecordChangeKind::Add,
        };
        let changeset = ChangeSet {
            changes: vec![change],
        };
        self.reactor.notify_change(changeset);

        Ok(())
    }

    // /// Delete a record and notify subscribers
    // pub(crate) async fn delete_record(
    //     self: &Arc<Self>,
    //     record: Arc<RecordInner>,
    // ) -> anyhow::Result<()> {
    //     // Remove from storage
    //     self.bucket(record.bucket_name())
    //         .await
    //         .delete_record(record.id())
    //         .await?;

    //     // Remove from node's records
    //     let mut records = self.records.write().await;
    //     records.remove(&(record.id(), record.bucket_name().to_string()));

    //     // Notify subscribers
    //     let change = RecordChange {
    //         record: record.clone(),
    //         updates: vec![],
    //         kind: RecordChangeKind::Remove,
    //     };
    //     let changeset = ChangeSet {
    //         changes: vec![change],
    //     };
    //     self.reactor.notify_change(changeset);

    //     Ok(())
    // }

    /// Update a record and notify subscribers
    pub(crate) async fn update_record(
        self: &Arc<Self>,
        record: Arc<RecordInner>,
        events: Vec<proto::RecordEvent>,
    ) -> anyhow::Result<()> {
        // Store the record
        let record_state = record.to_record_state()?;
        self.bucket(record.bucket_name())
            .await
            .set_record(record.id(), &record_state)
            .await?;

        // Notify subscribers
        let change = RecordChange {
            record: record.clone(),
            updates: events,
            kind: RecordChangeKind::Edit,
        };
        let changeset = ChangeSet {
            changes: vec![change],
        };
        self.reactor.notify_change(changeset);

        Ok(())
    }

    /// Apply events to local state buffer and broadcast to peers.
    pub async fn commit_events(
        self: &Arc<Self>,
        events: &Vec<proto::RecordEvent>,
    ) -> anyhow::Result<()> {
        debug!("node.commit_events");

        self.commit_events_local(events).await?;

        debug!("node.commit_events: done");

        // Then propagate to all peers
        let mut futures = Vec::new();
        let peer_ids = {
            self.peer_connections
                .read()
                .await
                .iter()
                .map(|(peer_id, _)| peer_id.clone())
                .collect::<Vec<_>>()
        };

        info!("node.commit_events: peer_ids: {:?}", peer_ids);
        // Send commit request to all peers
        for peer_id in peer_ids {
            info!("node.commit_events: sending to peer {:?}", peer_id);
            futures.push(self.request(
                peer_id.clone(),
                proto::NodeRequestBody::CommitEvents(events.to_vec()),
            ));
        }

        // // Wait for all peers to confirm
        for future in futures {
            match future.await {
                Ok(proto::NodeResponseBody::CommitComplete) => Ok(()),
                Ok(proto::NodeResponseBody::Error(e)) => Err(anyhow!(e)),
                Ok(_) => Err(anyhow!("Unexpected response type")),
                Err(e) => Err(e),
            }?;
        }

        Ok(())
    }

    pub async fn get_record_state(
        &self,
        id: proto::ID,
        bucket_name: &str,
    ) -> Result<proto::RecordState, RetrievalError> {
        let raw_bucket = self.bucket(bucket_name).await;
        raw_bucket.0.get_record(id).await
    }

    // ----  Node record management ----
    /// Add record to the node's list of records, if this returns false, then the erased record was already present.
    #[must_use]
    pub(crate) async fn add_record(&self, record: &Arc<RecordInner>) -> bool {
        // info!("node.add_record");
        // Assuming that we should propagate panics to the whole program.
        let mut records = self.records.write().await;

        match records.entry((record.id(), record.bucket_name.clone())) {
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

    pub(crate) async fn fetch_record_from_node(
        &self,
        id: proto::ID,
        bucket_name: &str,
    ) -> Option<Arc<RecordInner>> {
        let records = self.records.read().await;
        if let Some(record) = records.get(&(id, bucket_name.to_string())) {
            record.upgrade()
        } else {
            None
        }
    }

    /// Grab the record state if it exists and create a new [`RecordInner`] based on that.
    pub(crate) async fn fetch_record_from_storage(
        &self,
        id: proto::ID,
        bucket_name: &str,
    ) -> Result<RecordInner, RetrievalError> {
        match self.get_record_state(id, bucket_name).await {
            Ok(record_state) => {
                // info!("fetched record state: {:?}", record_state);
                RecordInner::from_record_state(id, bucket_name, &record_state)
            }
            Err(RetrievalError::NotFound(id)) => {
                // info!("ID not found, creating new record");
                Ok(RecordInner::new(id, bucket_name.to_string()))
            }
            Err(err) => Err(err),
        }
    }

    /// Fetch a record.
    pub async fn fetch_record_inner(
        &self,
        id: proto::ID,
        bucket_name: &str,
    ) -> Result<Arc<RecordInner>, RetrievalError> {
        debug!("fetch_record {:?}-{:?}", id, bucket_name);
        if let Some(local) = self.fetch_record_from_node(id, bucket_name).await {
            info!("passing ref to existing record");
            return Ok(local);
        }
        debug!("fetch_record 2");

        let scoped_record = self.fetch_record_from_storage(id, bucket_name).await?;
        debug!("fetch_record 3");
        let ref_record = Arc::new(scoped_record);
        debug!("fetch_record 4");
        let already_present = self.add_record(&ref_record).await;
        if already_present {
            // We hit a small edge case where the record was fetched twice
            // at the same time and the other fetch added the record first.
            // So try this function again.
            // TODO: lock only once to prevent this?
            if let Some(local) = self.fetch_record_from_node(id, bucket_name).await {
                return Ok(local);
            }
        }

        Ok(ref_record)
    }

    pub async fn get_record<R: Record>(&self, id: proto::ID) -> Result<R, RetrievalError> {
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
            .map_err(|e| anyhow!("Failed to parse predicate: {}", e))?;

        // Fetch raw states from storage
        let states = self
            .storage_engine
            .fetch_states(collection_name.to_string(), &predicate)
            .await?;

        // Convert states to records
        let mut records = Vec::new();
        for (id, state) in states {
            let record_inner = RecordInner::from_record_state(id, collection_name, &state)?;
            let ref_record = Arc::new(record_inner);
            // Only add the record if it doesn't exist yet in the node's state
            if self
                .fetch_record_from_node(id, collection_name)
                .await
                .is_none()
            {
                let _ = self.add_record(&ref_record).await;
            }
            records.push(R::from_record_inner(ref_record));
        }

        Ok(ResultSet { records })
    }

    /// Subscribe to changes in records matching a predicate
    pub async fn subscribe<F>(
        self: &Arc<Self>,
        bucket_name: &str,
        predicate: &ankql::ast::Predicate,
        callback: F,
    ) -> Result<crate::subscription::SubscriptionHandle>
    where
        F: Fn(crate::changes::ChangeSet) + Send + Sync + 'static,
    {
        self.reactor
            .subscribe(bucket_name, predicate, callback)
            .await
    }
}
