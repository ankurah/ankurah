use ankurah_proto as proto;
use anyhow::anyhow;
use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::{Arc, Weak},
};
use tokio::sync::{oneshot, RwLock};

use crate::{
    changes::{EntityChange, ItemChange},
    connector::PeerSender,
    error::RetrievalError,
    model::{Record, RecordInner},
    reactor::Reactor,
    resultset::ResultSet,
    storage::{Bucket, StorageEngine},
    subscription::SubscriptionHandle,
    transaction::Transaction,
};
use tracing::{debug, info};

pub struct PeerState {
    sender: Box<dyn PeerSender>,
    durable: bool,
    subscriptions: BTreeMap<proto::SubscriptionId, Vec<SubscriptionHandle>>,
}

pub struct FetchArgs {
    pub predicate: ankql::ast::Predicate,
    pub cached: bool,
}

impl TryInto<FetchArgs> for &str {
    type Error = ankql::error::ParseError;
    fn try_into(self) -> Result<FetchArgs, Self::Error> {
        Ok(FetchArgs {
            predicate: ankql::parser::parse_selection(self)?,
            cached: false,
        })
    }
}

/// Manager for all records and their properties on this client.
pub struct Node {
    pub id: proto::NodeId,
    pub durable: bool,
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
    peer_connections: tokio::sync::RwLock<BTreeMap<proto::NodeId, PeerState>>,
    pending_requests:
        tokio::sync::RwLock<BTreeMap<proto::RequestId, oneshot::Sender<proto::NodeResponseBody>>>,

    /// The reactor for handling subscriptions
    reactor: Arc<Reactor>,
}

type NodeRecords = BTreeMap<(proto::ID, String), Weak<RecordInner>>;

impl Node {
    pub fn new(engine: Arc<dyn StorageEngine>) -> Self {
        let reactor = Reactor::new(engine.clone());
        Self {
            id: proto::NodeId::new(),
            storage_engine: engine,
            collections: RwLock::new(BTreeMap::new()),
            records: Arc::new(RwLock::new(BTreeMap::new())),
            peer_connections: tokio::sync::RwLock::new(BTreeMap::new()),
            pending_requests: tokio::sync::RwLock::new(BTreeMap::new()),
            reactor,
            durable: false,
        }
    }
    pub fn new_durable(engine: Arc<dyn StorageEngine>) -> Self {
        let reactor = Reactor::new(engine.clone());
        Self {
            id: proto::NodeId::new(),
            storage_engine: engine,
            collections: RwLock::new(BTreeMap::new()),
            records: Arc::new(RwLock::new(BTreeMap::new())),
            peer_connections: tokio::sync::RwLock::new(BTreeMap::new()),
            pending_requests: tokio::sync::RwLock::new(BTreeMap::new()),
            reactor,
            durable: true,
        }
    }

    pub async fn register_peer(&self, presence: proto::Presence, sender: Box<dyn PeerSender>) {
        let mut peer_connections = self.peer_connections.write().await;
        peer_connections.insert(
            presence.node_id,
            PeerState {
                sender,
                durable: presence.durable,
                subscriptions: BTreeMap::new(),
            },
        );
        // TODO send hello message to the peer, including present head state for all relevant collections
    }
    pub async fn deregister_peer(&self, node_id: proto::NodeId) {
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
                .sender
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
        match message {
            proto::PeerMessage::Request(request) => {
                info!("{} {request}", self.id);
                // TODO: Should we spawn a task here and make handle_message synchronous?
                // I think this depends on how we want to handle timeouts.
                // I think we want timeouts to be handled by the node, not the connector,
                // which would lend itself to spawning a task here and making this function synchronous.

                // double check to make sure we have a connection to the peer based on the node id
                if let Some(sender) = {
                    let peer_connections = self.peer_connections.read().await;
                    peer_connections
                        .get(&request.from)
                        .map(|c| c.sender.cloned())
                } {
                    let from = request.from.clone();
                    let to = request.to.clone();
                    let request_id = request.id.clone();

                    let body = match self.handle_request(request).await {
                        Ok(result) => result,
                        Err(e) => proto::NodeResponseBody::Error(e.to_string()),
                    };
                    let _result = sender
                        .send_message(proto::PeerMessage::Response(proto::NodeResponse {
                            request_id,
                            from: self.id.clone(),
                            to: from,
                            body,
                        }))
                        .await;
                    // TODO - do something with this error. Should probably send it to a log file
                }
            }
            proto::PeerMessage::Response(response) => {
                info!("{} {response}", self.id);
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
                collection: bucket_name,
                predicate,
            } => {
                let states: Vec<_> = self
                    .storage_engine
                    .fetch_states(bucket_name, &predicate)
                    .await?
                    .into_iter()
                    .collect();
                Ok(proto::NodeResponseBody::Fetch(states))
            }
            proto::NodeRequestBody::Subscribe {
                collection: bucket_name,
                predicate,
            } => {
                self.handle_subscribe_request(request.from, bucket_name, predicate)
                    .await
            }
            proto::NodeRequestBody::Unsubscribe { subscription_id } => {
                // Remove and drop the subscription handle
                let mut peer_connections = self.peer_connections.write().await;
                if let Some(peer_state) = peer_connections.get_mut(&request.from) {
                    peer_state.subscriptions.remove(&subscription_id);
                }
                Ok(proto::NodeResponseBody::Success)
            }
        }
    }

    async fn handle_subscribe_request(
        self: &Arc<Self>,
        peer_id: proto::NodeId,
        bucket_name: String,
        predicate: ankql::ast::Predicate,
    ) -> anyhow::Result<proto::NodeResponseBody> {
        // First fetch initial state
        let states = self
            .storage_engine
            .fetch_states(bucket_name.clone(), &predicate)
            .await?;

        // Set up subscription that forwards changes to the peer
        let node = self.clone();
        let handle = {
            let peer_id = peer_id.clone();
            self.reactor
                .subscribe(&bucket_name, predicate, move |changeset| {
                    // When changes occur, send them to the peer as CommitEvents
                    let events: Vec<_> = changeset
                        .changes
                        .iter()
                        .flat_map(|change| match change {
                            ItemChange::Add {
                                events: updates, ..
                            }
                            | ItemChange::Update {
                                events: updates, ..
                            }
                            | ItemChange::Remove {
                                events: updates, ..
                            } => &updates[..],
                            ItemChange::Initial { .. } => &[],
                        })
                        .cloned()
                        .collect();

                    if !events.is_empty() {
                        let node = node.clone();
                        let peer_id = peer_id.clone();
                        tokio::spawn(async move {
                            let _ = node
                                .request(peer_id, proto::NodeRequestBody::CommitEvents(events))
                                .await;
                        });
                    }
                })
                .await?
        };

        let subscription_id = handle.id.clone();
        // Store the subscription handle
        let mut peer_connections = self.peer_connections.write().await;
        if let Some(peer_state) = peer_connections.get_mut(&peer_id) {
            peer_state
                .subscriptions
                .entry(handle.id)
                .or_insert_with(Vec::new)
                .push(handle);
        }

        Ok(proto::NodeResponseBody::Subscribe {
            initial: states,
            subscription_id,
        })
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
    // TODO: Fix this - arghhh async lifetimes
    // pub async fn trx<T, F, Fut>(self: &Arc<Self>, f: F) -> anyhow::Result<T>
    // where
    //     F: for<'a> FnOnce(&'a Transaction) -> Fut,
    //     Fut: std::future::Future<Output = anyhow::Result<T>>,
    // {
    //     let trx = self.begin();
    //     let result = f(&trx).await?;
    //     trx.commit().await?;
    //     Ok(result)
    // }

    pub async fn commit_events_local(
        self: &Arc<Self>,
        events: &Vec<proto::RecordEvent>,
    ) -> anyhow::Result<()> {
        let mut changes = Vec::new();

        // First apply events locally
        for record_event in events {
            // Apply record events to the Node's global records first.
            let record = self
                .fetch_record_inner(record_event.id, &record_event.bucket_name)
                .await?;

            record.apply_record_event(record_event)?;

            let record_state = record.to_record_state()?;
            // Push the state buffers to storage.
            self.bucket(record_event.bucket_name())
                .await
                .set_record(record_event.id(), &record_state)
                .await?;

            changes.push(EntityChange {
                record: record.clone(),
                events: vec![record_event.clone()],
            });
        }
        self.reactor.notify_change(changes);

        Ok(())
    }

    // /// Create a new record and notify subscribers
    // pub(crate) async fn create_record(
    //     self: &Arc<Self>,
    //     record: Arc<RecordInner>,
    // ) -> anyhow::Result<()> {
    //     // Store the record
    //     let record_state = record.to_record_state()?;
    //     self.bucket(record.bucket_name())
    //         .await
    //         .set_record(record.id(), &record_state)
    //         .await?;

    //     // Add to node's records
    //     let _ = self.add_record(&record).await;

    //     // Notify subscribers about the creation
    //     let change = RecordChange {
    //         record: record.clone(),
    //         updates: vec![], // The record was just created, so no updates yet
    //         kind: ChangeKind::Add,
    //     };
    //     let changeset = ChangeSet {
    //         changes: vec![change],
    //     };
    //     self.reactor.notify_change(changeset);

    //     Ok(())
    // }

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
    //         kind: ChangeKind::Remove,
    //     };
    //     let changeset = ChangeSet {
    //         changes: vec![change],
    //     };
    //     self.reactor.notify_change(changeset);

    //     Ok(())
    // }

    /// Update a record and notify subscribers
    // pub(crate) async fn update_record(
    //     self: &Arc<Self>,
    //     record: Arc<RecordInner>,
    //     events: Vec<proto::RecordEvent>,
    // ) -> anyhow::Result<()> {
    //     // Store the record
    //     let record_state = record.to_record_state()?;
    //     self.bucket(record.bucket_name())
    //         .await
    //         .set_record(record.id(), &record_state)
    //         .await?;

    //     // Notify subscribers
    //     let change = RecordChange {
    //         record: record.clone(),
    //         updates: events,
    //         kind: ChangeKind::Edit,
    //     };
    //     let changeset = ChangeSet {
    //         changes: vec![change],
    //     };
    //     self.reactor.notify_change(changeset);

    //     Ok(())
    // }

    /// Apply events to local state buffer and broadcast to peers.
    pub async fn commit_events(
        self: &Arc<Self>,
        events: &Vec<proto::RecordEvent>,
    ) -> anyhow::Result<()> {
        self.commit_events_local(events).await?;

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

        // Send commit request to all peers
        for peer_id in peer_ids {
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

    /// Register a RecordInner with the node, with the intention of preventing duplicate resident records.
    /// Returns true if the record was already present.
    /// Note that this does not actually store the record in the storage engine.
    #[must_use]
    pub(crate) async fn assert_record(
        &self,
        collection: &str,
        id: proto::ID,
        state: &proto::RecordState,
    ) -> Result<Arc<RecordInner>, RetrievalError> {
        let mut records = self.records.write().await;

        match records.entry((id, collection.to_string())) {
            Entry::Occupied(mut entry) => {
                if let Some(record) = entry.get().upgrade() {
                    record.apply_record_state(state)?;
                    Ok(record)
                } else {
                    let record = Arc::new(RecordInner::from_record_state(id, collection, state)?);
                    entry.insert(Arc::downgrade(&record));
                    Ok(record)
                }
            }
            Entry::Vacant(entry) => {
                let record = Arc::new(RecordInner::from_record_state(id, collection, state)?);
                entry.insert(Arc::downgrade(&record));
                Ok(record)
            }
        }
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
    // pub(crate) async fn fetch_record_from_storage(
    //     &self,
    //     id: proto::ID,
    //     bucket_name: &str,
    // ) -> Result<RecordInner, RetrievalError> {
    //     match self.get_record_state(id, bucket_name).await {
    //         Ok(record_state) => {
    //             // info!("fetched record state: {:?}", record_state);
    //             RecordInner::from_record_state(id, bucket_name, &record_state)
    //         }
    //         Err(RetrievalError::NotFound(id)) => {
    //             // info!("ID not found, creating new record");
    //             Ok(RecordInner::new(id, bucket_name.to_string()))
    //         }
    //         Err(err) => Err(err),
    //     }
    // }

    /// Fetch a record.
    pub async fn fetch_record_inner(
        &self,
        id: proto::ID,
        bucket_name: &str,
    ) -> Result<Arc<RecordInner>, RetrievalError> {
        debug!("fetch_record {:?}-{:?}", id, bucket_name);

        if let Some(local) = self.fetch_record_from_node(id, bucket_name).await {
            return Ok(local);
        }
        debug!("fetch_record 2");

        let raw_bucket = self.bucket(bucket_name).await;
        match raw_bucket.0.get_record(id).await {
            Ok(record_state) => {
                return Ok(self.assert_record(bucket_name, id, &record_state).await?);
            }
            Err(RetrievalError::NotFound(id)) => {
                // let scoped_record = RecordInner::new(id, bucket_name.to_string());
                // let ref_record = Arc::new(scoped_record);
                // Revisit this
                let record = self
                    .assert_record(bucket_name, id, &proto::RecordState::default())
                    .await?;
                Ok(record)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_record<R: Record>(&self, id: proto::ID) -> Result<R, RetrievalError> {
        use crate::model::Model;
        let collection_name = R::Model::bucket_name();
        let record_inner = self.fetch_record_inner(id, collection_name).await?;
        Ok(R::from_record_inner(record_inner))
    }

    /// Fetch records from the first available durable peer.
    async fn fetch_from_peer(
        self: &Arc<Self>,
        collection_name: &str,
        predicate: &ankql::ast::Predicate,
    ) -> anyhow::Result<(), RetrievalError> {
        // Get first durable peer
        let peer_id = {
            self.peer_connections
                .read()
                .await
                .iter()
                .find(|(_, state)| state.durable)
                .map(|(id, _)| id.clone())
                .ok_or(RetrievalError::NoDurablePeers)?
        };

        match self
            .request(
                peer_id.clone(),
                proto::NodeRequestBody::FetchRecords {
                    collection: collection_name.to_string(),
                    predicate: predicate.clone(),
                },
            )
            .await
            .map_err(|e| RetrievalError::Other(format!("{:?}", e)))?
        {
            proto::NodeResponseBody::Fetch(states) => {
                let raw_bucket = self.bucket(collection_name).await;
                // do we have the ability to merge states?
                // because that's what we have to do I think
                for (id, state) in states {
                    raw_bucket
                        .0
                        .set_record(id, &state)
                        .await
                        .map_err(|e| RetrievalError::Other(format!("{:?}", e)))?;
                }
                Ok(())
            }
            proto::NodeResponseBody::Error(e) => {
                debug!("Error from peer fetch: {}", e);
                Err(RetrievalError::Other(format!("{:?}", e)))
            }
            _ => {
                debug!("Unexpected response type from peer fetch");
                Err(RetrievalError::Other(
                    "Unexpected response type".to_string(),
                ))
            }
        }
    }

    pub async fn fetch<R: Record>(
        self: &Arc<Self>,
        args: impl TryInto<FetchArgs>,
    ) -> anyhow::Result<ResultSet<R>> {
        let args = args
            .try_into()
            .map_err(|_| anyhow!("Failed to parse predicate:"))?;

        let predicate = args.predicate;

        use crate::model::Model;
        let collection_name = R::Model::bucket_name();

        if !self.durable {
            // Fetch from peers and commit first response
            match self.fetch_from_peer(collection_name, &predicate).await {
                Ok(_) => (),
                Err(RetrievalError::NoDurablePeers) if args.cached => (),
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        // Fetch raw states from storage
        let states = self
            .storage_engine
            .fetch_states(collection_name.to_string(), &predicate)
            .await?;

        // Convert states to records
        let mut records = Vec::new();
        for (id, state) in states {
            let record = self.assert_record(collection_name, id, &state).await?;
            records.push(R::from_record_inner(record));
        }

        Ok(ResultSet { records })
    }

    /// Subscribe to changes in records matching a predicate
    pub async fn subscribe<F, P>(
        self: &Arc<Self>,
        bucket_name: &str,
        predicate: P,
        callback: F,
    ) -> anyhow::Result<crate::subscription::SubscriptionHandle>
    where
        F: Fn(crate::changes::ChangeSet) + Send + Sync + 'static,
        P: TryInto<ankql::ast::Predicate>,
        P::Error: std::error::Error + Send + Sync + 'static,
    {
        let predicate = predicate
            .try_into()
            .map_err(|_e| anyhow!("Failed to parse predicate:"))?;

        // First, find any durable nodes to subscribe to
        let durable_peer_id = {
            let peer_connections = self.peer_connections.read().await;
            peer_connections
                .iter()
                .find(|(_, state)| state.durable)
                .map(|(id, _)| id.clone())
        };

        // If we have a durable node, send a subscription request to it
        if let Some(peer_id) = durable_peer_id {
            match self
                .request(
                    peer_id,
                    proto::NodeRequestBody::Subscribe {
                        collection: bucket_name.to_string(),
                        predicate: predicate.clone(),
                    },
                )
                .await?
            {
                proto::NodeResponseBody::Subscribe {
                    initial,
                    subscription_id: _,
                } => {
                    // Apply initial states to our storage
                    let raw_bucket = self.bucket(bucket_name).await;
                    for (id, state) in initial {
                        raw_bucket
                            .set_record(id, &state)
                            .await
                            .map_err(|e| anyhow!("Failed to set record: {:?}", e))?;
                    }
                }
                proto::NodeResponseBody::Error(e) => {
                    return Err(anyhow!("Error from peer subscription: {}", e));
                }
                _ => {
                    return Err(anyhow!("Unexpected response type from peer subscription"));
                }
            }
        }

        // Now set up our local subscription
        self.reactor
            .subscribe(bucket_name, predicate, callback)
            .await
    }
}
