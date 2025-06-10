use ankurah_proto::{self as proto, Attested, Clock, CollectionId, EntityState};
use anyhow::anyhow;

use async_trait::async_trait;
use rand::prelude::*;
use std::{
    fmt,
    ops::Deref,
    sync::{Arc, Weak},
};
use tokio::sync::oneshot;

use crate::{
    action_debug, action_error, action_info, action_warn,
    changes::{ChangeSet, EntityChange, ItemChange},
    collectionset::CollectionSet,
    connector::{PeerSender, SendError},
    context::{Context, NodeAndContext},
    datagetter::{self, DataGetter, LocalGetter, NetworkGetter},
    entity::{Entity, EntityManager},
    error::{MutationError, RequestError, RetrievalError},
    notice_info,
    policy::{AccessDenied, PolicyAgent},
    reactor::Reactor,
    storage::StorageEngine,
    subscription::SubscriptionHandle,
    subscription_relay::SubscriptionRelay,
    system::SystemManager,
    task::spawn,
    util::{safemap::SafeMap, safeset::SafeSet},
};
#[cfg(feature = "instrument")]
use tracing::instrument;

use tracing::{debug, error, info, warn};

pub struct PeerState {
    sender: Box<dyn PeerSender>,
    _durable: bool,
    subscriptions: SafeSet<proto::SubscriptionId>,
    pending_requests: SafeMap<proto::RequestId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
    pending_updates: SafeMap<proto::UpdateId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
}

impl PeerState {
    pub fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> { self.sender.send_message(message) }
}

pub struct MatchArgs {
    pub predicate: ankql::ast::Predicate,
    pub cached: bool,
}

impl TryInto<MatchArgs> for &str {
    type Error = ankql::error::ParseError;
    fn try_into(self) -> Result<MatchArgs, Self::Error> {
        Ok(MatchArgs { predicate: ankql::parser::parse_selection(self)?, cached: false })
    }
}
impl TryInto<MatchArgs> for String {
    type Error = ankql::error::ParseError;
    fn try_into(self) -> Result<MatchArgs, Self::Error> {
        Ok(MatchArgs { predicate: ankql::parser::parse_selection(&self)?, cached: false })
    }
}

impl From<ankql::ast::Predicate> for MatchArgs {
    fn from(val: ankql::ast::Predicate) -> Self { MatchArgs { predicate: val, cached: false } }
}

impl From<ankql::error::ParseError> for RetrievalError {
    fn from(e: ankql::error::ParseError) -> Self { RetrievalError::ParseError(e) }
}

/// A participant in the Ankurah network, and primary place where queries are initiated

pub struct Node<SE, PA, D>(pub(crate) Arc<NodeInner<SE, PA, D>>)
where
    PA: PolicyAgent,
    D: DataGetter<PA::ContextData> + Send + Sync + 'static;
impl<SE, PA, D> Clone for Node<SE, PA, D>
where
    PA: PolicyAgent,
    D: DataGetter<PA::ContextData> + Send + Sync + 'static,
{
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

pub struct WeakNode<SE, PA, D>(Weak<NodeInner<SE, PA, D>>)
where
    PA: PolicyAgent,
    D: DataGetter<PA::ContextData> + Send + Sync + 'static;
impl<SE, PA, D> Clone for WeakNode<SE, PA, D>
where
    PA: PolicyAgent,
    D: DataGetter<PA::ContextData> + Send + Sync + 'static,
{
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<SE, PA, D> WeakNode<SE, PA, D>
where
    PA: PolicyAgent,
    D: DataGetter<PA::ContextData> + Send + Sync + 'static,
{
    pub fn upgrade(&self) -> Option<Node<SE, PA, D>> { self.0.upgrade().map(Node) }
}

impl<SE, PA, D> Deref for Node<SE, PA, D>
where
    PA: PolicyAgent,
    D: DataGetter<PA::ContextData> + Send + Sync + 'static,
{
    type Target = Arc<NodeInner<SE, PA, D>>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

/// Represents the user session - or whatever other context the PolicyAgent
/// Needs to perform it's evaluation.
#[async_trait]
pub trait ContextData: Send + Sync + Clone + 'static {}

pub struct NodeInner<SE, PA, DG>
where
    PA: PolicyAgent,
    DG: DataGetter<PA::ContextData> + Send + Sync + 'static,
{
    pub id: proto::EntityId,
    pub durable: bool,
    pub collections: CollectionSet<SE>,

    pub(crate) entities: EntityManager<SE, PA, DG>,
    peer_connections: SafeMap<proto::EntityId, Arc<PeerState>>,
    durable_peers: SafeSet<proto::EntityId>,

    pub(crate) subscription_context: SafeMap<proto::SubscriptionId, PA::ContextData>,

    /// The reactor for handling subscriptions
    pub(crate) reactor: Arc<Reactor<SE, PA>>,
    pub(crate) policy_agent: PA,
    pub system: SystemManager<SE, PA, DG>,

    pub(crate) subscription_relay: Option<SubscriptionRelay<Entity, PA::ContextData>>,
}

impl<SE, PA, DG> Node<SE, PA, DG>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    DG: DataGetter<PA::ContextData> + Send + Sync + 'static,
{
    pub fn new(engine: Arc<SE>, policy_agent: PA) -> Node<SE, PA, NetworkGetter<SE, PA>> {
        let collections = CollectionSet::new(engine);
        // Create NetworkDataBroker for ephemeral nodes (can access both local and remote)
        let network_broker = crate::datagetter::NetworkGetter::new(collections.clone());

        Self::new_internal(collections, network_broker, policy_agent, false, true)
    }

    pub fn new_durable(engine: Arc<SE>, policy_agent: PA) -> Node<SE, PA, LocalGetter<SE>> {
        let collections = CollectionSet::new(engine);
        // Create LocalDataBroker for durable nodes (local storage only)
        let local_broker = Arc::new(LocalGetter::new(collections.clone()));

        Self::new_internal(collections, local_broker, policy_agent, true, false)
    }

    fn new_internal<DB>(
        collections: CollectionSet<SE>,
        data_getter: DB,
        policy_agent: PA,
        durable: bool,
        needs_subscription_relay: bool,
    ) -> Node<SE, PA, DB>
    where
        DB: DataGetter<PA::ContextData> + Send + Sync + 'static,
    {
        let reactor = Reactor::new(collections.clone(), policy_agent.clone());
        let entityset = EntityManager::new(data_getter.clone(), collections.clone(), reactor.clone(), policy_agent.clone());
        let id = proto::EntityId::new();

        let system_manager = SystemManager::new(collections.clone(), entityset.clone(), reactor.clone(), durable);

        // Create subscription relay for ephemeral nodes only
        let subscription_relay = if needs_subscription_relay { Some(SubscriptionRelay::new(reactor.clone())) } else { None };

        let node = Node(Arc::new(NodeInner {
            id,
            collections,
            entities: entityset,
            peer_connections: SafeMap::new(),
            durable_peers: SafeSet::new(),
            reactor,
            durable,
            policy_agent,
            system: system_manager,
            subscription_context: SafeMap::new(),
            subscription_relay,
        }));

        // Bind the node to both policy agent and data broker
        data_getter.bind_node(&node);
        node.policy_agent.bind_node(&node);
        node.subscription_relay.as_ref().map(|relay| relay.bind_node(&node));

        let node_type = if durable { "durable" } else { "ephemeral" };
        notice_info!("Node {id:#} created as {node_type}");

        node
    }
    pub fn weak(&self) -> WeakNode<SE, PA, DG> { WeakNode(Arc::downgrade(&self.0)) }

    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(node_id = %presence.node_id.to_base64_short(), durable = %presence.durable)))]
    pub fn register_peer(&self, presence: proto::Presence, sender: Box<dyn PeerSender>) {
        action_info!(self, "register_peer", "{}", &presence);

        self.peer_connections.insert(
            presence.node_id,
            Arc::new(PeerState {
                sender,
                _durable: presence.durable,
                subscriptions: SafeSet::new(),
                pending_requests: SafeMap::new(),
                pending_updates: SafeMap::new(),
            }),
        );
        if presence.durable {
            self.durable_peers.insert(presence.node_id);

            // Notify subscription relay of new durable peer connection
            if let Some(ref relay) = self.subscription_relay {
                relay.notify_peer_connected(presence.node_id);
            }

            if !self.durable {
                if let Some(system_root) = presence.system_root {
                    action_info!(self, "received system root", "{}", &system_root.payload);
                    let me = self.clone();
                    crate::task::spawn(async move {
                        if let Err(e) = me.system.join_system(system_root).await {
                            action_error!(me, "failed to join system", "{}", &e);
                        } else {
                            action_info!(me, "successfully joined system");
                        }
                    });
                } else {
                    error!("Node({}) durable peer {} has no system root", self.id, presence.node_id);
                }
            }
        }
        // TODO send hello message to the peer, including present head state for all relevant collections
    }
    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(node_id = %node_id.to_base64_short())))]
    pub fn deregister_peer(&self, node_id: proto::EntityId) {
        notice_info!("Node({:#}) deregister_peer {:#}", self.id, node_id);

        // Get and cleanup subscriptions before removing the peer
        if let Some(peer_state) = self.peer_connections.get(&node_id) {
            // Get all subscription IDs
            let subscriptions = peer_state.subscriptions.to_vec();

            // Unsubscribe each one from the reactor
            for sub_id in subscriptions {
                action_info!(self, "unsubscribing", "subscription {} for peer {}", sub_id, node_id);
                self.reactor.unsubscribe(sub_id);
            }
        }

        // Notify subscription relay of peer disconnection (unconditional - relay handles filtering)
        if let Some(ref relay) = self.subscription_relay {
            relay.notify_peer_disconnected(node_id);
        }

        // Remove the peer connection and durable status
        self.peer_connections.remove(&node_id);
        self.durable_peers.remove(&node_id);
    }
    #[cfg_attr(feature = "instrument", instrument(skip_all, fields(node_id = %node_id, request_body = %request_body)))]
    pub async fn request(
        &self,
        node_id: proto::EntityId,
        cdata: &PA::ContextData,
        request_body: proto::NodeRequestBody,
    ) -> Result<proto::NodeResponseBody, RequestError> {
        let (response_tx, response_rx) = oneshot::channel::<Result<proto::NodeResponseBody, RequestError>>();
        let request_id = proto::RequestId::new();

        let request = proto::NodeRequest { id: request_id.clone(), to: node_id, from: self.id, body: request_body };
        let auth = self.policy_agent.sign_request(cdata, &request);

        // Get the peer connection
        let connection = self.peer_connections.get(&node_id).ok_or(RequestError::PeerNotConnected)?;

        connection.pending_requests.insert(request_id, response_tx);
        connection.send_message(proto::NodeMessage::Request { auth, request })?;

        // Wait for response
        response_rx.await.map_err(|_| RequestError::InternalChannelClosed)?
    }

    // TODO LATER: rework this to be retried in the background some number of times
    pub fn send_update(&self, node_id: proto::EntityId, notification: proto::NodeUpdateBody) {
        // same as request, minus cdata and the sign_request step
        debug!("{self}.send_update({node_id:#}, {notification})");
        let (response_tx, _response_rx) = oneshot::channel::<Result<proto::NodeResponseBody, RequestError>>();
        let id = proto::UpdateId::new();

        // Get the peer connection
        let Some(connection) = self.peer_connections.get(&node_id) else {
            warn!("Failed to send update to peer {}: {}", node_id, RequestError::PeerNotConnected);
            return;
        };

        // Store the response channel
        connection.pending_updates.insert(id.clone(), response_tx);

        let notification = proto::NodeMessage::Update(proto::NodeUpdate { id, from: self.id, to: node_id, body: notification });

        match connection.send_message(notification) {
            Ok(_) => {}
            Err(e) => {
                warn!("Failed to send update to peer {}: {}", node_id, e);
            }
        };

        // response_rx.await.map_err(|_| RequestError::InternalChannelClosed)??;
    }

    // TODO add a node id argument to this function rather than getting it from the message
    // (does this actually make it more secure? or just move the place they could lie to us to the handshake?)
    // Not if its signed by a node key.
    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(message = %message)))]
    pub async fn handle_message(&self, message: proto::NodeMessage) -> anyhow::Result<()> {
        match message {
            proto::NodeMessage::Update(update) => {
                debug!("Node({}) received update {}", self.id, update);

                if let Some(sender) = { self.peer_connections.get(&update.from).map(|c| c.sender.cloned()) } {
                    let _from = update.from;
                    let _id = update.id.clone();
                    if update.to != self.id {
                        warn!("{} received message from {} but is not the intended recipient", self.id, update.from);
                        return Ok(());
                    }

                    // take down the return address
                    let id = update.id.clone();
                    let to = update.from;
                    let from = self.id;

                    // TODO - validate the from node id is the one we're connected to
                    let body = match self.handle_update(update).await {
                        Ok(_) => proto::NodeUpdateAckBody::Success,
                        Err(e) => proto::NodeUpdateAckBody::Error(e.to_string()),
                    };

                    sender.send_message(proto::NodeMessage::UpdateAck(proto::NodeUpdateAck { id, from, to, body }))?;
                }
            }
            proto::NodeMessage::UpdateAck(ack) => {
                debug!("Node({}) received ack notification {} {}", self.id, ack.id, ack.body);
                // let connection = self.peer_connections.get(&ack.from).ok_or(RequestError::PeerNotConnected)?;
                // if let Some(tx) = connection.pending_updates.remove(&ack.id) {
                //     tx.send(Ok(proto::NodeResponseBody::Success)).unwrap();
                // }
            }
            proto::NodeMessage::Request { auth, request } => {
                debug!("Node({}) received request {}", self.id, request);
                // TODO: Should we spawn a task here and make handle_message synchronous?
                // I think this depends on how we want to handle timeouts.
                // I think we want timeouts to be handled by the node, not the connector,
                // which would lend itself to spawning a task here and making this function synchronous.

                let cdata = self.policy_agent.check_request(&auth, &request).await?;

                // double check to make sure we have a connection to the peer based on the node id
                if let Some(sender) = { self.peer_connections.get(&request.from).map(|c| c.sender.cloned()) } {
                    let from = request.from;
                    let request_id = request.id.clone();
                    if request.to != self.id {
                        warn!("{} received message from {} but is not the intended recipient", self.id, request.from);
                        return Ok(());
                    }

                    let body = match self.handle_request(&cdata, request).await {
                        Ok(result) => result,
                        Err(e) => proto::NodeResponseBody::Error(e.to_string()),
                    };
                    let _result = sender.send_message(proto::NodeMessage::Response(proto::NodeResponse {
                        request_id,
                        from: self.id,
                        to: from,
                        body,
                    }));
                }
            }
            proto::NodeMessage::Response(response) => {
                debug!("Node {} received response {}", self.id, response);
                let connection = self.peer_connections.get(&response.from).ok_or(RequestError::PeerNotConnected)?;
                if let Some(tx) = connection.pending_requests.remove(&response.request_id) {
                    tx.send(Ok(response.body)).map_err(|e| anyhow!("Failed to send response: {:?}", e))?;
                }
            }
            proto::NodeMessage::Unsubscribe { from, subscription_id } => {
                self.reactor.unsubscribe(subscription_id);
                // Remove and drop the subscription handle
                if let Some(peer_state) = self.peer_connections.get(&from) {
                    peer_state.subscriptions.remove(&subscription_id);
                }
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(request = %request)))]
    async fn handle_request(&self, cdata: &PA::ContextData, request: proto::NodeRequest) -> anyhow::Result<proto::NodeResponseBody> {
        match request.body {
            proto::NodeRequestBody::CommitTransaction { id, events } => {
                // TODO - relay to peers in a gossipy/resource-available manner, so as to improve propagation
                // With moderate potential for duplication, while not creating message loops
                // Doing so would be a secondary/tertiary/etc hop for this message
                match self.commit_remote_transaction(cdata, id.clone(), events).await {
                    Ok(_) => Ok(proto::NodeResponseBody::CommitComplete { id }),
                    Err(e) => Ok(proto::NodeResponseBody::Error(e.to_string())),
                }
            }
            proto::NodeRequestBody::Fetch { collection, predicate } => {
                self.policy_agent.can_access_collection(cdata, &collection)?;
                let storage_collection = self.collections.get(&collection).await?;
                let predicate = self.policy_agent.filter_predicate(cdata, &collection, predicate)?;

                let mut states = Vec::new();
                for state in storage_collection.fetch_states(&predicate).await? {
                    if self.policy_agent.check_read(cdata, &state.payload.entity_id, &collection, &state.payload.state).is_ok() {
                        states.push(state);
                    }
                }
                Ok(proto::NodeResponseBody::Fetch(states))
            }
            proto::NodeRequestBody::Get { collection, ids } => {
                self.policy_agent.can_access_collection(cdata, &collection)?;
                let storage_collection = self.collections.get(&collection).await?;

                // filter out any that the policy agent says we don't have access to
                let mut states = Vec::new();
                for state in storage_collection.get_states(ids).await? {
                    match self.policy_agent.check_read(cdata, &state.payload.entity_id, &collection, &state.payload.state) {
                        Ok(_) => states.push(state),
                        Err(AccessDenied::ByPolicy(_)) => {}
                        // TODO: we need to have a cleaner delineation between actual access denied versus processing errors
                        Err(e) => return Err(anyhow!("Error from peer get: {}", e)),
                    }
                }

                Ok(proto::NodeResponseBody::Get(states))
            }
            proto::NodeRequestBody::GetEvents { collection, event_ids } => {
                self.policy_agent.can_access_collection(cdata, &collection)?;
                let storage_collection = self.collections.get(&collection).await?;

                // filter out any that the policy agent says we don't have access to
                let mut events = Vec::new();
                for event in storage_collection.get_events(event_ids).await? {
                    match self.policy_agent.check_read_event(cdata, &event.1) {
                        Ok(_) => events.push(event),
                        Err(AccessDenied::ByPolicy(_)) => {}
                        // TODO: we need to have a cleaner delineation between actual access denied versus processing errors
                        Err(e) => return Err(anyhow!("Error from peer subscription: {}", e)),
                    }
                }

                Ok(proto::NodeResponseBody::GetEvents(events.into_iter().map(|(_, event)| event).collect()))
            }
            proto::NodeRequestBody::Subscribe { subscription_id, collection, predicate } => {
                self.handle_subscribe_request(cdata, request.from, subscription_id, collection, predicate).await
            }
        }
    }

    async fn handle_update(&self, notification: proto::NodeUpdate) -> anyhow::Result<()> {
        let Some(_connection) = self.peer_connections.get(&notification.from) else {
            return Err(anyhow!("Rejected notification from unknown node {}", notification.from));
        };

        match notification.body {
            proto::NodeUpdateBody::SubscriptionUpdate { subscription_id, items, initial } => {
                // TODO check if this is a valid subscription
                action_debug!(self, "received subscription update for {} items", "{}", items.len());
                if let Some(cdata) = self.subscription_context.get(&subscription_id) {
                    let nodeandcontext = NodeAndContext { node: self.clone(), cdata };

                    self.apply_subscription_updates(&notification.from, subscription_id, items, nodeandcontext, initial).await?;
                } else {
                    error!("Received subscription update for unknown subscription {}", subscription_id);
                    return Err(anyhow!("Received subscription update for unknown subscription {}", subscription_id));
                }

                Ok(())
            }
        }
    }

    pub(crate) async fn relay_to_required_peers(
        &self,
        cdata: &PA::ContextData,
        id: proto::TransactionId,
        events: &[Attested<proto::Event>],
    ) -> Result<(), MutationError> {
        // TODO determine how many durable peers need to respond before we can proceed. The others should continue in the background.
        // as of this writing, we only have one durable peer, so we can just await the response from "all" of them
        for peer_id in self.get_durable_peers() {
            match self.request(peer_id, cdata, proto::NodeRequestBody::CommitTransaction { id: id.clone(), events: events.to_vec() }).await
            {
                Ok(proto::NodeResponseBody::CommitComplete { .. }) => (),
                Ok(proto::NodeResponseBody::Error(e)) => {
                    return Err(MutationError::General(Box::new(std::io::Error::other(format!("Peer {} rejected: {}", peer_id, e)))));
                }
                _ => {
                    return Err(MutationError::General(Box::new(std::io::Error::other(format!(
                        "Peer {} returned unexpected response",
                        peer_id
                    )))));
                }
            }
        }
        Ok(())
    }

    /// Does all the things necessary to commit a remote transaction
    pub async fn commit_remote_transaction(
        &self,
        cdata: &PA::ContextData,
        id: proto::TransactionId,
        mut events: Vec<Attested<proto::Event>>,
    ) -> Result<(), MutationError> {
        debug!("{self} commiting transaction {id} with {} events", events.len());

        // Use LocalDataBroker to ensure we only consider already-local state when applying
        let getter = LocalGetter::new(self.collections.clone());
        self.entities.apply_events_with_getter(cdata, events.into_iter(), getter).await?;

        Ok(())
    }

    // Similar to commit_transaction, except that we check event attestations instead of checking write permissions
    // we also don't need to fan events out to peers because we're receiving them from a peer
    pub async fn apply_subscription_updates(
        &self,
        from_peer_id: &proto::EntityId,
        subscription_id: proto::SubscriptionId,
        updates: Vec<proto::SubscriptionUpdateItem>,
        nodeandcontext: NodeAndContext<SE, PA, DG>,
        initial: bool,
    ) -> Result<(), MutationError> {
        let mut initial_entity_ids = Vec::new();
        let mut processed_updates = Vec::new();

        // Process each update, validate, and prepare for batch application
        for update in updates {
            // Collect entity IDs if this is initial data
            if initial {
                initial_entity_ids.push(update.entity_id());
            }

            let (entity_id, collection_id, state, event) = update.into_parts();

            // Validate and prepare event if present
            let attested_event = match event {
                Some(event) => {
                    // validate and store the events, in case we need them for lineage comparison
                    let event = (entity_id, collection_id.clone(), event.clone()).into();
                    self.policy_agent.validate_received_event(from_peer_id, &event)?;
                    // Note: We don't store the event here as apply_events_and_states will handle it
                    Some(event)
                }
                None => None,
            };

            // Validate and prepare state if present
            let attested_state = match state {
                Some(state) => {
                    let state = (entity_id, collection_id.clone(), state).into();
                    // validate that we trust the state given to us
                    self.policy_agent.validate_received_state(from_peer_id, &state)?;
                    Some(state)
                }
                None => None,
            };

            // Add to batch for processing
            processed_updates.push((attested_event, attested_state));
        }

        // Apply all updates in a single batch with unified reactor notification
        self.entities.apply_events_and_states(processed_updates, &nodeandcontext.cdata).await?;

        // Handle subscription relay notifications
        if initial {
            if let Some(relay) = self.subscription_relay.as_ref() {
                relay.notify_applied_initial_state(subscription_id, initial_entity_ids).await?;
            }
        }

        Ok(())
    }

    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(peer_id = %peer_id.to_base64_short(), sub_id = %sub_id, collection_id = %collection_id, predicate = %predicate)))]
    async fn handle_subscribe_request(
        &self,
        cdata: &PA::ContextData,
        peer_id: proto::EntityId,
        sub_id: proto::SubscriptionId,
        collection_id: CollectionId,
        predicate: ankql::ast::Predicate,
    ) -> anyhow::Result<proto::NodeResponseBody> {
        self.policy_agent.can_access_collection(cdata, &collection_id)?;
        let predicate = self.policy_agent.filter_predicate(cdata, &collection_id, predicate)?;

        // Set up subscription that forwards changes to the peer
        let node = self.clone();
        {
            let peer_id = peer_id;

            // Create the subscription with callback (synchronous)
            let subscription = crate::subscription::Subscription::new(
                sub_id,
                collection_id.clone(),
                predicate,
                Arc::new(Box::new(move |changeset: ChangeSet<Entity>| {
                    // TODO move this into a task being fed by a channel and reorg into a function
                    let mut updates: Vec<proto::SubscriptionUpdateItem> = Vec::new();

                    // When changes occur, collect events and states
                    for change in changeset.changes.into_iter() {
                        match change {
                            ItemChange::Initial { item } => {
                                // For initial state, include both events and state
                                if let Ok(es) = item.to_entity_state() {
                                    let attestation = node.policy_agent.attest_state(&es);

                                    updates.push(proto::SubscriptionUpdateItem::initial(
                                        item.id(),
                                        item.collection.clone(),
                                        Attested::opt(es, attestation),
                                    ));
                                }
                            }
                            ItemChange::Add { item, event } => {
                                // For entities which were not previously matched, state AND events should be included
                                // but it's weird because EntityState and Event redundantly hinclude entity_id and collection
                                // But we want to Attest events independently, and we need to attest State - but we can't just attest a naked state,
                                // it has to have the entity_id

                                let state = match item.to_state() {
                                    Ok(state) => state,
                                    Err(e) => {
                                        warn!("Node {} entity {} state experienced an error - {}", node.id, item.id, e);
                                        continue;
                                    }
                                };

                                let es = EntityState { entity_id: item.id, collection: item.collection.clone(), state };
                                let attestation = node.policy_agent.attest_state(&es);

                                updates.push(proto::SubscriptionUpdateItem::add(
                                    item.id,
                                    item.collection.clone(),
                                    Attested::opt(es, attestation),
                                    event,
                                ));
                            }
                            ItemChange::Update { item, event } | ItemChange::Remove { item, event } => {
                                updates.push(proto::SubscriptionUpdateItem::change(item.id, item.collection.clone(), event));
                            }
                        }
                    }

                    // Always send subscription update, even if empty
                    node.send_update(
                        peer_id,
                        proto::NodeUpdateBody::SubscriptionUpdate { subscription_id: sub_id, items: updates, initial: changeset.initial },
                    );
                })),
            );

            match &self.subscription_relay {
                None => {
                    let fetcher = LocalGetter::new(self.collections.clone());
                    self.reactor.register(subscription, fetcher)?;
                }
                Some(_relay) => {
                    warn!("subscribe requests with relay are not supported");
                    return Err(anyhow!("subscribe requests with relay are not supported"));
                    // DO WE ACTUALLY WANT TO HANDLE THIS CASE?
                    // I'm not certain we do.

                    // Register with subscription relay and get the oneshot receiver
                    // let remote_ready_rx = relay.register(subscription.clone(), cdata.clone())?;

                    // let retriever = RemoteEntityFetcher::new(
                    //     self.collections.clone(),
                    //     self.entities.clone(),
                    //     Arc::new(relay),
                    //     cdata.clone(),
                    //     Some(remote_ready_rx),
                    //     false, // For subscriptions, we don't use cache mode - we wait for remote data
                    // );

                    // self.reactor.register(subscription, retriever)?;
                }
            }
        };

        // Store the subscription handle
        if let Some(peer_state) = self.peer_connections.get(&peer_id) {
            peer_state.subscriptions.insert(sub_id);
        }

        Ok(proto::NodeResponseBody::Subscribed { subscription_id: sub_id })
    }

    pub fn next_entity_id(&self) -> proto::EntityId { proto::EntityId::new() }

    pub fn context(&self, data: PA::ContextData) -> Result<Context, anyhow::Error> {
        if !self.system.is_system_ready() {
            return Err(anyhow!("System is not ready"));
        }
        Ok(Context::new(Node::clone(self), data))
    }

    pub async fn context_async(&self, data: PA::ContextData) -> Context {
        self.system.wait_system_ready().await;
        Context::new(Node::clone(self), data)
    }

    /// Get a random durable peer node ID
    pub fn get_durable_peer_random(&self) -> Option<proto::EntityId> {
        let mut rng = rand::thread_rng();
        // Convert to Vec since DashSet iterator doesn't support random selection
        let peers: Vec<_> = self.durable_peers.to_vec();
        peers.choose(&mut rng).copied()
    }

    /// Get all durable peer node IDs
    pub fn get_durable_peers(&self) -> Vec<proto::EntityId> { self.durable_peers.to_vec() }
}

impl<SE, PA, D> NodeInner<SE, PA, D>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    D: DataGetter<PA::ContextData> + Send + Sync + 'static,
{
    pub async fn request_remote_unsubscribe(&self, sub_id: proto::SubscriptionId, peers: Vec<proto::EntityId>) -> anyhow::Result<()> {
        for (peer_id, item) in self.peer_connections.get_list(peers) {
            if let Some(connection) = item {
                let sub_id = sub_id;
                connection.send_message(proto::NodeMessage::Unsubscribe { from: peer_id, subscription_id: sub_id })?;
            } else {
                warn!("Peer {} not connected", peer_id);
            }
        }

        Ok(())
    }

    pub fn unsubscribe(self: &Arc<Self>, handle: &SubscriptionHandle) -> anyhow::Result<()> {
        let node = Node(self.clone());
        let sub_id = handle.id;
        spawn(async move {
            // Clean up subscription context
            node.subscription_context.remove(&sub_id);

            // Unsubscribe from local reactor
            node.reactor.unsubscribe(sub_id);

            // Notify subscription relay for remote cleanup
            if let Some(ref relay) = node.subscription_relay {
                relay.notify_unsubscribe(sub_id);
            }
        });
        Ok(())
    }
}

impl<SE, PA, D> Drop for NodeInner<SE, PA, D>
where
    PA: PolicyAgent,
    D: DataGetter<PA::ContextData> + Send + Sync + 'static,
{
    fn drop(&mut self) {
        notice_info!("Node({}) dropped", self.id);
    }
}

pub trait TNodeErased: Send + Sync + 'static {
    fn unsubscribe(&self, handle: &SubscriptionHandle);
}

impl<SE, PA, D> TNodeErased for Node<SE, PA, D>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    D: DataGetter<PA::ContextData> + Send + Sync + 'static,
{
    fn unsubscribe(&self, handle: &SubscriptionHandle) { let _ = self.0.unsubscribe(handle); }
}

impl<SE, PA, D> fmt::Display for Node<SE, PA, D>
where
    PA: PolicyAgent,
    D: DataGetter<PA::ContextData> + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // bold blue, dimmed brackets
        write!(f, "\x1b[1;34mnode\x1b[2m[\x1b[1;34m{}\x1b[2m]\x1b[0m", self.id.to_base64_short())
    }
}
