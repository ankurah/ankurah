use crate::{
    connector::{PeerSender, SendError},
    error::RequestError,
    internal::prelude::*,
    peer_subscription::SubscriptionHandler,
    session::ContextData,
    util::safemap::SafeMap,
};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::oneshot;

pub struct PeerState<CD: ContextData> {
    pub(super) sender: Box<dyn PeerSender>,
    _durable: bool,
    pub(super) subscription_handler: SubscriptionHandler<CD>,
    pub(super) pending_requests: SafeMap<proto::RequestId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
    pub(super) pending_updates: SafeMap<proto::UpdateId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
    pub(super) fetches: FetchesInFlight<CD>,
}

impl<CD: ContextData> PeerState<CD> {
    pub(super) fn new(sender: Box<dyn PeerSender>, durable: bool, subscription_handler: SubscriptionHandler<CD>) -> Self {
        Self {
            sender,
            _durable: durable,
            subscription_handler,
            pending_requests: SafeMap::new(),
            pending_updates: SafeMap::new(),
            fetches: FetchesInFlight::default(),
        }
    }

    pub fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> { self.sender.send_message(message) }
}

/// A request's slot in its peer's pending requests, freed when the request
/// stops waiting, so that a request whose waiter gave up, as a timed-out
/// comparison does, leaves nothing behind for a peer that never answers.
pub(super) struct PendingRequest<CD: ContextData> {
    connection: Weak<PeerState<CD>>,
    id: proto::RequestId,
}

impl<CD: ContextData> PendingRequest<CD> {
    pub(super) fn new(connection: &Arc<PeerState<CD>>, id: proto::RequestId) -> Self { Self { connection: Arc::downgrade(connection), id } }
}

impl<CD: ContextData> Drop for PendingRequest<CD> {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.upgrade() {
            connection.pending_requests.remove(&self.id);
        }
    }
}

/// The Fetches this node has in flight with one peer, each with the credentials
/// it carried. While one is, the applier admits the peer's pushes, under those
/// credentials as well as its subscriptions'. Kept with the connection, so no
/// registration outlives the connection its Fetch was sent on.
pub(crate) struct FetchesInFlight<CD: ContextData>(Arc<Mutex<Vec<Arc<Vec<CD>>>>>);

impl<CD: ContextData> Default for FetchesInFlight<CD> {
    fn default() -> Self { Self(Arc::default()) }
}

impl<CD: ContextData> FetchesInFlight<CD> {
    /// Register a Fetch carrying `credentials`, before its request is sent.
    pub(crate) fn register(&self, credentials: Vec<CD>) -> FetchInFlight<CD> {
        let credentials = Arc::new(credentials);
        self.0.lock().unwrap().push(credentials.clone());
        FetchInFlight { fetches: self.0.clone(), credentials }
    }

    /// The credentials of every Fetch in flight, or none when no Fetch is.
    pub(crate) fn credentials(&self) -> Option<HashSet<CD>> {
        let fetches = self.0.lock().unwrap();
        (!fetches.is_empty()).then(|| fetches.iter().flat_map(|credentials| credentials.iter().cloned()).collect())
    }
}

/// One Fetch's place in [`FetchesInFlight`], from before its request is sent
/// until the Fetch is ready or fails.
pub(crate) struct FetchInFlight<CD: ContextData> {
    fetches: Arc<Mutex<Vec<Arc<Vec<CD>>>>>,
    credentials: Arc<Vec<CD>>,
}

impl<CD: ContextData> Drop for FetchInFlight<CD> {
    fn drop(&mut self) { self.fetches.lock().unwrap().retain(|credentials| !Arc::ptr_eq(credentials, &self.credentials)); }
}
