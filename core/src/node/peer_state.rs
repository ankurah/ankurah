use crate::{
    connector::{PeerSender, SendError},
    error::RequestError,
    internal::prelude::*,
    peer_subscription::SubscriptionHandler,
    session::ContextData,
    util::safemap::SafeMap,
};
use tokio::sync::oneshot;

pub struct PeerState<CD: ContextData> {
    pub(super) sender: Box<dyn PeerSender>,
    pub(super) durable: bool,
    pub(super) subscription_handler: SubscriptionHandler<CD>,
    pub(super) comparisons: super::comparison::IncomingComparisons<CD>,
    pub(super) pending_requests: SafeMap<proto::RequestId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
    pub(super) pending_updates: SafeMap<proto::UpdateId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
}

impl<CD: ContextData> PeerState<CD> {
    pub(super) fn new(sender: Box<dyn PeerSender>, durable: bool, subscription_handler: SubscriptionHandler<CD>) -> Self {
        Self {
            sender,
            durable,
            subscription_handler,
            comparisons: Default::default(),
            pending_requests: SafeMap::new(),
            pending_updates: SafeMap::new(),
        }
    }

    pub(super) fn clear_pending(&self) {
        self.pending_requests.clear();
        self.pending_updates.clear();
        self.comparisons.clear();
    }

    pub fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> { self.sender.send_message(message) }
}

/// Pending request registration follows its waiting future, including cancellation.
pub(super) struct PendingRequest<CD: ContextData> {
    pub connection: std::sync::Weak<PeerState<CD>>,
    pub id: proto::RequestId,
}
impl<CD: ContextData> Drop for PendingRequest<CD> {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.upgrade() {
            connection.pending_requests.remove(&self.id);
        }
    }
}
