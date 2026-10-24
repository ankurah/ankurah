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
    _durable: bool,
    pub(super) subscription_handler: SubscriptionHandler<CD>,
    pub(super) pending_requests: SafeMap<proto::RequestId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
    pub(crate) initial_state_stream: tokio::sync::Mutex<()>,
    pub(crate) state_streams: super::state_sync::StateStreams<CD>,
    pub(crate) outgoing_state_streams: std::sync::Arc<SafeMap<proto::StateStreamId, bool>>,
    pub(crate) incoming_states: std::sync::Arc<super::state_sync::IncomingStates>,
}

impl<CD: ContextData> PeerState<CD> {
    pub(super) fn new(sender: Box<dyn PeerSender>, durable: bool, subscription_handler: SubscriptionHandler<CD>) -> Self {
        Self {
            sender,
            _durable: durable,
            subscription_handler,
            pending_requests: SafeMap::new(),
            initial_state_stream: tokio::sync::Mutex::new(()),
            state_streams: Default::default(),
            outgoing_state_streams: Default::default(),
            incoming_states: Default::default(),
        }
    }

    pub fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> { self.sender.send_message(message) }
}

/// Remove a request waiter even when its caller is canceled or times out.
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
