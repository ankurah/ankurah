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
    pub(in crate::node) sender: Box<dyn PeerSender>,
    _durable: bool,
    pub(in crate::node) subscription_handler: SubscriptionHandler<CD>,
    pub(in crate::node) pending_requests: SafeMap<proto::RequestId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
    pub(in crate::node) pending_updates: SafeMap<proto::UpdateId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
}

impl<CD: ContextData> PeerState<CD> {
    pub(in crate::node) fn new(sender: Box<dyn PeerSender>, durable: bool, subscription_handler: SubscriptionHandler<CD>) -> Self {
        Self { sender, _durable: durable, subscription_handler, pending_requests: SafeMap::new(), pending_updates: SafeMap::new() }
    }

    pub fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> { self.sender.send_message(message) }
}
