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
    pub(super) pending_updates: SafeMap<proto::UpdateId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
}

impl<CD: ContextData> PeerState<CD> {
    pub(super) fn new(sender: Box<dyn PeerSender>, durable: bool, subscription_handler: SubscriptionHandler<CD>) -> Self {
        Self { sender, _durable: durable, subscription_handler, pending_requests: SafeMap::new(), pending_updates: SafeMap::new() }
    }

    pub fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> { self.sender.send_message(message) }
}
