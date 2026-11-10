use super::ConnectionRegistration;
use crate::{
    connector::{PeerSender, SendError},
    error::RequestError,
    internal::prelude::*,
    peer_subscription::SubscriptionHandler,
    policy::PeerTrust,
    session::ContextData,
    util::safemap::SafeMap,
};
use tokio::sync::oneshot;

pub struct PeerState<CD: ContextData> {
    pub(in crate::node) registration: ConnectionRegistration,
    pub(in crate::node) sender: Box<dyn PeerSender>,
    pub(in crate::node) trust: PeerTrust,
    pub(in crate::node) subscription_handler: SubscriptionHandler<CD>,
    pub(in crate::node) pending_requests: SafeMap<proto::RequestId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
    pub(in crate::node) pending_updates: SafeMap<proto::UpdateId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
}

impl<CD: ContextData> PeerState<CD> {
    pub(super) fn new(
        registration: ConnectionRegistration,
        sender: Box<dyn PeerSender>,
        trust: PeerTrust,
        subscription_handler: SubscriptionHandler<CD>,
    ) -> Self {
        Self { registration, sender, trust, subscription_handler, pending_requests: SafeMap::new(), pending_updates: SafeMap::new() }
    }

    pub fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> { self.sender.send_message(message) }
}
