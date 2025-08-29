use ankurah_proto::{self as proto, CollectionId};

pub struct SubscriptionHandler {}

impl SubscriptionHandler {
    pub fn new() -> Self { Self {} }
    pub fn peer_connected(&self, peer_id: proto::EntityId) {}
    pub fn peer_disconnected(&self, peer_id: proto::EntityId) {}
    pub fn peer_subscribe(&self /*...*/) {}
    pub fn peer_unsubscribe(&self, peer_id: proto::EntityId, predicate_id: proto::PredicateId) {}
}
