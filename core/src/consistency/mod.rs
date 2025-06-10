// pub mod diff_resolver;

// use ankurah_proto::EntityId;

// /// Marker trait for consistency resolution mechanisms
// pub trait ConsistencyResolver: Send + Sync + 'static {}

// /// Trait for receiving notification when the first subscription update arrives from a remote peer
// pub trait OnFirstSubscriptionUpdate: Send + Sync + 'static {
//     /// Called when the first subscription update arrives with the list of entity IDs
//     fn on_first_update(&self, entity_ids: Vec<EntityId>);
// }

// impl OnFirstSubscriptionUpdate for () {
//     fn on_first_update(&self, _entity_ids: Vec<EntityId>) {}
// }
