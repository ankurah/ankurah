pub mod diff_resolver;

pub trait OnFirstSubscriptionUpdate {
    fn on_first_update(&self, entity_ids: Vec<EntityId>);
}

impl OnFirstSubscriptionUpdate for () {
    fn on_first_update(&self, _entity_ids: Vec<EntityId>) {}
}
