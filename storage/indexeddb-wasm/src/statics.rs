// Cache the property keys so we only have to create the js values once
use crate::object::Property;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref ID_KEY: Property = Property::new("id"); // Special case - no prefix
    pub static ref HEAD_KEY: Property = Property::new("__head");
    pub static ref COLLECTION_KEY: Property = Property::new("__collection");
    pub static ref STATE_BUFFER_KEY: Property = Property::new("__state_buffer");
    pub static ref ENTITY_ID_KEY: Property = Property::new("__entity_id");
    pub static ref OPERATIONS_KEY: Property = Property::new("__operations");
    pub static ref ATTESTATIONS_KEY: Property = Property::new("__attestations");
    pub static ref PARENT_KEY: Property = Property::new("__parent");
}
