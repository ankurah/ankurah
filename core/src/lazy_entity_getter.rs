//! Lazy, single-entity lookup for policy checks: a check receives a getter
//! bound to one entity and fetches the row only if its verdict turns on row
//! content, so verdicts that never look at the row never pay a read.

use crate::{
    entity::Entity,
    error::RetrievalError,
    node::Node,
    policy::PolicyAgent,
    retrieval::{LocalEventGetter, LocalStateGetter},
    storage::StorageEngine,
};
use ankurah_proto::{CollectionId, EntityId};

/// Fetches one preordained entity, on demand. Which entity is fixed at
/// construction; [`Self::get`] takes nothing and fetches it, so a check that
/// is handed one of these cannot be pointed at any other row, and a check
/// that never calls `get` costs nothing. Construct via
/// [`Node::entity_getter`](crate::node::Node::entity_getter).
pub struct EntityGetter<'a, SE, PA>
where PA: PolicyAgent
{
    entity_id: EntityId,
    collection: CollectionId,
    node: &'a Node<SE, PA>,
}

impl<'a, SE, PA> EntityGetter<'a, SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub(crate) fn new(node: &'a Node<SE, PA>, entity_id: EntityId, collection: CollectionId) -> Self {
        Self { entity_id, collection, node }
    }

    /// The entity this getter is bound to.
    pub fn id(&self) -> EntityId { self.entity_id }

    /// The entity as it currently stands, or `None` when nothing current
    /// exists for it. The resident entity when the node holds one, otherwise
    /// materialized from stored state into the node's entity set, which is
    /// what makes repeated calls not repeat the lookup.
    pub async fn get(&self) -> Result<Option<Entity>, RetrievalError> {
        let collection = self.node.collections.get(&self.collection).await?;
        let state_getter = LocalStateGetter::new(collection.clone());
        let event_getter = LocalEventGetter::new(collection, self.node.durable);
        let Some(entity) = self.node.entities.get_or_retrieve(&state_getter, &event_getter, &self.collection, &self.entity_id).await?
        else {
            return Ok(None);
        };
        // An id can only name one entity, but nothing above checked which
        // collection that entity belongs to; a row from another collection is
        // not this getter's row.
        if entity.collection() != &self.collection {
            return Ok(None);
        }
        Ok(Some(entity))
    }
}
