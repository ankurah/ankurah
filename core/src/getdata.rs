// //! Traits and impls for getting state and event data for specific IDs
// //! It's generic so we can use it for unit tests as well as the real implementation

// use crate::{context::NodeAndContext, entity::Entity, error::RetrievalError, policy::PolicyAgent, storage::StorageEngine};
// use ankurah_proto::{self as proto, Attested, CollectionId, EntityId, EntityState, Event, EventId};
// use async_trait::async_trait;

// // TODO: make this generic
// #[async_trait]
// pub trait GetState {
//     // Each implementation of Retrieve determines whether to use local or remote storage
//     async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError>;

//     /// Estimate the budget cost for retrieving a batch of events
//     /// This allows different implementations to model their cost structure
//     fn estimate_cost(&self, _batch_size: usize) -> usize {
//         // Default implementation: fixed cost of 1 per batch
//         1
//     }
// }

// #[async_trait]
// pub trait GetEntities {
//     async fn get_entities(&self, entity_ids: Vec<EntityId>) -> Result<Vec<Entity>, RetrievalError>;
// }

// #[async_trait]
// impl<SE, PA> GetState for (proto::CollectionId, &NodeAndContext<SE, PA>)
// where
//     SE: StorageEngine + Send + Sync + 'static,
//     PA: PolicyAgent + Send + Sync + 'static,
// {
//     // this used to be called get_local_state
//     // and for some reason we have a use case that wants local or remote event fetching, but not the same for state

//     async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError> {
//         let collection = self.1.node.collections.get(&self.0).await?;
//         match collection.get_state(entity_id).await {
//             Ok(state) => Ok(Some(state)),
//             Err(RetrievalError::EntityNotFound(_)) => Ok(None),
//             Err(e) => Err(e),
//         }
//     }
// }
