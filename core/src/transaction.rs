use ankurah_proto as proto;
use std::sync::Arc;

use crate::{
    context::TContext,
    error::RetrievalError,
    model::{Entity, Mutable},
    Model,
};

use append_only_vec::AppendOnlyVec;

#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

// Q. When do we want unified vs individual property storage for TypeEngine operations?
// A. When we start to care about differentiating possible recipients for different properties.

#[cfg_attr(feature = "wasm", wasm_bindgen)]
pub struct Transaction {
    pub(crate) dyncontext: Box<dyn TContext + Send + Sync + 'static>,

    entities: AppendOnlyVec<Arc<Entity>>,

    // markers
    implicit: bool,
    consumed: bool,
}

#[cfg(feature = "wasm")]
#[wasm_bindgen]
impl Transaction {
    #[wasm_bindgen(js_name = "commit")]
    pub async fn js_commit(mut self) -> Result<(), JsValue> { self.commit_mut_ref().await.map_err(|e| JsValue::from_str(&e.to_string())) }
}

impl Transaction {
    pub(crate) fn new(dyncontext: Box<dyn TContext + Send + Sync + 'static>) -> Self {
        Self { dyncontext, entities: AppendOnlyVec::new(), implicit: true, consumed: false }
    }

    /// Fetch an entity already in the transaction.
    async fn get_entity(&self, id: proto::ID, collection: &proto::CollectionId) -> Result<&Arc<Entity>, RetrievalError> {
        if let Some(entity) = self.entities.iter().find(|entity| entity.id == id && entity.collection == *collection) {
            return Ok(entity);
        }

        let upstream = self.dyncontext.get_entity(id, collection).await?;
        Ok(self.add_entity(upstream.snapshot()))
    }

    fn add_entity(&self, entity: Arc<Entity>) -> &Arc<Entity> {
        let index = self.entities.push(entity);
        &self.entities[index]
    }

    pub async fn create<'rec, 'trx: 'rec, M: Model>(&'trx self, model: &M) -> M::Mutable<'rec> {
        let id = self.dyncontext.next_entity_id();
        let new_entity = Arc::new(model.create_entity(id));
        let entity_ref = self.add_entity(new_entity);
        <M::Mutable<'rec> as Mutable<'rec>>::new(entity_ref)
    }
    // TODO - get rid of this in favor of directly cloning the entity of the ModelView struct
    pub async fn edit<'rec, 'trx: 'rec, M: Model>(
        &'trx self,
        id: impl Into<proto::ID>,
    ) -> Result<M::Mutable<'rec>, crate::error::RetrievalError> {
        let id = id.into();
        let entity = self.get_entity(id, &M::collection()).await?;

        Ok(<M::Mutable<'rec> as Mutable<'rec>>::new(entity))
    }

    #[must_use]
    pub async fn commit(mut self) -> anyhow::Result<()> { self.commit_mut_ref().await }

    #[must_use]
    // only because Drop is &mut self not mut self
    pub(crate) async fn commit_mut_ref(&mut self) -> anyhow::Result<()> {
        tracing::debug!("trx.commit");
        self.consumed = true;
        // this should probably be done in parallel, but microoptimizations
        let mut entity_events = Vec::new();
        for entity in self.entities.iter() {
            if let Some(entity_event) = entity.commit()? {
                if let Some(upstream) = &entity.upstream {
                    upstream.apply_event(&entity_event)?;
                } else {
                    self.dyncontext.insert_entity(entity.clone()).await?;
                }
                entity_events.push(entity_event);
            }
        }
        self.dyncontext.commit_events(&entity_events).await?;

        Ok(())
    }

    pub fn rollback(mut self) {
        tracing::info!("trx.rollback");
        self.consumed = true; // just do nothing on drop
    }

    // TODO: Implement delete functionality after core query/edit operations are stable
    // For now, "removal" from result sets is handled by edits that cause entities to no longer match queries
    /*
    pub async fn delete<'rec, 'trx: 'rec, M: Model>(
        &'trx self,
        id: impl Into<ID>,
    ) -> Result<(), crate::error::RetrievalError> {
        let id = id.into();
        let entity = self.fetch_entity(id, M::collection()).await?;
        let entity = Arc::new(entity.clone());
        self.node.delete_entity(entity).await?;
        Ok(())
    }
    */
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.implicit && !self.consumed {
            // Since we can't make drop async, we'll need to block on the commit
            // This is not ideal, but necessary for the implicit transaction case
            // TODO: Make this a rollback, which will also mean we don't need use block_on
            // #[cfg(not(target_arch = "wasm32"))]
            // tokio::spawn(async move {
            //     self.commit_mut_ref()
            //         .await
            //         .expect("Failed to commit implicit transaction");
            // });

            // #[cfg(target_arch = "wasm32")]
            // wasm_bindgen_futures::spawn_local(async move {
            //     if let Err(e) = fut.await {
            //         tracing::error!("Failed to commit implicit transaction: {}", e);
            //     }
            // });
            // todo!()
        }
    }
}
