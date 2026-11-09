//! A node-level case each engine runs: a membership removed in a later update
//! leaves the model's live query, its fetches and its typed reads, while the
//! entity stays readable through the membership it keeps.

use ankurah::{core::error::RetrievalError, policy::DEFAULT_CONTEXT, storage::StorageEngine, Model, Mutable, Node, PermissiveAgent, View};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Shelf {
    pub label: String,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Featured {
    pub label: String,
}

pub async fn check_live_removal<SE: StorageEngine + Send + Sync + 'static>(engine: SE) -> anyhow::Result<()> {
    let node = Node::new_durable(Arc::new(engine), PermissiveAgent::new());
    let context = node.context_async(DEFAULT_CONTEXT).await?;
    let featured_model = context.resolve_model_id::<Featured>().await?;
    let featured = context.query_wait::<FeaturedView>("true").await?;

    let transaction = context.begin();
    let shelf = transaction.create(&Shelf { label: "kept".into() }).await?;
    shelf.entity().add_membership(featured_model)?;
    let id = shelf.id();
    drop(shelf);
    transaction.commit().await?;
    assert_eq!(featured.ids(), vec![id]);

    let transaction = context.begin();
    transaction.get::<Shelf>(&id).await?.entity().remove_membership(&featured_model)?;
    transaction.commit().await?;

    assert!(featured.ids().is_empty(), "the live query observes the removal");
    assert!(context.fetch::<FeaturedView>("true").await?.is_empty(), "a fetch of the model no longer finds it");
    assert!(matches!(
        context.get::<FeaturedView>(id).await,
        Err(RetrievalError::MissingComponent { entity_id, model_id }) if entity_id == id && model_id == featured_model
    ));
    assert_eq!(context.get::<ShelfView>(id).await?.label()?, "kept");
    assert_eq!(context.fetch::<ShelfView>("true").await?.iter().map(View::id).collect::<Vec<_>>(), vec![id]);
    Ok(())
}
