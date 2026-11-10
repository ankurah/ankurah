//! A cast between views of one entity: `task.cast::<AssignmentView>()`.

mod component_fixture;
use component_fixture::*;

use ankurah::entity::Entity;
use ankurah::error::RetrievalError;
use ankurah::View;

async fn cast_between_views(f: Fixture) -> anyhow::Result<()> {
    let engine = f.engine;
    let task = f.ctx.get::<TaskView>(f.id("ada")).await?;
    let assignment: AssignmentView = task.cast()?;
    assert_eq!(assignment.assignee()?, "ada", "{engine}");
    assert_eq!(task.entity(), assignment.entity(), "{engine}: the cast view holds the same entity");

    let closure_model = f.ctx.resolve_model_id::<Closure>().await?;
    let missing = task.cast::<ClosureView>();
    assert!(
        matches!(missing, Err(RetrievalError::MissingComponent { entity_id, model_id }) if entity_id == task.id() && model_id == closure_model),
        "{engine}: {missing:?}"
    );

    let entity: &Entity = assignment.entity();
    let back: TaskView = entity.cast()?;
    assert_eq!(back.title()?, "ship", "{engine}");
    Ok(())
}

/// `task.cast::<AssignmentView>()`, reached through the view's deref to its entity, sees the same entity
/// through another component it has, and fails with `MissingComponent` for one it lacks.
#[tokio::test]
async fn cast_sees_the_entity_through_another_component_it_has() -> anyhow::Result<()> {
    cast_between_views(on_sled().await?).await?;
    cast_between_views(on_sqlite().await?).await
}
