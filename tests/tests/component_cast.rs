//! A cast between views of one entity: `task.cast::<AssignmentView>()`.

mod component_fixture;
use component_fixture::*;

use ankurah::entity::Entity;
use ankurah::error::RetrievalError;
use ankurah::View;

/// `task.cast::<AssignmentView>()`, reached through the view's deref to its entity, sees the same entity
/// through another component it has, and fails with `MissingComponent` for one it lacks.
#[tokio::test]
async fn cast_sees_the_entity_through_another_component_it_has() -> anyhow::Result<()> {
    let f = Fixture::new().await?;
    let task = f.ctx.get::<TaskView>(f.id("ada")).await?;
    let assignment: AssignmentView = task.cast()?;
    assert_eq!(assignment.assignee()?, "ada");
    assert_eq!(task.entity(), assignment.entity(), "the cast view holds the same entity");

    let closure_model = f.ctx.resolve_model_id::<Closure>().await?;
    let missing = task.cast::<ClosureView>();
    assert!(
        matches!(missing, Err(RetrievalError::MissingComponent { entity_id, model_id }) if entity_id == task.id() && model_id == closure_model),
        "{missing:?}"
    );

    let entity: &Entity = assignment.entity();
    let back: TaskView = entity.cast()?;
    assert_eq!(back.title()?, "ship");
    Ok(())
}
