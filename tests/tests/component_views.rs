//! Views of one entity's several components: a cast between views, `task.cast::<AssignmentView>()`.
//! Each scenario runs on sled and on in-memory SQLite.

use ankurah::core::storage::StorageEngine;
use ankurah::entity::{Entity, LocalTrxEntity};
use ankurah::error::RetrievalError;
use ankurah::policy::DEFAULT_CONTEXT;
use ankurah::property::PropertyError;
use ankurah::{Context, EntityId, Model, ModelId, Mutable, Node, PermissiveAgent, View};
use ankurah_storage_sled::SledStorageEngine;
use ankurah_storage_sqlite::SqliteStorageEngine;
use std::sync::Arc;

/// The `project` property Task and Assignment share. An explicit id must name a property the catalog
/// already holds, so each node is started on storage where a forged Task catalog minted it.
const PROJECT: &str = "W1bfWXTa_DqEotxEr8dFtSdzk0GmEQv_ls3KXyxBujg";

#[derive(Model, Debug, Clone)]
pub struct Task {
    pub status: String,
    pub title: String,
    #[active_type(LWW)]
    #[property(id = "W1bfWXTa_DqEotxEr8dFtSdzk0GmEQv_ls3KXyxBujg")]
    pub project: String,
    /// The same name as `Assignment::note`, but a different property.
    pub note: String,
}

#[derive(Model, Debug, Clone)]
pub struct Assignment {
    pub assignee: String,
    #[active_type(LWW)]
    #[property(id = "W1bfWXTa_DqEotxEr8dFtSdzk0GmEQv_ls3KXyxBujg")]
    pub project: String,
    pub note: String,
}

#[derive(Model, Debug, Clone)]
#[model(label = "records.closure")]
pub struct Closure {
    pub closed_at: i64,
}

fn task(status: &str, title: &str) -> Task {
    Task { status: status.into(), title: title.into(), project: "atlas".into(), note: format!("task note {title}") }
}

fn assignment(assignee: &str) -> Assignment {
    Assignment { assignee: assignee.into(), project: "atlas".into(), note: format!("assignment note {assignee}") }
}

async fn on_sled() -> anyhow::Result<Fixture> { Fixture::build("sled", SledStorageEngine::new_test()?).await }

async fn on_sqlite() -> anyhow::Result<Fixture> { Fixture::build("sqlite", SqliteStorageEngine::open_in_memory().await?).await }

/// One durable node holding these entities, each named for the assertions:
/// - `ada`: an open Task, assigned to ada.
/// - `bob`: an open Task, assigned to bob.
/// - `done`: a done Task, assigned to ada, with a Closure.
/// - `unassigned`: an open Task with no Assignment, in project zeus.
/// - `stray`: an Assignment to ada that is not a Task.
struct Fixture {
    engine: &'static str,
    ctx: Context,
    names: Vec<(EntityId, &'static str)>,
}

impl Fixture {
    async fn build<SE: StorageEngine + Send + Sync + 'static>(engine: &'static str, storage: SE) -> anyhow::Result<Self> {
        let forged = ankurah_tests::catalog_forge::forge_catalog("task", "Task", &[("project", "lww", "string")], b"component-views");
        assert_eq!(forged.properties[0].to_base64(), PROJECT);
        ankurah_tests::catalog_forge::plant(&storage, &forged).await?;
        let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
        node.system.create().await?;
        node.wait_ready().await?;
        let ctx = node.context_async(DEFAULT_CONTEXT).await?;
        ctx.resolve_model_id::<Closure>().await?;

        let mut fixture = Self { engine, ctx, names: Vec::new() };
        fixture.create("ada", task("open", "ship"), Some(assignment("ada")), None).await?;
        fixture.create("bob", task("open", "review"), Some(assignment("bob")), None).await?;
        fixture.create("done", task("done", "archive"), Some(assignment("ada")), Some(Closure { closed_at: 5 })).await?;
        let mut zeus = task("open", "triage");
        zeus.project = "zeus".into();
        fixture.create("unassigned", zeus, None, None).await?;
        let trx = fixture.ctx.begin();
        let stray = trx.create(&assignment("ada")).await?.id();
        trx.commit().await?;
        fixture.names.push((stray, "stray"));
        Ok(fixture)
    }

    /// Create one entity with each given component and its values, in one transaction: `Transaction::create`
    /// stages the Task, and `Model::initialize_new_entity` each further component (no typed call takes several).
    async fn create(
        &mut self,
        name: &'static str,
        task: Task,
        assignment: Option<Assignment>,
        closure: Option<Closure>,
    ) -> anyhow::Result<()> {
        let trx = self.ctx.begin();
        let record = trx.create(&task).await?;
        if let Some(assignment) = assignment {
            stage(record.entity(), &assignment, self.ctx.resolve_model_id::<Assignment>().await?)?;
        }
        if let Some(closure) = closure {
            stage(record.entity(), &closure, self.ctx.resolve_model_id::<Closure>().await?)?;
        }
        // Asking for the id freezes the genesis, so every component is staged first.
        self.names.push((record.id(), name));
        trx.commit().await?;
        Ok(())
    }

    fn id(&self, name: &str) -> EntityId { self.names.iter().find(|(_, known)| *known == name).expect("a fixture name").0 }
}

fn stage<M: Model>(entity: &LocalTrxEntity, component: &M, model: ModelId) -> Result<(), PropertyError> {
    component.initialize_new_entity(entity, model, entity.system_epoch())
}

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
