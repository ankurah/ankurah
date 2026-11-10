//! The components, entities and nodes the component-view tests share: Task and Assignment, which share
//! `project` and each declare their own `note`; Closure, outside every tuple the tests query; and Card and
//! Pin, whose names make a dotted path read two ways. Each scenario runs on sled and on in-memory SQLite.
// Each test file uses part of this fixture.
#![allow(dead_code)]

use ankurah::core::storage::StorageEngine;
use ankurah::entity::LocalTrxEntity;
use ankurah::error::{ComponentScopeError, RetrievalError};
use ankurah::policy::DEFAULT_CONTEXT;
use ankurah::property::{Json, PropertyError};
use ankurah::{Context, EntityId, Model, ModelId, Mutable, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use ankurah_storage_sqlite::SqliteStorageEngine;
use std::sync::Arc;

/// The `project` property Task and Assignment share. An explicit id must name a property the catalog
/// already holds, so each node is started on storage where a forged Task catalog minted it.
pub const PROJECT: &str = "W1bfWXTa_DqEotxEr8dFtSdzk0GmEQv_ls3KXyxBujg";

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

/// A component whose JSON property `pin` shares its name with the label of the `Pin` component.
#[derive(Model, Debug, Clone)]
pub struct Card {
    pub pin: Json,
}

#[derive(Model, Debug, Clone)]
pub struct Pin {
    pub owner: String,
}

pub fn task(status: &str, title: &str) -> Task {
    Task { status: status.into(), title: title.into(), project: "atlas".into(), note: format!("task note {title}") }
}

pub fn assignment(assignee: &str) -> Assignment {
    Assignment { assignee: assignee.into(), project: "atlas".into(), note: format!("assignment note {assignee}") }
}

pub async fn on_sled() -> anyhow::Result<Fixture> { Fixture::build("sled", SledStorageEngine::new_test()?).await }

pub async fn on_sqlite() -> anyhow::Result<Fixture> { Fixture::build("sqlite", SqliteStorageEngine::open_in_memory().await?).await }

/// One durable node holding these entities, each named for the assertions:
/// - `ada`: an open Task, assigned to ada.
/// - `bob`: an open Task, assigned to bob.
/// - `done`: a done Task, assigned to ada, with a Closure.
/// - `unassigned`: an open Task with no Assignment, in project zeus.
/// - `stray`: an Assignment to ada that is not a Task.
pub struct Fixture {
    pub engine: &'static str,
    pub ctx: Context,
    pub names: Vec<(EntityId, &'static str)>,
}

impl Fixture {
    pub async fn build<SE: StorageEngine + Send + Sync + 'static>(engine: &'static str, storage: SE) -> anyhow::Result<Self> {
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
    pub async fn create(
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

    pub fn id(&self, name: &str) -> EntityId { self.names.iter().find(|(_, known)| *known == name).expect("a fixture name").0 }

    pub fn name(&self, id: EntityId) -> &'static str { self.names.iter().find(|(known, _)| *known == id).expect("a fixture entity").1 }

    /// Fixture names of `ids`, in the order given.
    pub fn ordered(&self, ids: impl IntoIterator<Item = EntityId>) -> Vec<&'static str> {
        ids.into_iter().map(|id| self.name(id)).collect()
    }

    /// Fixture names of `ids`, sorted, for results whose order is not part of the query.
    pub fn names(&self, ids: impl IntoIterator<Item = EntityId>) -> Vec<&'static str> {
        let mut names = self.ordered(ids);
        names.sort();
        names
    }

    pub async fn fetch_pairs(&self, selection: &str) -> anyhow::Result<Vec<&'static str>> {
        let pairs = self.ctx.fetch::<(TaskView, AssignmentView)>(selection).await?;
        Ok(self.names(pairs.iter().map(|(task, _)| task.id())))
    }

    pub async fn fetch_pairs_in_order(&self, selection: &str) -> anyhow::Result<Vec<&'static str>> {
        let pairs = self.ctx.fetch::<(TaskView, AssignmentView)>(selection).await?;
        Ok(self.ordered(pairs.iter().map(|(task, _)| task.id())))
    }

    /// The component-scope error a pair fetch refuses `selection` with.
    pub async fn refusal(&self, selection: &str) -> ComponentScopeError {
        match self.ctx.fetch::<(TaskView, AssignmentView)>(selection).await {
            Err(RetrievalError::ComponentScope(error)) => error,
            Err(other) => panic!("{}: `{selection}` failed with {other:?}", self.engine),
            Ok(pairs) => panic!("{}: `{selection}` returned {} pairs", self.engine, pairs.len()),
        }
    }

    /// Commit one change to a component's property on an existing entity.
    pub async fn edit<M: Model>(&self, name: &str, change: impl FnOnce(&M::Mutable) -> anyhow::Result<()>) -> anyhow::Result<()> {
        let trx = self.ctx.begin();
        let record = trx.get::<M>(&self.id(name)).await?;
        change(&record)?;
        trx.commit().await?;
        Ok(())
    }

    /// Commit a further component, with its values, on an existing entity.
    pub async fn add<M: Model>(&self, name: &str, component: &M) -> anyhow::Result<()> {
        let model = self.ctx.resolve_model_id::<M>().await?;
        let trx = self.ctx.begin();
        let record = trx.get::<Task>(&self.id(name)).await?;
        stage(record.entity(), component, model)?;
        trx.commit().await?;
        Ok(())
    }
}

pub fn stage<M: Model>(entity: &LocalTrxEntity, component: &M, model: ModelId) -> Result<(), PropertyError> {
    component.initialize_new_entity(entity, model, entity.system_epoch())
}
