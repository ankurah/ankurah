//! Views of one entity's several components: a typed query over a tuple of views,
//! `ctx.fetch::<(TaskView, AssignmentView)>(..)`, and a cast between views, `task.cast::<AssignmentView>()`.
//! A query's matches belong to every component in its tuple; bare names resolve across those components,
//! and `Component::name` names a property of one component, in the tuple or outside it.
//! Each scenario runs on sled and on in-memory SQLite.

use ankurah::core::storage::StorageEngine;
use ankurah::entity::{Entity, LocalTrxEntity};
use ankurah::error::{ComponentScopeError, RetrievalError};
use ankurah::policy::DEFAULT_CONTEXT;
use ankurah::property::{Json, PropertyError};
use ankurah::signals::Peek;
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

/// A component whose JSON property `pin` shares its name with the label of the `Pin` component.
#[derive(Model, Debug, Clone)]
pub struct Card {
    pub pin: Json,
}

#[derive(Model, Debug, Clone)]
pub struct Pin {
    pub owner: String,
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

    fn name(&self, id: EntityId) -> &'static str { self.names.iter().find(|(known, _)| *known == id).expect("a fixture entity").1 }

    /// Fixture names of `ids`, in the order given.
    fn ordered(&self, ids: impl IntoIterator<Item = EntityId>) -> Vec<&'static str> { ids.into_iter().map(|id| self.name(id)).collect() }

    /// Fixture names of `ids`, sorted, for results whose order is not part of the query.
    fn names(&self, ids: impl IntoIterator<Item = EntityId>) -> Vec<&'static str> {
        let mut names = self.ordered(ids);
        names.sort();
        names
    }

    async fn fetch_pairs(&self, selection: &str) -> anyhow::Result<Vec<&'static str>> {
        let pairs = self.ctx.fetch::<(TaskView, AssignmentView)>(selection).await?;
        Ok(self.names(pairs.iter().map(|(task, _)| task.id())))
    }

    async fn fetch_pairs_in_order(&self, selection: &str) -> anyhow::Result<Vec<&'static str>> {
        let pairs = self.ctx.fetch::<(TaskView, AssignmentView)>(selection).await?;
        Ok(self.ordered(pairs.iter().map(|(task, _)| task.id())))
    }

    /// The component-scope error a pair fetch refuses `selection` with.
    async fn refusal(&self, selection: &str) -> ComponentScopeError {
        match self.ctx.fetch::<(TaskView, AssignmentView)>(selection).await {
            Err(RetrievalError::ComponentScope(error)) => error,
            Err(other) => panic!("{}: `{selection}` failed with {other:?}", self.engine),
            Ok(pairs) => panic!("{}: `{selection}` returned {} pairs", self.engine, pairs.len()),
        }
    }

    /// Commit one change to a component's property on an existing entity.
    async fn edit<M: Model>(&self, name: &str, change: impl FnOnce(&M::Mutable) -> anyhow::Result<()>) -> anyhow::Result<()> {
        let trx = self.ctx.begin();
        let record = trx.get::<M>(&self.id(name)).await?;
        change(&record)?;
        trx.commit().await?;
        Ok(())
    }

    /// Commit a further component, with its values, on an existing entity.
    async fn add<M: Model>(&self, name: &str, component: &M) -> anyhow::Result<()> {
        let model = self.ctx.resolve_model_id::<M>().await?;
        let trx = self.ctx.begin();
        let record = trx.get::<Task>(&self.id(name)).await?;
        stage(record.entity(), component, model)?;
        trx.commit().await?;
        Ok(())
    }
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

async fn filter_across_components(f: Fixture) -> anyhow::Result<()> {
    let engine = f.engine;
    let pairs: Vec<(TaskView, AssignmentView)> = f.ctx.fetch::<(TaskView, AssignmentView)>("status = 'open' AND assignee = 'ada'").await?;
    assert_eq!(f.names(pairs.iter().map(|(task, _)| task.id())), ["ada"], "{engine}");
    let (task, assignment) = &pairs[0];
    assert_eq!((task.title()?, assignment.assignee()?), ("ship".to_owned(), "ada".to_owned()), "{engine}");
    assert_eq!(task.entity(), assignment.entity(), "{engine}: both views hold the same entity");

    // Every component is required: the stray Assignment is no Task, and the unassigned Task has no Assignment.
    assert_eq!(f.fetch_pairs("assignee = 'ada'").await?, ["ada", "done"], "{engine}");
    assert_eq!(
        f.names(f.ctx.fetch::<AssignmentView>("assignee = 'ada'").await?.iter().map(View::id)),
        ["ada", "done", "stray"],
        "{engine}"
    );
    assert_eq!(f.fetch_pairs("status = 'open'").await?, ["ada", "bob"], "{engine}");

    let all_three = f.ctx.fetch::<(TaskView, AssignmentView, ClosureView)>("closed_at = 5 AND assignee = 'ada'").await?;
    assert_eq!(f.names(all_three.iter().map(|(task, _, _)| task.id())), ["done"], "{engine}");
    Ok(())
}

/// `status` is Task's and `assignee` is Assignment's; neither needs a qualifier in a tuple holding both.
#[tokio::test]
async fn bare_names_filter_across_the_components_of_a_tuple() -> anyhow::Result<()> {
    filter_across_components(on_sled().await?).await?;
    filter_across_components(on_sqlite().await?).await
}

async fn order_by_second_component(f: Fixture) -> anyhow::Result<()> {
    let engine = f.engine;
    assert_eq!(f.fetch_pairs_in_order("status = 'open' ORDER BY assignee").await?, ["ada", "bob"], "{engine}");
    assert_eq!(f.fetch_pairs_in_order("status = 'open' ORDER BY assignee DESC").await?, ["bob", "ada"], "{engine}");

    let live = f.ctx.query_wait::<(TaskView, AssignmentView)>("status = 'open' ORDER BY assignee").await?;
    assert_eq!(f.ordered(live.peek().iter().map(|(task, _)| task.id())), ["ada", "bob"], "{engine}");
    f.add("unassigned", &assignment("abe")).await?;
    assert_eq!(f.ordered(live.peek().iter().map(|(task, _)| task.id())), ["unassigned", "ada", "bob"], "{engine}");
    Ok(())
}

/// ORDER BY takes a property of the tuple's second component, in a fetch and in a live query, which places
/// an entity that comes to match by that property.
#[tokio::test]
async fn order_by_a_property_of_the_second_component() -> anyhow::Result<()> {
    order_by_second_component(on_sled().await?).await?;
    order_by_second_component(on_sqlite().await?).await
}

async fn shared_and_colliding_names(f: Fixture) -> anyhow::Result<()> {
    let engine = f.engine;
    // `project` is one property, declared by both components through the same explicit id.
    assert_eq!(f.fetch_pairs("project = 'atlas'").await?, ["ada", "bob", "done"], "{engine}");

    // `note` is two properties: Task's and Assignment's.
    let ambiguous = ComponentScopeError::Ambiguous { name: "note".into(), first: "Task".into(), second: "Assignment".into() };
    assert_eq!(
        ambiguous.to_string(),
        "'note' names different properties in components 'Task' and 'Assignment'; qualify it as Task::note or Assignment::note"
    );
    for selection in ["note = 'task note ship'", "status = 'open' ORDER BY note"] {
        assert_eq!(f.refusal(selection).await, ambiguous, "{engine}: {selection}");
    }
    let live = f.ctx.query_wait::<(TaskView, AssignmentView)>("note = 'task note ship'").await.map(|_| ());
    assert!(matches!(&live, Err(RetrievalError::ComponentScope(error)) if *error == ambiguous), "{engine}: {live:?}");

    // A one-view query reads its own component's `note`, which no other component in it shares.
    let tasks = f.ctx.fetch::<TaskView>("note = 'task note triage'").await?;
    assert_eq!(f.names(tasks.iter().map(View::id)), ["unassigned"], "{engine}");
    Ok(())
}

/// A name the two components declare as the same property resolves bare; a name they declare as different
/// properties is refused, naming both components.
#[tokio::test]
async fn a_shared_property_resolves_bare_and_a_colliding_name_is_refused() -> anyhow::Result<()> {
    shared_and_colliding_names(on_sled().await?).await?;
    shared_and_colliding_names(on_sqlite().await?).await
}

async fn live_tuple_query(f: Fixture) -> anyhow::Result<()> {
    let engine = f.engine;
    let open = f.ctx.query_wait::<(TaskView, AssignmentView)>("status = 'open'").await?;
    let open_to_ada = f.ctx.query_wait::<(TaskView, AssignmentView)>("status = 'open' AND assignee = 'ada'").await?;
    assert_eq!((f.names(open.ids()), f.names(open_to_ada.ids())), (vec!["ada", "bob"], vec!["ada"]), "{engine}");

    f.add("unassigned", &assignment("ada")).await?;
    assert_eq!(f.names(open.ids()), ["ada", "bob", "unassigned"], "{engine}: gains a Task when Assignment is added to it");
    assert_eq!(f.names(open_to_ada.ids()), ["ada", "unassigned"], "{engine}");

    f.edit::<Task>("ada", |task| Ok(task.status()?.replace("done")?)).await?;
    assert_eq!(f.names(open_to_ada.ids()), ["unassigned"], "{engine}: loses it when a Task value stops matching");

    f.edit::<Assignment>("unassigned", |assignment| Ok(assignment.assignee()?.replace("bob")?)).await?;
    assert!(open_to_ada.ids().is_empty(), "{engine}: loses it when an Assignment value stops matching");

    f.edit::<Assignment>("unassigned", |assignment| Ok(assignment.assignee()?.replace("ada")?)).await?;
    f.edit::<Task>("unassigned", |task| Ok(task.title()?.replace("triaged")?)).await?;
    let pairs = open_to_ada.peek();
    let [(task, assignment)] = pairs.as_slice() else { panic!("{engine}: expected one match, got {}", pairs.len()) };
    assert_eq!((f.name(task.id()), task.title()?, assignment.assignee()?), ("unassigned", "triaged".into(), "ada".into()), "{engine}");
    Ok(())
}

/// A live tuple query gains an entity when the second component is added to it, loses it when a value of
/// either component stops matching, and its views follow changes to properties of either component.
#[tokio::test]
async fn a_live_tuple_query_follows_both_components() -> anyhow::Result<()> {
    live_tuple_query(on_sled().await?).await?;
    live_tuple_query(on_sqlite().await?).await
}

async fn qualified_names(f: Fixture) -> anyhow::Result<()> {
    let engine = f.engine;
    // A qualifier picks one of the two properties named `note`: a component of the tuple by display name, or by label.
    assert_eq!(f.fetch_pairs("Task::note = 'task note ship'").await?, ["ada"], "{engine}");
    assert_eq!(f.fetch_pairs("Assignment::note = 'assignment note bob'").await?, ["bob"], "{engine}");
    assert_eq!(f.fetch_pairs("assignment::note = 'assignment note bob'").await?, ["bob"], "{engine}");
    assert!(f.fetch_pairs("Assignment::note = 'task note ship'").await?.is_empty(), "{engine}");
    let live = f.ctx.query_wait::<(TaskView, AssignmentView)>("Assignment::note = 'assignment note bob'").await?;
    assert_eq!(f.names(live.ids()), ["bob"], "{engine}");

    // The shared property is the same property whichever component names it.
    for selection in ["Task::project = 'atlas'", "Assignment::project = 'atlas'"] {
        assert_eq!(f.fetch_pairs(selection).await?, ["ada", "bob", "done"], "{engine}: {selection}");
    }

    // ORDER BY takes a qualifier too.
    assert_eq!(f.fetch_pairs_in_order("status = 'open' ORDER BY Assignment::assignee DESC").await?, ["bob", "ada"], "{engine}");
    assert_eq!(f.fetch_pairs_in_order("assignee = 'ada' ORDER BY Task::note").await?, ["done", "ada"], "{engine}");

    // An ORDER BY key belongs to a component in the tuple, though a predicate may read one outside it.
    for (selection, component) in
        [("true ORDER BY \"records.closure\"::closed_at", "records.closure"), ("true ORDER BY Nobody::closed_at", "Nobody")]
    {
        let outside = ComponentScopeError::SortKeyOutsideQuery { component: component.into(), name: "closed_at".into() };
        assert_eq!(f.refusal(selection).await, outside, "{engine}: {selection}");
        let live = f.ctx.query_wait::<(TaskView, AssignmentView)>(selection).await.map(|_| ());
        assert!(matches!(&live, Err(RetrievalError::ComponentScope(error)) if *error == outside), "{engine}: {live:?}");
    }

    // A qualifier must name a known component, and its component must declare the property.
    assert_eq!(f.refusal("Nobody::closed_at = 5").await, ComponentScopeError::UnknownComponent("Nobody".into()), "{engine}");
    assert_eq!(f.refusal("Closure::closed_at = 5").await, ComponentScopeError::UnknownComponent("Closure".into()), "{engine}");
    let undeclared = ComponentScopeError::Undeclared { name: "closed_at".into(), components: vec!["Task".into()] };
    assert_eq!(f.refusal("Task::closed_at = 5").await, undeclared, "{engine}");

    // After a qualifier, as after a bare name, a subpath needs a JSON property.
    let subpath = f.ctx.fetch::<(TaskView, AssignmentView)>("Assignment::assignee.display_name = 'Ada'").await.map(|_| ());
    assert!(
        matches!(&subpath, Err(RetrievalError::Other(message)) if message.contains("only JSON properties support subpaths")),
        "{engine}: {subpath:?}"
    );
    Ok(())
}

/// `Component::name` names the component a property belongs to, in a predicate and in ORDER BY: a component
/// of the tuple by display name or label. A qualifier naming no such component and no label, a property its
/// component does not declare, or an ORDER BY key on a component outside the tuple, is refused.
#[tokio::test]
async fn a_qualifier_names_the_component_a_property_belongs_to() -> anyhow::Result<()> {
    qualified_names(on_sled().await?).await?;
    qualified_names(on_sqlite().await?).await
}

/// The matches of a pair query three ways: a fetch, a live query's initial answer, and that live query after
/// each candidate changed, which the reactor re-evaluates in memory.
async fn three_answers(f: &Fixture, selection: &str) -> anyhow::Result<[Vec<&'static str>; 3]> {
    let fetched = f.fetch_pairs(selection).await?;
    let live = f.ctx.query_wait::<(TaskView, AssignmentView)>(selection).await?;
    let initial = f.names(live.ids());
    for name in ["ada", "bob", "done"] {
        f.edit::<Task>(name, |task| Ok(task.title()?.insert(0, "re-")?)).await?;
    }
    Ok([fetched, initial, f.names(live.ids())])
}

async fn qualified_outside_component(f: Fixture) -> anyhow::Result<()> {
    let engine = f.engine;
    // Closure is outside the tuple: its label is found through the catalog, double-quoted for its dot.
    assert_eq!(three_answers(&f, "\"records.closure\"::closed_at = 5").await?, [["done"], ["done"], ["done"]], "{engine}");
    // ada and bob lack Closure, so its property is absent from them: NULL to IS NULL, and a comparison with it
    // is never true, even beneath NOT. Fetch and live query agree on both engines.
    assert_eq!(
        three_answers(&f, "\"records.closure\"::closed_at IS NULL").await?,
        [["ada", "bob"], ["ada", "bob"], ["ada", "bob"]],
        "{engine}"
    );
    assert_eq!(
        three_answers(&f, "NOT \"records.closure\"::closed_at = 5").await?,
        [Vec::<&str>::new(), Vec::new(), Vec::new()],
        "{engine}"
    );
    assert_eq!(three_answers(&f, "NOT \"records.closure\"::closed_at = 6").await?, [["done"], ["done"], ["done"]], "{engine}");
    Ok(())
}

/// `"records.closure"::closed_at` reads a property of a component outside the tuple; on an entity that lacks
/// that component the property is absent, with the same answer from a fetch and a live query.
#[tokio::test]
async fn a_qualified_property_of_a_component_outside_the_tuple_reads_absent_where_it_is_missing() -> anyhow::Result<()> {
    qualified_outside_component(on_sled().await?).await?;
    qualified_outside_component(on_sqlite().await?).await
}

async fn dotted_paths(f: Fixture) -> anyhow::Result<()> {
    let engine = f.engine;
    let trx = f.ctx.begin();
    let card = trx.create(&Card { pin: Json::new(serde_json::json!({ "owner": "zed" })) }).await?;
    stage(card.entity(), &Pin { owner: "amy".into() }, f.ctx.resolve_model_id::<Pin>().await?)?;
    let id = card.id();
    trx.commit().await?;
    let cards = |cards: Vec<CardView>| cards.iter().map(View::id).collect::<Vec<_>>();
    let pairs = |pairs: Vec<(CardView, PinView)>| pairs.iter().map(|(card, _)| card.id()).collect::<Vec<_>>();

    // `pin.owner` is Card's JSON property `pin` and its subpath, whether the query names Pin's component or not.
    assert_eq!(cards(f.ctx.fetch("pin.owner = 'zed'").await?), [id], "{engine}");
    assert_eq!(pairs(f.ctx.fetch("pin.owner = 'zed'").await?), [id], "{engine}");
    assert_eq!(pairs(f.ctx.fetch("Pin::owner = 'amy'").await?), [id], "{engine}");

    // A one-view query reads a leading label of its own component as a qualifier; a tuple query does not.
    assert_eq!(cards(f.ctx.fetch("card.pin.owner = 'zed'").await?), [id], "{engine}");
    assert_eq!(f.names(f.ctx.fetch::<TaskView>("task.title = 'ship'").await?.iter().map(View::id)), ["ada"], "{engine}");
    let undeclared = ComponentScopeError::Undeclared { name: "card".into(), components: vec!["Card".into(), "Pin".into()] };
    let refused = f.ctx.fetch::<(CardView, PinView)>("card.pin.owner = 'zed'").await.map(|_| ());
    assert!(matches!(&refused, Err(RetrievalError::ComponentScope(error)) if *error == undeclared), "{engine}: {refused:?}");
    Ok(())
}

/// In a query over a tuple a dot only ever starts a subpath, so `pin.owner` reads Card's JSON property `pin`
/// beside a Pin component exactly as it does in a one-view query, and `Pin::owner` reads the component.
/// A one-view query reads a leading label of its own component as a qualifier.
#[tokio::test]
async fn a_dot_in_a_tuple_query_starts_a_subpath_and_never_names_a_component() -> anyhow::Result<()> {
    dotted_paths(on_sled().await?).await?;
    dotted_paths(on_sqlite().await?).await
}

async fn names_checked_before_ready<SE: StorageEngine + Send + Sync + 'static>(engine: &str, storage: SE) -> anyhow::Result<()> {
    let forged = ankurah_tests::catalog_forge::forge_catalog("task", "Task", &[("project", "lww", "string")], b"component-views");
    ankurah_tests::catalog_forge::plant(&storage, &forged).await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    let ctx = Context::new(node.clone(), DEFAULT_CONTEXT);

    let pair = || vec!["Task".to_owned(), "Assignment".to_owned()];
    for (selection, expected) in [
        ("Task::nope = 'x'", ComponentScopeError::Undeclared { name: "nope".into(), components: vec!["Task".into()] }),
        ("task.assignee = 'ada'", ComponentScopeError::Undeclared { name: "task".into(), components: pair() }),
        ("nope = 'x'", ComponentScopeError::Undeclared { name: "nope".into(), components: pair() }),
        (
            "true ORDER BY Nobody::closed_at",
            ComponentScopeError::SortKeyOutsideQuery { component: "Nobody".into(), name: "closed_at".into() },
        ),
    ] {
        let refused = ctx.query::<(TaskView, AssignmentView)>(selection).map(|_| ());
        assert!(matches!(&refused, Err(RetrievalError::ComponentScope(error)) if *error == expected), "{engine}: {selection}: {refused:?}");
    }

    let open = ctx.query::<(TaskView, AssignmentView)>("status = 'open'")?;
    node.system.create().await?;
    open.wait_initialized().await?;
    assert!(open.ids().is_empty(), "{engine}");
    Ok(())
}

/// A tuple live query made before its node is ready is refused at once when a name cannot resolve, and
/// otherwise waits for the node and answers.
#[tokio::test]
async fn a_tuple_live_query_made_before_its_node_is_ready_checks_its_names_at_once() -> anyhow::Result<()> {
    names_checked_before_ready("sled", SledStorageEngine::new_test()?).await?;
    names_checked_before_ready("sqlite", SqliteStorageEngine::open_in_memory().await?).await
}
