//! How the names in a tuple query resolve: a bare name across the tuple's components, `Component::name` for
//! one component in the tuple or outside it, a dot only to start a subpath, and which names a live query
//! checks before its node is ready.

mod component_fixture;
use component_fixture::*;

use ankurah::core::storage::StorageEngine;
use ankurah::error::{ComponentScopeError, RetrievalError};
use ankurah::policy::DEFAULT_CONTEXT;
use ankurah::property::Json;
use ankurah::{Context, Mutable, Node, PermissiveAgent, View};
use ankurah_storage_sled::SledStorageEngine;
use ankurah_storage_sqlite::SqliteStorageEngine;
use std::sync::Arc;

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

    // Whether two components declare `note` as one property, and whether `Nobody` is a component at all, waits
    // for their bound identities and the catalog.
    let open = ctx.query::<(TaskView, AssignmentView)>("status = 'open'")?;
    let ambiguous = ctx.query::<(TaskView, AssignmentView)>("note = 'x'")?;
    let unknown = ctx.query::<(TaskView, AssignmentView)>("Nobody::closed_at = 5")?;
    node.system.create().await?;
    open.wait_initialized().await?;
    assert!(open.ids().is_empty(), "{engine}");
    let expected = ComponentScopeError::Ambiguous { name: "note".into(), first: "Task".into(), second: "Assignment".into() };
    let refused = ambiguous.wait_initialized().await;
    assert!(matches!(&refused, Err(RetrievalError::ComponentScope(error)) if *error == expected), "{engine}: {refused:?}");
    let refused = unknown.wait_initialized().await;
    assert!(
        matches!(&refused, Err(RetrievalError::ComponentScope(ComponentScopeError::UnknownComponent(name))) if name == "Nobody"),
        "{engine}: {refused:?}"
    );
    Ok(())
}

/// A tuple live query made before its node is ready is refused at once when its components do not declare a
/// name, and otherwise waits for the node; names that need bound identities or the catalog are refused then.
#[tokio::test]
async fn a_tuple_live_query_made_before_its_node_is_ready_checks_its_names_at_once() -> anyhow::Result<()> {
    names_checked_before_ready("sled", SledStorageEngine::new_test()?).await?;
    names_checked_before_ready("sqlite", SqliteStorageEngine::open_in_memory().await?).await
}
