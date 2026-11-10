//! A typed query over a tuple of views, `ctx.fetch::<(TaskView, AssignmentView)>(..)`: its matches belong to
//! every component in the tuple, it filters and sorts by their properties, and a live query follows both.

mod component_fixture;
use component_fixture::*;

use ankurah::signals::Peek;
use ankurah::View;

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
