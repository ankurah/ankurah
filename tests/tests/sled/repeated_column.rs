//! Repeated-column conditions must be enforced by either index bounds or residual filtering.

use ankurah::property::Ref;
use ankurah::{policy::DEFAULT_CONTEXT, selection, Model, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct DupUser {
    pub name: String,
}

#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct DupNote {
    #[active_type(LWW)]
    pub title: String,
    pub rank: i32,
    pub owner: Ref<DupUser>,
    pub reviewer: Ref<DupUser>,
}

async fn setup_context() -> Result<ankurah::Context> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    node.system.create().await?;
    Ok(node.context_async(DEFAULT_CONTEXT).await?)
}

fn sorted_titles(notes: &[DupNoteView]) -> Vec<String> {
    let mut titles: Vec<String> = notes.iter().map(|n| n.title().unwrap()).collect();
    titles.sort();
    titles
}

async fn two_owners_two_notes(ctx: &ankurah::Context) -> Result<(ankurah::EntityId, ankurah::EntityId)> {
    let (alice, bob) = {
        let trx = ctx.begin();
        let alice = trx.create(&DupUser { name: "Alice".into() }).await?;
        let bob = trx.create(&DupUser { name: "Bob".into() }).await?;
        let ids = (alice.id(), bob.id());
        trx.commit().await?;
        ids
    };
    {
        let trx = ctx.begin();
        trx.create(&DupNote { title: "alice note".into(), rank: 1, owner: Ref::new(alice), reviewer: Ref::new(bob) }).await?;
        trx.create(&DupNote { title: "bob note".into(), rank: 2, owner: Ref::new(bob), reviewer: Ref::new(alice) }).await?;
        trx.commit().await?;
    }
    Ok((alice, bob))
}

#[tokio::test]
async fn two_equalities_on_one_string_column_match_nothing() -> Result<()> {
    let ctx = setup_context().await?;
    two_owners_two_notes(&ctx).await?;

    assert_eq!(sorted_titles(&ctx.fetch::<DupNoteView>("title = 'alice note' AND title = 'bob note'").await?), Vec::<String>::new());

    assert_eq!(sorted_titles(&ctx.fetch::<DupNoteView>("title = 'alice note'").await?), vec!["alice note"]);

    Ok(())
}

#[tokio::test]
async fn repeated_identical_equality_still_matches() -> Result<()> {
    let ctx = setup_context().await?;
    two_owners_two_notes(&ctx).await?;

    assert_eq!(sorted_titles(&ctx.fetch::<DupNoteView>("title = 'alice note' AND title = 'alice note'").await?), vec!["alice note"]);

    Ok(())
}

#[tokio::test]
async fn equality_and_range_on_one_column_match_nothing() -> Result<()> {
    let ctx = setup_context().await?;
    two_owners_two_notes(&ctx).await?;

    assert_eq!(sorted_titles(&ctx.fetch::<DupNoteView>("rank = 1 AND rank > 5").await?), Vec::<String>::new());
    assert_eq!(sorted_titles(&ctx.fetch::<DupNoteView>("rank > 5 AND rank = 1").await?), Vec::<String>::new());
    assert_eq!(sorted_titles(&ctx.fetch::<DupNoteView>("rank = 1 AND rank < 0").await?), Vec::<String>::new());

    assert_eq!(sorted_titles(&ctx.fetch::<DupNoteView>("rank = 1 AND rank > 0").await?), vec!["alice note"]);
    assert_eq!(sorted_titles(&ctx.fetch::<DupNoteView>("rank = 2 AND rank <= 2").await?), vec!["bob note"]);

    Ok(())
}

#[tokio::test]
async fn two_ranges_on_one_column_bracket_correctly() -> Result<()> {
    let ctx = setup_context().await?;
    two_owners_two_notes(&ctx).await?;

    assert_eq!(sorted_titles(&ctx.fetch::<DupNoteView>("rank > 0 AND rank < 2").await?), vec!["alice note"]);
    assert_eq!(sorted_titles(&ctx.fetch::<DupNoteView>("rank >= 1 AND rank <= 2").await?), vec!["alice note", "bob note"]);
    assert_eq!(sorted_titles(&ctx.fetch::<DupNoteView>("rank > 5 AND rank < 1").await?), Vec::<String>::new());

    Ok(())
}

#[tokio::test]
async fn repeated_column_alongside_other_columns() -> Result<()> {
    let ctx = setup_context().await?;
    let (alice, bob) = two_owners_two_notes(&ctx).await?;

    assert_eq!(
        sorted_titles(&ctx.fetch::<DupNoteView>(selection!("owner = {alice} AND rank = 1 AND owner = {bob}")).await?),
        Vec::<String>::new()
    );

    assert_eq!(
        sorted_titles(&ctx.fetch::<DupNoteView>(selection!("owner = {alice} AND rank = 1 AND owner = {alice}")).await?),
        vec!["alice note"]
    );

    Ok(())
}

#[tokio::test]
async fn two_equalities_on_one_ref_column_match_nothing() -> Result<()> {
    let ctx = setup_context().await?;
    let (alice, bob) = two_owners_two_notes(&ctx).await?;

    assert_eq!(sorted_titles(&ctx.fetch::<DupNoteView>(selection!("owner = {bob} AND owner = {alice}")).await?), Vec::<String>::new());

    assert_eq!(sorted_titles(&ctx.fetch::<DupNoteView>(selection!("owner = {bob}")).await?), vec!["bob note"]);
    assert_eq!(sorted_titles(&ctx.fetch::<DupNoteView>(selection!("owner = {alice}")).await?), vec!["alice note"]);

    Ok(())
}

#[tokio::test]
async fn or_over_two_columns_and_a_repeated_column() -> Result<()> {
    let ctx = setup_context().await?;
    let (alice, bob) = two_owners_two_notes(&ctx).await?;

    {
        let trx = ctx.begin();
        trx.create(&DupNote { title: "bob solo".into(), rank: 3, owner: Ref::new(bob), reviewer: Ref::new(bob) }).await?;
        trx.commit().await?;
    }

    assert_eq!(
        sorted_titles(&ctx.fetch::<DupNoteView>(selection!("(owner = {alice} OR reviewer = {alice}) AND owner = {bob}")).await?),
        vec!["bob note"]
    );

    assert_eq!(
        sorted_titles(&ctx.fetch::<DupNoteView>(selection!("(owner = {alice} OR reviewer = {alice}) AND owner = {alice}")).await?),
        vec!["alice note"]
    );

    assert_eq!(
        sorted_titles(
            &ctx.fetch::<DupNoteView>(selection!("(owner = {alice} OR reviewer = {alice}) AND owner = {alice} AND owner = {bob}")).await?
        ),
        Vec::<String>::new()
    );

    Ok(())
}

#[tokio::test]
async fn repeated_column_under_order_by() -> Result<()> {
    let ctx = setup_context().await?;
    let (alice, bob) = two_owners_two_notes(&ctx).await?;

    assert_eq!(
        sorted_titles(&ctx.fetch::<DupNoteView>(selection!("owner = {alice} AND owner = {bob} ORDER BY owner, rank")).await?),
        Vec::<String>::new()
    );

    assert_eq!(
        sorted_titles(&ctx.fetch::<DupNoteView>(selection!("owner = {alice} AND owner = {alice} ORDER BY owner, rank")).await?),
        vec!["alice note"]
    );

    Ok(())
}
