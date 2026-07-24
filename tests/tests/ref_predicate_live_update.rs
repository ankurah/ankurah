//! Regression tests for Ref-field equality predicates receiving live updates.
//!
//! https://github.com/ankurah/ankurah/issues/259: predicates comparing a Ref
//! field against a string literal get correct initial fetches but are never
//! notified of later commits, because the reactor's comparison index keys
//! watchers by the literal's collation (String) while change notifications key
//! by the property value's collation (EntityId). Typed EntityId literals
//! collate consistently on both sides and must receive live updates.

mod common;

use crate::common::*;
use ankurah::{Model, Ref};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct RefPredRoom {
    pub name: String,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct RefPredMessage {
    pub room: Ref<RefPredRoom>,
    pub text: String,
}

async fn setup() -> Result<Context> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    node.system.create().await?;
    Ok(node.context(DEFAULT_CONTEXT)?)
}

async fn create_room(ctx: &Context, name: &str) -> Result<ankurah::proto::EntityId> {
    let trx = ctx.begin();
    let room = trx.create(&RefPredRoom { name: name.to_string() }).await?;
    let id = room.id();
    trx.commit().await?;
    Ok(id)
}

async fn create_message(ctx: &Context, room: ankurah::proto::EntityId, text: &str) -> Result<()> {
    let trx = ctx.begin();
    trx.create(&RefPredMessage { room: Ref::new(room), text: text.to_string() }).await?;
    trx.commit().await?;
    Ok(())
}

async fn eventually(mut check: impl FnMut() -> bool) -> bool {
    for _ in 0..40 {
        if check() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    check()
}

#[tokio::test]
async fn typed_ref_literal_receives_live_updates() -> Result<()> {
    let ctx = setup().await?;
    let room_a = create_room(&ctx, "a").await?;
    let room_b = create_room(&ctx, "b").await?;

    let mut selection: ankurah::ankql::ast::Selection = "room = ?".try_into()?;
    selection.predicate =
        selection.predicate.populate([ankurah::ankql::ast::Expr::Literal(ankurah::ankql::ast::Value::EntityId(room_a))])?;

    ctx.model_id::<RefPredMessage>().await?;
    let lq = ctx.query::<RefPredMessageView>(selection)?;
    lq.wait_initialized().await;
    assert_eq!(lq.ids().len(), 0, "no messages yet");

    create_message(&ctx, room_a, "in room a").await?;
    assert!(eventually(|| lq.ids().len() == 1).await, "typed Ref literal should receive the live update for a matching commit");

    create_message(&ctx, room_b, "in room b").await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(lq.ids().len(), 1, "non-matching room must not leak into the query");

    Ok(())
}

/// Documents https://github.com/ankurah/ankurah/issues/259: the same flow with
/// a string literal (all that can be expressed from selection strings today)
/// never receives the live update. Un-ignore when the collation mismatch is
/// fixed.
#[tokio::test]
#[ignore = "known gap: string-literal Ref comparisons miss live updates (issue #259)"]
async fn string_ref_literal_receives_live_updates() -> Result<()> {
    let ctx = setup().await?;
    let room_a = create_room(&ctx, "a").await?;

    let selection: ankurah::ankql::ast::Selection = format!("room = '{}'", room_a.to_base64()).as_str().try_into()?;

    ctx.model_id::<RefPredMessage>().await?;
    let lq = ctx.query::<RefPredMessageView>(selection)?;
    lq.wait_initialized().await;

    create_message(&ctx, room_a, "in room a").await?;
    assert!(eventually(|| lq.ids().len() == 1).await, "string Ref literal should receive the live update for a matching commit");

    Ok(())
}
