//! Regression coverage for scope rules on Ref fields receiving live updates.
//!
//! `$jwt.*` substitution previously spliced claim values in as quoted string
//! literals. String literals collate as text in the reactor's watcher index
//! while Ref property values collate as raw EntityId bytes (ankurah#259), so
//! a scoped LiveQuery whose clauses are all Ref comparisons fetched correctly
//! but never received commit-time updates. Substitution now populates typed
//! EntityId literals for values that parse as EntityIds, closing the gap.

mod common;

use ankurah::{Model, Node, Ref};
use ankurah_jwt_auth::{JwtAgent, JwtClaims, JwtContext, JwtKeys, PolicyConfig};
use ankurah_storage_sled::SledStorageEngine;
use jwt_simple::prelude::Duration;
use std::sync::Arc;

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct ScopeTarget {
    pub name: String,
}

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct ScopeItem {
    pub owner: Ref<ScopeTarget>,
    pub label: String,
}

const CONFIG_JSON: &str = r#"{
    "roles": {
        "Member": ["item:read", "target:read"]
    },
    "collections": {
        "scopeitem": {
            "read": "item:read",
            "scope": [
                { "filter": "owner = $jwt.sub" }
            ]
        },
        "scopetarget": {
            "read": "target:read"
        }
    }
}"#;

async fn eventually_count(lq: &ankurah::LiveQuery<ScopeItemView>, expected: usize) -> usize {
    for _ in 0..60 {
        let count = lq.ids().len();
        if count == expected {
            return count;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    lq.ids().len()
}

/// A Member's LiveQuery whose only clauses are Ref comparisons (the user's own
/// `owner = '<base64>'` plus the injected `owner = $jwt.sub` scope rule) must
/// receive entities committed after the subscription was established.
#[tokio::test]
async fn ref_scope_rule_receives_live_updates() -> anyhow::Result<()> {
    let keys = common::test_keys();

    let agent = JwtAgent::new_ephemeral();
    agent.update_config(serde_json::from_str::<PolicyConfig>(CONFIG_JSON)?);
    agent.set_keys(JwtKeys::Signing(keys.clone()));
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent);
    node.system.create().await?;

    let root = node.context(JwtContext::system())?;
    // Schema definition is the ADMIN's act (rev 4, REN 2 second ruling):
    // the Member role carries no catalog privileges, and a query against a
    // collection nobody registered fails loud instead of idling (pinned by
    // unregistered_collection_query_fails_loud below). Register the
    // collection up front so this test exercises its actual subject, the
    // scoped LIVE delivery.
    root.register::<ScopeItem>().await?;
    let (owner_id, other_id) = {
        let trx = root.begin();
        let owner = trx.create(&ScopeTarget { name: "owner".into() }).await?;
        let other = trx.create(&ScopeTarget { name: "other".into() }).await?;
        let ids = (owner.id(), other.id());
        trx.commit().await?;
        ids
    };

    let member_claims = JwtClaims {
        sub: owner_id.to_base64(),
        roles: vec!["Member".into()],
        email: "member@example.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let member_token = keys.sign(&member_claims, Duration::from_hours(1))?;
    let member_ctx = node.context(JwtContext::from_claims(member_claims, member_token))?;

    let query = format!("owner = '{}'", owner_id.to_base64());
    let lq = member_ctx.query::<ScopeItemView>(query.as_str())?;
    lq.wait_initialized().await;
    assert_eq!(lq.ids().len(), 0, "no items yet");

    // Committed after the subscription: must arrive as a live update.
    {
        let trx = root.begin();
        trx.create(&ScopeItem { owner: owner_id.into(), label: "mine".into() }).await?;
        trx.commit().await?;
    }
    assert_eq!(eventually_count(&lq, 1).await, 1, "in-scope item committed after subscribe must reach the scoped LiveQuery");

    // Out-of-scope item: must not leak in.
    {
        let trx = root.begin();
        trx.create(&ScopeItem { owner: other_id.into(), label: "not mine".into() }).await?;
        trx.commit().await?;
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    assert_eq!(lq.ids().len(), 1, "out-of-scope item must not appear in the scoped LiveQuery");

    Ok(())
}

/// The flip side of the pre-registration above (REN 2 second ruling,
/// 2026-07-06): a principal without catalog privileges querying a
/// collection NOBODY has registered gets a loud, actionable error -- not a
/// fabricated empty answer idling on a subscription that can never say
/// anything truthful.
#[tokio::test]
async fn unregistered_collection_query_fails_loud() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(serde_json::from_str::<PolicyConfig>(CONFIG_JSON)?);
    agent.set_keys(JwtKeys::Signing(keys.clone()));
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent);
    node.system.create().await?;

    let member_claims = JwtClaims {
        sub: "member-1".into(),
        roles: vec!["Member".into()],
        email: "member@example.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&member_claims, Duration::from_hours(1))?;
    let member_ctx = node.context(JwtContext::from_claims(member_claims, token))?;

    // Nobody registered `scopeitem`, and the Member role may not define
    // schema: first-use registration is refused, and the read fails loud.
    let err = member_ctx.fetch::<ScopeItemView>("label = 'x'").await.expect_err("fetch over an unregistered collection must fail loud");
    let msg = err.to_string();
    assert!(msg.contains("not registered") && msg.contains("scopeitem"), "actionable error naming the collection, got: {msg}");

    Ok(())
}

/// The same fail-loud contract surfaces through a LIVE QUERY (the ruling
/// covers fetch and live query alike): the sync query() constructor
/// succeeds -- resolution is deferred into the async init task --
/// wait_initialized returns instead of hanging, and the loud
/// unregistered-collection error lands in the query's error slot.
#[tokio::test]
async fn unregistered_collection_live_query_fails_loud() -> anyhow::Result<()> {
    use ankurah::signals::With;

    let keys = common::test_keys();
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(serde_json::from_str::<PolicyConfig>(CONFIG_JSON)?);
    agent.set_keys(JwtKeys::Signing(keys.clone()));
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent);
    node.system.create().await?;

    let member_claims = JwtClaims {
        sub: "member-1".into(),
        roles: vec!["Member".into()],
        email: "member@example.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&member_claims, Duration::from_hours(1))?;
    let member_ctx = node.context(JwtContext::from_claims(member_claims, token))?;

    let lq = member_ctx.query::<ScopeItemView>("label = 'x'")?;
    lq.wait_initialized().await;

    let msg = lq
        .error()
        .with(|e| e.as_ref().map(|e| e.to_string()))
        .expect("live query over an unregistered collection must surface the loud error");
    assert!(msg.contains("not registered") && msg.contains("scopeitem"), "actionable error naming the collection, got: {msg}");
    assert!(lq.ids().is_empty(), "an errored live query must not fabricate results");

    Ok(())
}
