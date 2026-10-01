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
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node.system.create().await?;
    agent.set_policy(&node, &agent.config()).await?;

    let root = node.context_async(JwtContext::system()).await?;
    // This test isolates live delivery; first-use registration is covered below.
    root.resolve_model_id::<ScopeItem>().await?;
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
    let member_ctx = node.context_async(JwtContext::from_claims(member_claims, member_token)).await?;

    let query = format!("owner = '{}'", owner_id.to_base64());
    let lq = member_ctx.query::<ScopeItemView>(query.as_str())?;
    lq.wait_initialized().await?;
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

#[tokio::test]
async fn first_use_fetch_registers_schema_without_granting_write_access() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(serde_json::from_str::<PolicyConfig>(CONFIG_JSON)?);
    agent.set_keys(JwtKeys::Signing(keys.clone()));
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node.system.create().await?;
    agent.set_policy(&node, &agent.config()).await?;

    let member_claims = JwtClaims {
        sub: ankurah::proto::EntityId::random().to_base64(),
        roles: vec!["Member".into()],
        email: "member@example.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&member_claims, Duration::from_hours(1))?;
    let member_ctx = node.context_async(JwtContext::from_claims(member_claims, token)).await?;

    let epoch = node.system.system_epoch().unwrap();
    assert!(ScopeItem::descriptor().bind_local(&node.catalog, epoch).is_err());
    assert!(member_ctx.fetch::<ScopeItemView>("label = 'x'").await?.is_empty());
    assert!(ScopeItem::descriptor().bind_local(&node.catalog, epoch).is_ok());
    let transaction = member_ctx.begin();
    assert!(transaction.create(&ScopeItem { owner: ankurah::proto::EntityId::random().into(), label: "x".into() }).await.is_err(),
        "schema registration does not grant CRUD permissions");

    Ok(())
}

#[tokio::test]
async fn first_use_live_query_grants_nothing_for_an_invalid_scope_claim() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = JwtAgent::new_ephemeral();
    let mut config = serde_json::from_str::<PolicyConfig>(CONFIG_JSON)?;
    config.collections.remove("scopetarget"); // No other membership may grant this query read access.
    agent.update_config(config);
    agent.set_keys(JwtKeys::Signing(keys.clone()));
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node.system.create().await?;
    agent.set_policy(&node, &agent.config()).await?;

    let member_claims = JwtClaims {
        sub: "member-1".into(),
        roles: vec!["Member".into()],
        email: "member@example.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&member_claims, Duration::from_hours(1))?;
    let member_ctx = node.context_async(JwtContext::from_claims(member_claims, token)).await?;

    assert!(ScopeItem::descriptor().bind_local(&node.catalog, node.system.system_epoch().unwrap()).is_err());
    let query = member_ctx.query::<ScopeItemView>("label = 'x'")?;
    query.wait_initialized().await?;

    let root = node.context_async(JwtContext::Root).await?;
    let transaction = root.begin();
    let id = transaction.create(&ScopeItem { owner: ankurah::proto::EntityId::random().into(), label: "x".into() }).await?.id();
    transaction.commit().await?;
    assert!(query.ids().is_empty(), "the non-EntityId subject supplies no grant");
    assert!(member_ctx.fetch::<ScopeItemView>("label = 'x'").await?.is_empty());
    assert!(member_ctx.get::<ScopeItemView>(id).await.is_err(), "known-ID retrieval must deny the same invalid credential");

    Ok(())
}
