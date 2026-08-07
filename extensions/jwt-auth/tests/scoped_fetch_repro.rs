//! Repro attempt for a downstream observation: EndUser-scoped cached queries
//! (`user = ?` over a relay) returned empty initial fetches in the browser
//! while account-owner queries returned rows. Mirrors the IDP credential
//! policy shape: account scope always, domain scope unless account_scope,
//! self scope unless manage — composed client-side AND server-side.

mod common;

use ankurah::{Model, Node, Ref};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_jwt_auth::{JwtAgent, JwtClaims, JwtContext, JwtKeys, JwtPolicy, PolicyConfig};
use ankurah_storage_sled::SledStorageEngine;
use jwt_simple::prelude::Duration;
use std::sync::Arc;

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct ScopeTarget {
    pub name: String,
}

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct ScopeCred {
    pub user: Ref<ScopeTarget>,
    pub account: Ref<ScopeTarget>,
    pub domain: Ref<ScopeTarget>,
    pub label: String,
}

const CONFIG_JSON: &str = r#"{
    "roles": {
        "Owner": ["credential:read", "credential:manage", "credential:account_scope", "target:read"],
        "EndUser": ["credential:read", "target:read"]
    },
    "collections": {
        "scopecred": {
            "read": "credential:read",
            "scope": [
                { "filter": "account = $jwt.custom.account_id" },
                { "filter": "domain = $jwt.custom.domain_id", "unless_privilege": "credential:account_scope" },
                { "filter": "user = $jwt.sub", "unless_privilege": "credential:manage" }
            ]
        },
        "scopetarget": {
            "read": "target:read"
        }
    }
}"#;

async fn eventually_count(lq: &ankurah::LiveQuery<ScopeCredView>, expected: usize) -> usize {
    for _ in 0..60 {
        let count = lq.ids().len();
        if count == expected {
            return count;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    lq.ids().len()
}

#[tokio::test]
async fn enduser_scoped_cached_fetch_over_relay() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let public_pem = keys.public_key_pem()?;

    let server_agent = JwtAgent::new_ephemeral();
    server_agent.update_config(serde_json::from_str::<PolicyConfig>(CONFIG_JSON)?);
    server_agent.set_keys(JwtKeys::Signing(keys.clone()));
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), server_agent);
    server.system.create().await?;

    let root = server.context(JwtContext::system())?;
    {
        let trx = root.begin();
        trx.create(&JwtPolicy { config_json: CONFIG_JSON.to_string(), public_key_pem: public_pem.clone(), trust_json: None }).await?;
        trx.commit().await?;
    }

    let (user_id, account_id, domain_id) = {
        let trx = root.begin();
        let user = trx.create(&ScopeTarget { name: "user".into() }).await?;
        let account = trx.create(&ScopeTarget { name: "account".into() }).await?;
        let domain = trx.create(&ScopeTarget { name: "domain".into() }).await?;
        let ids = (user.id(), account.id(), domain.id());
        trx.commit().await?;
        let trx = root.begin();
        trx.create(&ScopeCred { user: ids.0.into(), account: ids.1.into(), domain: ids.2.into(), label: "the credential".into() }).await?;
        trx.commit().await?;
        ids
    };

    // Browser-shaped client: ephemeral node whose agent syncs policy/keys.
    let client_agent = JwtAgent::new_ephemeral();
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), client_agent.clone());
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    let mut synced = false;
    for _ in 0..100 {
        if !client_agent.config().roles.is_empty() {
            synced = true;
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    assert!(synced, "client agent should sync policy config");

    // Account owner: manage + account_scope privileges, account claim only.
    let mut owner_custom = serde_json::Map::new();
    owner_custom.insert("account_id".into(), serde_json::Value::String(account_id.to_base64()));
    let owner_claims = JwtClaims {
        sub: "owner-1".into(),
        roles: vec!["Owner".into()],
        email: "owner@example.com".into(),
        name: None,
        custom: owner_custom,
    };
    let owner_token = keys.sign(&owner_claims, Duration::from_hours(1))?;
    let owner_ctx = client.context(JwtContext::from_claims(owner_claims, owner_token))?;

    let owner_q = format!("user = '{}'", user_id.to_base64());
    let owner_lq = owner_ctx.query::<ScopeCredView>(owner_q.as_str())?;
    owner_lq.wait_initialized().await;
    assert_eq!(eventually_count(&owner_lq, 1).await, 1, "owner-scoped cached fetch should return the credential");

    // EndUser: read only; account+domain claims; sub = the credential's user id.
    let mut enduser_custom = serde_json::Map::new();
    enduser_custom.insert("account_id".into(), serde_json::Value::String(account_id.to_base64()));
    enduser_custom.insert("domain_id".into(), serde_json::Value::String(domain_id.to_base64()));
    let enduser_claims = JwtClaims {
        sub: user_id.to_base64(),
        roles: vec!["EndUser".into()],
        email: "user@example.com".into(),
        name: None,
        custom: enduser_custom,
    };
    let enduser_token = keys.sign(&enduser_claims, Duration::from_hours(1))?;
    let enduser_ctx = client.context(JwtContext::from_claims(enduser_claims, enduser_token))?;

    let enduser_q = format!("user = '{}'", user_id.to_base64());
    let enduser_lq = enduser_ctx.query::<ScopeCredView>(enduser_q.as_str())?;
    enduser_lq.wait_initialized().await;
    assert_eq!(
        eventually_count(&enduser_lq, 1).await,
        1,
        "EndUser-scoped cached fetch (composed account+domain+self clauses) should return the credential"
    );

    Ok(())
}
