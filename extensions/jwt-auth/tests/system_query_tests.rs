//! Set-backed (system) queries: a livequery credentialed by the node's
//! whole SessionSet reads under the UNION of live credentials and follows
//! logins, logouts, and refreshes reactively. Credential denial is a
//! reported status, not a failure: with no eligible credential the query
//! exists, serves nothing, and heals itself when a session arrives. This
//! is the exact shape the catalog manager's standing queries use.

mod common;
use common::*;

use ankurah::core::livequery::{EntityLiveQuery, LocalStatus, QueryStatus, RemoteStatus};
use ankurah::core::node::MatchArgs;
use ankurah::signals::Peek;
use ankurah::{Model, Node, Ref};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_jwt_auth::{JwtAgent, JwtContext, JwtKeys, PolicyConfig};
use ankurah_storage_sled::SledStorageEngine;
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

/// Both sides scope scopeitem reads to the caller's own subject, so the
/// set-backed query's visibility is exactly the union of the live
/// subjects' slices.
const SCOPED_CONFIG: &str = r#"{
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

async fn eventually(mut condition: impl FnMut() -> bool) -> bool {
    for _ in 0..100 {
        if condition() {
            return true;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    condition()
}

/// The full arc of a set-backed query's life: denied with no sessions,
/// scoped to one login, widened to the union of two, narrowed by a
/// logout, and denied again when the last session culls.
#[tokio::test]
async fn set_backed_query_follows_the_session_set() -> anyhow::Result<()> {
    let keys = common::test_keys();

    let server_agent = JwtAgent::new_ephemeral();
    server_agent.update_config(serde_json::from_str::<PolicyConfig>(SCOPED_CONFIG)?);
    server_agent.set_keys(JwtKeys::Signing(keys.clone()));
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), server_agent);
    server.system.create().await?;

    let client_agent = JwtAgent::new_ephemeral();
    client_agent.update_config(serde_json::from_str::<PolicyConfig>(SCOPED_CONFIG)?);
    client_agent.set_keys(JwtKeys::Signing(keys.clone()));
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), client_agent);

    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let root = server.context(JwtContext::system())?;
    let (alice_id, bob_id) = {
        let trx = root.begin();
        let alice = trx.create(&ScopeTarget { name: "alice".into() }).await?;
        let bob = trx.create(&ScopeTarget { name: "bob".into() }).await?;
        let ids = (alice.id(), bob.id());
        trx.commit().await?;
        ids
    };
    let (alice_item, bob_item) = {
        let trx = root.begin();
        let a = trx.create(&ScopeItem { owner: alice_id.into(), label: "alice-1".into() }).await?.id();
        let b = trx.create(&ScopeItem { owner: bob_id.into(), label: "bob-1".into() }).await?.id();
        trx.commit().await?;
        (a, b)
    };

    // The set-backed query, created with ZERO live sessions. Construction
    // succeeds; denial is its status.
    let args = MatchArgs { selection: "true".try_into()?, cached: true };
    let lq = EntityLiveQuery::new(&client, "scopeitem".into(), args, client.sessions.clone())?.map::<ScopeItemView>();

    let status: QueryStatus = lq.status().peek();
    assert!(matches!(status.local, LocalStatus::Denied { .. }), "no credential grants: local leg is Denied, got {:?}", status.local);
    assert!(lq.ids().is_empty(), "a denied query serves nothing");
    // The remote leg settles into a refusal (the server rejects the
    // credential-less subscribe; its refusal arrives as error text).
    assert!(
        eventually(|| matches!(lq.status().peek().remote, RemoteStatus::Error { .. } | RemoteStatus::Denied { .. })).await,
        "remote leg reports the server's refusal, got {:?}",
        lq.status().peek().remote
    );

    // Alice logs in: her session joins the set, the query re-derives,
    // re-subscribes, and serves her slice.
    let alice_claims = make_claims(&alice_id.to_base64(), &["Member"], "alice@example.com");
    let alice_token = sign_token(&keys, &alice_claims);
    let alice_ctx = client.context(JwtContext::from_claims(alice_claims, alice_token))?;

    assert!(
        eventually(|| matches!(
            lq.status().peek(),
            QueryStatus { local: LocalStatus::Active { .. }, remote: RemoteStatus::Established { .. } }
        ))
        .await,
        "one login heals both legs, got {:?}",
        lq.status().peek()
    );
    assert!(eventually(|| lq.ids() == vec![alice_item]).await, "alice's login scopes the query to her item, got {:?}", lq.ids());
    // A heal re-enters the reactor's first-activation path at the
    // CURRENT version, so initialization completes and waiters return;
    // registering it at any earlier version would leave
    // initialized_version behind the current version and hang this wait
    // forever.
    tokio::time::timeout(std::time::Duration::from_secs(5), lq.wait_initialized())
        .await
        .expect("initialization must complete after the query heals from Denied");

    // Bob logs in too: the union widens to both slices.
    let bob_claims = make_claims(&bob_id.to_base64(), &["Member"], "bob@example.com");
    let bob_token = sign_token(&keys, &bob_claims);
    let bob_ctx = client.context(JwtContext::from_claims(bob_claims, bob_token))?;

    let both = {
        let mut both = vec![alice_item, bob_item];
        both.sort();
        both
    };
    assert!(eventually(|| lq.ids_sorted() == both).await, "two logins union their slices, got {:?}", lq.ids_sorted());

    // Alice logs out (her context drops, RAII culls her session): the
    // union narrows to bob's slice.
    drop(alice_ctx);
    assert!(eventually(|| lq.ids() == vec![bob_item]).await, "a logout narrows the union, got {:?}", lq.ids());

    // Bob logs out too: nothing grants, and the query returns to Denied
    // rather than dying.
    drop(bob_ctx);
    assert!(
        eventually(|| matches!(lq.status().peek().local, LocalStatus::Denied { .. })).await,
        "the last logout returns the query to Denied, got {:?}",
        lq.status().peek().local
    );
    assert!(eventually(|| lq.ids().is_empty()).await, "a denied query serves nothing again, got {:?}", lq.ids());
    Ok(())
}
