mod common;

use ankurah::Model;
use ankurah::Node;
use ankurah_jwt_auth::{JwtAgent, JwtContext, JwtPolicy};
use ankurah_storage_sled::SledStorageEngine;
use common::{blog_config_path, make_claims, sign_token};
use jwt_simple::prelude::Duration;
use std::sync::Arc;

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct Post {
    #[active_type(LWW)]
    pub title: String,
    #[active_type(LWW)]
    pub body: String,
}

/// End-to-end test: ephemeral node receives policy from durable node via LiveQuery
/// and can then enforce RBAC using the synced config and verification keys.
#[tokio::test]
async fn test_ephemeral_receives_policy_via_livequery() -> anyhow::Result<()> {
    use ankurah_connector_local_process::LocalProcessConnection;

    let keys = common::test_keys();
    let public_pem = keys.public_key_pem()?;
    let config_json = include_str!("fixtures/simple_blog.json").to_string();

    // Create durable node with full agent
    let durable_agent = JwtAgent::new_durable(keys.clone(), blog_config_path())?;
    let node1 = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), durable_agent);
    node1.system.create().await?;

    // Manually create the JwtPolicy entity (simulating what the watcher does)
    let root_ctx = node1.context(JwtContext::system())?;
    {
        let trx = root_ctx.begin();
        trx.create(&JwtPolicy { config_json: config_json.clone(), public_key_pem: public_pem.clone() }).await?;
        trx.commit().await?;
    }

    // Create ephemeral node with deny-all agent (no keys, empty config)
    let ephemeral_agent = JwtAgent::new_ephemeral();
    let node2 = Node::new(Arc::new(SledStorageEngine::new_test()?), ephemeral_agent.clone());

    // Connect and wait for system ready
    let _conn = LocalProcessConnection::new(&node1, &node2).await?;
    node2.system.wait_system_ready().await;

    // Wait for the ephemeral agent to receive policy via LiveQuery
    let mut synced = false;
    for _ in 0..100 {
        if !ephemeral_agent.config().roles.is_empty() {
            synced = true;
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    assert!(synced, "Ephemeral agent should have received policy config from durable node");

    // Verify the config was correctly synced
    {
        let config = ephemeral_agent.config();
        assert!(config.roles.contains_key("Admin"));
        assert!(config.roles.contains_key("Editor"));
        assert!(config.roles.contains_key("Reader"));
        assert!(config.collections.contains_key("post"));
    }

    // Verify verification keys were set and can verify tokens from the durable node
    {
        let state_arc = ephemeral_agent.state_handle();
        let state_guard = state_arc.read().unwrap_or_else(|e| e.into_inner());
        let ephem_keys = state_guard.keys.as_ref().expect("Ephemeral should have verification keys after sync");
        let claims = make_claims("verify-test", &["Editor"], "verify@test.com");
        let token = keys.sign(&claims, Duration::from_hours(1))?;
        let verified = ephem_keys.verify(&token)?;
        assert_eq!(verified.sub, "verify-test");
    }

    // Editor should be able to create a post (has manage_posts privilege)
    let editor_claims = make_claims("editor-sync", &["Editor"], "editor@sync.com");
    let editor_token = sign_token(&keys, &editor_claims);
    let editor_ctx = JwtContext::from_claims(editor_claims, editor_token);
    let ctx2 = node2.context(editor_ctx)?;

    let trx = ctx2.begin();
    let _post = trx.create(&Post { title: "Synced Policy Post".into(), body: "RBAC works on ephemeral!".into() }).await?;
    trx.commit().await?;

    // Reader should NOT be able to create a post (RBAC enforced on ephemeral)
    let reader_claims = make_claims("reader-sync", &["Reader"], "reader@sync.com");
    let reader_token = sign_token(&keys, &reader_claims);
    let reader_ctx = JwtContext::from_claims(reader_claims, reader_token);
    let reader_context = node2.context(reader_ctx)?;

    let trx2 = reader_context.begin();
    let result = trx2.create(&Post { title: "Should Fail".into(), body: "No write access".into() }).await;
    assert!(result.is_err(), "Reader should not be able to create a post (RBAC enforced on ephemeral)");

    Ok(())
}
