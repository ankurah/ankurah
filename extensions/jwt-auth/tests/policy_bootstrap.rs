mod common;

use std::sync::Arc;

use ankurah::Node;
use ankurah_core::storage::StorageEngine;
use ankurah::signals::Wait;
use ankurah_jwt_auth::{JwtAgent, JwtContext, JwtKeys, ModelPolicyView, RuleStatus};
use ankurah_storage_sled::SledStorageEngine;

#[tokio::test]
async fn policy_is_written_explicitly_and_loaded_on_restart() -> anyhow::Result<()> {
    let storage = Arc::new(SledStorageEngine::new_test()?);
    let keys = common::test_keys();
    let policy_id = {
        let policy = JwtAgent::new_ephemeral();
        policy.set_keys(JwtKeys::Signing(keys.clone()));
        let node = Node::new_durable(storage.clone(), policy.clone());
        node.system.create().await?;
        policy.set_policy_from_file(&node, common::blog_config_path()).await?;
        node.wait_ready().await?;
        assert!(policy.config().collections.contains_key("post"));
        assert!(policy.signing_keys().is_some(), "loading the public policy must retain the local signing key");
        let context = node.context_async(JwtContext::system()).await?;
        context.fetch::<ModelPolicyView>("true").await?[0].id()
    };
    let head = storage.get_state(policy_id).await?.payload.state.head;

    // A different local file is not an instruction to replace this system's policy.
    let path = format!("{}/tests/fixtures/simple_minimal.json", env!("CARGO_MANIFEST_DIR"));
    let policy = JwtAgent::new_durable(keys, path)?;
    let node = Node::new_durable(storage.clone(), policy.clone());
    node.wait_ready().await?;
    assert!(policy.config().collections.contains_key("post"));
    let context = node.context_async(JwtContext::system()).await?;
    let policies = context.fetch::<ModelPolicyView>("true").await?;
    assert!(policies.iter().any(|policy| policy.id() == policy_id));
    assert_eq!(storage.get_state(policy_id).await?.payload.state.head, head);
    drop(context);
    drop(node);
    drop(policy);

    // A durable restart can load policy without having any source file or keys supplied.
    let policy = JwtAgent::new_ephemeral();
    let node = Node::new_durable(storage.clone(), policy.clone());
    node.wait_ready().await?;
    assert!(policy.config().collections.contains_key("post"));
    assert_eq!(storage.get_state(policy_id).await?.payload.state.head, head);

    // A bad file must leave the stored policy untouched.
    let invalid = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
    assert!(policy.set_policy_from_file(&node, invalid).await.is_err());
    assert_eq!(storage.get_state(policy_id).await?.payload.state.head, head);

    let replacement = common::load_minimal_config();
    policy.set_policy(&node, &replacement).await?;
    let context = node.context_async(JwtContext::system()).await?;
    let stored = context.get::<ModelPolicyView>(policy_id).await?;
    assert_eq!(stored.status()?, RuleStatus::Retired);
    policy.state_handle().wait_for(|state| state.config.collections.is_empty().then_some(())).await;
    Ok(())
}
