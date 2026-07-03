mod common;

use ankurah::Model;
use ankurah::Node;
use ankurah_jwt_auth::{JwtAgent, JwtContext};
use ankurah_proto::CollectionId;
use ankurah_storage_sled::SledStorageEngine;
use common::{blog_config_path, load_blog_config, make_claims, sign_token};
use std::sync::Arc;

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct Post {
    #[active_type(LWW)]
    pub title: String,
    #[active_type(LWW)]
    pub body: String,
}

// ---- Role escalation (server-side RBAC) ---------------------------------

/// A Reader-role user on the ephemeral node attempts to create a Post.
#[tokio::test]
async fn test_reader_cannot_write_post_via_ephemeral() -> anyhow::Result<()> {
    use ankurah_connector_local_process::LocalProcessConnection;

    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path())?;

    let node1 = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node1.system.create().await?;

    let node2 = Node::new(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    let _conn = LocalProcessConnection::new(&node1, &node2).await?;
    node2.system.wait_system_ready().await;

    let reader_claims = make_claims("reader-adversarial", &["Reader"], "reader@adversarial.com");
    let reader_token = sign_token(&keys, &reader_claims);
    let reader_ctx = JwtContext::from_claims(reader_claims, reader_token);
    let context = node2.context(reader_ctx)?;

    let trx = context.begin();
    let result = trx.create(&Post { title: "Reader Escalation Attempt".into(), body: "Should be denied by server RBAC".into() }).await;

    assert!(result.is_err(), "Reader must not be able to create a post via the ephemeral node");

    Ok(())
}

/// A Reader-role user attempts to write to the "user" collection.
#[tokio::test]
async fn test_reader_cannot_write_to_user_collection() -> anyhow::Result<()> {
    use ankurah_core::policy::PolicyAgent;

    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path())?;

    let reader_claims = make_claims("reader-user-attack", &["Reader"], "reader@attack.com");
    let reader_token = sign_token(&keys, &reader_claims);
    let reader_ctx = JwtContext::from_claims(reader_claims, reader_token);

    let user_collection = CollectionId::from("user");
    let result = agent.can_access_collection(&reader_ctx, &user_collection);
    assert!(result.is_err(), "Reader must not be able to access the user collection");

    let config = load_blog_config();
    assert!(
        !config.can_write_collection(&[String::from("Reader")], &user_collection),
        "Reader role must not have write access to the user collection"
    );
    assert!(
        !config.can_access_collection(&[String::from("Reader")], &user_collection),
        "Reader role must not have any access to the user collection"
    );

    Ok(())
}

/// A NoUser context (unauthenticated) attempts to write to any collection.
#[tokio::test]
async fn test_nouser_cannot_write_anything() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path())?;

    let storage = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage), agent);
    node.system.create().await?;

    let ctx = JwtContext::NoUser;
    let context = node.context(ctx)?;

    let trx = context.begin();
    let result = trx.create(&Post { title: "NoUser Post".into(), body: "Should be denied".into() }).await;

    assert!(result.is_err(), "NoUser must not be able to write to any collection");

    Ok(())
}

// ---- Root context wire safety -------------------------------------------

/// Root context must not be serializable to auth data for wire transmission.
#[tokio::test]
async fn test_root_context_cannot_be_sent_over_wire() -> anyhow::Result<()> {
    use ankurah_connector_local_process::LocalProcessConnection;

    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path())?;

    let node1 = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node1.system.create().await?;

    let node2 = Node::new(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    let _conn = LocalProcessConnection::new(&node1, &node2).await?;
    node2.system.wait_system_ready().await;

    let root_ctx = JwtContext::system();

    assert!(root_ctx.auth_data().is_err(), "Root context must not produce auth_data");

    let context = node2.context(root_ctx)?;
    let trx = context.begin();
    let create_result = trx.create(&Post { title: "Root Over Wire".into(), body: "Should fail at sign_request".into() }).await;

    if create_result.is_ok() {
        let commit_result = trx.commit().await;
        assert!(commit_result.is_err(), "Root context must fail at commit when relaying to durable node");
    }

    Ok(())
}

// ---- jwtpolicy write protection -----------------------------------------

/// Only Root (privileged) contexts may write to the jwtpolicy collection.
#[tokio::test]
async fn test_non_privileged_cannot_write_jwtpolicy() -> anyhow::Result<()> {
    use ankurah_jwt_auth::JwtPolicy;

    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path())?;

    let storage = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage), agent);
    node.system.create().await?;

    let admin_claims = make_claims("admin-1", &["Admin"], "admin@blog.com");
    let admin_token = sign_token(&keys, &admin_claims);
    let admin_ctx = JwtContext::from_claims(admin_claims, admin_token);
    let context = node.context(admin_ctx)?;

    let trx = context.begin();
    let result = trx.create(&JwtPolicy { config_json: r#"{"roles":{},"collections":{}}"#.into(), public_key_pem: "fake-pem".into() }).await;

    assert!(result.is_err(), "Admin (non-Root) must not be able to write to the jwtpolicy collection");

    let root_ctx = JwtContext::system();
    let root_context = node.context(root_ctx)?;
    let trx2 = root_context.begin();
    let root_result =
        trx2.create(&JwtPolicy { config_json: r#"{"roles":{},"collections":{}}"#.into(), public_key_pem: "fake-pem".into() }).await;

    assert!(root_result.is_ok(), "Root must be able to write to the jwtpolicy collection");
    trx2.commit().await?;

    Ok(())
}

// ---- NoUser collection access -------------------------------------------

/// NoUser can read jwtpolicy but must not access any other collection.
#[tokio::test]
async fn test_nouser_can_read_jwtpolicy_but_not_other_collections() -> anyhow::Result<()> {
    use ankurah_core::policy::PolicyAgent;

    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys, blog_config_path())?;

    let nouser = JwtContext::NoUser;

    let jwtpolicy = CollectionId::from("jwtpolicy");
    assert!(agent.can_access_collection(&nouser, &jwtpolicy).is_ok(), "NoUser must be able to access jwtpolicy for bootstrap");

    for name in &["post", "user", "comment", "tag", "secret_stuff"] {
        let col = CollectionId::from(*name);
        assert!(agent.can_access_collection(&nouser, &col).is_err(), "NoUser must not be able to access the {} collection", name);
    }

    Ok(())
}
