mod common;

use ankurah::{Model, Node, Ref};
use ankurah_core::model::Mutable;
use ankurah_core::policy::PolicyAgent;
use ankurah_jwt_auth::{JwtAgent, JwtClaims, JwtContext, JwtKeys, PolicyConfig};
use ankurah_proto::{Clock, Event, OperationSet};
use ankurah_storage_sled::SledStorageEngine;
use common::{blog_config_path, make_claims, sign_token};
use jwt_simple::prelude::Duration;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct Post {
    #[active_type(LWW)]
    pub title: String,
    #[active_type(LWW)]
    pub body: String,
}

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct Account {
    pub name: String,
}

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct ScopedRecord {
    pub account: Ref<Account>,
    pub label: String,
}

#[tokio::test]
async fn test_jwt_agent_single_durable_node() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path())?;

    let storage = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage), agent);
    node.system.create().await?;

    let claims = make_claims("editor-1", &["Editor"], "editor@blog.com");
    let token = sign_token(&keys, &claims);
    let ctx = JwtContext::from_claims(claims, token);
    let context = node.context(ctx)?;

    let trx = context.begin();
    let _post = trx.create(&Post { title: "Hello World".into(), body: "First post!".into() }).await?;
    trx.commit().await?;

    let results = context.fetch::<PostView>("title = 'Hello World'").await?;
    assert_eq!(results.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_jwt_agent_reader_cannot_write_post() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path())?;

    let storage = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage), agent);
    node.system.create().await?;

    let claims = make_claims("reader-1", &["Reader"], "reader@blog.com");
    let token = sign_token(&keys, &claims);
    let ctx = JwtContext::from_claims(claims, token);
    let context = node.context(ctx)?;

    let trx = context.begin();
    let result = trx.create(&Post { title: "Sneaky Post".into(), body: "Should fail".into() }).await;
    assert!(result.is_err(), "Reader should not be able to create a post");

    Ok(())
}

#[tokio::test]
async fn test_write_scope_allows_matching_ref_and_denies_other_ref() -> anyhow::Result<()> {
    let config_json = r#"{
        "roles": {
            "AccountWriter": ["manage_scoped_records"]
        },
        "collections": {
            "scopedrecord": {
                "read": "manage_scoped_records",
                "write": "manage_scoped_records",
                "scope": [
                    { "filter": "account = $jwt.custom.account_id" }
                ]
            }
        }
    }"#;

    let keys = common::test_keys();
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(serde_json::from_str::<PolicyConfig>(config_json)?);
    agent.set_keys(JwtKeys::Signing(keys.clone()));

    let storage = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage), agent);
    node.system.create().await?;

    let root = node.context(JwtContext::system())?;
    // Schema definition is the ADMIN's act (rev 4: registration is
    // policy-gated, and this config grants the writer role no catalog
    // privileges): the system context registers the collection; the scoped
    // writer below only writes data into it.
    root.register::<ScopedRecord>().await?;
    let (allowed_account_id, denied_account_id) = {
        let trx = root.begin();
        let allowed_account = trx.create(&Account { name: "Allowed".into() }).await?;
        let denied_account = trx.create(&Account { name: "Denied".into() }).await?;
        let ids = (allowed_account.id(), denied_account.id());
        trx.commit().await?;
        ids
    };

    let mut custom = serde_json::Map::new();
    custom.insert("account_id".to_string(), serde_json::Value::String(allowed_account_id.to_base64()));
    let claims =
        JwtClaims { sub: "writer-1".into(), roles: vec!["AccountWriter".into()], email: "writer@example.com".into(), name: None, custom };
    let token = keys.sign(&claims, Duration::from_hours(1))?;
    let context = node.context(JwtContext::from_claims(claims, token))?;

    let trx = context.begin();
    trx.create(&ScopedRecord { account: allowed_account_id.clone().into(), label: "allowed".into() }).await?;
    trx.commit().await?;

    let trx = context.begin();
    let denied = trx.create(&ScopedRecord { account: denied_account_id.into(), label: "denied".into() }).await;
    assert!(denied.is_err(), "write scope must deny records outside the JWT account claim");

    Ok(())
}

#[tokio::test]
async fn test_read_scope_denies_direct_get_for_out_of_scope_resident_entity() -> anyhow::Result<()> {
    let config_json = r#"{
        "roles": {
            "AccountReader": ["read_scoped_records"]
        },
        "collections": {
            "scopedrecord": {
                "read": "read_scoped_records",
                "write": null,
                "scope": [
                    { "filter": "account = $jwt.custom.account_id" }
                ]
            }
        }
    }"#;

    let keys = common::test_keys();
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(serde_json::from_str::<PolicyConfig>(config_json)?);
    agent.set_keys(JwtKeys::Signing(keys.clone()));

    let storage = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage), agent);
    node.system.create().await?;

    let root = node.context(JwtContext::system())?;
    let (allowed_record_id, denied_record_id, allowed_account_id) = {
        let trx = root.begin();
        let allowed_account = trx.create(&Account { name: "Allowed".into() }).await?;
        let denied_account = trx.create(&Account { name: "Denied".into() }).await?;
        let allowed_record = trx.create(&ScopedRecord { account: allowed_account.id().into(), label: "allowed".into() }).await?;
        let denied_record = trx.create(&ScopedRecord { account: denied_account.id().into(), label: "denied".into() }).await?;
        let ids = (allowed_record.id(), denied_record.id(), allowed_account.id());
        trx.commit().await?;
        ids
    };

    let mut custom = serde_json::Map::new();
    custom.insert("account_id".to_string(), serde_json::Value::String(allowed_account_id.to_base64()));
    let claims =
        JwtClaims { sub: "reader-1".into(), roles: vec!["AccountReader".into()], email: "reader@example.com".into(), name: None, custom };
    let token = keys.sign(&claims, Duration::from_hours(1))?;
    let context = node.context(JwtContext::from_claims(claims, token))?;

    let allowed = context.get::<ScopedRecordView>(allowed_record_id).await?;
    assert_eq!(allowed.label().unwrap(), "allowed");

    let denied = context.get::<ScopedRecordView>(denied_record_id).await;
    assert!(denied.is_err(), "direct get must deny resident rows outside the JWT account claim");

    Ok(())
}

#[tokio::test]
async fn test_update_scope_requires_before_and_after_state() -> anyhow::Result<()> {
    let config_json = r#"{
        "roles": {
            "AccountWriter": ["manage_scoped_records"]
        },
        "collections": {
            "scopedrecord": {
                "read": "manage_scoped_records",
                "write": "manage_scoped_records",
                "scope": [
                    { "filter": "account = $jwt.custom.account_id" }
                ]
            }
        }
    }"#;

    let keys = common::test_keys();
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(serde_json::from_str::<PolicyConfig>(config_json)?);
    agent.set_keys(JwtKeys::Signing(keys.clone()));

    let storage = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage), agent.clone());
    node.system.create().await?;

    let root = node.context(JwtContext::system())?;
    let (allowed_account_id, denied_record, allowed_record) = {
        let trx = root.begin();
        let allowed_account = trx.create(&Account { name: "Allowed".into() }).await?;
        let denied_account = trx.create(&Account { name: "Denied".into() }).await?;
        let denied_record = trx.create(&ScopedRecord { account: denied_account.id().into(), label: "denied".into() }).await?;
        let allowed_record = trx.create(&ScopedRecord { account: allowed_account.id().into(), label: "allowed".into() }).await?;
        let values = (allowed_account.id(), denied_record.entity().clone(), allowed_record.entity().clone());
        trx.commit().await?;
        values
    };

    let mut custom = serde_json::Map::new();
    custom.insert("account_id".to_string(), serde_json::Value::String(allowed_account_id.to_base64()));
    let claims =
        JwtClaims { sub: "writer-1".into(), roles: vec!["AccountWriter".into()], email: "writer@example.com".into(), name: None, custom };
    let token = keys.sign(&claims, Duration::from_hours(1))?;
    let context = JwtContext::from_claims(claims, token);

    let retag_event = Event {
        entity_id: denied_record.id(),
        // #330: Event carries a model id, not a collection name; ScopedRecord was
        // registered by the creates above. check_event keys on the entity's
        // collection, not this field, but the struct still requires a valid id.
        model: node.catalog.model_id_for(denied_record.collection().as_str()).expect("ScopedRecord registered by the creates above"),
        body: ankurah_proto::EventBody::Update { operations: OperationSet(BTreeMap::new()) },
        parent: Clock::new(denied_record.head().to_vec()),
    };

    let result = agent.check_event(&node, &context, &denied_record, &allowed_record, &retag_event);
    assert!(result.is_err(), "updates must not retag an out-of-scope row into the caller's scope");

    Ok(())
}

#[tokio::test]
async fn test_jwt_agent_durable_ephemeral_pair() -> anyhow::Result<()> {
    use ankurah_connector_local_process::LocalProcessConnection;

    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path())?;

    let node1 = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    let node2 = Node::new(Arc::new(SledStorageEngine::new_test()?), agent.clone());

    node1.system.create().await?;
    let _conn = LocalProcessConnection::new(&node1, &node2).await?;
    node2.system.wait_system_ready().await;

    let editor_claims = make_claims("editor-1", &["Editor"], "editor@blog.com");
    let editor_token = sign_token(&keys, &editor_claims);
    let editor_ctx = JwtContext::from_claims(editor_claims, editor_token);
    let ctx1 = node1.context(editor_ctx)?;

    let post_id = {
        let trx = ctx1.begin();
        let post = trx.create(&Post { title: "Cross-Node Post".into(), body: "Created on node1".into() }).await?;
        let id = post.id();
        trx.commit().await?;
        id
    };

    let editor_claims2 = make_claims("editor-1", &["Editor"], "editor@blog.com");
    let editor_token2 = sign_token(&keys, &editor_claims2);
    let editor_ctx2 = JwtContext::from_claims(editor_claims2, editor_token2);
    let ctx2 = node2.context(editor_ctx2)?;

    let results = ctx2.fetch::<PostView>("title = 'Cross-Node Post'").await?;
    assert_eq!(results.len(), 1);

    let fetched = &results[0];
    assert_eq!(fetched.id(), post_id);

    Ok(())
}

#[tokio::test]
async fn test_jwt_check_request_roundtrip() -> anyhow::Result<()> {
    use ankurah_connector_local_process::LocalProcessConnection;

    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path())?;

    let node1 = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    let node2 = Node::new(Arc::new(SledStorageEngine::new_test()?), agent.clone());

    node1.system.create().await?;
    let _conn = LocalProcessConnection::new(&node1, &node2).await?;
    node2.system.wait_system_ready().await;

    let claims = make_claims("editor-2", &["Editor"], "ed2@blog.com");
    let token = sign_token(&keys, &claims);
    let ctx = JwtContext::from_claims(claims, token);
    let ctx2 = node2.context(ctx)?;

    let trx = ctx2.begin();
    let _post = trx.create(&Post { title: "From Ephemeral".into(), body: "Sent via JWT auth".into() }).await?;
    trx.commit().await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let editor_claims_n1 = make_claims("editor-2", &["Editor"], "ed2@blog.com");
    let token_n1 = sign_token(&keys, &editor_claims_n1);
    let ctx_n1 = JwtContext::from_claims(editor_claims_n1, token_n1);
    let ctx1 = node1.context(ctx_n1)?;

    let results = ctx1.fetch::<PostView>("title = 'From Ephemeral'").await?;
    assert_eq!(results.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_jwtpolicy_collection_always_accessible() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path())?;

    let collection = ankurah_proto::CollectionId::from("jwtpolicy");

    let claims = make_claims("reader-1", &["Reader"], "reader@blog.com");
    let token = sign_token(&keys, &claims);
    let ctx = JwtContext::from_claims(claims, token);

    use ankurah_core::policy::PolicyAgent;
    let result = agent.can_access_collection(&ctx, &collection);
    assert!(result.is_ok(), "jwtpolicy collection should always be accessible");

    Ok(())
}

#[tokio::test]
async fn test_jwt_agent_collection_access_denied() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path())?;

    let collection = ankurah_proto::CollectionId::from("post");

    let claims = make_claims("reader-1", &["Reader"], "reader@blog.com");
    let token = sign_token(&keys, &claims);
    let ctx = JwtContext::from_claims(claims, token);

    use ankurah_core::policy::PolicyAgent;
    let result = agent.can_access_collection(&ctx, &collection);
    assert!(result.is_err(), "Reader should not be able to access post collection");

    Ok(())
}

#[tokio::test]
async fn test_root_context_bypasses_all_policy_checks() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path())?;

    let storage = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage), agent);
    node.system.create().await?;

    let root_ctx = JwtContext::system();
    let context = node.context(root_ctx)?;

    let trx = context.begin();
    let _post = trx.create(&Post { title: "Root Post".into(), body: "Created by root".into() }).await?;
    trx.commit().await?;

    let results = context.fetch::<PostView>("title = 'Root Post'").await?;
    assert_eq!(results.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_root_bypasses_collection_access() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path())?;

    use ankurah_core::policy::PolicyAgent;
    let root = JwtContext::system();

    let post = ankurah_proto::CollectionId::from("post");
    let secret = ankurah_proto::CollectionId::from("secret_stuff");
    assert!(agent.can_access_collection(&root, &post).is_ok());
    assert!(agent.can_access_collection(&root, &secret).is_ok());

    Ok(())
}
