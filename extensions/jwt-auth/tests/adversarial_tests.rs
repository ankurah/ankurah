mod common;

use ankurah::Model;
use ankurah::Node;
use ankurah_jwt_auth::{JwtAgent, JwtClaims, JwtContext};
use ankurah_storage_sled::SledStorageEngine;
use common::{blog_config_path, make_claims};
use jwt_simple::prelude::Duration;
use std::sync::Arc;

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct Post {
    #[active_type(LWW)]
    pub title: String,
    #[active_type(LWW)]
    pub body: String,
}

// ---- 1. Token attacks (server-side rejection) ---------------------------

/// A client signs a JWT with different keys than the server expects.
#[tokio::test]
async fn test_server_rejects_wrongly_signed_token() -> anyhow::Result<()> {
    use ankurah_connector_local_process::LocalProcessConnection;

    let server_keys = common::test_keys();
    let attacker_keys = common::test_keys_alt();
    let agent = JwtAgent::new_durable(server_keys.clone(), blog_config_path())?;

    let node1 = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node1.system.create().await?;

    let node2 = Node::new(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    let _conn = LocalProcessConnection::new(&node1, &node2).await?;
    node2.system.wait_system_ready().await;

    let claims = JwtClaims {
        sub: "attacker-1".into(),
        roles: vec!["Admin".into()],
        email: "attacker@evil.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let bad_token = attacker_keys.sign(&claims, Duration::from_hours(24))?;
    let ctx = JwtContext::from_claims(claims, bad_token);
    let context = node2.context(ctx)?;

    let trx = context.begin();
    let create_result = trx.create(&Post { title: "Wrongly Signed".into(), body: "Should be rejected by server".into() }).await;

    if create_result.is_ok() {
        let commit_result = trx.commit().await;
        assert!(commit_result.is_err(), "Server must reject a JWT signed with a different key at commit time");
    }

    Ok(())
}

/// Verify that expired tokens are correctly rejected with strict time tolerance.
#[tokio::test]
async fn test_server_rejects_expired_token() -> anyhow::Result<()> {
    let keys = common::test_keys();

    let claims = make_claims("expired-user", &["Editor"], "expired@blog.com");
    let short_lived_token = keys.sign(&claims, Duration::from_millis(1))?;

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    use jwt_simple::prelude::*;
    let pub_key = RS256PublicKey::from_pem(&keys.public_key_pem()?)?;
    let strict_opts = VerificationOptions { time_tolerance: Some(jwt_simple::prelude::Duration::from_secs(0)), ..Default::default() };
    let strict_result = pub_key.verify_token::<JwtClaims>(&short_lived_token, Some(strict_opts));
    assert!(strict_result.is_err(), "Expired token must be rejected when verified with zero time tolerance");

    let lenient_result = pub_key.verify_token::<JwtClaims>(&short_lived_token, None);
    assert!(lenient_result.is_ok(), "Expired-by-50ms token should still pass with default 15min tolerance (control)");

    Ok(())
}

/// Test that check_request rejects garbage (non-JWT) auth data.
#[tokio::test]
async fn test_server_rejects_garbage_auth_data() -> anyhow::Result<()> {
    use ankurah_core::policy::PolicyAgent;

    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path())?;

    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node.system.create().await?;

    let garbage_auth = vec![ankurah_proto::AuthData(b"NOT_A_JWT_TOKEN".to_vec())];

    let request = ankurah_proto::NodeRequest {
        id: ankurah_proto::RequestId::new(),
        to: ankurah_proto::EntityId::new(),
        from: ankurah_proto::EntityId::new(),
        body: ankurah_proto::NodeRequestBody::Fetch {
            model: ankurah::ModelId::EntityId(ankurah::EntityId::new()),
            selection: "1 = 1".try_into().unwrap(),
            known_matches: vec![],
        },
    };

    let result = agent.check_request(&node, &garbage_auth, &request).await;
    assert!(result.is_err(), "check_request must reject garbage (non-JWT) auth data");

    let empty_string_auth = vec![ankurah_proto::AuthData(b" ".to_vec())];
    let result2 = agent.check_request(&node, &empty_string_auth, &request).await;
    assert!(result2.is_err(), "check_request must reject whitespace-only auth data");

    let partial_jwt_auth = vec![ankurah_proto::AuthData(b"eyJhbGciOiJSUzI1NiJ9.bad.data".to_vec())];
    let result3 = agent.check_request(&node, &partial_jwt_auth, &request).await;
    assert!(result3.is_err(), "check_request must reject malformed JWT tokens");

    Ok(())
}
