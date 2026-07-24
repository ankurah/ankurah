mod common;

use ankurah_jwt_auth::{JwtAgent, JwtClaims, JwtContext};
use common::blog_config_path;

// ---------------------------------------------------------------------------
// JwtContext accessors
// ---------------------------------------------------------------------------

#[test]
fn test_jwt_context_accessors() {
    let claims = JwtClaims {
        sub: "user-42".into(),
        roles: vec!["Editor".into()],
        email: "editor@blog.com".into(),
        name: Some("Jane".into()),
        custom: serde_json::Map::new(),
    };
    let ctx = JwtContext::from_claims(claims, "fake-token".into());
    assert_eq!(ctx.user_id(), Some("user-42"));
    assert_eq!(ctx.roles(), &[String::from("Editor")]);
    assert_eq!(ctx.email(), Some("editor@blog.com"));
    assert_eq!(ctx.name(), Some("Jane"));
    assert_eq!(ctx.token_bytes(), Some(b"fake-token".as_slice()));
}

/// Equality is full-value (operational identity per the ContextData
/// contract): same subject with different claims or token compares
/// UNEQUAL — Session update delivery gates on this, and a token refresh
/// must register as a change — and only an identical value compares
/// equal, with the hash agreeing.
#[test]
fn test_jwt_context_equality_is_full_value() {
    let ctx1 = JwtContext::from_claims(
        JwtClaims {
            sub: "user-1".into(),
            roles: vec!["Admin".into()],
            email: "a@b.com".into(),
            name: None,
            custom: serde_json::Map::new(),
        },
        "token-1".into(),
    );
    // Same subject, different claims and token: a re-login or refresh.
    let ctx2 = JwtContext::from_claims(
        JwtClaims {
            sub: "user-1".into(),
            roles: vec!["Reader".into()],
            email: "x@y.com".into(),
            name: Some("Other".into()),
            custom: serde_json::Map::new(),
        },
        "token-2".into(),
    );
    let ctx3 = JwtContext::from_claims(
        JwtClaims {
            sub: "user-2".into(),
            roles: vec!["Admin".into()],
            email: "a@b.com".into(),
            name: None,
            custom: serde_json::Map::new(),
        },
        "token-3".into(),
    );
    assert_ne!(ctx1, ctx2, "same subject with different claims or token is a different credential");
    assert_ne!(ctx1, ctx3);
    let identical = ctx1.clone();
    assert_eq!(ctx1, identical, "only an identical value compares equal");

    let mut set = std::collections::HashSet::new();
    set.insert(ctx1);
    assert!(set.contains(&identical), "hash agrees with eq");
    assert!(!set.contains(&ctx2));
}

/// Hash agrees with Eq over custom claims regardless of map insertion
/// order: json map equality is order-independent, so the hash walks
/// objects in sorted key order.
#[test]
fn test_jwt_context_hash_ignores_custom_claim_insertion_order() {
    let mut ab = serde_json::Map::new();
    ab.insert("a".into(), serde_json::json!({"n": 1, "s": ["x", 2.5, null]}));
    ab.insert("b".into(), serde_json::json!(true));
    let mut ba = serde_json::Map::new();
    ba.insert("b".into(), serde_json::json!(true));
    ba.insert("a".into(), serde_json::json!({"n": 1, "s": ["x", 2.5, null]}));

    let claims = |custom: serde_json::Map<String, serde_json::Value>| JwtClaims {
        sub: "user-1".into(),
        roles: vec![],
        email: "a@b.com".into(),
        name: None,
        custom,
    };
    let c1 = JwtContext::from_claims(claims(ab), "t".into());
    let c2 = JwtContext::from_claims(claims(ba), "t".into());
    assert_eq!(c1, c2, "map equality is insertion-order independent");

    let mut set = std::collections::HashSet::new();
    set.insert(c1);
    assert!(set.contains(&c2), "the hash must agree");
}

// ---------------------------------------------------------------------------
// Root context
// ---------------------------------------------------------------------------

#[test]
fn test_root_context_is_privileged() {
    let ctx = JwtContext::system();
    assert!(ctx.is_privileged());
    assert_eq!(ctx.user_id(), None);
    assert_eq!(ctx.roles(), &[] as &[String]);
    assert_eq!(ctx.email(), None);
    assert_eq!(ctx.name(), None);
    assert_eq!(ctx.token_bytes(), None);
}

#[test]
fn test_root_context_cannot_produce_auth_data() {
    let ctx = JwtContext::system();
    assert!(ctx.auth_data().is_err());
}

#[test]
fn test_root_context_equality() {
    let root1 = JwtContext::system();
    let root2 = JwtContext::system();
    assert_eq!(root1, root2);

    let user = JwtContext::from_claims(
        JwtClaims { sub: "user-1".into(), roles: vec![], email: "a@b.com".into(), name: None, custom: serde_json::Map::new() },
        "token".into(),
    );
    assert_ne!(root1, user);
}

#[test]
fn test_authenticated_context_is_not_privileged() {
    let ctx = JwtContext::from_claims(
        JwtClaims { sub: "user-1".into(), roles: vec![], email: "a@b.com".into(), name: None, custom: serde_json::Map::new() },
        "token".into(),
    );
    assert!(!ctx.is_privileged());
}

#[test]
fn test_authenticated_context_auth_data() {
    let ctx = JwtContext::from_claims(
        JwtClaims { sub: "user-1".into(), roles: vec![], email: "a@b.com".into(), name: None, custom: serde_json::Map::new() },
        "my-token".into(),
    );
    let auth_data = ctx.auth_data().unwrap();
    assert_eq!(auth_data.0, b"my-token");
}

// ---------------------------------------------------------------------------
// NoUser context
// ---------------------------------------------------------------------------

#[test]
fn test_nouser_context_is_not_privileged() {
    let ctx = JwtContext::NoUser;
    assert!(!ctx.is_privileged());
}

#[test]
fn test_nouser_context_has_no_identity() {
    let ctx = JwtContext::NoUser;
    assert_eq!(ctx.user_id(), None);
    assert_eq!(ctx.roles(), &[] as &[String]);
    assert_eq!(ctx.email(), None);
    assert_eq!(ctx.name(), None);
    assert_eq!(ctx.token_bytes(), None);
}

#[test]
fn test_nouser_context_produces_empty_auth_data() {
    let ctx = JwtContext::NoUser;
    let auth = ctx.auth_data().unwrap();
    assert!(auth.0.is_empty(), "NoUser auth data should be empty");
}

#[test]
fn test_nouser_context_equality() {
    assert_eq!(JwtContext::NoUser, JwtContext::NoUser);
    assert_ne!(JwtContext::NoUser, JwtContext::Root);
    assert_ne!(
        JwtContext::NoUser,
        JwtContext::from_claims(
            JwtClaims { sub: "u".into(), roles: vec![], email: "a@b.com".into(), name: None, custom: serde_json::Map::new() },
            "t".into(),
        )
    );
}

#[test]
fn test_nouser_can_access_jwtpolicy_collection() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys, blog_config_path()).unwrap();

    use ankurah_core::policy::PolicyAgent;
    let ctx = JwtContext::NoUser;
    let models = common::policy_models(&agent, &["jwtpolicy", "post"]);
    let jwtpolicy = models.id("jwtpolicy");
    assert!(agent.can_access_collection(&ctx, &jwtpolicy).is_ok());

    // But NoUser cannot access other collections
    let post = models.id("post");
    assert!(agent.can_access_collection(&ctx, &post).is_err());
}
