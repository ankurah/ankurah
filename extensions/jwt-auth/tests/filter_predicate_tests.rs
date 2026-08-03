mod common;

use ankql::ast::Predicate;
use ankurah_core::policy::PolicyAgent;
use ankurah_jwt_auth::{JwtAgent, JwtClaims, JwtContext, JwtKeys, PolicyConfig};
use ankurah_proto::CollectionId;
use common::{blog_config_path, make_predicate};
use jwt_simple::prelude::Duration;

/// Root context returns predicate unchanged (bypasses all filtering)
#[test]
fn test_filter_predicate_privileged_bypasses() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys, blog_config_path()).unwrap();

    let ctx = JwtContext::Root;
    let predicate = make_predicate("title = 'hello'");
    let collection = CollectionId::from("post");

    let result = agent.filter_predicate(&ctx, &collection, predicate.clone()).unwrap();
    assert_eq!(result, predicate, "Root context should return predicate unchanged");
}

/// Collection without scope rules returns predicate unchanged
#[test]
fn test_filter_predicate_no_scope_rules() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path()).unwrap();

    let claims = JwtClaims {
        sub: "user-1".into(),
        roles: vec!["Editor".into()],
        email: "editor@blog.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();
    let ctx = JwtContext::from_claims(claims, token);
    let predicate = make_predicate("body = 'test'");
    let collection = CollectionId::from("comment");

    let result = agent.filter_predicate(&ctx, &collection, predicate.clone()).unwrap();
    assert_eq!(result, predicate, "No scope rules should return predicate unchanged");
}

/// Author role (lacks manage_posts) gets scope filter AND-ed in
#[test]
fn test_filter_predicate_applies_scope_rule() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path()).unwrap();

    let claims = JwtClaims {
        sub: "author-42".into(),
        roles: vec!["Author".into()],
        email: "author@blog.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();
    let ctx = JwtContext::from_claims(claims, token);
    let predicate = make_predicate("title = 'hello'");
    let collection = CollectionId::from("post");

    let result = agent.filter_predicate(&ctx, &collection, predicate).unwrap();

    match &result {
        Predicate::And(left, right) => {
            let display = format!("{}", right);
            assert!(display.contains("author-42"), "got: {}", display);
            let left_display = format!("{}", left);
            assert!(left_display.contains("hello"), "got: {}", left_display);
        }
        other => panic!("Expected AND predicate, got: {:?}", other),
    }
}

/// Editor role (has manage_posts) sees no scope filter (unless_privilege bypasses)
#[test]
fn test_filter_predicate_unless_privilege_bypasses() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path()).unwrap();

    let claims = JwtClaims {
        sub: "editor-1".into(),
        roles: vec!["Editor".into()],
        email: "editor@blog.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();
    let ctx = JwtContext::from_claims(claims, token);
    let predicate = make_predicate("title = 'hello'");
    let collection = CollectionId::from("post");

    let result = agent.filter_predicate(&ctx, &collection, predicate.clone()).unwrap();
    assert_eq!(result, predicate, "Editor should bypass the scope filter");
}

/// NoUser context with scope rules should return AccessDenied
#[test]
fn test_filter_predicate_nouser_denied() {
    let config_json = r#"{
        "roles": { "Worker": ["view_records"] },
        "collections": {
            "record": {
                "read": "view_records",
                "write": "manage_records",
                "scope": [{ "filter": "owner = $jwt.sub" }]
            }
        }
    }"#;
    let config: PolicyConfig = serde_json::from_str(config_json).unwrap();
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(config);

    let ctx = JwtContext::NoUser;
    let predicate = make_predicate("status = 'active'");
    let collection = CollectionId::from("record");

    let result = agent.filter_predicate(&ctx, &collection, predicate);
    assert!(result.is_err(), "NoUser context with scope rules should be denied");
}

/// Scope rule with no unless_privilege applies unconditionally
#[test]
fn test_filter_predicate_unconditional_scope_rule() {
    let config_json = r#"{
        "roles": { "Admin": ["*"], "Worker": ["view_records"] },
        "collections": {
            "record": {
                "read": "view_records",
                "write": "manage_records",
                "scope": [{ "filter": "owner = $jwt.sub" }]
            }
        }
    }"#;
    let config: PolicyConfig = serde_json::from_str(config_json).unwrap();

    let keys = common::test_keys();
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(config);
    agent.set_keys(JwtKeys::Signing(keys.clone()));

    let claims = JwtClaims {
        sub: "worker-1".into(),
        roles: vec!["Worker".into()],
        email: "w@co.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();
    let ctx = JwtContext::from_claims(claims, token);
    let predicate = make_predicate("status = 'active'");
    let collection = CollectionId::from("record");

    let result = agent.filter_predicate(&ctx, &collection, predicate).unwrap();
    let display = format!("{}", result);
    assert!(display.contains("worker-1"), "got: {}", display);
}

/// Injection attempt with quotes fails-closed
#[test]
fn test_filter_predicate_injection_payload_is_inert() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path()).unwrap();

    let payload = "'; DROP TABLE posts; --";
    let claims = JwtClaims {
        sub: payload.into(),
        roles: vec!["Author".into()],
        email: "ob@blog.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();
    let ctx = JwtContext::from_claims(claims, token);
    let predicate = make_predicate("title = 'hello'");
    let collection = CollectionId::from("post");

    // Claim values are populated into the parsed AST, never spliced into the
    // filter text — the payload lands as an ordinary string literal and the
    // scope clause structure is preserved intact.
    let result = agent.filter_predicate(&ctx, &collection, predicate).expect("injection payload must be inert, not an error");
    match &result {
        Predicate::And(left, right) => {
            let scope_display = format!("{}", right);
            assert!(scope_display.contains(payload), "payload should appear as an inert literal in: {}", scope_display);
            assert!(format!("{}", left).contains("hello"), "original predicate must be preserved");
        }
        other => panic!("Expected AND predicate, got: {:?}", other),
    }
}

/// Multiple authenticated contexts see the UNION of their scoped slices:
/// the effective filter is the OR of each context's narrowed predicate,
/// never just the first context's.
#[test]
fn test_filter_predicate_unions_across_contexts() {
    let config_json = r#"{
        "roles": { "Worker": ["view_records"] },
        "collections": {
            "record": {
                "read": "view_records",
                "write": "manage_records",
                "scope": [{ "filter": "owner = $jwt.sub" }]
            }
        }
    }"#;
    let config: PolicyConfig = serde_json::from_str(config_json).unwrap();

    let keys = common::test_keys();
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(config);
    agent.set_keys(JwtKeys::Signing(keys.clone()));

    let make_ctx = |sub: &str| {
        let claims = JwtClaims {
            sub: sub.into(),
            roles: vec!["Worker".into()],
            email: format!("{sub}@co.com"),
            name: None,
            custom: serde_json::Map::new(),
        };
        let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();
        JwtContext::from_claims(claims, token)
    };

    let contexts = vec![make_ctx("worker-1"), make_ctx("worker-2")];
    let predicate = make_predicate("status = 'active'");
    let collection = CollectionId::from("record");

    let result = agent.filter_predicate(&contexts, &collection, predicate).unwrap();

    match &result {
        Predicate::Or(first, second) => {
            let first = format!("{}", first);
            let second = format!("{}", second);
            assert!(first.contains("worker-1") && first.contains("active"), "first narrowing: {}", first);
            assert!(second.contains("worker-2") && second.contains("active"), "second narrowing: {}", second);
        }
        other => panic!("Expected OR of per-context narrowings, got: {:?}", other),
    }
}

/// The union is over AUTHORIZED contexts only: an authenticated context
/// whose roles lack the collection's read privilege contributes no
/// branch (its scoped slice would expose rows no authorized credential
/// could read), and a set with no authorized context is denied outright.
#[test]
fn test_filter_predicate_excludes_unauthorized_contexts() {
    let config_json = r#"{
        "roles": { "Worker": ["view_records"], "Guest": ["view_lobby"] },
        "collections": {
            "record": {
                "read": "view_records",
                "write": "manage_records",
                "scope": [{ "filter": "owner = $jwt.sub" }]
            }
        }
    }"#;
    let config: PolicyConfig = serde_json::from_str(config_json).unwrap();

    let keys = common::test_keys();
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(config);
    agent.set_keys(JwtKeys::Signing(keys.clone()));

    let make_ctx = |sub: &str, role: &str| {
        let claims = JwtClaims {
            sub: sub.into(),
            roles: vec![role.into()],
            email: format!("{sub}@co.com"),
            name: None,
            custom: serde_json::Map::new(),
        };
        let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();
        JwtContext::from_claims(claims, token)
    };

    let alice = make_ctx("alice-authorized", "Worker");
    let bob = make_ctx("bob-unauthorized", "Guest");
    let predicate = make_predicate("status = 'active'");
    let collection = CollectionId::from("record");

    let result = agent.filter_predicate(&vec![alice, bob.clone()], &collection, predicate.clone()).unwrap();
    let display = format!("{}", result);
    assert!(matches!(result, Predicate::And(..)), "one authorized context degenerates to its single narrowing, got: {}", display);
    assert!(display.contains("alice-authorized"), "the authorized context's scope must be present in: {}", display);
    assert!(!display.contains("bob-unauthorized"), "the unauthorized context must contribute no branch to: {}", display);

    let denied = agent.filter_predicate(&vec![bob], &collection, predicate);
    assert!(denied.is_err(), "a set with no authorized context is denied, got: {:?}", denied);
}
