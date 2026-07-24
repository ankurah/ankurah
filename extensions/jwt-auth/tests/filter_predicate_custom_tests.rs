mod common;

use ankurah_core::policy::PolicyAgent;
use ankurah_jwt_auth::{JwtAgent, JwtClaims, JwtContext, JwtKeys, PolicyConfig};
use common::make_predicate;
use jwt_simple::prelude::Duration;

/// Test $jwt.custom.field_office substitution
#[test]
fn test_filter_predicate_custom_claim_variable() {
    let config_json = r#"{
        "roles": {
            "Admin": ["*"],
            "FieldWorker": ["view_records"]
        },
        "collections": {
            "record": {
                "read": "view_records",
                "write": "manage_records",
                "scope": [
                    {
                        "filter": "field_office = $jwt.custom.field_office",
                        "unless_privilege": "view_all_records"
                    }
                ]
            }
        }
    }"#;
    let config: PolicyConfig = serde_json::from_str(config_json).unwrap();

    let keys = common::test_keys();
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(config);
    agent.set_keys(JwtKeys::Signing(keys.clone()));

    let mut custom = serde_json::Map::new();
    custom.insert("field_office".to_string(), serde_json::Value::String("NYC".to_string()));
    let claims = JwtClaims { sub: "worker-1".into(), roles: vec!["FieldWorker".into()], email: "worker@co.com".into(), name: None, custom };
    let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();
    let ctx = JwtContext::from_claims(claims, token);
    let predicate = make_predicate("status = 'active'");
    let models = common::policy_models(&agent, &["record"]);
    let collection = models.id("record");

    let result = agent.filter_predicate(&ctx, &collection, predicate).unwrap();

    let display = format!("{}", result);
    assert!(display.contains("NYC"), "got: {}", display);
}

/// Missing custom claim returns AccessDenied
#[test]
fn test_filter_predicate_missing_claim_denied() {
    let config_json = r#"{
        "roles": {
            "Worker": ["view_records"]
        },
        "collections": {
            "record": {
                "read": "view_records",
                "write": "manage_records",
                "scope": [
                    {
                        "filter": "field_office = $jwt.custom.field_office",
                        "unless_privilege": "view_all_records"
                    }
                ]
            }
        }
    }"#;
    let config: PolicyConfig = serde_json::from_str(config_json).unwrap();

    let keys = common::test_keys();
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(config);
    agent.set_keys(JwtKeys::Signing(keys.clone()));

    let claims = JwtClaims {
        sub: "worker-2".into(),
        roles: vec!["Worker".into()],
        email: "worker2@co.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();
    let ctx = JwtContext::from_claims(claims, token);
    let predicate = make_predicate("status = 'active'");
    let models = common::policy_models(&agent, &["record"]);
    let collection = models.id("record");

    let result = agent.filter_predicate(&ctx, &collection, predicate);
    assert!(result.is_err(), "Missing custom claim should return AccessDenied");
}

/// Multiple scope rules all AND together
#[test]
fn test_filter_predicate_multiple_rules_anded() {
    let config_json = r#"{
        "roles": {
            "Technician": ["view_jobs"]
        },
        "collections": {
            "job": {
                "read": "view_jobs",
                "write": "manage_jobs",
                "scope": [
                    {
                        "filter": "field_office = $jwt.custom.field_office",
                        "unless_privilege": "view_all_records"
                    },
                    {
                        "filter": "assigned_to = $jwt.sub",
                        "unless_privilege": "view_office_records"
                    }
                ]
            }
        }
    }"#;
    let config: PolicyConfig = serde_json::from_str(config_json).unwrap();

    let keys = common::test_keys();
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(config);
    agent.set_keys(JwtKeys::Signing(keys.clone()));

    let mut custom = serde_json::Map::new();
    custom.insert("field_office".to_string(), serde_json::Value::String("LA".to_string()));
    let claims = JwtClaims { sub: "tech-7".into(), roles: vec!["Technician".into()], email: "tech@co.com".into(), name: None, custom };
    let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();
    let ctx = JwtContext::from_claims(claims, token);
    let predicate = make_predicate("status = 'open'");
    let models = common::policy_models(&agent, &["job"]);
    let collection = models.id("job");

    let result = agent.filter_predicate(&ctx, &collection, predicate).unwrap();

    let display = format!("{}", result);
    assert!(display.contains("LA"), "got: {}", display);
    assert!(display.contains("tech-7"), "got: {}", display);
}
