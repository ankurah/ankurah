mod common;

use ankurah_jwt_auth::{parse_claims_unverified, JwtAgent, JwtClaims, JwtKeys, SigningKeys};
use jwt_simple::prelude::Duration;

// ---------------------------------------------------------------------------
// SigningKeys and JwtClaims
// ---------------------------------------------------------------------------

#[test]
fn test_sign_and_verify() {
    let keys = SigningKeys::generate().unwrap();
    let claims = JwtClaims {
        sub: "user-123".into(),
        roles: vec!["Admin".into()],
        email: "test@example.com".into(),
        name: Some("Test User".into()),
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&claims, Duration::from_hours(24)).unwrap();
    let verified = keys.verify(&token).unwrap();
    assert_eq!(verified.sub, "user-123");
    assert_eq!(verified.roles, vec!["Admin"]);
    assert_eq!(verified.email, "test@example.com");
    assert_eq!(verified.name.as_deref(), Some("Test User"));
}

#[test]
fn test_sign_and_verify_no_name() {
    let keys = SigningKeys::generate().unwrap();
    let claims = JwtClaims {
        sub: "user-456".into(),
        roles: vec!["Reader".into()],
        email: "reader@example.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();
    let verified = keys.verify(&token).unwrap();
    assert_eq!(verified.sub, "user-456");
    assert_eq!(verified.roles, vec!["Reader"]);
    assert!(verified.name.is_none());
}

#[test]
fn test_verify_wrong_key_fails() {
    let keys1 = SigningKeys::generate().unwrap();
    let keys2 = SigningKeys::generate().unwrap();
    let claims = JwtClaims {
        sub: "user-123".into(),
        roles: vec!["Admin".into()],
        email: "test@example.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys1.sign(&claims, Duration::from_hours(24)).unwrap();
    assert!(keys2.verify(&token).is_err());
}

#[test]
fn test_verify_garbage_token_fails() {
    let keys = SigningKeys::generate().unwrap();
    assert!(keys.verify("not.a.jwt").is_err());
    assert!(keys.verify("").is_err());
}

#[test]
fn test_public_key_pem_export() {
    let keys = SigningKeys::generate().unwrap();
    let pem = keys.public_key_pem().unwrap();
    assert!(pem.contains("BEGIN PUBLIC KEY"));
    assert!(pem.contains("END PUBLIC KEY"));
}

// ---------------------------------------------------------------------------
// JwtKeys::VerifyOnly
// ---------------------------------------------------------------------------

#[test]
fn test_jwt_keys_verify_only() {
    let signing_keys = SigningKeys::generate().unwrap();
    let public_pem = signing_keys.public_key_pem().unwrap();
    let verify_keys = JwtKeys::from_public_pem(&public_pem).unwrap();

    let claims = JwtClaims {
        sub: "user-99".into(),
        roles: vec!["Admin".into()],
        email: "test@test.com".into(),
        name: Some("Tester".into()),
        custom: serde_json::Map::new(),
    };
    let token = signing_keys.sign(&claims, Duration::from_hours(1)).unwrap();

    let verified = verify_keys.verify(&token).unwrap();
    assert_eq!(verified.sub, "user-99");
    assert_eq!(verified.roles, vec!["Admin"]);
    assert_eq!(verified.email, "test@test.com");
}

#[test]
fn test_jwt_keys_signing_variant_verify() {
    let signing_keys = SigningKeys::generate().unwrap();
    let jwt_keys = JwtKeys::Signing(signing_keys.clone());

    let claims = JwtClaims {
        sub: "user-1".into(),
        roles: vec!["Editor".into()],
        email: "e@e.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = signing_keys.sign(&claims, Duration::from_hours(1)).unwrap();
    let verified = jwt_keys.verify(&token).unwrap();
    assert_eq!(verified.sub, "user-1");
}

#[test]
fn test_jwt_keys_public_key_pem() {
    let signing_keys = SigningKeys::generate().unwrap();
    let jwt_keys = JwtKeys::Signing(signing_keys.clone());
    let pem = jwt_keys.public_key_pem().unwrap();
    assert!(pem.contains("BEGIN PUBLIC KEY"));

    let verify_keys = JwtKeys::from_public_pem(&pem).unwrap();
    let pem2 = verify_keys.public_key_pem().unwrap();
    assert_eq!(pem, pem2);
}

// ---------------------------------------------------------------------------
// Ephemeral agent key management
// ---------------------------------------------------------------------------

#[test]
fn test_ephemeral_agent_starts_with_no_keys() {
    let agent = JwtAgent::new_ephemeral();
    assert!(agent.signing_keys().is_none());
    assert!(agent.config().roles.is_empty());
    assert!(agent.config().collections.is_empty());
}

#[test]
fn test_ephemeral_agent_set_keys() {
    let agent = JwtAgent::new_ephemeral();
    let signing_keys = SigningKeys::generate().unwrap();
    agent.set_keys(JwtKeys::Signing(signing_keys.clone()));
    assert!(agent.signing_keys().is_some());
}

// ---------------------------------------------------------------------------
// parse_claims_unverified
// ---------------------------------------------------------------------------

#[test]
fn test_parse_claims_unverified() {
    let keys = SigningKeys::generate().unwrap();
    let claims = JwtClaims {
        sub: "user-77".into(),
        roles: vec!["Admin".into(), "Editor".into()],
        email: "admin@test.com".into(),
        name: Some("Admin User".into()),
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();

    let parsed = parse_claims_unverified(&token).unwrap();
    assert_eq!(parsed.sub, "user-77");
    assert_eq!(parsed.roles, vec!["Admin", "Editor"]);
    assert_eq!(parsed.email, "admin@test.com");
    assert_eq!(parsed.name, Some("Admin User".into()));
}

#[test]
fn test_parse_claims_unverified_no_name() {
    let keys = SigningKeys::generate().unwrap();
    let claims = JwtClaims {
        sub: "user-88".into(),
        roles: vec!["Reader".into()],
        email: "reader@test.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();

    let parsed = parse_claims_unverified(&token).unwrap();
    assert_eq!(parsed.sub, "user-88");
    assert_eq!(parsed.roles, vec!["Reader"]);
    assert_eq!(parsed.email, "reader@test.com");
    assert!(parsed.name.is_none());
}

#[test]
fn test_parse_claims_unverified_invalid_format() {
    assert!(parse_claims_unverified("not-a-jwt").is_err());
    assert!(parse_claims_unverified("").is_err());
    assert!(parse_claims_unverified("a.b").is_err());
}
