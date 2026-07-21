mod common;

use ankurah_jwt_auth::{IssuerTrustDescriptor, IssuerVerifier};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use jwt_simple::prelude::*;
use serde::Serialize;
use std::{collections::HashSet, time::Duration as StdDuration};

const ISSUER: &str = "https://issuer.example";
const AUDIENCE: &str = "app_client";

#[derive(Clone, Serialize, serde::Deserialize)]
struct ExternalClaims {
    #[serde(skip_serializing_if = "Option::is_none")]
    token_use: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    roles: Option<Vec<String>>,
}

fn key_pair(kid: Option<&str>) -> RS256KeyPair {
    let key = RS256KeyPair::from_pem(include_str!("fixtures/test_key_a.pem")).unwrap();
    match kid {
        Some(kid) => key.with_key_id(kid),
        None => key,
    }
}

fn jwks(pair: &RS256KeyPair, kid: &str) -> String {
    let components = pair.public_key().to_components();
    serde_json::json!({
        "keys": [{
            "kid": kid,
            "kty": "RSA",
            "alg": "RS256",
            "use": "sig",
            "n": URL_SAFE_NO_PAD.encode(components.n),
            "e": URL_SAFE_NO_PAD.encode(components.e)
        }]
    })
    .to_string()
}

fn descriptor() -> IssuerTrustDescriptor {
    IssuerTrustDescriptor {
        version: 1,
        issuer: ISSUER.into(),
        jwks_uri: "https://issuer.example/jwks".into(),
        required_audiences: HashSet::from([AUDIENCE.into(), "another_client".into()]),
        required_typ: "at+jwt".into(),
        required_token_use: "access".into(),
        default_roles: vec!["user".into()],
        refresh_ttl: StdDuration::from_secs(900),
        time_tolerance: StdDuration::from_secs(60),
    }
}

fn verifier() -> IssuerVerifier {
    let pair = key_pair(Some("key-a"));
    IssuerVerifier::from_jwks_json(descriptor(), &jwks(&pair, "key-a")).unwrap()
}

fn claims(token_use: &str) -> JWTClaims<ExternalClaims> {
    Claims::with_custom_claims(ExternalClaims { token_use: Some(token_use.into()), roles: None }, Duration::from_mins(5))
        .with_issuer(ISSUER)
        .with_audience(AUDIENCE)
        .with_subject("user-123")
}

fn sign(pair: &RS256KeyPair, claims: JWTClaims<ExternalClaims>, typ: &str) -> String {
    pair.sign_with_options(claims, &HeaderOptions { signature_type: Some(typ.into()), content_type: None }).unwrap()
}

#[test]
fn issuer_verifier_accepts_strict_access_token_and_synthesizes_roles() {
    let pair = key_pair(Some("key-a"));
    let verified = verifier().verify(&sign(&pair, claims("access"), "at+jwt")).unwrap();
    assert_eq!(verified.sub, "user-123");
    assert_eq!(verified.roles, ["user"]); // untrusted token roles are overwritten
    assert_eq!(verified.email, ""); // optional IDP claim is tolerated

    let attacker_roles = Claims::with_custom_claims(
        ExternalClaims { token_use: Some("access".into()), roles: Some(vec!["admin".into()]) },
        Duration::from_mins(5),
    )
    .with_issuer(ISSUER)
    .with_audience(AUDIENCE)
    .with_subject("user-123");
    assert_eq!(verifier().verify(&sign(&pair, attacker_roles, "at+jwt")).unwrap().roles, ["user"]);
}

#[test]
fn issuer_verifier_rejects_wrong_typ_issuer_audience_and_token_use() {
    let pair = key_pair(Some("key-a"));
    let verifier = verifier();

    assert!(verifier.verify(&sign(&pair, claims("access"), "JWT")).is_err());
    assert!(verifier.verify(&sign(&pair, claims("access").with_issuer("https://attacker.example"), "at+jwt")).is_err());
    assert!(verifier.verify(&sign(&pair, claims("access").with_audience("wrong"), "at+jwt")).is_err());
    assert!(verifier.verify(&sign(&pair, claims("id"), "at+jwt")).is_err());
}

#[test]
fn issuer_verifier_requires_kid_subject_and_expiration() {
    let verifier = verifier();
    assert!(verifier.verify(&sign(&key_pair(None), claims("access"), "at+jwt")).is_err());

    let pair = key_pair(Some("key-a"));
    let no_subject = Claims::with_custom_claims(ExternalClaims { token_use: Some("access".into()), roles: None }, Duration::from_mins(5))
        .with_issuer(ISSUER)
        .with_audience(AUDIENCE);
    assert!(verifier.verify(&sign(&pair, no_subject, "at+jwt")).is_err());

    let mut no_exp = claims("access");
    no_exp.expires_at = None;
    assert!(verifier.verify(&sign(&pair, no_exp, "at+jwt")).is_err());

    let no_audience = Claims::with_custom_claims(ExternalClaims { token_use: Some("access".into()), roles: None }, Duration::from_mins(5))
        .with_issuer(ISSUER)
        .with_subject("user-123");
    assert!(verifier.verify(&sign(&pair, no_audience, "at+jwt")).is_err());

    let no_token_use = Claims::with_custom_claims(ExternalClaims { token_use: None, roles: None }, Duration::from_mins(5))
        .with_issuer(ISSUER)
        .with_audience(AUDIENCE)
        .with_subject("user-123");
    assert!(verifier.verify(&sign(&pair, no_token_use, "at+jwt")).is_err());

    let token = sign(&pair, claims("access"), "at+jwt");
    let mut parts: Vec<String> = token.split('.').map(ToString::to_string).collect();
    let mut header: serde_json::Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(&parts[0]).unwrap()).unwrap();
    header.as_object_mut().unwrap().remove("typ");
    parts[0] = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap());
    assert!(verifier.verify(&parts.join(".")).is_err());
}

#[test]
fn issuer_verifier_rejects_expired_unknown_kid_and_critical_header() {
    let verifier = verifier();
    let pair = key_pair(Some("key-a"));
    let mut expired = claims("access");
    expired.expires_at = Some(Clock::now_since_epoch() - Duration::from_secs(61));
    assert!(verifier.verify(&sign(&pair, expired, "at+jwt")).is_err());

    let unknown_pair = RS256KeyPair::from_pem(include_str!("fixtures/test_key_b.pem")).unwrap().with_key_id("unknown");
    assert!(verifier.verify(&sign(&unknown_pair, claims("access"), "at+jwt")).is_err());

    let token = sign(&pair, claims("access"), "at+jwt");
    let mut parts: Vec<String> = token.split('.').map(ToString::to_string).collect();
    let mut header: serde_json::Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(&parts[0]).unwrap()).unwrap();
    header["crit"] = serde_json::json!(["future-extension"]);
    parts[0] = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap());
    assert!(verifier.verify(&parts.join(".")).is_err());
}

#[test]
fn jwks_rotation_is_atomic_and_retains_last_good_nonempty_snapshot() {
    let pair_a = key_pair(Some("key-a"));
    let verifier = verifier();
    let token_a = sign(&pair_a, claims("access"), "at+jwt");
    assert!(verifier.verify(&token_a).is_ok());

    assert!(verifier.replace_jwks_json(r#"{"keys":[]}"#).is_err());
    assert!(verifier.verify(&token_a).is_ok());

    let pair_b = RS256KeyPair::from_pem(include_str!("fixtures/test_key_b.pem")).unwrap().with_key_id("key-b");
    verifier.replace_jwks_json(&jwks(&pair_b, "key-b")).unwrap();
    assert!(verifier.verify(&token_a).is_err());
    assert!(verifier.verify(&sign(&pair_b, claims("access"), "at+jwt")).is_ok());
}

#[test]
fn descriptor_rejects_empty_audiences_unsafe_urls_and_weak_rsa() {
    let mut invalid = descriptor();
    invalid.required_audiences.clear();
    assert!(invalid.validate().is_err());

    let mut unsafe_url = descriptor();
    unsafe_url.jwks_uri = "http://issuer.example/jwks".into();
    assert!(unsafe_url.validate().is_err());

    let weak_jwks = serde_json::json!({
        "keys": [{"kid":"weak", "kty":"RSA", "alg":"RS256", "n":URL_SAFE_NO_PAD.encode([1_u8; 128]), "e":"AQAB"}]
    });
    assert!(IssuerVerifier::from_jwks_json(descriptor(), &weak_jwks.to_string()).is_err());
}

#[test]
fn descriptor_json_round_trip_uses_seconds() {
    let descriptor = descriptor();
    let json = serde_json::to_value(&descriptor).unwrap();
    assert_eq!(json["refresh_ttl"], 900);
    assert_eq!(json["time_tolerance"], 60);
    assert_eq!(serde_json::from_value::<IssuerTrustDescriptor>(json).unwrap(), descriptor);
}
