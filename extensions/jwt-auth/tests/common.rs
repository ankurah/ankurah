use ankql::ast::Predicate;
use ankurah_jwt_auth::{JwtClaims, PolicyConfig, SigningKeys};
use jwt_simple::prelude::Duration;

/// Pre-generated 4096-bit RSA test keys. Generating a fresh keypair takes
/// upward of a minute per call on CI hardware, and nearly every test needs
/// one, so tests load these committed throwaway keys instead. They exist
/// only for tests: never use them outside this suite.
/// keys_tests::test_sign_and_verify keeps one real SigningKeys::generate()
/// call so key generation itself stays covered.
pub fn test_keys() -> SigningKeys { SigningKeys::from_pem(include_str!("fixtures/test_key_a.pem")).unwrap() }

/// A second, distinct test keypair for wrong-key rejection tests.
pub fn test_keys_alt() -> SigningKeys { SigningKeys::from_pem(include_str!("fixtures/test_key_b.pem")).unwrap() }

pub fn blog_config_path() -> String { format!("{}/tests/fixtures/simple_blog.json", env!("CARGO_MANIFEST_DIR")) }

pub fn load_blog_config() -> PolicyConfig { serde_json::from_str(include_str!("fixtures/simple_blog.json")).unwrap() }

pub fn load_minimal_config() -> PolicyConfig { serde_json::from_str(include_str!("fixtures/simple_minimal.json")).unwrap() }

pub fn make_claims(sub: &str, roles: &[&str], email: &str) -> JwtClaims {
    JwtClaims {
        sub: sub.into(),
        roles: roles.iter().map(|r| String::from(*r)).collect(),
        email: email.into(),
        name: None,
        custom: serde_json::Map::new(),
    }
}

pub fn sign_token(keys: &SigningKeys, claims: &JwtClaims) -> String { keys.sign(claims, Duration::from_hours(1)).unwrap() }

pub fn make_predicate(input: &str) -> Predicate { ankql::parser::parse_selection(input).unwrap().predicate }
