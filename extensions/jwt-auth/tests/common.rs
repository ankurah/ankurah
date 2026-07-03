use ankql::ast::Predicate;
use ankurah_jwt_auth::{JwtClaims, PolicyConfig, SigningKeys};
use jwt_simple::prelude::Duration;

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
