use ankql::ast::Predicate;
use ankurah_jwt_auth::{JwtClaims, PolicyConfig, SigningKeys};
use jwt_simple::prelude::Duration;
use std::{collections::BTreeMap, sync::Arc};

use ankql::ast::PropertyId;
use ankurah::{value::ValueType, EntityId, ModelId};
use ankurah_jwt_auth::JwtAgent;

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

struct TestCatalogResolver {
    by_name: BTreeMap<String, ModelId>,
    by_model: BTreeMap<ModelId, String>,
}

impl ankurah::core::schema::CatalogResolver for TestCatalogResolver {
    fn resolve_model(&self, name: &str) -> anyhow::Result<Option<ModelId>> { Ok(self.by_name.get(name).copied()) }

    fn resolve_model_property(&self, _model: &ModelId, _property_name: &str) -> anyhow::Result<Option<PropertyId>> { Ok(None) }

    fn model_properties(&self, _model: &ModelId) -> anyhow::Result<Vec<PropertyId>> { Ok(Vec::new()) }

    fn model_name(&self, model: &ModelId) -> anyhow::Result<String> {
        self.by_model.get(model).cloned().ok_or_else(|| anyhow::anyhow!("unknown test model {model}"))
    }

    fn property_name(&self, id: &PropertyId) -> anyhow::Result<String> {
        anyhow::bail!("test policy resolver has no property metadata for {id}")
    }

    fn property_value_type(&self, property: &PropertyId) -> anyhow::Result<ValueType> {
        anyhow::bail!("test policy resolver has no property type for {property}")
    }
}

/// Exact identities plus the resolver lifetime needed by standalone
/// PolicyAgent unit tests. The random ids are ordinary test entities, never
/// reserved or name-derived runtime identities.
pub struct PolicyModels {
    by_name: BTreeMap<String, ModelId>,
    _resolver: Arc<dyn ankurah::core::schema::CatalogResolver>,
}

impl PolicyModels {
    pub fn id(&self, name: &str) -> ModelId { *self.by_name.get(name).unwrap_or_else(|| panic!("test model {name:?} was not installed")) }
}

pub fn policy_models(agent: &JwtAgent, names: &[&str]) -> PolicyModels {
    let by_name: BTreeMap<_, _> = names.iter().map(|name| ((*name).to_owned(), ModelId::EntityId(EntityId::new()))).collect();
    let by_model = by_name.iter().map(|(name, model)| (*model, name.clone())).collect();
    let resolver: Arc<dyn ankurah::core::schema::CatalogResolver> = Arc::new(TestCatalogResolver { by_name: by_name.clone(), by_model });
    agent.set_catalog_resolver(Arc::downgrade(&resolver));
    PolicyModels { by_name, _resolver: resolver }
}
