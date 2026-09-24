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

/// A deterministic durable identity for a fixture field name, shared by the
/// test resolvers so a predicate built here compares equal to one a test
/// binds itself.
pub fn prop(name: &str) -> ankql::ast::PropertyId {
    let mut bytes = [0u8; 32];
    let n = name.as_bytes();
    let len = n.len().min(32);
    bytes[..len].copy_from_slice(&n[..len]);
    ankql::ast::PropertyId::EntityId(ankurah_proto::EntityId::from_bytes(bytes))
}

/// Bind a predicate's names to the fixture identities.
pub fn resolve_fixture(predicate: Predicate<ankql::ast::Parsed>) -> Predicate<ankql::ast::Resolved> {
    use ankurah_core::schema::resolver::{resolve_selection, ModelResolutionError, ModelResolver, ResolvedProperty};
    struct FixtureResolver;
    impl ModelResolver for FixtureResolver {
        fn resolve_property(&self, _model: &ankurah_proto::ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
            Ok(Some(ResolvedProperty { id: prop(name), value_type: ankurah_core::value::ValueType::String }))
        }
    }

    let model = ankurah_proto::ModelId::EntityId(ankurah_proto::EntityId::from_bytes([0x77; 32]));
    resolve_selection(&model, &FixtureResolver, predicate.into()).expect("fixture predicates resolve").predicate
}

/// A caller's predicate resolved to the fixture's durable identities.
pub fn make_predicate(input: &str) -> Predicate<ankql::ast::Resolved> {
    resolve_fixture(ankql::parser::parse_selection(input).unwrap().predicate)
}

pub struct PolicyModels(std::collections::BTreeMap<String, ankurah_proto::ModelId>);

impl PolicyModels {
    pub fn id(&self, label: &str) -> ankurah_proto::ModelId { self.0[label] }
}

/// Model identities and labels supplied by the catalog in node-backed tests.
pub fn policy_models(agent: &ankurah_jwt_auth::JwtAgent, labels: &[&str]) -> PolicyModels {
    let by_label: std::collections::BTreeMap<_, _> =
        labels.iter().map(|label| ((*label).to_owned(), model(label))).collect();
    agent.set_catalog(std::sync::Arc::new(FixtureCatalog {
        labels: by_label.iter().map(|(label, id)| (*id, label.clone())).collect(),
        resolve: |predicate| Ok(resolve_fixture(predicate)),
        property_type: |property| if property == ankql::ast::PropertyId::Id {
            ankurah_core_types::ValueType::EntityId
        } else { ankurah_core_types::ValueType::String },
    }));
    PolicyModels(by_label)
}

pub fn model(label: &str) -> ankurah_proto::ModelId {
    match prop(label) {
        ankql::ast::PropertyId::EntityId(id) => ankurah_proto::ModelId::EntityId(id),
        _ => unreachable!(),
    }
}

pub fn in_model(model: ankurah_proto::ModelId, predicate: Predicate<ankql::ast::Resolved>) -> Predicate<ankql::ast::Resolved> {
    ankql::ast::Selection::from(predicate).and_member_of(model).predicate
}

pub struct FixtureCatalog {
    pub labels: std::collections::BTreeMap<ankurah_proto::ModelId, String>,
    pub resolve: fn(Predicate<ankql::ast::Parsed>) -> Result<Predicate<ankql::ast::Resolved>, String>,
    pub property_type: fn(ankql::ast::PropertyId) -> ankurah_core_types::ValueType,
}

impl ankurah_jwt_auth::PolicyCatalog for FixtureCatalog {
    fn property(&self, _model: &ankurah_proto::ModelId, name: &str)
        -> Result<Option<ankurah_core::schema::resolver::ResolvedProperty>, String> {
        let id = prop(name);
        Ok(Some(ankurah_core::schema::resolver::ResolvedProperty { id, value_type: (self.property_type)(id) }))
    }
    fn model_labels(&self) -> Vec<(ankurah_proto::ModelId, String)> {
        self.labels.iter().map(|(id, label)| (*id, label.clone())).collect()
    }
    fn property_type(&self, _model: &ankurah_proto::ModelId, property: &ankql::ast::PropertyId)
        -> Result<ankurah_core_types::ValueType, String> {
        Ok((self.property_type)(*property))
    }
    fn resolve_predicate(&self, _model: &ankurah_proto::ModelId, predicate: Predicate<ankql::ast::Parsed>)
        -> Result<Predicate<ankql::ast::Resolved>, String> {
        (self.resolve)(predicate)
    }
}
