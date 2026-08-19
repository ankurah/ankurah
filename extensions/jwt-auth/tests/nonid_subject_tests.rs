//! What a scope rule does when the subject is not an entity id.
//!
//! A deployment may mint a subject that names no user — a distinguished
//! literal such as `guest`, for a reader who has not signed in — and every
//! `$jwt.sub` scope rule in its policy then compares a field against that
//! literal. Evaluating such a comparison must answer false rather than error,
//! because an error escapes the whole predicate: it takes down the OR arms
//! that would have admitted rows, and `enforce_read_scope` turns it into a
//! refusal of the row outright. False denies only what it compares and leaves
//! the rest of the predicate standing.
//!
//! The pieces this rests on are pinned where each one lives: substitution
//! producing a String literal rather than an error, in `variables`'s own tests;
//! an EntityId-typed field answering false rather than erroring against a
//! String, in ankurah-core's `selection::filter`; and an absent subject staying
//! a decode error on both paths, in `keys_tests`. What is pinned here is the
//! composition of those pieces — an OR of scope clauses, and the two access
//! paths a caller actually reaches.

mod common;

use ankql::ast::Predicate;
use ankurah::{Model, Node, Ref};
use ankurah_core::{
    policy::{AccessDenied, PolicyAgent},
    selection::filter::{evaluate_predicate, Filterable},
    value::Value,
};
use ankurah_jwt_auth::{JwtAgent, JwtContext, JwtKeys, PolicyConfig, SigningKeys};
use ankurah_proto::EntityId;
use ankurah_storage_sled::SledStorageEngine;
use std::sync::Arc;

/// A credential holding one role, signed by `keys`.
fn context(keys: &SigningKeys, sub: &str, role: &str) -> JwtContext {
    let claims = common::make_claims(sub, &[role], "reader@example.com");
    let token = common::sign_token(keys, &claims);
    JwtContext::from_claims(claims, token)
}

/// A deterministic durable identity for a fixture field name.
fn prop(name: &str) -> ankql::ast::PropertyId {
    let mut bytes = [0u8; 32];
    let n = name.as_bytes();
    let len = n.len().min(32);
    bytes[..len].copy_from_slice(&n[..len]);
    ankql::ast::PropertyId::EntityId(EntityId::from_bytes(bytes))
}

/// Bind a rule predicate's names to the fixture identities, the way node
/// attach binds them through the catalog.
fn try_resolve_fixture(
    predicate: Predicate<ankql::ast::Parsed>,
) -> Result<Predicate<ankql::ast::Resolved>, ankurah_core::ModelResolutionError> {
    use ankurah_core::schema::resolver::{resolve_selection, ModelResolutionError, ModelResolver, ResolvedProperty};
    struct FixtureResolver;
    impl ModelResolver for FixtureResolver {
        fn resolve_property(&self, _model: &ankurah_proto::ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
            let id = prop(name);
            let value_type = if id == prop("owner") || id == prop("reviewer") {
                ankurah_core::value::ValueType::EntityId
            } else {
                ankurah_core::value::ValueType::String
            };
            Ok(Some(ResolvedProperty { id, value_type }))
        }
    }
    let model = ankurah_proto::ModelId::EntityId(EntityId::from_bytes([0x77; 32]));
    Ok(resolve_selection(&model, &FixtureResolver, predicate.into())?.predicate)
}

fn resolve_fixture(predicate: Predicate<ankql::ast::Parsed>) -> Predicate<ankql::ast::Resolved> {
    try_resolve_fixture(predicate).expect("fixture rule predicates resolve")
}

fn agent_with(config_json: &str, keys: &SigningKeys) -> JwtAgent {
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(serde_json::from_str::<PolicyConfig>(config_json).expect("test policy must parse"));
    agent.set_keys(JwtKeys::Signing(keys.clone()));
    // What node attach installs from the node's catalog: scope rules are
    // authored in names and everything that consumes one addresses ids.
    agent.set_selection_resolver(std::sync::Arc::new(|_collection, predicate| {
        try_resolve_fixture(predicate).map_err(|error| error.to_string())
    }));
    agent.set_model_lookup(common::fixture_models());
    agent
}

// ---------------------------------------------------------------------------
// An OR of scope clauses
// ---------------------------------------------------------------------------

/// A note whose two owner-ish fields are `Ref`s, so they evaluate as EntityIds
/// — the same shape a `Ref<User>` column takes when a scope rule reads it.
#[derive(Clone, Copy)]
struct NoteRow {
    owner: EntityId,
    reviewer: EntityId,
    visibility: &'static str,
}

impl Filterable for NoteRow {
    fn value(&self, property: &ankql::ast::PropertyId) -> Option<Value> {
        if *property == prop("owner") {
            Some(Value::EntityId(self.owner))
        } else if *property == prop("reviewer") {
            Some(Value::EntityId(self.reviewer))
        } else if *property == prop("visibility") {
            Some(Value::String(self.visibility.to_string()))
        } else {
            None
        }
    }
}

const OR_SCOPE_CONFIG: &str = r#"{
    "roles": { "reader": ["note:read"] },
    "collections": {
        "note": {
            "read": "note:read",
            "write": null,
            "scope": [{ "filter": "owner = $jwt.sub OR reviewer = $jwt.sub OR visibility = 'shared'" }]
        }
    }
}"#;

/// A subject that is not an entity id cannot take the id-comparing clauses'
/// registered type, so binding the composed rule is a TYPE ERROR
/// (`Canonicalization`) even though one OR clause reads no subject at all.
/// A slice that cannot bind admits nothing, so the credential contributes
/// none, and a caller left with no slice at all is refused -- never
/// silently narrowed to a false clause, which inverts under negation and
/// would grant instead of deny. A rule that must serve both distinguished
/// string subjects and entity ids is a schema/rule design question,
/// recorded on https://github.com/ankurah/ankurah/issues/472.
#[test]
fn test_or_composed_scope_with_a_non_id_subject_is_a_type_error() {
    let keys = common::test_keys();
    let agent = agent_with(OR_SCOPE_CONFIG, &keys);
    let collection = common::model("note");

    let owner = EntityId::random();
    let reviewer = EntityId::random();
    let private = NoteRow { owner, reviewer, visibility: "private" };

    // The rule binding itself is what refuses the guest's subject: naming it
    // here pins that the refusal below is that type error and not some other
    // denial.
    let guest_rule = ankql::parser::parse_selection(&format!("owner = 'guest' OR reviewer = 'guest' OR visibility = 'shared'"))
        .expect("the rule text parses")
        .predicate;
    let error = try_resolve_fixture(guest_rule).expect_err("a subject that is not an id must fail binding as a type error");
    assert!(
        matches!(error, ankurah_core::ModelResolutionError::Canonicalization { .. }),
        "expected a canonicalization type error, got {error:?}"
    );

    // The caller asks for everything, so the scope rule is the only thing
    // narrowing the query -- and its slice cannot bind, so this lone
    // credential leaves nothing to read by.
    let guest = context(&keys, "guest", "reader");
    let refused = agent.filter_predicate(&guest, &collection, Predicate::True);
    assert!(
        matches!(refused, Err(AccessDenied::ByPolicy("No authorized context for row filtering"))),
        "a credential whose scope cannot bind leaves nothing to read by, got: {refused:?}"
    );

    // The control that keeps the refusal honest: a subject that IS an id
    // binds, and the same clauses admit that owner's own row.
    let member = context(&keys, &owner.to_base64(), "reader");
    let filtered = agent.filter_predicate(&member, &collection, Predicate::True).expect("a member's subject filters too");
    assert!(
        evaluate_predicate(&private, &filtered).expect("the bound predicate must evaluate"),
        "the owner's own row must pass the clauses that refused the guest's credential"
    );
}

// ---------------------------------------------------------------------------
// Both access paths, end to end
// ---------------------------------------------------------------------------

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct Person {
    pub name: String,
}

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct Doc {
    pub owner: Ref<Person>,
    pub body: String,
}

const OWNER_SCOPE_CONFIG: &str = r#"{
    "roles": { "user": ["doc:read"], "guest": ["doc:read"] },
    "collections": {
        "doc": {
            "read": "doc:read",
            "write": null,
            "scope": [{ "filter": "owner = $jwt.sub" }]
        }
    }
}"#;

/// The whole posture, against a real node: a caller whose subject is not an
/// entity id reaches the collection — its role grants that — and the row-local
/// scope rule then leaves it nothing, on both paths a reader can take. The
/// owner's own credential takes both paths successfully over the same row, so
/// what the other caller is denied is its subject and not the fixture.
///
/// The claim under test is that no row reaches such a caller, which is why the
/// query half asserts that and not a particular refusal: the two paths refuse
/// differently, and the query path's refusal depends on the scoped column's
/// type. Read by id, the scope is evaluated against the row and denies it.
/// Queried over the `Ref` column here, the storage planner cannot encode a
/// string that is not an id into an index key over EntityIds, so the fetch
/// fails outright rather than answering with nothing — fail-closed, but an
/// error rather than an empty result. (Scope the same rule on a String column
/// instead and the comparison is representable, so the fetch simply returns no
/// rows.) A change that turned the middle case into an empty answer would be an
/// improvement and must not fail this test; a change that handed over a row
/// must fail it.
#[tokio::test]
async fn test_non_id_subject_reaches_no_row_on_either_path() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent_with(OWNER_SCOPE_CONFIG, &keys));
    node.system.create().await?;

    let root = node.context(JwtContext::system())?;
    let (person_id, doc_id) = {
        let trx = root.begin();
        let person = trx.create(&Person { name: "Owner".into() }).await?;
        let doc = trx.create(&Doc { owner: person.id().into(), body: "hello".into() }).await?;
        let ids = (person.id(), doc.id());
        trx.commit().await?;
        ids
    };

    let member = node.context(context(&keys, &person_id.to_base64(), "user"))?;
    assert_eq!(member.fetch::<DocView>("body = 'hello'").await?.len(), 1, "the owner's query returns the owner's row");
    assert!(member.get::<DocView>(doc_id).await.is_ok(), "the owner may read its own row by id");

    let guest = node.context(context(&keys, "guest", "guest"))?;
    // A refused query and an empty one are both fail-closed; a returned row is
    // not. See the note above on why this path refuses rather than empties.
    if let Ok(rows) = guest.fetch::<DocView>("body = 'hello'").await {
        assert!(rows.is_empty(), "a subject that is not an id owns no row, but the query handed over {}", rows.len());
    }
    assert!(guest.get::<DocView>(doc_id).await.is_err(), "the same row read by id is refused rather than handed over");

    Ok(())
}
