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
    policy::PolicyAgent,
    selection::filter::{evaluate_predicate, Filterable},
    value::Value,
};
use ankurah_jwt_auth::{JwtAgent, JwtContext, JwtKeys, PolicyConfig, SigningKeys};
use ankurah_proto::{CollectionId, EntityId};
use ankurah_storage_sled::SledStorageEngine;
use std::sync::Arc;

/// A credential holding one role, signed by `keys`.
fn context(keys: &SigningKeys, sub: &str, role: &str) -> JwtContext {
    let claims = common::make_claims(sub, &[role], "reader@example.com");
    let token = common::sign_token(keys, &claims);
    JwtContext::from_claims(claims, token)
}

fn agent_with(config_json: &str, keys: &SigningKeys) -> JwtAgent {
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(serde_json::from_str::<PolicyConfig>(config_json).expect("test policy must parse"));
    agent.set_keys(JwtKeys::Signing(keys.clone()));
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
    fn collection(&self) -> &str { "note" }

    fn value(&self, name: &str) -> Option<Value> {
        match name {
            "owner" => Some(Value::EntityId(self.owner)),
            "reviewer" => Some(Value::EntityId(self.reviewer)),
            "visibility" => Some(Value::String(self.visibility.to_string())),
            _ => None,
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

/// A subject that is not an entity id makes each `$jwt.sub` clause answer
/// false, and a false clause is only a false clause: it denies the rows no
/// other clause admits, and admits the rows another clause does. An error in
/// one clause would instead escape the whole OR — the row-by-row half via
/// `enforce_read_scope`, which turns any evaluation error into a refusal, and
/// the query half by failing the caller's fetch outright.
#[test]
fn test_or_composed_scope_denies_the_row_without_erroring() {
    let keys = common::test_keys();
    let agent = agent_with(OR_SCOPE_CONFIG, &keys);
    let collection = CollectionId::from("note");

    let owner = EntityId::random();
    let reviewer = EntityId::random();
    let private = NoteRow { owner, reviewer, visibility: "private" };
    let shared = NoteRow { owner, reviewer, visibility: "shared" };

    // The caller asks for everything, so the scope rule is the only thing
    // narrowing the query.
    let guest = context(&keys, "guest", "reader");
    let filtered = agent.filter_predicate(&guest, &collection, Predicate::True).expect("a subject that is not an id must still filter");

    let admits = |row: NoteRow| evaluate_predicate(&row, &filtered).expect("the filtered predicate must evaluate, not error");
    assert!(!admits(private), "neither id clause can match a subject that is not an id, so the row is denied");
    assert!(admits(shared), "the clause that does not read the subject still admits its rows");

    // The control that keeps the denial honest: the same clauses admit the
    // owner's own row when the subject is that owner's id, so the false above
    // is a comparison that answered no rather than a clause that never matches.
    let member = context(&keys, &owner.to_base64(), "reader");
    let filtered = agent.filter_predicate(&member, &collection, Predicate::True).expect("a member's subject filters too");
    assert!(
        evaluate_predicate(&private, &filtered).expect("the filtered predicate must evaluate, not error"),
        "the owner's own row must pass the same clause that denied the guest"
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
