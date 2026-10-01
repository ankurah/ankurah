//! The `retrieve` privilege: rows to whoever names them, scans to the
//! privileged.
//!
//! The shape under test is community's user directory: message authors
//! carry user ids (refs), so a guest rendering names needs exactly the rows
//! it can name — never the roster. `retrieve` admits the by-id path (`Ref`
//! follows, `get`); every predicate remains a scan for now, even id-shaped
//! ones — admitting id-bounded predicates at this tier is a deliberate
//! follow-up. Scans compose to `False` (empty, not an error) for a caller
//! the gate admitted without scan privilege; row scope rules still bind
//! by-id reads; and a policy without the field means what it always meant.

mod common;

use ankurah_core::{error::RetrievalError, policy::ContextPolicy};

use ankql::ast::Predicate;
use ankurah::{Model, Node};
use ankurah_jwt_auth::{JwtAgent, JwtContext, PolicyConfig};
use ankurah_storage_sled::SledStorageEngine;
use common::{make_claims, sign_token};
use std::sync::Arc;

fn config_path() -> String { format!("{}/tests/fixtures/retrieval_privilege.json", env!("CARGO_MANIFEST_DIR")) }

fn load_config() -> PolicyConfig { serde_json::from_str(&std::fs::read_to_string(config_path()).unwrap()).unwrap() }

/// A caller's predicate resolved to the fixture's durable identities.
fn parse(predicate: &str) -> Predicate<ankql::ast::Resolved> { common::make_predicate(predicate) }

fn agent() -> JwtAgent {
    let agent = JwtAgent::new_durable(common::test_keys(), config_path()).unwrap();
    // What node attach installs from the node's catalog: scope rules are
    // authored in names and everything that consumes one addresses ids.
    agent
}

fn guest_ctx() -> JwtContext {
    let claims = make_claims("guest", &["guest"], "");
    let token = sign_token(&common::test_keys(), &claims);
    JwtContext::from_claims(claims, token)
}

fn member_ctx(sub: &str) -> JwtContext {
    let claims = make_claims(sub, &["member"], "member@example.com");
    let token = sign_token(&common::test_keys(), &claims);
    JwtContext::from_claims(claims, token)
}

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct User {
    pub name: String,
}

/// A row-scoped collection (`owner = $jwt.sub`), for pinning that scopes
/// keep binding by-id reads and that an absent `retrieve` field weakens
/// nothing.
#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct Note {
    pub owner: String,
    pub body: String,
}

/// The gate split at the config tier: the entry gate admits the retrieval
/// tier, the scan check does not.
#[test]
fn gate_is_wide_and_scan_check_is_narrow() {
    let config = load_config();
    let guest = [String::from("guest")];
    let member = [String::from("member")];
    let user = "user";
    let note = "note";

    assert!(config.can_access_collection(&guest, user), "retrieval admits the entry gate");
    assert!(!config.can_scan_collection(&guest, user), "retrieval never admits a scan");
    assert!(config.can_access_collection(&member, user));
    assert!(config.can_scan_collection(&member, user));

    // No retrieve field on note: the gate means what it meant before the
    // field existed.
    assert!(!config.can_access_collection(&guest, note), "absent retrieve field, no weakening");
}

/// At the retrieval tier every predicate is a scan and composes to `False`
/// — empty, not an error — INCLUDING id-shaped ones: predicate-shaped
/// retrieval is the deliberate follow-up, and until it lands the only
/// retrieval surface is the by-id get path.
#[test]
fn retrieval_tier_scans_nothing() {
    let agent = agent();
    let models = common::policy_models(&agent, &["user"]);
    let user = models.id("user");
    let guest = vec![guest_ctx()];

    for scan in [
        "true",
        "name = 'Ada'",
        "id = 'AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8'",
        "id IN ('AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8', 'AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA')",
        "id > 'AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8'",
        "id = 'AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8' AND name = 'Ada'",
    ] {
        let out = ContextPolicy::from_credentials(&agent, &guest).filter_predicate(common::in_model(user, parse(scan))).unwrap();
        assert_eq!(out, Predicate::And(Box::new(common::in_model(user, parse(scan))), Box::new(Predicate::False)), "every predicate is a scan at the retrieval tier: {scan}");
    }
}

/// Read privilege admits queries but still restricts them to the granted membership.
#[test]
fn scan_tier_predicates_require_granted_membership() {
    let agent = agent();
    let models = common::policy_models(&agent, &["user"]);
    let user = models.id("user");
    let member = vec![member_ctx("member-1")];

    for predicate in ["true", "name = 'Ada'", "id = 'AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8'"] {
        let out = ContextPolicy::from_credentials(&agent, &member).filter_predicate(common::in_model(user, parse(predicate))).unwrap();
        assert_eq!(out, Predicate::And(Box::new(common::in_model(user, parse(predicate))), Box::new(Predicate::MemberOf(user))),
            "read privilege scans members freely: {predicate}");
    }
}

/// A retrieval-only credential contributes no query grant.
#[test]
fn retrieval_credential_never_widens_a_scoped_scan() {
    let agent = agent();
    let models = common::policy_models(&agent, &["note"]);
    let note = models.id("note");

    // The guest's only privilege is view; note grants view nothing. The
    // scan reaches the scoped arm and no credential contributes a slice.
    let guest = vec![guest_ctx()];
    let filtered = ContextPolicy::from_credentials(&agent, &guest).filter_predicate(common::in_model(note, parse("true")));
    assert!(matches!(filtered, Ok(Predicate::And(_, grant)) if *grant == Predicate::False));

    // A member scans its own slice, composed as before.
    let member = vec![member_ctx("member-1")];
    let out = ContextPolicy::from_credentials(&agent, &member).filter_predicate(common::in_model(note, parse("true"))).unwrap();
    assert_eq!(out, Predicate::And(
        Box::new(common::in_model(note, Predicate::True)), Box::new(common::in_model(note, parse("owner = 'member-1'"))),
    ), "scope composition retains the membership predicate");
}

/// End to end through a node: the guest retrieves the user it names by the
/// get path, is answered empty (not an error) for every predicate — even an
/// id-shaped one — and row scopes still bind by-id reads on the scoped
/// collection.
#[tokio::test]
async fn guest_retrieves_named_rows_and_scans_nothing() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), config_path())?;
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node.system.create().await?;
    agent.set_policy(&node, &agent.config()).await?;

    let member = node.context_async(member_ctx("member-1")).await?;
    let trx = member.begin();
    let ada = trx.create(&User { name: "Ada".into() }).await?;
    let user_id = ada.id();
    let note = trx.create(&Note { owner: "member-1".into(), body: "mine".into() }).await?;
    let note_id = note.id();
    trx.commit().await?;

    let guest = node.context_async(guest_ctx()).await?;

    // The retrieval surface: the by-id get path (what a `Ref` follow runs).
    assert_eq!(guest.get::<UserView>(user_id).await?.name()?, "Ada");

    // Every predicate answers empty — id-shaped included. Predicate-shaped
    // retrieval (and with it, named-row liveness) is the follow-up change.
    assert_eq!(guest.fetch::<UserView>(format!("id = '{user_id}'").as_str()).await?.len(), 0, "an id predicate is still a scan");
    assert_eq!(guest.fetch::<UserView>("true").await?.len(), 0, "a guest's roster scan answers empty");
    assert_eq!(guest.fetch::<UserView>("name = 'Ada'").await?.len(), 0);

    // Row scopes still bind by-id reads: no retrieve field on note, so the
    // guest dies at the gate; a different member passes the gate and dies
    // at the scope; the owner reads its row.
    assert!(matches!(guest.get::<NoteView>(note_id).await, Err(RetrievalError::AccessDenied(_))));
    let other = node.context_async(member_ctx("member-2")).await?;
    assert!(matches!(other.get::<NoteView>(note_id).await, Err(RetrievalError::AccessDenied(_))));
    assert_eq!(member.get::<NoteView>(note_id).await?.body()?, "mine");

    let missing = ankurah::proto::EntityId::random();
    assert!(matches!(other.get::<NoteView>(missing).await, Err(RetrievalError::EntityNotFound(id)) if id == missing));

    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), JwtAgent::new_ephemeral());
    let _connection = ankurah_connector_local_process::LocalProcessConnection::new(&client, &node).await?;
    let remote = client.context_async(member_ctx("member-2")).await?;
    assert_eq!(remote.get::<UserView>(user_id).await?.name()?, "Ada");
    assert!(matches!(remote.get::<NoteView>(note_id).await, Err(RetrievalError::AccessDenied(_))));
    assert!(matches!(remote.get::<NoteView>(missing).await, Err(RetrievalError::EntityNotFound(id)) if id == missing));
    assert!(remote.fetch::<NoteView>("true").await?.is_empty(), "a scan still filters unauthorized rows silently");

    // The member's roster is intact.
    assert_eq!(member.fetch::<UserView>("true").await?.len(), 1);

    Ok(())
}
