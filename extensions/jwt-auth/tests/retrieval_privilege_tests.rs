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

use ankql::ast::Predicate;
use ankurah::policy::PolicyAgent;
use ankurah::{Model, Node};
use ankurah_jwt_auth::{JwtAgent, JwtContext, PolicyConfig};
use ankurah_storage_sled::SledStorageEngine;
use common::{make_claims, sign_token};
use std::sync::Arc;

fn config_path() -> String { format!("{}/tests/fixtures/retrieval_privilege.json", env!("CARGO_MANIFEST_DIR")) }

fn load_config() -> PolicyConfig { serde_json::from_str(&std::fs::read_to_string(config_path()).unwrap()).unwrap() }

/// The caller's predicate as the agent receives it: bound to durable
/// identities, the way the query entry binds one before the agent narrows it.
fn parse(predicate: &str) -> Predicate<ankql::ast::Resolved> { common::make_predicate(predicate) }

fn agent() -> JwtAgent {
    let agent = JwtAgent::new_durable(common::test_keys(), config_path()).unwrap();
    // What node attach installs from the node's catalog: scope rules are
    // authored in names and everything that consumes one addresses ids.
    common::install_fixture_bindings(&agent);
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
    let user = common::model("user");
    let note = common::model("note");

    assert!(config.can_access_collection(&guest, Some("user")), "retrieval admits the entry gate");
    assert!(!config.can_scan_collection(&guest, Some("user")), "retrieval never admits a scan");
    assert!(config.can_access_collection(&member, Some("user")));
    assert!(config.can_scan_collection(&member, Some("user")));

    // No retrieve field on note: the gate means what it meant before the
    // field existed.
    assert!(!config.can_access_collection(&guest, Some("note")), "absent retrieve field, no weakening");
}

/// At the retrieval tier every predicate is a scan and composes to `False`
/// — empty, not an error — INCLUDING id-shaped ones: predicate-shaped
/// retrieval is the deliberate follow-up, and until it lands the only
/// retrieval surface is the by-id get path.
#[test]
fn retrieval_tier_scans_nothing() {
    let agent = agent();
    let user = common::model("user");
    let guest = vec![guest_ctx()];

    for scan in [
        "true",
        "name = 'Ada'",
        "id = 'AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8'",
        "id IN ('AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8', 'AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA')",
        "id > 'AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8'",
        "id = 'AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8' AND name = 'Ada'",
    ] {
        let out = agent.filter_predicate(&guest, &user, parse(scan)).unwrap();
        assert_eq!(out, Predicate::False, "every predicate is a scan at the retrieval tier: {scan}");
    }
}

/// The same shapes for a scan-privileged caller are untouched — the
/// unscoped-collection fast path is unchanged for readers.
#[test]
fn scan_tier_predicates_pass_untouched() {
    let agent = agent();
    let user = common::model("user");
    let member = vec![member_ctx("member-1")];

    for predicate in ["true", "name = 'Ada'", "id = 'AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8'"] {
        let out = agent.filter_predicate(&member, &user, parse(predicate)).unwrap();
        assert_eq!(out, parse(predicate), "read privilege scans freely: {predicate}");
    }
}

/// On a scoped collection, a retrieval-only credential contributes no slice
/// to scan composition — today's refusal texture for a caller with no
/// scan-authorized credential is preserved.
#[test]
fn retrieval_credential_never_widens_a_scoped_scan() {
    let agent = agent();
    let note = common::model("note");

    // The guest's only privilege is view; note grants view nothing. The
    // scan reaches the scoped arm and no credential contributes a slice.
    let guest = vec![guest_ctx()];
    assert!(agent.filter_predicate(&guest, &note, parse("true")).is_err(), "no scan-authorized credential, refused as before");

    // A member scans its own slice, composed as before.
    let member = vec![member_ctx("member-1")];
    let out = agent.filter_predicate(&member, &note, parse("true")).unwrap();
    assert_eq!(out.to_string(), parse("true AND owner = 'member-1'").to_string(), "scope composition unchanged for scanners");
}

/// End to end through a node: the guest retrieves the user it names by the
/// get path, is answered empty (not an error) for every predicate — even an
/// id-shaped one — and row scopes still bind by-id reads on the scoped
/// collection.
#[tokio::test]
async fn guest_retrieves_named_rows_and_scans_nothing() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), config_path())?;
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent);
    node.system.create().await?;

    let member = node.context(member_ctx("member-1"))?;
    let trx = member.begin();
    let ada = trx.create(&User { name: "Ada".into() }).await?;
    let user_id = ada.id();
    let note = trx.create(&Note { owner: "member-1".into(), body: "mine".into() }).await?;
    let note_id = note.id();
    trx.commit().await?;

    let guest = node.context(guest_ctx())?;

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
    assert!(guest.get::<NoteView>(note_id).await.is_err(), "absent retrieve field, by-id read refused as before");
    let other = node.context(member_ctx("member-2"))?;
    assert!(other.get::<NoteView>(note_id).await.is_err(), "a scope still binds a by-id read");
    assert_eq!(member.get::<NoteView>(note_id).await?.body()?, "mine");

    // The member's roster is intact.
    assert_eq!(member.fetch::<UserView>("true").await?.len(), 1);

    Ok(())
}
