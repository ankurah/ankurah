//! The `get` privilege: a collection can hand out rows to whoever names
//! their ids while refusing to enumerate itself.
//!
//! The shape under test is community's user directory: every user row is
//! public to anyone holding its id (message authors carry those ids), but
//! only the signed-in may list or query users. `get` admits
//! [`ReadKind::Get`] alone; `read` and `write` admit every kind; an absent
//! `get` changes nothing about a policy written before it existed.

mod common;

use ankurah::{Model, Node};
use ankurah_core::policy::ReadKind;
use ankurah_jwt_auth::{JwtAgent, JwtContext, PolicyConfig};
use ankurah_proto::CollectionId;
use ankurah_storage_sled::SledStorageEngine;
use common::{make_claims, sign_token};
use std::sync::Arc;

fn config_path() -> String { format!("{}/tests/fixtures/get_privilege.json", env!("CARGO_MANIFEST_DIR")) }

fn load_config() -> PolicyConfig { serde_json::from_str(&std::fs::read_to_string(config_path()).unwrap()).unwrap() }

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct User {
    pub name: String,
}

/// The split itself, at the config tier: `view` reaches a user row by id
/// and nothing else.
#[test]
fn get_admits_by_id_and_never_a_scan() {
    let config = load_config();
    let guest = [String::from("guest")];
    let user = CollectionId::fixed_name("user");

    assert!(config.can_access_collection(&guest, &user, ReadKind::Get), "view is the get tier for user");
    assert!(!config.can_access_collection(&guest, &user, ReadKind::Scan), "get must never admit a scan");
}

/// A collection without a `get` field behaves exactly as before the field
/// existed: by-id reads require the read privilege.
#[test]
fn absent_get_falls_back_to_read() {
    let config = load_config();
    let guest = [String::from("guest")];
    let modaction = CollectionId::fixed_name("modaction");

    assert!(!config.can_access_collection(&guest, &modaction, ReadKind::Get), "no get field, no weakening");
    assert!(!config.can_access_collection(&guest, &modaction, ReadKind::Scan));
}

/// `read` (and `write`) admit every kind — a role that may query needs no
/// `get` grant to keep loading rows by id, and a collection that is fully
/// readable gains nothing from one.
#[test]
fn read_privilege_admits_both_kinds() {
    let config = load_config();
    let member = [String::from("member")];
    let guest = [String::from("guest")];
    let user = CollectionId::fixed_name("user");
    let message = CollectionId::fixed_name("message");

    assert!(config.can_access_collection(&member, &user, ReadKind::Get));
    assert!(config.can_access_collection(&member, &user, ReadKind::Scan));
    assert!(config.can_access_collection(&guest, &message, ReadKind::Get));
    assert!(config.can_access_collection(&guest, &message, ReadKind::Scan));
}

/// End to end through a node: the guest context loads a user it can name
/// and is refused the roster, under the same agent and store.
#[tokio::test]
async fn guest_gets_a_named_user_and_cannot_list() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), config_path())?;
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent);
    node.system.create().await?;

    // A member writes the row the guest will name.
    let member_claims = make_claims("member-1", &["member"], "member@example.com");
    let member_token = sign_token(&keys, &member_claims);
    let member = node.context(JwtContext::from_claims(member_claims, member_token))?;

    let trx = member.begin();
    let created = trx.create(&User { name: "Ada".into() }).await?;
    let user_id = created.id();
    trx.commit().await?;

    let guest_claims = make_claims("guest", &["guest"], "");
    let guest_token = sign_token(&keys, &guest_claims);
    let guest = node.context(JwtContext::from_claims(guest_claims, guest_token))?;

    // By id: admitted at the view tier.
    let fetched = guest.get::<UserView>(user_id).await?;
    assert_eq!(fetched.name()?, "Ada");

    // By predicate: refused at the collection gate, whatever the predicate
    // says — enumerating is what the tier withholds.
    assert!(guest.fetch::<UserView>("true").await.is_err(), "a guest must not list users");
    assert!(guest.fetch::<UserView>("name = 'Ada'").await.is_err(), "naming a value is still a scan");

    // The member keeps the roster.
    let listed = member.fetch::<UserView>("true").await?;
    assert_eq!(listed.len(), 1);

    Ok(())
}
