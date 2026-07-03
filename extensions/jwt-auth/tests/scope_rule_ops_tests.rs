//! Tests for per-rule `applies_to` on scope rules: a `"write"`-only rule
//! gates what may be written without hiding rows from reads. The fixture
//! models the role-assignment shape: everyone with admin privileges may see
//! all assignments, but only holders of `account:write` may write rows whose
//! `role_key` is outside the allowlist.

mod common;

use ankurah::{Model, Node};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_jwt_auth::{JwtAgent, JwtContext, ScopeRuleOp, SigningKeys};
use ankurah_storage_sled::SledStorageEngine;
use common::{make_claims, sign_token};
use std::sync::Arc;

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct Assignment {
    #[active_type(LWW)]
    pub role_key: String,
    #[active_type(LWW)]
    pub label: String,
}

fn scope_ops_config_path() -> String { format!("{}/tests/fixtures/scope_ops.json", env!("CARGO_MANIFEST_DIR")) }

#[test]
fn applies_to_defaults_to_read_write_and_parses() {
    let rule: ankurah_jwt_auth::ScopeRule = serde_json::from_str(r#"{"filter": "a = $jwt.sub", "unless_privilege": null}"#).unwrap();
    assert_eq!(rule.applies_to, ScopeRuleOp::ReadWrite);
    assert!(rule.applies_to.applies_to_reads());
    assert!(rule.applies_to.applies_to_writes());

    let rule: ankurah_jwt_auth::ScopeRule =
        serde_json::from_str(r#"{"filter": "a = $jwt.sub", "unless_privilege": null, "applies_to": "write"}"#).unwrap();
    assert_eq!(rule.applies_to, ScopeRuleOp::Write);
    assert!(!rule.applies_to.applies_to_reads());
    assert!(rule.applies_to.applies_to_writes());

    let rule: ankurah_jwt_auth::ScopeRule =
        serde_json::from_str(r#"{"filter": "a = $jwt.sub", "unless_privilege": null, "applies_to": "read"}"#).unwrap();
    assert_eq!(rule.applies_to, ScopeRuleOp::Read);
    assert!(rule.applies_to.applies_to_reads());
    assert!(!rule.applies_to.applies_to_writes());
}

/// Write-only scope rule: reads see everything; writes outside the filter are
/// denied unless the caller holds the bypass privilege — including updates to
/// pre-existing rows outside the filter.
#[tokio::test]
async fn write_only_scope_rule_gates_writes_not_reads() -> anyhow::Result<()> {
    let keys = SigningKeys::generate()?;
    let agent = JwtAgent::new_durable(keys.clone(), scope_ops_config_path())?;

    let node1 = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node1.system.create().await?;

    // Seed one owner-keyed and one administrator-keyed assignment as system.
    let system_ctx = node1.context(JwtContext::system())?;
    let trx = system_ctx.begin();
    let owner_row = trx.create(&Assignment { role_key: "owner".into(), label: "seed owner".into() }).await?;
    let owner_row_id = owner_row.id();
    drop(owner_row);
    trx.create(&Assignment { role_key: "administrator".into(), label: "seed admin".into() }).await?;
    trx.commit().await?;

    let node2 = Node::new(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    let _conn = LocalProcessConnection::new(&node1, &node2).await?;
    node2.system.wait_system_ready().await;

    let admin_claims = make_claims("admin-1", &["Administrator"], "admin@scope.test");
    let admin_token = sign_token(&keys, &admin_claims);
    let admin_ctx = node2.context(JwtContext::from_claims(admin_claims, admin_token))?;

    // Reads are NOT filtered by the write-only rule: both rows visible.
    let visible = admin_ctx.fetch::<AssignmentView>("true").await.expect("admin read fetch");
    assert_eq!(visible.len(), 2, "write-only rule must not hide rows from reads");

    // Creating a row outside the filter is denied for Administrator. The
    // denial may surface at create time (local check) or at commit (relay).
    let trx = admin_ctx.begin();
    let create_denied = match trx.create(&Assignment { role_key: "owner".into(), label: "escalation".into() }).await {
        Err(_) => true,
        Ok(entity) => {
            drop(entity);
            trx.commit().await.is_err()
        }
    };
    assert!(create_denied, "Administrator must not create owner-keyed assignments");

    // Updating a pre-existing row outside the filter is denied too (the
    // before-state is gated as well as the after-state).
    let owner_ref: ankurah::Ref<Assignment> = ankurah::Ref::from(owner_row_id);
    let owner_view: AssignmentView = owner_ref.get(&admin_ctx).await.expect("admin read of owner row");
    let trx = admin_ctx.begin();
    let update_denied = match owner_view.edit(&trx) {
        Err(_) => true,
        Ok(owner_mut) => match owner_mut.label().set(&"tampered".to_string()) {
            Err(_) => true,
            Ok(()) => trx.commit().await.is_err(),
        },
    };
    assert!(update_denied, "Administrator must not modify owner-keyed assignments");

    // Rows inside the filter remain writable.
    let trx = admin_ctx.begin();
    trx.create(&Assignment { role_key: "administrator".into(), label: "fine".into() }).await?;
    trx.commit().await.expect("administrator-keyed create should be allowed");

    // The bypass privilege (account:write) skips the rule entirely.
    let owner_claims = make_claims("owner-1", &["Owner"], "owner@scope.test");
    let owner_token = sign_token(&keys, &owner_claims);
    let owner_ctx = node2.context(JwtContext::from_claims(owner_claims, owner_token))?;
    let trx = owner_ctx.begin();
    trx.create(&Assignment { role_key: "owner".into(), label: "legit grant".into() }).await.expect("owner create");
    trx.commit().await.expect("owner bypass commit should be allowed");

    Ok(())
}
