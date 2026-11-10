//! Tests for per-rule `applies_to` on scope rules: a `"write"`-only rule
//! gates what may be written without hiding rows from reads. The fixture
//! models the role-assignment shape: everyone with admin privileges may see
//! all assignments, but only holders of `account:write` may write rows whose
//! `role_key` is outside the allowlist.

mod common;

use ankurah::{Model, Node};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_jwt_auth::{JwtAgent, JwtContext, PolicyAuthoringError, PolicyConfig, ScopeRuleOp};
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

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct Badge {
    #[active_type(LWW)]
    pub label: String,
    pub revoked_by: Option<String>,
}

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
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), scope_ops_config_path())?;

    let node1 = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node1.system.create().await?;
    agent.set_policy(&node1, &agent.config()).await?;

    // Seed one owner-keyed and one administrator-keyed assignment as system.
    let system_ctx = node1.context_async(JwtContext::system()).await?;
    let trx = system_ctx.begin();
    let owner_row = trx.create(&Assignment { role_key: "owner".into(), label: "seed owner".into() }).await?;
    let owner_row_id = owner_row.id();
    drop(owner_row);
    trx.create(&Assignment { role_key: "administrator".into(), label: "seed admin".into() }).await?;
    trx.commit().await?;

    let node2 = Node::new(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    let _conn = LocalProcessConnection::new(&node1, &node2).await?;
    node2.system.wait_system_ready().await.unwrap();

    let admin_claims = make_claims("admin-1", &["Administrator"], "admin@scope.test");
    let admin_token = sign_token(&keys, &admin_claims);
    let admin_ctx = node2.context_async(JwtContext::from_claims(admin_claims, admin_token)).await?;

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
        Ok(owner_mut) => match owner_mut.label().and_then(|label| label.set(&"tampered".to_string())) {
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
    let owner_ctx = node2.context_async(JwtContext::from_claims(owner_claims, owner_token)).await?;
    let trx = owner_ctx.begin();
    trx.create(&Assignment { role_key: "owner".into(), label: "legit grant".into() }).await.expect("owner create");
    trx.commit().await.expect("owner bypass commit should be allowed");

    Ok(())
}

/// A scope restricts one model's entities by that model's own properties, so a filter that names a property
/// as `Component::name` is refused when the policy is set, and when a fixture binds it.
#[tokio::test]
async fn a_scope_filter_with_a_component_qualifier_is_refused_when_the_policy_is_set() -> anyhow::Result<()> {
    let agent = JwtAgent::new_durable(common::test_keys(), scope_ops_config_path())?;
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node.system.create().await?;
    let filter = "Other::owner = $jwt.sub";
    let config: PolicyConfig = serde_json::from_value(serde_json::json!({
        "roles": { "Owner": ["account:write"] },
        "collections": { "assignment": { "read": "account:write", "scope": [{ "filter": filter, "unless_privilege": null }] } }
    }))?;
    let error = agent.set_policy(&node, &config).await.expect_err("a qualified scope filter must be refused");
    let expected =
        PolicyAuthoringError::QualifiedScopeProperty { model: "assignment".into(), filter: filter.into(), component: "Other".into() };
    assert_eq!(error.downcast_ref::<PolicyAuthoringError>(), Some(&expected), "{error:#}");

    // A fixture binds a configuration only if authoring would install it, and names the qualifier as written.
    let filter = "\"records.closure\"::closed_by = $jwt.sub";
    agent.update_config(serde_json::from_value(serde_json::json!({
        "roles": { "Owner": ["account:write"] },
        "collections": { "assignment": { "read": "account:write", "scope": [{ "filter": filter, "unless_privilege": null }] } }
    }))?);
    let refused = agent
        .set_catalog(Arc::new(common::FixtureCatalog {
            labels: [(common::model("assignment"), "assignment".to_owned())].into(),
            resolve: |predicate| Ok(common::resolve_fixture(predicate)),
            property_type: |_| ankurah_core_types::ValueType::String,
        }))
        .expect_err("a fixture must refuse a scope that authoring refuses");
    assert_eq!(
        refused.to_string(),
        "the scope filter '\"records.closure\"::closed_by = $jwt.sub' on 'assignment' qualifies a property with \"records.closure\"::; \
         a scope names its own model's properties without a qualifier"
    );
    Ok(())
}

/// A missing property is NULL to a write scope as to a query: the write-only scope `revoked_by IS NULL` lets a
/// member create a badge without `revoked_by`, and refuses one that has it.
#[tokio::test]
async fn a_write_scope_that_a_property_is_null_grants_a_write_to_an_entity_without_it() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), format!("{}/tests/fixtures/scope_is_null.json", env!("CARGO_MANIFEST_DIR")))?;
    let node1 = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node1.system.create().await?;
    agent.set_policy(&node1, &agent.config()).await?;
    let node2 = Node::new(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    let _conn = LocalProcessConnection::new(&node1, &node2).await?;
    node2.system.wait_system_ready().await.unwrap();

    let claims = make_claims("member-1", &["Member"], "member@scope.test");
    let token = sign_token(&keys, &claims);
    let member = node2.context_async(JwtContext::from_claims(claims, token)).await?;

    let trx = member.begin();
    trx.create(&Badge { label: "active".into(), revoked_by: None }).await?;
    trx.commit().await.expect("a badge without revoked_by is inside the write scope");

    // The denial may surface at create time (local check) or at commit (relay).
    let trx = member.begin();
    let denied = match trx.create(&Badge { label: "revoked".into(), revoked_by: Some("steward".into()) }).await {
        Err(_) => true,
        Ok(badge) => {
            drop(badge);
            trx.commit().await.is_err()
        }
    };
    assert!(denied, "a badge with revoked_by is outside the write scope");
    Ok(())
}
