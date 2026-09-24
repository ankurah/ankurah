mod common;

use ankurah_core::policy::ContextPolicy;

use std::sync::Arc;
use ankurah::{Model, Node, model::{Mutable, View}};
use ankurah::signals::Wait;
use ankurah_jwt_auth::{Binding, JwtAgent, JwtContext, JwtKeys, ModelPolicyView, PolicyPropertyView, PolicyScopeView};
use ankurah_storage_sled::SledStorageEngine;

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
#[model(label = "document")]
pub struct Document {
    pub owner: String,
}

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
#[model(label = "document")]
pub struct DocumentWithEmbargo {
    pub owner: String,
    pub embargo: bool,
}

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
#[model(label = "document")]
pub struct InvalidDocument {
    pub owner: String,
    pub embargo: Vec<u8>,
}

#[tokio::test]
async fn registration_fills_pending_bindings_and_restart_does_not_rebind() -> anyhow::Result<()> {
    let storage = Arc::new(SledStorageEngine::new_test()?);
    let agent = JwtAgent::new_ephemeral();
    let keys = common::test_keys();
    agent.set_keys(JwtKeys::Signing(keys.clone()));
    let node = Node::new_durable(storage.clone(), agent.clone());
    node.system.create().await?;
    let config = serde_json::from_value(serde_json::json!({
        "roles": {"reader": ["read"]},
        "collections": {"document": {"read": "read", "scope": [{"filter": "owner = $jwt.sub AND embargo = false"}]}}
    }))?;
    agent.set_policy(&node, &config).await?;
    let root = node.context_async(JwtContext::Root).await?;
    let policies = root.fetch::<ModelPolicyView>("true").await?;
    let policy = policies[0].id();
    assert_eq!(policies[0].model()?, Binding::Pending);

    let model = root.resolve_model_id::<Document>().await?;
    assert_eq!(root.get::<ModelPolicyView>(policy).await?.model()?, Binding::AtRegistration(model));
    let properties = root.fetch::<PolicyPropertyView>("true").await?;
    let owner = properties.iter().find(|property| property.label().unwrap() == "owner").unwrap();
    let owner_id = owner.id();
    let owner_binding = owner.property()?;
    assert!(matches!(owner_binding, Binding::AtRegistration(_)));
    assert_eq!(properties.iter().find(|property| property.label().unwrap() == "embargo").unwrap().property()?, Binding::Pending);

    let claims = common::make_claims("alice", &["reader"], "alice@example.com");
    let reader = JwtContext::from_claims(claims.clone(), common::sign_token(&keys, &claims));
    let selection = ankql::ast::Predicate::MemberOf(model);
    let transaction = root.begin();
    let document = transaction.create(&Document { owner: "alice".into() }).await?.read()?;
    transaction.commit().await?;
    assert!(ContextPolicy::from_credentials(&agent, &reader).check_read(&document.id(), &document.entity().to_state()?).is_err(),
        "an unresolved restriction cannot become an unrestricted grant");

    assert!(root.resolve_model_id::<InvalidDocument>().await.is_err(), "an invalid scope binding must abort its schema transaction");
    let properties = root.fetch::<PolicyPropertyView>("true").await?;
    assert_eq!(properties.iter().find(|property| property.label().unwrap() == "embargo").unwrap().property()?, Binding::Pending);
    assert!(node.catalog.property_by_name(&match model { ankurah::proto::ModelId::EntityId(id) => id, _ => unreachable!() }, "embargo")?.is_none());
    assert_eq!(root.resolve_model_id::<DocumentWithEmbargo>().await?, model);
    let transaction = root.begin();
    let document = transaction.create(&DocumentWithEmbargo { owner: "alice".into(), embargo: false }).await?.read()?;
    transaction.commit().await?;
    let id = document.id();
    let state = document.entity().to_state()?;
    agent.state_handle().wait_for({
        let agent = agent.clone();
        let reader = reader.clone();
        move |_| ContextPolicy::from_credentials(&agent, &reader).check_read(&id, &state).ok()
    }).await;
    let scope = root.fetch::<PolicyScopeView>("true").await?[0].resolved()?;
    assert!(scope.is_some());
    assert_eq!(root.get::<PolicyPropertyView>(owner_id).await?.property()?, owner_binding, "adding a property must retain earlier bindings");
    let before_restart = ContextPolicy::from_credentials(&agent, &reader).filter_predicate(selection.clone())?;
    drop(root);
    drop(node);
    drop(agent);

    let agent = JwtAgent::new_ephemeral();
    let node = Node::new_durable(storage, agent.clone());
    let root = node.context_async(JwtContext::Root).await?;
    assert_eq!(ContextPolicy::from_credentials(&agent, &reader).filter_predicate(selection)?, before_restart);
    assert_eq!(root.get::<ModelPolicyView>(policy).await?.model()?, Binding::AtRegistration(model));
    assert_eq!(root.get::<PolicyPropertyView>(owner_id).await?.property()?, owner_binding);
    assert_eq!(root.fetch::<PolicyScopeView>("true").await?[0].resolved()?.map(|reference| reference.id()), scope.map(|reference| reference.id()));
    Ok(())
}
