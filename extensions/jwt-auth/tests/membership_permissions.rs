use ankurah_core::test_helpers::commit_transaction;
mod common;

use std::sync::Arc;

use ankurah::{Model, Node, View};
use ankurah::model::Mutable;
use ankurah::core::storage::StorageEngine;
use ankurah::policy::PolicyAgent;
use ankurah_jwt_auth::{JwtAgent, JwtContext, JwtKeys, PolicyConfig};
use ankurah_proto as proto;
use ankurah_storage_sled::SledStorageEngine;

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct Document {
    #[active_type(LWW)]
    pub owner: String,
}

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct Attachment {
    pub caption: Option<String>,
}

#[tokio::test]
async fn event_state_checks_and_late_denial_roll_back_the_transaction() -> anyhow::Result<()> {
    use ankurah_core::{
        property::backend::{LWWBackend, PropertyBackend},
        schema::resolver::ModelResolver,
        value::Value,
    };

    let keys = common::test_keys();
    let agent = JwtAgent::new_ephemeral();
    agent.set_keys(JwtKeys::Signing(keys.clone()));
    let storage = Arc::new(SledStorageEngine::new_test()?);
    let node = Node::new_durable(storage.clone(), agent.clone());
    node.system.create().await?;
    agent.set_policy(&node, &serde_json::from_str(r#"{
        "roles": { "editor": ["edit"] },
        "collections": { "document": {
            "read": "edit", "write": "edit", "scope": [{ "filter": "owner = $jwt.sub" }]
        } }
    }"#)?).await?;
    let root = node.context_async(JwtContext::Root).await?;
    let model = root.resolve_model_id::<Document>().await?;
    let credential = |subject| {
        let claims = common::make_claims(subject, &["editor"], "editor@example.com");
        JwtContext::from_claims(claims.clone(), common::sign_token(&keys, &claims))
    };
    let context = node.context_async(credential("alice")).await?;
    let trx = context.begin();
    let local = trx.create(&Document { owner: "alice".into() }).await?;
    local.owner()?.set(&"bob".into())?;
    let local_id = local.id();
    assert!(trx.commit().await.is_err());
    assert!(storage.dump_entity_events(local_id).await?.is_empty());
    assert!(matches!(storage.get_state(local_id).await, Err(ankurah_core::error::RetrievalError::EntityNotFound(_))));

    let owner = node.catalog.resolve_property(&model, "owner")?.unwrap().id;
    let set_owner = |value: &str| -> anyhow::Result<proto::Operation> {
        let backend = LWWBackend::new();
        backend.set(owner, Some(Value::String(value.into())));
        Ok(proto::Operation::Backend { backend: "lww".into(), operations: backend.to_operations()?.unwrap() })
    };
    let create = |value: &str| -> anyhow::Result<proto::Event> {
        Ok(proto::Event::genesis(node.system.root_id(), proto::AuthorId::Unknown, proto::OperationSet(vec![
            proto::Operation::Membership(proto::Membership::Add(model)), set_owner(value)?,
        ])))
    };
    let genesis = create("alice")?;
    let remote_id = genesis.entity_id;
    let completion = proto::Event::update(remote_id, genesis.id().into(), proto::AuthorId::Unknown, proto::OperationSet(vec![set_owner("alice")?]));
    commit_transaction(&node, &credential("alice"), proto::TransactionId::new(), vec![genesis.into(), completion.into()]).await?;
    assert_eq!(context.get::<DocumentView>(remote_id).await?.owner()?, "alice");
    assert_eq!(storage.dump_entity_events(remote_id).await?.len(), 2);

    let genesis = create("bob")?;
    let denied_id = genesis.entity_id;
    let completion = proto::Event::update(denied_id, genesis.id().into(), proto::AuthorId::Unknown, proto::OperationSet(vec![set_owner("alice")?]));
    assert!(commit_transaction(&node,
        &credential("alice"), proto::TransactionId::new(), vec![genesis.into(), completion.into()],
    ).await.is_err(), "a later event cannot rescue an unauthorized earlier state");
    assert!(storage.dump_entity_events(denied_id).await?.is_empty());

    let allowed = create("alice")?;
    let denied = create("alice")?;
    let ids = [allowed.entity_id, denied.entity_id];
    let outside_scope = proto::Event::update(denied.entity_id, denied.id().into(), proto::AuthorId::Unknown, proto::OperationSet(vec![set_owner("bob")?]));
    assert!(commit_transaction(&node,
        &credential("alice"), proto::TransactionId::new(), vec![allowed.into(), denied.into(), outside_scope.into()],
    ).await.is_err());
    for id in ids {
        assert!(matches!(storage.get_state(id).await, Err(ankurah_core::error::RetrievalError::EntityNotFound(_))));
        assert!(storage.dump_entity_events(id).await?.is_empty(), "a state denial must roll back earlier entities and events too");
    }
    Ok(())
}

#[tokio::test]
async fn existing_membership_authorizes_addition_but_new_membership_cannot_authorize_itself() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = JwtAgent::new_ephemeral();
    agent.set_keys(JwtKeys::Signing(keys.clone()));
    agent.update_config(serde_json::from_str::<PolicyConfig>(r#"{
        "roles": {
            "editor": ["document:read", "document:write"],
            "reader": ["document:read", "attachment:write"]
        },
        "collections": {
            "document": {
                "read": "document:read", "write": "document:write",
                "scope": [{ "filter": "owner = $jwt.sub" }]
            },
            "attachment": { "read": "attachment:read", "write": "attachment:write" }
        }
    }"#)?);
    let storage = Arc::new(SledStorageEngine::new_test()?);
    let node = Node::new_durable(storage.clone(), agent.clone());
    node.system.create().await?;
    agent.set_policy(&node, &agent.config()).await?;
    let root = node.context_async(JwtContext::Root).await?;
    let attachment = root.resolve_model_id::<Attachment>().await?;
    let trx = root.begin();
    let local = trx.create(&Document { owner: "alice".into() }).await?.id();
    let remote = trx.create(&Document { owner: "alice".into() }).await?.id();
    let foreign = trx.create(&Document { owner: "bob".into() }).await?.id();
    trx.commit().await?;

    let credential = |role| {
        let claims = common::make_claims("alice", &[role], "alice@example.com");
        let token = common::sign_token(&keys, &claims);
        JwtContext::from_claims(claims, token)
    };
    let editor = node.context_async(credential("editor")).await?;
    let reader = node.context_async(credential("reader")).await?;
    let trx = reader.begin();
    assert!(trx.get::<Document>(&local).await.is_err(), "read access is insufficient to add a membership");
    drop(trx);

    let add_attachment = |state: proto::Attested<proto::EntityState>| proto::Attested::from(proto::Event::update(
        state.payload.entity_id, state.payload.state.head, proto::AuthorId::Unknown,
        proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(attachment))]),
    ));
    let local_before = storage.get_state(local).await?;
    assert!(commit_transaction(&node,
        &credential("reader"), proto::TransactionId::new(), vec![add_attachment(local_before.clone())],
    ).await.is_err(), "permission for the proposed membership cannot authorize its addition");
    assert_eq!(storage.get_state(local).await?, local_before);
    let foreign_before = storage.get_state(foreign).await?;
    assert!(commit_transaction(&node,
        &credential("editor"), proto::TransactionId::new(), vec![add_attachment(foreign_before.clone())],
    ).await.is_err(), "existing-membership row scope still applies");
    assert_eq!(storage.get_state(foreign).await?, foreign_before);

    let trx = editor.begin();
    trx.get::<Document>(&local).await?.entity().add_membership(attachment)?;
    trx.commit().await?;
    commit_transaction(&node,
        &credential("editor"), proto::TransactionId::new(), vec![add_attachment(storage.get_state(remote).await?)],
    ).await?;
    for id in [local, remote] {
        assert!(storage.get_state(id).await?.payload.state.memberships.contains(&attachment));
        assert_eq!(editor.get::<AttachmentView>(id).await?.id(), id, "read authorization uses ANY actual membership");
    }
    let mut ids: Vec<_> = editor.fetch::<AttachmentView>("true").await?.iter().map(View::id).collect();
    ids.sort();
    let mut expected = vec![local, remote];
    expected.sort();
    assert_eq!(ids, expected, "query authorization agrees with direct reads through another model");
    Ok(())
}

#[tokio::test]
async fn wildcard_cannot_use_unconfigured_membership_to_escape_a_scope() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = JwtAgent::new_ephemeral();
    agent.set_keys(JwtKeys::Signing(keys.clone()));
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node.system.create().await?;
    agent.set_policy(&node, &serde_json::from_str(r#"{
        "roles": { "admin": ["*"] },
        "collections": {
            "document": {
                "read": "read", "write": "write",
                "scope": [{ "filter": "owner = $jwt.sub" }]
            }
        }
    }"#)?).await?;
    let root = node.context_async(JwtContext::Root).await?;
    let attachment = root.resolve_model_id::<Attachment>().await?;
    let trx = root.begin();
    let owned = trx.create(&Document { owner: "alice".into() }).await?;
    owned.entity().add_membership(attachment)?;
    let foreign = trx.create(&Document { owner: "bob".into() }).await?;
    foreign.entity().add_membership(attachment)?;
    let unconfigured = trx.create(&Attachment { caption: None }).await?.id();
    let (owned, foreign) = (owned.id(), foreign.id());
    trx.commit().await?;

    let claims = common::make_claims("alice", &["admin"], "alice@example.com");
    let credential = JwtContext::from_claims(claims.clone(), common::sign_token(&keys, &claims));
    let admin = node.context_async(credential.clone()).await?;
    assert_eq!(admin.get::<AttachmentView>(owned).await?.id(), owned, "the document grant applies to the whole entity");
    assert!(admin.get::<DocumentView>(foreign).await.is_err(), "an unconfigured component must not bypass the document scope");
    assert!(admin.get::<AttachmentView>(unconfigured).await.is_err());
    assert!(agent.can_access_model(&credential, &attachment).is_err());
    for ids in [
        admin.fetch::<DocumentView>("true").await?.iter().map(View::id).collect::<Vec<_>>(),
        admin.fetch::<AttachmentView>("true").await?.iter().map(View::id).collect(),
    ] {
        assert_eq!(ids, vec![owned]);
    }

    let foreign = root.get::<DocumentView>(foreign).await?;
    assert!(agent.check_write(&credential, foreign.entity(), None).is_err());
    let trx = admin.begin();
    assert!(trx.create(&Attachment { caption: None }).await.is_err(), "wildcard must not invent a write grant");
    assert!(trx.get::<Document>(&owned).await.is_ok(), "the configured write privilege is satisfied");
    Ok(())
}
