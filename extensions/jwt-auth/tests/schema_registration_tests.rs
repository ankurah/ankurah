mod common;

use ankurah::{Model, Node, Ref};
use ankurah_core::{
    connector::{PeerSender, SendError},
    error::RetrievalError,
    schema::{catalog::SysModelRowView, MODEL_COLLECTION_ID, MODEL_PROPERTY_COLLECTION_ID, PROPERTY_COLLECTION_ID},
    signals::Get,
};
use ankurah_jwt_auth::{JwtAgent, JwtContext};
use ankurah_proto::{self as proto, EntityId};
use ankurah_storage_sled::SledStorageEngine;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

type TestNode = Node<SledStorageEngine, JwtAgent>;

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct RegistrationOwner {
    pub name: String,
}

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct RegistrationRecord {
    pub owner: Ref<RegistrationOwner>,
    pub body: String,
}

async fn setup() -> anyhow::Result<TestNode> {
    let agent = JwtAgent::new_durable(common::test_keys(), common::blog_config_path())?;
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent);
    node.system.create().await?;
    node.wait_ready().await?;
    Ok(node)
}

async fn catalog_states(node: &TestNode) -> anyhow::Result<Vec<proto::Attested<proto::EntityState>>> {
    let mut states = Vec::new();
    for name in [MODEL_COLLECTION_ID, PROPERTY_COLLECTION_ID, MODEL_PROPERTY_COLLECTION_ID] {
        let collection = node.collections.get(&proto::CollectionId::fixed_name(name)).await?;
        states.extend(collection.fetch_states(&ankql::ast::Predicate::True.into()).await?);
    }
    states.sort_by_key(|state| state.payload.entity_id);
    Ok(states)
}

#[derive(Clone)]
struct ReplySender {
    peer: EntityId,
    tx: mpsc::UnboundedSender<proto::NodeMessage>,
}

impl PeerSender for ReplySender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.tx.send(message).map_err(|_| SendError::ConnectionClosed)
    }
    fn recipient_node_id(&self) -> EntityId { self.peer }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

async fn wire_register(
    node: &TestNode,
    peer: EntityId,
    replies: &mut mpsc::UnboundedReceiver<proto::NodeMessage>,
    auth: proto::AuthData,
    model: proto::RegisterModel,
) -> anyhow::Result<proto::NodeResponseBody> {
    let id = proto::RequestId::new();
    node.handle_message(proto::NodeMessage::Request {
        // One empty entry is an anonymous credential, not an empty credential set.
        auth: vec![auth],
        request: proto::NodeRequest { id: id.clone(), from: peer, to: node.id, body: proto::NodeRequestBody::RegisterSchema { model } },
    })
    .await?;
    match replies.try_recv()? {
        proto::NodeMessage::Response(response) => {
            assert_eq!(response.request_id, id);
            Ok(response.body)
        }
        other => anyhow::bail!("expected registration response, got {other:?}"),
    }
}

#[tokio::test]
async fn anonymous_registration_cannot_mutate_catalog_locally_or_over_wire() -> anyhow::Result<()> {
    let node = setup().await?;
    let anonymous = node.context(JwtContext::NoUser)?;
    let before = catalog_states(&node).await?;
    let error = anonymous.register_model::<RegistrationRecord>().await.expect_err("anonymous registration must be refused");
    assert!(error.to_string().contains("Anonymous contexts cannot change the catalog"), "{error}");
    assert_eq!(catalog_states(&node).await?, before, "no model, reference target, property, or membership may be persisted");

    let peer = EntityId::random();
    let (tx, mut replies) = mpsc::unbounded_channel();
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(ReplySender { peer, tx }),
    )
    .await?;
    let model = proto::RegisterModel::from(RegistrationRecord::descriptor());
    let response = wire_register(&node, peer, &mut replies, proto::AuthData::default(), model.clone()).await?;
    assert!(
        matches!(response, proto::NodeResponseBody::Error(ref error) if error.contains("Anonymous contexts cannot change the catalog")),
        "{response:?}"
    );
    assert_eq!(catalog_states(&node).await?, before);

    let claims = common::make_claims("editor", &["Editor"], "editor@example.com");
    let token = common::sign_token(&common::test_keys(), &claims);
    let response = wire_register(&node, peer, &mut replies, proto::AuthData(token.into_bytes()), model.clone()).await?;
    let registered = match response {
        proto::NodeResponseBody::SchemaRegistered { model } => model,
        other => anyhow::bail!("authenticated registration must succeed: {other:?}"),
    };
    let after = catalog_states(&node).await?;
    assert!(after.len() > before.len());

    let response = wire_register(&node, peer, &mut replies, proto::AuthData::default(), model.clone()).await?;
    assert!(matches!(response, proto::NodeResponseBody::SchemaRegistered { model } if model.id == registered.id));
    assert_eq!(catalog_states(&node).await?, after, "anonymous no-op registration is still allowed");
    assert_eq!(anonymous.register_model::<RegistrationRecord>().await?, proto::ModelId::EntityId(registered.id));
    assert_eq!(anonymous.get::<SysModelRowView>(registered.id).await?.id(), registered.id, "catalog reads remain public");

    let mut extension = model;
    let mut extra = extension.properties[0].clone();
    extra.name = "extra".into();
    extra.build_id = [0x42; 16];
    extension.properties.push(extra);
    let response = wire_register(&node, peer, &mut replies, proto::AuthData::default(), extension).await?;
    assert!(
        matches!(response, proto::NodeResponseBody::Error(ref error) if error.contains("Anonymous contexts cannot change the catalog")),
        "{response:?}"
    );
    assert_eq!(catalog_states(&node).await?, after, "an existing model must not permit anonymous schema extension");
    node.deregister_peer(peer);
    Ok(())
}

#[tokio::test]
async fn catalog_read_exemption_does_not_cover_a_resident_application_entity() -> anyhow::Result<()> {
    let node = setup().await?;
    let root = node.context(JwtContext::system())?;
    let id = {
        let trx = root.begin();
        let owner = trx.create(&RegistrationOwner { name: "Owner".into() }).await?;
        let id = trx.create(&RegistrationRecord { owner: owner.id().into(), body: "private".into() }).await?.id();
        trx.commit().await?;
        id
    };
    let resident = root.get::<RegistrationRecordView>(id).await?;
    let anonymous = node.context(JwtContext::NoUser)?;
    assert!(matches!(anonymous.get::<RegistrationRecordView>(id).await, Err(RetrievalError::AccessDenied(_))));
    assert!(matches!(anonymous.get::<SysModelRowView>(id).await, Err(RetrievalError::EntityNotFound(found)) if found == id));
    let model_id = node.catalog.model_by_label("registrationrecord").unwrap().expect("registered model").0;
    assert_eq!(anonymous.get::<SysModelRowView>(model_id).await?.id(), model_id);
    assert_eq!(resident.id(), id, "keep the inaccessible entity resident through the mismatched read");
    Ok(())
}

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct ScopeProbe {
    pub body: String,
}

/// The registration response binds the declaration independently of catalog subscription delivery.
#[tokio::test]
async fn registration_completes_before_catalog_delivery() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        let config = r#"{
        "roles": { "writer": ["records"] },
        "collections": { "scopeprobe": {
            "read": "records", "write": "records",
            "scope": [{ "filter": "body = $jwt.sub" }]
        }}
    }"#;
        let agent = JwtAgent::new_durable(common::test_keys(), common::blog_config_path())?;
        agent.update_config(serde_json::from_str(config)?);
        let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent);
        server.system.create().await?;
        let agent = JwtAgent::new_durable(common::test_keys(), common::blog_config_path())?;
        agent.update_config(serde_json::from_str(config)?);
        let client = Node::new(Arc::new(SledStorageEngine::new_test()?), agent);
        let (server_tx, mut server_rx) = mpsc::unbounded_channel();
        let (client_tx, mut client_rx) = mpsc::unbounded_channel();
        client
            .register_peer(
                proto::Presence {
                    node_id: server.id,
                    durable: true,
                    system_root: server.system.root(),
                    protocol_version: proto::PROTOCOL_VERSION,
                },
                Box::new(ReplySender { peer: server.id, tx: server_tx }),
            )
            .await?;
        server
            .register_peer(
                proto::Presence {
                    node_id: client.id,
                    durable: false,
                    system_root: client.system.root(),
                    protocol_version: proto::PROTOCOL_VERSION,
                },
                Box::new(ReplySender { peer: client.id, tx: client_tx }),
            )
            .await?;

        let held = Arc::new(Mutex::new(None::<Vec<proto::NodeMessage>>));
        let server_task = {
            let server = server.clone();
            tokio::spawn(async move {
                while let Some(message) = server_rx.recv().await {
                    let server = server.clone();
                    tokio::spawn(async move {
                        server.handle_message(message).await.unwrap();
                    });
                }
            })
        };
        let client_task = {
            let client = client.clone();
            let held = held.clone();
            tokio::spawn(async move {
                while let Some(message) = client_rx.recv().await {
                    if let proto::NodeMessage::Update(update) = &message {
                        let proto::NodeUpdateBody::SubscriptionUpdate { items } = &update.body;
                        if items.iter().all(|item| {
                            matches!(item.collection.as_str(), MODEL_COLLECTION_ID | PROPERTY_COLLECTION_ID | MODEL_PROPERTY_COLLECTION_ID)
                        }) {
                            if let Some(held) = held.lock().unwrap().as_mut() {
                                held.push(message);
                                continue;
                            }
                        }
                    }
                    let client = client.clone();
                    tokio::spawn(async move {
                        client.handle_message(message).await.unwrap();
                    });
                }
            })
        };
        client.system.wait_system_ready().await?;
        client.wait_ready().await?;
        *held.lock().unwrap() = Some(Vec::new());

        let claims = common::make_claims("hello", &["writer"], "writer@example.com");
        let token = common::sign_token(&common::test_keys(), &claims);
        let context = client.context(JwtContext::from_claims(claims, token))?;
        let model = context.register_model::<ScopeProbe>().await?;
        let epoch = client.system.system_epoch().unwrap();
        assert_eq!(ScopeProbe::descriptor().resolved.get(epoch), Some(model));
        assert!(ScopeProbe::descriptor().properties.iter().all(|property| property.resolved.get(epoch).is_some()));
        assert!(server.catalog.model_by_label("scopeprobe").unwrap().is_some());
        assert!(client.catalog.model_by_label("scopeprobe").unwrap().is_none(), "registration must not wait for catalog delivery");

        loop {
            let count: usize = held
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .iter()
                .map(|message| match message {
                    proto::NodeMessage::Update(update) => match &update.body {
                        proto::NodeUpdateBody::SubscriptionUpdate { items } => items.len(),
                    },
                    _ => 0,
                })
                .sum();
            if count >= 3 {
                break;
            } // One model, property, and membership row.
            tokio::task::yield_now().await;
        }
        let updates = held.lock().unwrap().take().unwrap();
        for update in updates {
            client.handle_message(update).await?;
        }

        // Scoped operations still need JWT's catalog-only resolver; that is separate from registration.
        let trx = context.begin();
        let id = trx.create(&ScopeProbe { body: "hello".into() }).await?.id();
        trx.commit().await?;
        let query = context.query::<ScopeProbeView>("body = 'hello'")?;
        query.wait_durable_answered().await?;
        assert!(query.get().iter().any(|row| row.id() == id));
        assert_eq!(context.fetch::<ScopeProbeView>("body = 'hello'").await?.len(), 1);

        server.deregister_peer(client.id);
        client.deregister_peer(server.id);
        server_task.abort();
        client_task.abort();
        Ok::<(), anyhow::Error>(())
    })
    .await?
}
