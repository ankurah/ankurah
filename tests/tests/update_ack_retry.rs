use ankurah::core::connector::{PeerSender, SendError};
use ankurah::proto;
use ankurah::{Model, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

mod common;
use common::{InstrumentedEngine, Record};

#[derive(Clone)]
struct ChannelSender {
    peer: proto::EntityId,
    tx: mpsc::UnboundedSender<proto::NodeMessage>,
}

#[async_trait]
impl PeerSender for ChannelSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.tx.send(message).map_err(|_| SendError::ConnectionClosed)
    }

    fn recipient_node_id(&self) -> proto::EntityId { self.peer }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

#[tokio::test]
async fn retryable_update_ack_resends_until_success() -> anyhow::Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    node.system.create().await?;
    let peer = proto::EntityId::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(ChannelSender { peer, tx }),
    )?;

    node.send_update(peer, proto::NodeUpdateBody::SubscriptionUpdate { items: Vec::new() });
    let first = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await?.expect("first update");
    let proto::NodeMessage::Update(first) = first else { panic!("expected update") };

    node.handle_message(proto::NodeMessage::UpdateAck(proto::NodeUpdateAck {
        id: first.id.clone(),
        from: peer,
        to: node.id,
        body: proto::NodeUpdateAckBody::RetryableError("staging backpressure".to_owned()),
    }))
    .await?;

    let second = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await?.expect("retried update");
    let proto::NodeMessage::Update(second) = second else { panic!("expected retried update") };
    assert_ne!(first.id, second.id, "each retry gets a fresh waiter identity");

    node.handle_message(proto::NodeMessage::UpdateAck(proto::NodeUpdateAck {
        id: second.id,
        from: peer,
        to: node.id,
        body: proto::NodeUpdateAckBody::Success,
    }))
    .await?;

    assert!(tokio::time::timeout(Duration::from_millis(500), rx.recv()).await.is_err(), "success terminates the retry lease");
    Ok(())
}

fn model_update(model: proto::EntityId) -> proto::NodeUpdateBody {
    proto::NodeUpdateBody::SubscriptionUpdate {
        items: vec![proto::SubscriptionUpdateItem {
            entity_id: proto::EntityId::new(),
            model,
            content: proto::UpdateContent::EventOnly(Vec::new()),
            predicate_relevance: Vec::new(),
            source_queries: vec![],
        }],
    }
}

#[tokio::test]
async fn schema_is_repeated_until_a_schema_bearing_update_is_acknowledged() -> anyhow::Result<()> {
    let engine = InstrumentedEngine::new(SledStorageEngine::new_test()?);
    let instruments = engine.instruments();
    let node = Node::new_durable(Arc::new(engine), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(ankurah::policy::DEFAULT_CONTEXT)?;
    ctx.register::<Record>().await?;
    let model = node.catalog.model_id_for(Record::collection().as_str()).expect("Record registered");

    let peer = proto::EntityId::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(ChannelSender { peer, tx }),
    )?;

    instruments.hold_gets(0, 1);
    node.send_update(peer, model_update(model));
    instruments.wait_until_get_parked(1).await;
    node.send_update(peer, model_update(model));

    assert!(
        tokio::time::timeout(Duration::from_millis(150), rx.recv()).await.is_err(),
        "a schema-less followup cannot overtake the parked first announcement"
    );
    instruments.release_held_gets();

    let first = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await?.expect("first update");
    let proto::NodeMessage::Update(first) = first else { panic!("expected update") };
    assert!(!first.schema.is_empty(), "the first model body carries the complete descriptor bundle");

    let second = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await?.expect("second update");
    let proto::NodeMessage::Update(second) = second else { panic!("expected update") };
    assert!(!second.schema.is_empty(), "concurrent followups repeat descriptors until delivery is acknowledged");

    for update in [first, second] {
        node.handle_message(proto::NodeMessage::UpdateAck(proto::NodeUpdateAck {
            id: update.id,
            from: peer,
            to: node.id,
            body: proto::NodeUpdateAckBody::Success,
        }))
        .await?;
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    node.send_update(peer, model_update(model));
    let third = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await?.expect("post-ack update");
    let proto::NodeMessage::Update(third) = third else { panic!("expected update") };
    assert!(third.schema.is_empty(), "an acknowledged model may use the once-per-connection fast path");
    node.handle_message(proto::NodeMessage::UpdateAck(proto::NodeUpdateAck {
        id: third.id,
        from: peer,
        to: node.id,
        body: proto::NodeUpdateAckBody::Success,
    }))
    .await?;
    Ok(())
}

#[tokio::test]
async fn success_ack_owned_before_reset_cannot_reannounce_schema_afterward() -> anyhow::Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(ankurah::policy::DEFAULT_CONTEXT)?;
    ctx.register::<Record>().await?;
    let model = node.catalog.model_id_for(Record::collection().as_str()).expect("Record registered");

    let peer = proto::EntityId::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(ChannelSender { peer, tx }),
    )?;

    node.send_update(peer, model_update(model));
    let sent = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await?.expect("schema-bearing update");
    let proto::NodeMessage::Update(sent) = sent else { panic!("expected update") };
    assert!(!sent.schema.is_empty());

    let gate = node.gate_next_update_ack_after_waiter_removed();
    let ack = {
        let node = node.clone();
        tokio::spawn(async move {
            node.handle_message(proto::NodeMessage::UpdateAck(proto::NodeUpdateAck {
                id: sent.id,
                from: peer,
                to: node.id,
                body: proto::NodeUpdateAckBody::Success,
            }))
            .await
        })
    };

    gate.wait_until_waiter_removed().await;
    node.system.hard_reset().await?;
    gate.release();
    ack.await??;
    node.wait_for_update_ack_processed_for_test().await;

    assert!(
        !node.model_announced_to_peer_for_test(peer, model),
        "a pre-reset success ack must not restore old-system schema delivery state"
    );
    Ok(())
}

#[tokio::test]
async fn hard_reset_cancels_pending_old_system_requests() -> anyhow::Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    node.system.create().await?;
    let peer = proto::EntityId::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(ChannelSender { peer, tx }),
    )?;

    let request = {
        let node = node.clone();
        tokio::spawn(async move {
            node.request(
                peer,
                &ankurah::policy::DEFAULT_CONTEXT,
                proto::NodeRequestBody::Get { collection: proto::CollectionId::fixed_name("reset_request"), ids: Vec::new() },
            )
            .await
        })
    };
    let sent = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await?.expect("request message");
    let proto::NodeMessage::Request { request: sent, .. } = sent else { panic!("expected request") };

    node.system.hard_reset().await?;
    let result = request.await?;
    assert!(matches!(result, Err(ankurah::core::error::RequestError::InternalChannelClosed)));

    // A response from the dead system has no waiter and therefore cannot
    // ingest attached schema or complete a successor-system operation.
    node.handle_message(proto::NodeMessage::Response(proto::NodeResponse {
        request_id: sent.id,
        from: peer,
        to: node.id,
        body: proto::NodeResponseBody::Get(Vec::new()),
        schema: Vec::new(),
    }))
    .await?;
    Ok(())
}

#[tokio::test]
async fn response_owned_before_reset_cannot_deliver_or_rewarm_the_catalog_afterward() -> anyhow::Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(ankurah::policy::DEFAULT_CONTEXT)?;
    ctx.register::<Record>().await?;
    let model = node.catalog.model_id_for(Record::collection().as_str()).expect("Record registered");
    let model_state = node.collections.get(&ankurah::core::schema::model_collection()).await?.get_state(model).await?;

    let peer = proto::EntityId::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(ChannelSender { peer, tx }),
    )?;

    let request = {
        let node = node.clone();
        tokio::spawn(async move {
            node.request(
                peer,
                &ankurah::policy::DEFAULT_CONTEXT,
                proto::NodeRequestBody::Get { collection: proto::CollectionId::fixed_name("response_reset"), ids: Vec::new() },
            )
            .await
        })
    };
    let sent = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await?.expect("request message");
    let proto::NodeMessage::Request { request: sent, .. } = sent else { panic!("expected request") };

    let gate = node.gate_next_response_after_waiter_removed();
    let response = {
        let node = node.clone();
        tokio::spawn(async move {
            node.handle_message(proto::NodeMessage::Response(proto::NodeResponse {
                request_id: sent.id,
                from: peer,
                to: node.id,
                body: proto::NodeResponseBody::Get(Vec::new()),
                schema: vec![model_state],
            }))
            .await
        })
    };

    gate.wait_until_waiter_removed().await;
    node.system.hard_reset().await?;
    assert!(node.catalog.model_id_for(Record::collection().as_str()).is_none(), "reset clears old-system catalog identity");
    gate.release();

    response.await??;
    let result = request.await?;
    assert!(matches!(result, Err(ankurah::core::error::RequestError::ConnectionLost)));
    assert!(
        node.catalog.model_id_for(Record::collection().as_str()).is_none(),
        "the owned pre-reset response must not re-ingest its attached old-system schema"
    );
    Ok(())
}

#[tokio::test]
async fn disconnect_cancels_pending_requests() -> anyhow::Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    node.system.create().await?;
    let peer = proto::EntityId::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(ChannelSender { peer, tx }),
    )?;

    let request = {
        let node = node.clone();
        tokio::spawn(async move {
            node.request(
                peer,
                &ankurah::policy::DEFAULT_CONTEXT,
                proto::NodeRequestBody::Get { collection: proto::CollectionId::fixed_name("disconnect_request"), ids: Vec::new() },
            )
            .await
        })
    };
    let sent = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await?.expect("request message");
    assert!(matches!(sent, proto::NodeMessage::Request { .. }));

    node.deregister_peer(peer);
    let result = tokio::time::timeout(Duration::from_secs(1), request).await.expect("disconnect must release the pending request")?;
    assert!(matches!(result, Err(ankurah::core::error::RequestError::InternalChannelClosed)));
    Ok(())
}
