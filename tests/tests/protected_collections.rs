//! The system collection and the metadata catalog are not
//! mutable through ordinary transactions, and the `_ankurah_` collection
//! prefix is reserved.

mod common;
use ankurah::core::error::{InadmissibleEvent, RetrievalError};
use common::*;

const PROTECTED: [&str; 4] = ["_ankurah_system", "_ankurah_model", "_ankurah_property", "_ankurah_model_property"];

/// A durable node refuses CommitTransaction events targeting any protected
/// collection outright, regardless of what the sender claims.
#[tokio::test]
async fn server_refuses_commits_into_protected_collections() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await.unwrap();

    for collection in PROTECTED {
        let model = ankurah::core::schema::system_model_id(collection).expect("protected collection has a system model");
        // A well-formed genesis, so the refusal comes from the protected
        // collection rule rather than from structural validation.
        let event = proto::Event::genesis(
            proto::CollectionId::fixed_name(collection),
            Some(EntityId::random()),
            proto::AuthorId::Unknown,
            proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(model))]),
        );
        let resp = client
            .request(
                server.id,
                &DEFAULT_CONTEXT,
                proto::NodeRequestBody::CommitTransaction {
                    id: proto::TransactionId::new(),
                    events: vec![proto::Attested::opt(event, None)],
                },
            )
            .await?;
        match resp {
            proto::NodeResponseBody::Error(e) => assert!(e.contains("protected"), "unexpected refusal message for {collection}: {e}"),
            other => panic!("expected refusal for {collection}, got {other:?}"),
        }
    }
    Ok(())
}

#[tokio::test]
async fn direct_remote_commit_refuses_a_protected_batch_before_writing_any_event() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let context = server.context(DEFAULT_CONTEXT)?;
    let model = context.register_model::<Album>().await?;
    let ordinary = proto::Event::genesis(
        Album::collection(),
        server.system.root_id(),
        proto::AuthorId::Unknown,
        proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(model))]),
    );
    let collection = context.collection(&Album::collection()).await?;

    for protected in PROTECTED.into_iter().chain(["_ankurah_future"]) {
        let protected_collection = proto::CollectionId::fixed_name(protected);
        let protected_event = proto::Event::genesis(
            protected_collection.clone(),
            server.system.root_id(),
            proto::AuthorId::Unknown,
            proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(
                ankurah::core::schema::system_model_id(protected).unwrap_or_else(|| EntityId::random().into()),
            ))]),
        );
        let protected_id = protected_event.entity_id;
        let error = server
            .commit_remote_transaction(
                &DEFAULT_CONTEXT,
                proto::TransactionId::new(),
                vec![proto::Attested::opt(ordinary.clone(), None), proto::Attested::opt(protected_event, None)],
            )
            .await
            .expect_err("direct callers must not bypass collection protection");
        assert!(matches!(
            error,
            MutationError::InadmissibleEvent(InadmissibleEvent::ProtectedCollection(id)) if id == protected_collection
        ));
        assert!(collection.dump_entity_events(ordinary.entity_id).await?.is_empty());
        assert!(matches!(collection.get_state(ordinary.entity_id).await, Err(RetrievalError::EntityNotFound(_))));
        assert!(server.get_resident_entity(ordinary.entity_id).is_none());
        let protected_storage = context.collection(&protected_collection).await?;
        assert!(protected_storage.dump_entity_events(protected_id).await?.is_empty());
        assert!(matches!(protected_storage.get_state(protected_id).await, Err(RetrievalError::EntityNotFound(_))));
    }

    server
        .commit_remote_transaction(&DEFAULT_CONTEXT, proto::TransactionId::new(), vec![proto::Attested::opt(ordinary.clone(), None)])
        .await?;
    assert_eq!(collection.dump_entity_events(ordinary.entity_id).await?.len(), 1, "the ordinary event is valid on its own");
    Ok(())
}
