//! The system collection and the metadata catalog are not
//! mutable through ordinary transactions, and the `_ankurah_` collection
//! prefix is reserved.

mod common;
use ankurah::core::test_helpers::commit_transaction;
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
    let model = context.resolve_model_id::<Album>().await?;
    let ordinary = proto::Event::genesis(
        server.system.root_id(),
        proto::AuthorId::Unknown,
        proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(model))]),
    );
    let storage = &server.storage;

    for protected in PROTECTED {
        let protected_model = ankurah::core::schema::system_model_id(protected).unwrap();
        let protected_event = proto::Event::genesis(
            server.system.root_id(),
            proto::AuthorId::Unknown,
            proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(protected_model))]),
        );
        let protected_id = protected_event.entity_id;
        let error = commit_transaction(&server, &DEFAULT_CONTEXT, proto::TransactionId::new(), vec![proto::Attested::opt(ordinary.clone(), None), proto::Attested::opt(protected_event, None)])
            .await
            .expect_err("direct callers must not bypass collection protection");
        assert!(matches!(
            error.downcast_ref::<MutationError>(),
            Some(MutationError::InadmissibleEvent(InadmissibleEvent::ProtectedModel(id))) if *id == protected_model
        ));
        assert!(storage.dump_entity_events(ordinary.entity_id).await?.is_empty());
        assert!(matches!(storage.get_state(ordinary.entity_id).await, Err(RetrievalError::EntityNotFound(_))));
        assert!(server.get_resident_entity(ordinary.entity_id).is_none());
        assert!(storage.dump_entity_events(protected_id).await?.is_empty());
        assert!(matches!(storage.get_state(protected_id).await, Err(RetrievalError::EntityNotFound(_))));
    }

    commit_transaction(&server, &DEFAULT_CONTEXT, proto::TransactionId::new(), vec![proto::Attested::opt(ordinary.clone(), None)])
        .await?;
    assert_eq!(storage.dump_entity_events(ordinary.entity_id).await?.len(), 1, "the ordinary event is valid on its own");
    Ok(())
}
