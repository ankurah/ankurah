//! The system collection and the metadata catalog are not
//! mutable through ordinary transactions, and the `_ankurah_` collection
//! prefix is reserved.

mod common;
use ankurah::core::schema::catalog::rows::SysModelRow;
use common::*;

const PROTECTED: [&str; 4] = ["_ankurah_system", "_ankurah_model", "_ankurah_property", "_ankurah_model_property"];

/// A durable node refuses CommitTransaction events targeting any protected
/// collection outright, regardless of what the sender claims.
#[tokio::test]
async fn server_refuses_commits_into_protected_collections() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    for collection in PROTECTED {
        let model = ankurah::core::schema::system_model_id(collection).expect("protected collection has a system model");
        let event = proto::Event {
            collection: proto::CollectionId::fixed_name(collection),
            entity_id: EntityId::new(),
            operations: proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(model))]),
            parent: proto::Clock::default(),
        };
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

/// The built-in typed catalog rows are public for typed reads, but their
/// system identity must not turn an ordinary local transaction into a second
/// registration path.
#[tokio::test]
async fn local_transaction_refuses_typed_catalog_write() -> anyhow::Result<()> {
    let node = durable_sled_setup().await?;
    let context = node.context(DEFAULT_CONTEXT)?;

    let transaction = context.begin();
    transaction.create(&SysModelRow { label: "blocked".into(), name: "Blocked".into() }).await?;
    let error = transaction.commit().await.expect_err("ordinary catalog mutation must be rejected");
    let message = error.to_string();
    assert!(message.contains("protected"), "unexpected refusal: {message}");
    assert!(message.contains("_ankurah_model"), "refusal should identify the collection: {message}");
    assert!(node.catalog.model_by_label("blocked").is_none(), "rejected row must not reach the catalog map");

    Ok(())
}
