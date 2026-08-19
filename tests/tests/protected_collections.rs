//! The system collection and the metadata catalog are not
//! mutable through ordinary transactions, and the `_ankurah_` collection
//! prefix is reserved.

mod common;
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
        // A well-formed genesis, so the refusal comes from the protected
        // collection rule rather than from structural validation.
        let event = proto::Event::genesis(
            model,
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

// NOTE: the local-transaction path into a protected collection (the
// `commit_local_trx` guard, core/src/context.rs) is no longer reachable
// through the public API: a user model whose collection carries the
// reserved `_ankurah_` prefix is now REFUSED at derive time, so a struct
// like `_ankurah_model` cannot be defined at all. That compile-time
// rejection is exercised by the trybuild fixture
// `tests/tests/compile_fail/reserved_collection_prefix.rs`
// (see `derive_compile_fail.rs`). The runtime `commit_local_trx` guard
// remains in place as structural defense-in-depth; the receiver-side guard
// for all four protected collections is exercised by
// `server_refuses_commits_into_protected_collections` above.
