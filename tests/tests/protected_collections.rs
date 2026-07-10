//! RFC section 4 (specs/model-property-metadata/rfc.md): the system collection and the metadata catalog are not
//! mutable through ordinary transactions, and the `_ankurah_` collection
//! prefix is reserved.

mod common;
use common::*;
use std::collections::BTreeMap;

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
        let event = proto::Event {
            // #330: the protected-write guard now keys on well-known model ids
            // (`well_known_collection(&event.model)`), so the forged event must
            // carry the collection's well-known model id for the guard to fire.
            model: ankurah::core::schema::well_known_model_id(collection).expect("every protected collection has a well-known model id"),
            entity_id: EntityId::new(),
            operations: proto::OperationSet(BTreeMap::new()),
            parent: proto::Clock::default(),
        };
        let resp = client
            .request(
                server.id,
                &DEFAULT_CONTEXT,
                proto::NodeRequestBody::CommitTransaction { id: proto::TransactionId::new(), events: vec![event.into()] },
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
// reserved `_ankurah_` prefix is now REFUSED at derive time (RFC section 4,
// "rejected for user-model collection ids at derive time"), so a struct
// like `_ankurah_model` cannot be defined at all. That compile-time
// rejection is exercised by the trybuild fixture
// `tests/tests/compile_fail/reserved_collection_prefix.rs`
// (see `derive_compile_fail.rs`). The runtime `commit_local_trx` guard
// remains in place as structural defense-in-depth; the receiver-side guard
// for all four protected collections is exercised by
// `server_refuses_commits_into_protected_collections` above.

/// Unknown collections under the reserved prefix never get storage: a
/// fetch for one is refused rather than creating it on demand.
#[tokio::test]
async fn reserved_prefix_collections_get_no_storage() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let resp = client
        .request(
            server.id,
            &DEFAULT_CONTEXT,
            proto::NodeRequestBody::Fetch {
                collection: "_ankurah_evil".into(),
                selection: ankql::parser::parse_selection("true")?,
                known_matches: vec![],
            },
        )
        .await;

    match resp {
        Ok(proto::NodeResponseBody::Error(e)) => assert!(e.contains("reserved"), "unexpected error: {e}"),
        Err(e) => assert!(e.to_string().contains("reserved"), "unexpected error: {e}"),
        Ok(other) => panic!("expected reserved-prefix refusal, got {other:?}"),
    }
    Ok(())
}
