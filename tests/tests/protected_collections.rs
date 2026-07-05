//! RFC section 4: the system collection and the metadata catalog are not
//! mutable through ordinary transactions, and the `_ankurah_` collection
//! prefix is reserved.

mod common;
use common::*;
use serde::{Deserialize, Serialize};
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
            collection: collection.into(),
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

/// Local transactions are refused the same way (the only catalog mutation
/// path is the registration operation, which does not go through
/// client transactions).
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Model, Serialize, Deserialize)]
pub struct _ankurah_model {
    pub name: String,
}

#[tokio::test]
async fn local_commits_into_protected_collections_refused() -> anyhow::Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context(DEFAULT_CONTEXT)?;

    let trx = ctx.begin();
    trx.create(&_ankurah_model { name: "smuggled".into() }).await?;
    let err = trx.commit().await.expect_err("protected collection must refuse local commits");
    assert!(err.to_string().contains("protected"), "unexpected error: {err}");
    Ok(())
}

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
