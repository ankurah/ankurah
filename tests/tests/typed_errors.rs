mod common;

use ankurah::core::error::{ApplyError, IngestError, LineageRejection, MutationError};
use ankurah::core::node_applier::NodeApplier;
use ankurah::core::property::backend::{lww::LWWBackend, PropertyBackend};
use ankurah::core::property::PropertyKey;
use ankurah::core::value::Value;
use ankurah::proto::{self, Attested};
use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent, View};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::Arc;

use common::{Record, RecordView};

/// Forge a Record LWW event setting `title`, parented on the given clock,
/// with an EXPLICIT claimed generation (both forges in this file parent on
/// clocks with no resolvable true generation: a fabricated id and an empty
/// genesis clock).
fn forge_title_event(
    model: proto::EntityId,
    title_property: proto::EntityId,
    entity_id: proto::EntityId,
    parent: proto::Clock,
    title: &str,
    generation: u32,
) -> proto::Event {
    let backend = LWWBackend::new();
    backend.set(PropertyKey::Id(title_property), Some(Value::String(title.to_owned())));
    let ops = backend.to_operations().unwrap().expect("LWW backend with a write produces operations");
    ankurah_tests::forge::event_claiming(
        entity_id,
        model,
        proto::OperationSet(BTreeMap::from([("lww".to_owned(), ops)])),
        parent,
        generation,
    )
}

fn event_only_item(event: proto::Event) -> proto::SubscriptionUpdateItem {
    proto::SubscriptionUpdateItem {
        entity_id: event.entity_id,
        model: event.model,
        content: proto::UpdateContent::EventOnly(vec![Attested::opt(event, None).into()]),
        predicate_relevance: vec![],
        source_queries: vec![],
    }
}

/// M5 pin: a non-creation event for an entity this node knows nothing about
/// must surface as the typed lineage rejection, not the anonymous
/// InvalidEvent. The planner types this case (NeedsState) at plan time; the
/// per-item error the ack layer sees must say what actually happened.
#[tokio::test]
async fn test_unknown_entity_event_yields_typed_lineage_rejection() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;
    let ctx_c = client.context(c)?;

    // Relay context so apply_updates accepts updates from this peer.
    let _relay_context = ctx_c.query_wait::<RecordView>("title = 'no-such-title'").await?;
    let record_model = client.catalog.model_id_for(Record::collection().as_str()).expect("Record registered by query");
    let title_property = client.catalog.resolve(Record::collection().as_str(), "title").expect("Record.title registered by query");

    let unknown = proto::EntityId::new();
    // The parent id is fabricated, so no true generation exists; 2 is a
    // plausible claim and admission can never verify it (NeedsState preempts).
    let ev = forge_title_event(
        record_model,
        title_property,
        unknown,
        proto::Clock::from(vec![proto::EventId::from_bytes([7u8; 32])]),
        "ghost",
        2,
    );

    let err = NodeApplier::apply_updates_for_test(&client, &server.id, vec![event_only_item(ev)])
        .await
        .expect_err("a non-creation event for an unknown entity must fail the item");
    let ApplyError::Items(items) = err else {
        panic!("expected per-item aggregation, got {err:?}");
    };
    assert!(
        matches!(&items[0].cause, MutationError::Ingest(IngestError::Lineage(LineageRejection::NonCreationOverEmptyHead))),
        "expected typed NonCreationOverEmptyHead, got {:?}",
        items[0].cause
    );

    Ok(())
}

/// M5 pin: a forged second genesis for an entity with real history must
/// surface as the typed Disjoint lineage rejection. Today the comparison's
/// LineageError::Disjoint passes through raw; the typed taxonomy separates
/// it from BudgetExceeded, which shares that enum (C4-08: a budget exhaustion
/// is a resumable liveness anomaly, never a lineage verdict).
#[tokio::test]
async fn test_second_genesis_yields_typed_disjoint() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let ctx_s = server.context(c)?;
    let ctx_c = client.context(c)?;
    let _relay_context = ctx_c.query_wait::<RecordView>("title = 'no-such-title'").await?;

    let rec_id = {
        let trx = ctx_s.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        trx.commit().await?;
        id
    };
    let view = ctx_c.get::<RecordView>(rec_id).await?;
    assert_eq!(view.title().unwrap(), "t0");
    let record_model = client.catalog.model_id_for(Record::collection().as_str()).expect("Record registered by create");
    let title_property = client.catalog.resolve(Record::collection().as_str(), "title").expect("Record.title registered by create");

    // A creation event (empty parents) for an entity that already has
    // committed history: different genesis, provably disjoint. Claims the
    // correct genesis generation (1) so the Disjoint verdict, not a
    // generation check, is what rejects it.
    let evil_genesis = forge_title_event(record_model, title_property, rec_id, proto::Clock::default(), "evil-genesis", 1);

    let err = NodeApplier::apply_updates_for_test(&client, &server.id, vec![event_only_item(evil_genesis)])
        .await
        .expect_err("a second genesis must fail the item");
    let ApplyError::Items(items) = err else {
        panic!("expected per-item aggregation, got {err:?}");
    };
    assert!(
        matches!(&items[0].cause, MutationError::Ingest(IngestError::Lineage(LineageRejection::Disjoint))),
        "expected typed Disjoint, got {:?}",
        items[0].cause
    );
    // The forged genesis must not have perturbed the entity.
    assert_eq!(view.title().unwrap(), "t0");

    Ok(())
}
