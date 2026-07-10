mod common;

use ankurah::core::error::{ApplyError, IngestError, LineageRejection, MutationError};
use ankurah::core::node_applier::NodeApplier;
use ankurah::core::property::backend::{lww::LWWBackend, PropertyBackend};
use ankurah::core::value::Value;
use ankurah::proto::{self, Attested};
use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent, View};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::Arc;

use common::{Record, RecordView};

/// The LWW OperationSet for a title write.
fn title_ops(title: &str) -> proto::OperationSet {
    let backend = LWWBackend::new();
    backend.set("title".into(), Some(Value::String(title.to_owned())));
    let ops = backend.to_operations().unwrap().expect("LWW backend with a write produces operations");
    proto::OperationSet(BTreeMap::from([("lww".to_owned(), ops)]))
}

/// Forge a title event onto `parent` with an EXPLICIT claimed generation:
/// the mis-stamping constructor for these pins (and, with the correct claim,
/// their honestly-stamped twins).
fn forge_claiming(entity_id: proto::EntityId, parent: proto::Clock, title: &str, generation: u32) -> proto::Event {
    ankurah_tests::forge::event_claiming(entity_id, Record::collection(), title_ops(title), parent, generation)
}

fn event_only_item(event: proto::Event) -> proto::SubscriptionUpdateItem {
    proto::SubscriptionUpdateItem {
        entity_id: event.entity_id,
        collection: event.collection.clone(),
        content: proto::UpdateContent::EventOnly(vec![Attested::opt(event, None).into()]),
        predicate_relevance: vec![],
    }
}

/// A StateAndEvent update item: `state` travels with `events` as cargo (the
/// wire shape a durable peer's subscription push uses).
fn state_and_event_item(
    entity_id: proto::EntityId,
    collection: proto::CollectionId,
    state: proto::State,
    events: Vec<proto::Event>,
) -> proto::SubscriptionUpdateItem {
    proto::SubscriptionUpdateItem {
        entity_id,
        collection,
        content: proto::UpdateContent::StateAndEvent(
            proto::StateFragment { state, attestations: Default::default() },
            events.into_iter().map(|e| Attested::opt(e, None).into()).collect(),
        ),
        predicate_relevance: vec![],
    }
}

fn assert_generation_mismatch(cause: &MutationError, claimed: u32, expected: u32, context: &str) {
    match cause {
        MutationError::Ingest(IngestError::Lineage(LineageRejection::GenerationMismatch {
            claimed: got_claimed,
            expected: got_expected,
            ..
        })) => {
            assert_eq!((*got_claimed, *got_expected), (claimed, expected), "{context}: mismatch payload");
        }
        other => panic!("{context}: expected the typed GenerationMismatch lineage rejection, got {other:?}"),
    }
}

/// R-D2-2b, streaming lane (the executor admission boundary shared by the
/// subscription-update and delta arms): a forged event whose claimed
/// generation violates gen == 1 + max(parent generations) against locally
/// resolvable parents is rejected with the typed lineage error, the entity
/// is unperturbed, and the forgery is not committed. The SAME operations
/// correctly stamped apply cleanly (both directions).
#[tokio::test]
async fn r_d2_2b_streaming_lane_rejects_mis_stamp_and_applies_correct_stamp() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let ctx_s = server.context(c)?;
    let ctx_c = client.context(c)?;
    let _relay_context = ctx_c.query_wait::<RecordView>("title = 'no-such-title'").await?;

    let (rec_id, genesis) = {
        let trx = ctx_s.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let mut events = trx.commit_and_return_events().await?;
        (id, events.remove(0))
    };
    let view = ctx_c.get::<RecordView>(rec_id).await?;
    assert_eq!(view.title().unwrap(), "t0");

    // Make the parent LOCALLY RESOLVABLE on the client: the snapshot-adopted
    // head has no local event body, so deliver the genesis itself first (the
    // integrated-but-unstored backfill commits it to the client's log).
    NodeApplier::apply_updates_for_test(&client, &server.id, vec![event_only_item(genesis.clone())])
        .await
        .expect("backfilling the adopted head's own event is clean");
    let collection = ctx_c.collection(&Record::collection()).await?;
    assert!(collection.has_event(&genesis.id()).await?, "precondition: the parent body is in the client's local log");

    // The forgery: a child of the (generation 1) genesis claiming 3; the
    // one correct claim is 2.
    let parent = proto::Clock::from(vec![genesis.id()]);
    let forged = forge_claiming(rec_id, parent.clone(), "forged-title", 3);
    let forged_id = forged.id();

    let err = NodeApplier::apply_updates_for_test(&client, &server.id, vec![event_only_item(forged)])
        .await
        .expect_err("a mis-stamped event with locally resolvable parents must be rejected at admission");
    let ApplyError::Items(items) = err else {
        panic!("expected per-item aggregation, got {err:?}");
    };
    assert_generation_mismatch(&items[0].cause, 3, 2, "streaming lane");

    // Same containment as any malformed event: nothing applied, nothing
    // durable, no head movement.
    assert_eq!(view.title().unwrap(), "t0", "the forgery must not perturb the entity");
    assert!(!view.entity().head().contains(&forged_id), "the forgery must not enter the head");
    assert!(!collection.has_event(&forged_id).await?, "the forgery must not be committed");

    // The other direction: the same operations CORRECTLY stamped (2) apply.
    let honest = forge_claiming(rec_id, parent, "forged-title", 2);
    let honest_id = honest.id();
    NodeApplier::apply_updates_for_test(&client, &server.id, vec![event_only_item(honest)])
        .await
        .expect("the correctly stamped twin must apply cleanly");
    assert_eq!(view.title().unwrap(), "forged-title");
    assert!(view.entity().head().contains(&honest_id), "the honest twin advances the head");

    Ok(())
}

/// R-D2-2b, commit lane (phase-one admission boundary): a mis-stamped event
/// in a remote transaction is rejected typed BEFORE anything durable happens
/// (denial atomicity), and the correctly stamped twin commits.
#[tokio::test]
async fn r_d2_2b_commit_lane_rejects_mis_stamp_and_applies_correct_stamp() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let ctx_s = server.context(c)?;

    let (rec_id, genesis) = {
        let trx = ctx_s.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let mut events = trx.commit_and_return_events().await?;
        (id, events.remove(0))
    };
    let collection = ctx_s.collection(&Record::collection()).await?;
    assert!(collection.has_event(&genesis.id()).await?, "precondition: parent local on the durable receiver");

    let parent = proto::Clock::from(vec![genesis.id()]);
    let forged = forge_claiming(rec_id, parent.clone(), "commit-forged", 7);
    let forged_id = forged.id();

    let err = server
        .commit_remote_transaction(&c, proto::TransactionId::new(), vec![Attested::opt(forged, None)])
        .await
        .expect_err("a mis-stamped event must fail the transaction at phase one");
    assert_generation_mismatch(&err, 7, 2, "commit lane");

    // Denial atomicity: the forgery is not durable and the head did not move.
    assert!(!collection.has_event(&forged_id).await?, "the forgery must not be committed");
    let stored_head = collection.get_state(rec_id).await?.payload.state.head;
    assert_eq!(stored_head, proto::Clock::from(vec![genesis.id()]), "the persisted head must not move");

    // The correctly stamped twin commits cleanly.
    let honest = forge_claiming(rec_id, parent, "commit-forged", 2);
    let honest_id = honest.id();
    server
        .commit_remote_transaction(&c, proto::TransactionId::new(), vec![Attested::opt(honest, None)])
        .await
        .expect("the correctly stamped twin must commit");
    assert!(collection.has_event(&honest_id).await?, "the honest twin is durable");

    Ok(())
}

/// R-D2-2b, genesis rule: a genesis event (empty parent clock) must claim
/// exactly 1 and is ALWAYS verifiable; a mis-claiming genesis is rejected
/// typed on the commit lane, and a correctly claiming one applies.
#[tokio::test]
async fn r_d2_2b_genesis_must_claim_exactly_one() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let ctx_s = server.context(c)?;
    let collection = ctx_s.collection(&Record::collection()).await?;

    // A brand-new entity whose creation event claims generation 2.
    let bad_entity = proto::EntityId::new();
    let bad_genesis = forge_claiming(bad_entity, proto::Clock::default(), "bad-genesis", 2);
    let bad_id = bad_genesis.id();

    let err = server
        .commit_remote_transaction(&c, proto::TransactionId::new(), vec![Attested::opt(bad_genesis, None)])
        .await
        .expect_err("a genesis claiming a generation other than 1 must be rejected");
    assert_generation_mismatch(&err, 2, 1, "genesis rule");
    assert!(!collection.has_event(&bad_id).await?, "the mis-claiming genesis must not be committed");
    assert!(collection.get_state(bad_entity).await.is_err(), "no state buffer for the rejected creation");

    // The correct claim (1) applies.
    let good_entity = proto::EntityId::new();
    let good_genesis = forge_claiming(good_entity, proto::Clock::default(), "good-genesis", 1);
    let good_id = good_genesis.id();
    server
        .commit_remote_transaction(&c, proto::TransactionId::new(), vec![Attested::opt(good_genesis, None)])
        .await
        .expect("a genesis claiming exactly 1 must apply");
    assert!(collection.has_event(&good_id).await?, "the honest genesis is durable");

    Ok(())
}

/// Plan REV 5 section L (the StateAndEvent cargo ruling, reversing M2's
/// warn-and-store arm): an event traveling as StateAndEvent cargo whose
/// claimed generation PROVABLY contradicts locally held parents aborts the
/// ENTIRE update item with the typed lineage error, checked BEFORE the
/// state applies: no state adoption, nothing stored from that item. The
/// grievance is with the state that vouched for the malformed history;
/// verifiably invalid input errors loudly because somebody is tampering.
/// The red pins BOTH halves: the item aborts AND the state is not adopted.
/// The honest twin (same shape, correct stamp) must adopt cleanly, and
/// fresh adoption with unverifiable cargo stays unaffected (M2 behavior).
#[tokio::test]
async fn r_l_provably_mis_stamped_state_and_event_cargo_aborts_the_item() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let ctx_s = server.context(c)?;
    let ctx_c = client.context(c)?;
    let _relay_context = ctx_c.query_wait::<RecordView>("title = 'no-such-title'").await?;

    let (rec_id, genesis) = {
        let trx = ctx_s.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let mut events = trx.commit_and_return_events().await?;
        (id, events.remove(0))
    };
    let view = ctx_c.get::<RecordView>(rec_id).await?;
    assert_eq!(view.title().unwrap(), "t0");

    // Make the parent LOCALLY HELD on the client (the forgery must be
    // PROVABLY wrong, not merely unverifiable): deliver the genesis body so
    // the backfill commits it to the client's log.
    NodeApplier::apply_updates_for_test(&client, &server.id, vec![event_only_item(genesis.clone())])
        .await
        .expect("backfilling the adopted head's own event is clean");
    let collection = ctx_c.collection(&Record::collection()).await?;
    assert!(collection.has_event(&genesis.id()).await?, "precondition: the parent body is in the client's local log");
    let stored_head_before = collection.get_state(rec_id).await?.payload.state.head;

    // The forged cargo: a child of the (generation 1) genesis claiming 3;
    // the one correct claim is 2. The state vouching for it heads at the
    // forgery.
    let parent = proto::Clock::from(vec![genesis.id()]);
    let forged = forge_claiming(rec_id, parent.clone(), "tampered-title", 3);
    let forged_id = forged.id();
    let mut crafted_state = collection.get_state(rec_id).await?.payload.state.clone();
    crafted_state.head = proto::Clock::from(vec![forged_id.clone()]);
    // The forger annotates its head consistently with its own lie.
    crafted_state.head_generations = proto::GClock::from((forged.generation, forged_id.clone()));

    let err = NodeApplier::apply_updates_for_test(
        &client,
        &server.id,
        vec![state_and_event_item(rec_id, Record::collection(), crafted_state, vec![forged])],
    )
    .await
    .expect_err("a provably mis-stamped cargo event must abort the ENTIRE update item");
    let ApplyError::Items(items) = err else {
        panic!("expected per-item aggregation, got {err:?}");
    };
    assert_generation_mismatch(&items[0].cause, 3, 2, "StateAndEvent cargo");

    // The other half: the state was NOT adopted and nothing was stored.
    assert_eq!(view.title().unwrap(), "t0", "the vouching state must not be adopted");
    assert!(!view.entity().head().contains(&forged_id), "the forgery must not enter the resident head");
    assert!(!collection.has_event(&forged_id).await?, "the forged cargo must not be committed");
    assert_eq!(
        collection.get_state(rec_id).await?.payload.state.head,
        stored_head_before,
        "the persisted buffer must not move on an aborted item"
    );

    // The honest twin (same operations, correct stamp 2) adopts cleanly:
    // the reversal must reject forgeries, not the lane.
    let honest = forge_claiming(rec_id, parent, "tampered-title", 2);
    let honest_id = honest.id();
    let mut honest_state = collection.get_state(rec_id).await?.payload.state.clone();
    honest_state.head = proto::Clock::from(vec![honest_id.clone()]);
    honest_state.head_generations = proto::GClock::from((honest.generation, honest_id.clone()));
    NodeApplier::apply_updates_for_test(
        &client,
        &server.id,
        vec![state_and_event_item(rec_id, Record::collection(), honest_state, vec![honest])],
    )
    .await
    .expect("the correctly stamped twin adopts cleanly");
    assert!(view.entity().head().contains(&honest_id), "the honest twin advances the resident head");
    assert!(collection.has_event(&honest_id).await?, "the honest cargo is committed");

    // Fresh adoption with UNVERIFIABLE cargo (parents not locally held)
    // remains unaffected: store the body, adopt the state (the M2 arm the
    // ruling expressly keeps).
    let fresh_id = proto::EntityId::new();
    let h1 = forge_claiming(fresh_id, proto::Clock::default(), "fresh", 1);
    let h2 = ankurah_tests::forge::event_with_parents(fresh_id, Record::collection(), title_ops("fresh-2"), &[&h1]);
    let h2_id = h2.id();
    // Only h2 travels; its parent h1 is never given to the client, so the
    // check is UNVERIFIABLE, not provably wrong.
    let fresh_state = proto::State {
        state_buffers: proto::StateBuffers(BTreeMap::new()),
        head: proto::Clock::from(vec![h2_id.clone()]),
        head_generations: proto::GClock::from((h2.generation, h2_id.clone())),
    };
    NodeApplier::apply_updates_for_test(
        &client,
        &server.id,
        vec![state_and_event_item(fresh_id, Record::collection(), fresh_state, vec![h2])],
    )
    .await
    .expect("fresh adoption with unverifiable cargo must remain unaffected");
    assert!(collection.has_event(&h2_id).await?, "unverifiable cargo is stored (acceleration-ineligible), not rejected");

    Ok(())
}
