//! What an entity's id means, exercised through the real create path.
//!
//! An entity id IS the id of its genesis event, so `trx.create` has to freeze
//! the model's initial values and derive the id from them before it can return
//! one. These tests pin the properties that arrangement is FOR: the id a caller
//! receives names the genesis actually committed, two creations of the same
//! payload are two entities rather than one, and the genesis still carries the
//! single membership the commit funnels require.

mod common;

use ankurah::{policy::DEFAULT_CONTEXT as c, proto, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use common::{Album, AlbumView, GatedConnection, Record};
use std::sync::{Arc, Mutex};

async fn durable_node() -> Result<ankurah::Context> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    node.system.create().await?;
    Ok(node.context_async(c).await)
}

async fn entity_events(ctx: &ankurah::Context, id: proto::EntityId) -> Result<Vec<proto::Event>> {
    events_in(ctx, &common::model_id::<Album>(ctx).await?, id).await
}

async fn events_in(ctx: &ankurah::Context, collection: &proto::ModelId, id: proto::EntityId) -> Result<Vec<proto::Event>> {
    let collection = ctx.collection(collection).await?;
    Ok(collection.dump_entity_events(id).await?.into_iter().map(|e| e.payload).collect())
}

/// The id `create()` returns is the id of the genesis that lands in storage,
/// and that genesis derives it from its own content. This is the whole of
/// "EntityId = genesis EventId": if the two could differ, the id a caller
/// holds would name nothing.
#[tokio::test]
async fn created_id_is_the_committed_genesis_id() -> Result<()> {
    let ctx = durable_node().await?;

    let trx = ctx.begin();
    let album = trx.create(&Album { name: "Kind of Blue".to_owned(), year: "1959".to_owned() }).await?;
    // Available before commit, which is what the eager freeze buys.
    let id = album.id();
    trx.commit().await?;

    let events = entity_events(&ctx, id).await?;
    let genesis = events.iter().find(|e| e.is_entity_create()).expect("the entity has a committed genesis");
    assert_eq!(genesis.entity_id, id, "the id create() returned names the committed genesis");
    assert_eq!(proto::EntityId::from(genesis.id()), id, "the genesis derives that id from its own content");
    genesis.validate_structure().expect("the committed genesis is structurally well formed");

    // The view resolves under that id, so the resident entity and the stored
    // genesis agree.
    let view = ctx.get::<AlbumView>(id).await?;
    assert_eq!(view.name().unwrap(), "Kind of Blue");
    Ok(())
}

/// Two `create()` calls with byte-identical payloads are two entities, because
/// each mint draws its own nonce. Without the nonce the two genesis preimages
/// would be equal and the second creation would silently address the first.
///
/// The model here is LWW-backed on purpose: LWW encodes a field write to the
/// same bytes every time, so the two genesis events agree on their operations
/// and the nonce is demonstrably the only thing left that separates them. A
/// yrs-backed field would not show that, because a yrs update embeds the
/// document's own randomly chosen client id.
#[tokio::test]
async fn identical_payloads_mint_distinct_entities() -> Result<()> {
    let ctx = durable_node().await?;

    let payload = || Record { title: "Blue Train".to_owned(), artist: "John Coltrane".to_owned() };

    let trx = ctx.begin();
    let first = trx.create(&payload()).await?.id();
    let second = trx.create(&payload()).await?.id();
    trx.commit().await?;

    assert_ne!(first, second, "identical payloads are still two distinct entities");

    let collection = common::model_id::<Record>(&ctx).await?;
    let first_genesis = events_in(&ctx, &collection, first).await?.into_iter().find(|e| e.is_entity_create()).expect("first genesis");
    let second_genesis = events_in(&ctx, &collection, second).await?.into_iter().find(|e| e.is_entity_create()).expect("second genesis");
    assert_eq!(first_genesis.operations(), second_genesis.operations(), "the two mints froze identical operations");
    assert_ne!(first_genesis.nonce(), second_genesis.nonce(), "and the nonce is what separates them");
    Ok(())
}

/// Editing a created entity before commit produces two events, not one
/// rewritten genesis: the frozen genesis, then an update parented on it. A
/// genesis that absorbed post-create edits would change the entity id after
/// the caller already had it.
#[tokio::test]
async fn an_edit_after_create_becomes_an_update_on_the_frozen_genesis() -> Result<()> {
    let ctx = durable_node().await?;

    let trx = ctx.begin();
    let id = {
        let album = trx.create(&Album { name: "Giant Steps".to_owned(), year: "1959".to_owned() }).await?;
        let id = album.id();
        album.year()?.overwrite(0, 4, "1960")?;
        assert_eq!(album.id(), id, "the id did not move when the entity was edited");
        id
    };
    trx.commit().await?;

    let events = entity_events(&ctx, id).await?;
    assert_eq!(events.len(), 2, "one genesis plus one update, got {events:?}");
    let genesis = events.iter().find(|e| e.is_entity_create()).expect("genesis");
    let update = events.iter().find(|e| !e.is_entity_create()).expect("update");
    assert_eq!(proto::EntityId::from(genesis.id()), id);
    assert_eq!(update.parent.as_slice(), &[genesis.id()], "the update is parented on the genesis");
    update.validate_structure().expect("the update is structurally well formed");

    let view = ctx.get::<AlbumView>(id).await?;
    assert_eq!(view.year().unwrap(), "1960", "the post-create edit survives");
    Ok(())
}

/// The genesis carries exactly one `Membership::Add`, which is what
/// `check_membership_admissibility` requires of an entity's first event. The
/// membership is one of the frozen initial operations, so it is inside the
/// hash that derives the id.
#[tokio::test]
async fn the_genesis_carries_exactly_one_membership() -> Result<()> {
    let ctx = durable_node().await?;

    let trx = ctx.begin();
    let id = trx.create(&Album { name: "Mingus Ah Um".to_owned(), year: "1959".to_owned() }).await?.id();
    trx.commit().await?;

    let genesis = entity_events(&ctx, id).await?.into_iter().find(|e| e.is_entity_create()).expect("genesis");
    assert_eq!(genesis.operations().memberships().count(), 1, "an entity's first event carries exactly one membership addition");
    Ok(())
}

/// What a peer receives is the genesis `create()` minted, not a fresh one.
///
/// The nonce is drawn once, at the mint. If anything on the way out re-derived
/// the event -- a resend, a second durable peer, a relay that rebuilds from the
/// entity -- the same payload would land under a second id and the id the
/// caller already holds would name nothing on that peer.
///
/// This pins the sending half: the genesis on the wire and the genesis the
/// durable peer stores are both the one whose id `create()` returned, nonce
/// included. The receiving half -- the same bytes delivered again converge
/// rather than duplicate -- is pinned by the full-batch redelivery after a
/// mid-batch crash in tests/tests/crash_recovery/scenarios.rs
/// (`scenario_2_mid_batch`).
#[tokio::test]
async fn a_relayed_genesis_is_the_one_create_minted() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());

    // Record every event the client relays to the server. The filter returns
    // false for everything, so nothing is held and delivery is unchanged; it
    // is a tap on the wire, not a gate.
    let relayed: Arc<Mutex<Vec<proto::Event>>> = Arc::new(Mutex::new(Vec::new()));
    let (_connection, _gate) = {
        let relayed = relayed.clone();
        GatedConnection::new(&client, &server, move |message: &proto::NodeMessage| {
            if let proto::NodeMessage::Request { request, .. } = message {
                if let proto::NodeRequestBody::CommitTransaction { events, .. } = &request.body {
                    relayed.lock().unwrap().extend(events.iter().map(|event| event.payload.clone()));
                }
            }
            false
        })
    };
    client.system.wait_system_ready().await;
    let client_ctx = client.context_async(c).await;

    let trx = client_ctx.begin();
    let id = trx.create(&Album { name: "A Love Supreme".to_owned(), year: "1965".to_owned() }).await?.id();
    trx.commit().await?;

    let relayed_genesis = relayed
        .lock()
        .unwrap()
        .iter()
        .find(|event| event.entity_id == id && event.is_entity_create())
        .cloned()
        .expect("the client relayed the album's genesis to its durable peer");
    assert_eq!(proto::EntityId::from(relayed_genesis.id()), id, "the relayed genesis is the one create() returned an id for");

    let server_ctx = server.context_async(c).await;
    let stored_genesis = entity_events(&server_ctx, id).await?.into_iter().find(|e| e.is_entity_create()).expect("the peer stored it");
    assert_eq!(stored_genesis.id(), relayed_genesis.id(), "the peer stored the relayed genesis rather than deriving its own");
    assert_eq!(stored_genesis.nonce(), relayed_genesis.nonce(), "same nonce: the mint happened once, at create()");
    Ok(())
}

/// The system root is the one entity whose genesis binds no system above it,
/// and every other entity's genesis binds the root. Without that binding the
/// same content under a different system root would derive the same id.
#[tokio::test]
async fn the_root_binds_no_system_and_everything_else_binds_the_root() -> Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    node.system.create().await?;
    let root_id = node.system.root_id().expect("the system root exists after create");
    let ctx = node.context_async(c).await;

    let trx = ctx.begin();
    let id = trx.create(&Album { name: "Ascension".to_owned(), year: "1966".to_owned() }).await?.id();
    trx.commit().await?;

    let genesis = entity_events(&ctx, id).await?.into_iter().find(|e| e.is_entity_create()).expect("genesis");
    match &genesis.body {
        proto::EventBody::Genesis { system, .. } => assert_eq!(*system, Some(root_id), "a non-root genesis binds the system root"),
        proto::EventBody::Update { .. } => panic!("is_entity_create disagreed with the body"),
    }

    let root_collection = ctx.collection(&ankurah::core::system::system_collection()).await?;
    let root_genesis = root_collection
        .dump_entity_events(root_id)
        .await?
        .into_iter()
        .map(|e| e.payload)
        .find(|e| e.is_entity_create())
        .expect("the root has a committed genesis");
    assert_eq!(proto::EntityId::from(root_genesis.id()), root_id, "the root's id derives from its own genesis");
    match &root_genesis.body {
        proto::EventBody::Genesis { system, .. } => assert_eq!(*system, None, "the root has no system above it"),
        proto::EventBody::Update { .. } => panic!("the root's first event is not a genesis"),
    }
    Ok(())
}
