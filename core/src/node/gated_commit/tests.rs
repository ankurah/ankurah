//! Callers that drop a gated commit's future after the commit has taken the gate, as a user may drop
//! a transaction's commit or a `Context::get` whose response is being applied, and a connector a
//! peer's transaction whose message it handles.
use crate::{
    entity::{Entity, LocalTrxEntity},
    error::RetrievalError,
    node::{applier::NodeApplier, handler::commit_transaction, Node},
    policy::{PermissiveAgent, DEFAULT_CONTEXT},
    reactor::maintenance::ProjectedChange,
    retrieval::{LocalEventGetter, LocalStateGetter, SuspenseEvents},
    storage::StorageEngine,
    test_utils::TestStorage,
};
use ankurah_proto::{
    self as proto, Attested, AuthorId, Clock, EntityId, Event, Membership, ModelId, Operation, OperationSet, TransactionId,
};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::sync::oneshot;

type TestNode = Node<TestStorage, PermissiveAgent>;

/// A local transaction whose commit future is dropped after storage committed it inside the gate
/// still advances the resident entity and publishes to maintenance, which then serves later commits.
#[tokio::test]
async fn a_local_commit_dropped_inside_the_gate_still_lands_and_publishes() -> anyhow::Result<()> {
    let node = node().await?;
    let context = node.context(DEFAULT_CONTEXT)?;
    let trx = context.begin();
    let creation =
        trx.add_entity(LocalTrxEntity::new(node.system.root_id(), AuthorId::Unknown, node.entities.system_epoch(), trx.alive.clone()));
    creation.add_membership(model(1))?;
    let id = creation.id();
    trx.commit().await?;
    let resident = load_resident(&node, id).await?;
    let (_, mut maintenance) = node.reactor.subscribe_maintenance(heads).await?;

    let trx = context.begin();
    trx.add_entity(LocalTrxEntity::edit(&resident, AuthorId::Unknown, trx.alive.clone())?).add_membership(model(2))?;
    drop_inside_the_gate(&node, trx.commit()).await?;

    let committed = node.storage.get_state(id).await?.payload.state;
    assert!(committed.memberships.contains(&model(2)), "the dropped commit reached storage");
    let published = tokio::time::timeout(Duration::from_secs(10), maintenance.next()).await??;
    assert_eq!(published.changes, [(id, committed.head.clone())]);
    assert_eq!(resident.head(), committed.head);

    let trx = context.begin();
    trx.add_entity(LocalTrxEntity::edit(&resident, AuthorId::Unknown, trx.alive.clone())?).add_membership(model(3))?;
    trx.commit().await?;
    assert_eq!(maintenance.next().await?.changes, [(id, resident.head())], "maintenance still serves later commits");
    Ok(())
}

/// A local transaction's commit dropped while its writes stage, before it relays them to durable
/// peers and releases them to commit, commits nothing, and later commits proceed.
#[tokio::test]
async fn a_local_commit_dropped_before_releasing_its_writes_commits_nothing() -> anyhow::Result<()> {
    let node = node().await?;
    let context = node.context(DEFAULT_CONTEXT)?;
    let (_, mut maintenance) = node.reactor.subscribe_maintenance(heads).await?;
    let (entered, staging) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    *node.storage.hold_set_state.lock().unwrap() = Some((entered, resume));
    let trx = context.begin();
    let creation =
        trx.add_entity(LocalTrxEntity::new(node.system.root_id(), AuthorId::Unknown, node.entities.system_epoch(), trx.alive.clone()));
    creation.add_membership(model(1))?;
    let abandoned = creation.id();
    {
        let mut committing = std::pin::pin!(trx.commit());
        tokio::select! {
            _ = &mut committing => anyhow::bail!("the commit finished while storage held its staging"),
            staged = staging => staged?,
        }
    }
    // Staging may finish now; nothing released the writes to commit.
    let _ = release.send(());

    let trx = context.begin();
    let creation =
        trx.add_entity(LocalTrxEntity::new(node.system.root_id(), AuthorId::Unknown, node.entities.system_epoch(), trx.alive.clone()));
    creation.add_membership(model(2))?;
    let later = creation.id();
    trx.commit().await?;
    assert!(matches!(node.storage.get_state(abandoned).await, Err(RetrievalError::EntityNotFound(_))));
    let head = node.storage.get_state(later).await?.payload.state.head;
    assert_eq!(maintenance.next().await?.changes, [(later, head)], "maintenance heard only of the later commit");
    Ok(())
}

/// A peer's transaction whose commit is dropped after storage committed it inside the gate, as a
/// connector that bounds or cancels its handling of a message may drop it, still lands, advances
/// the resident entity and publishes to maintenance, which then serves later commits.
#[tokio::test]
async fn a_remote_transaction_dropped_inside_the_gate_still_lands_and_publishes() -> anyhow::Result<()> {
    let node = node().await?;
    let genesis = Event::genesis(node.system.root_id(), AuthorId::Unknown, adding_model(1));
    let id = genesis.entity_id;
    commit_transaction(&node, &DEFAULT_CONTEXT, TransactionId::new(), vec![genesis.clone().into()]).await?;
    let resident = load_resident(&node, id).await?;
    let (_, mut maintenance) = node.reactor.subscribe_maintenance(heads).await?;

    let update = Event::update(id, Clock::singleton(&genesis), AuthorId::Unknown, adding_model(2));
    drop_inside_the_gate(&node, commit_transaction(&node, &DEFAULT_CONTEXT, TransactionId::new(), vec![update.clone().into()])).await?;

    let head = Clock::singleton(&update);
    assert_eq!(node.storage.get_state(id).await?.payload.state.head, head, "the dropped commit reached storage");
    let published = tokio::time::timeout(Duration::from_secs(10), maintenance.next()).await??;
    assert_eq!(published.changes, [(id, head.clone())]);
    assert_eq!(resident.head(), head);

    let later = Event::genesis(node.system.root_id(), AuthorId::Unknown, adding_model(3));
    commit_transaction(&node, &DEFAULT_CONTEXT, TransactionId::new(), vec![later.clone().into()]).await?;
    assert_eq!(maintenance.next().await?.changes, [(later.entity_id, Clock::singleton(&later))], "maintenance still serves later commits");
    Ok(())
}

/// The applier's commit sites, each reached by one kind of delta in a peer's response.
#[derive(Clone, Copy, Debug)]
enum Route {
    /// A snapshot of an entity this node does not hold registers its resident.
    NewEntitySnapshot,
    /// The resident adopts a snapshot that extends it.
    ExtendingSnapshot,
    /// The resident applies events that extend it.
    EventBridge,
}

/// A peer's response whose application is dropped after storage committed it inside the gate, as
/// `get_from_peer` and `fetch_from_peer` apply responses inside their caller's future, still
/// advances the resident entity and publishes to maintenance, which then serves later commits.
#[tokio::test]
async fn an_application_dropped_inside_the_gate_still_lands_and_publishes() -> anyhow::Result<()> {
    for route in [Route::NewEntitySnapshot, Route::ExtendingSnapshot, Route::EventBridge] {
        let node = node().await?;
        let genesis = Event::genesis(node.system.root_id(), AuthorId::Unknown, adding_model(1));
        let id = genesis.entity_id;
        let update = Event::update(id, Clock::singleton(&genesis), AuthorId::Unknown, adding_model(2));
        let mut resident = None;
        if !matches!(route, Route::NewEntitySnapshot) {
            apply(&node, bridge(&genesis), Vec::new()).await?;
            resident = Some(load_resident(&node, id).await?);
        }
        let (_, mut maintenance) = node.reactor.subscribe_maintenance(heads).await?;

        let (head, applying) = match route {
            Route::NewEntitySnapshot => (Clock::singleton(&genesis), apply(&node, snapshot(&genesis, [1]), Vec::new())),
            // Only the caller's getter holds the update, which the snapshot's causal check reads;
            // storage never stores it, so committing the adoption must not read history again.
            Route::ExtendingSnapshot => (Clock::singleton(&update), apply(&node, snapshot(&update, [1, 2]), vec![update.clone()])),
            Route::EventBridge => (Clock::singleton(&update), apply(&node, bridge(&update), Vec::new())),
        };
        drop_inside_the_gate(&node, applying).await?;

        assert_eq!(node.storage.get_state(id).await?.payload.state.head, head, "{route:?}: the dropped commit reached storage");
        let published = tokio::time::timeout(Duration::from_secs(10), maintenance.next()).await??;
        assert_eq!(published.changes, [(id, head.clone())], "{route:?}");
        if let Some(resident) = &resident {
            assert_eq!(resident.head(), head, "{route:?}");
        }

        let later = Event::genesis(node.system.root_id(), AuthorId::Unknown, adding_model(3));
        apply(&node, bridge(&later), Vec::new()).await?;
        assert_eq!(
            maintenance.next().await?.changes,
            [(later.entity_id, Clock::singleton(&later))],
            "{route:?}: maintenance still serves later commits"
        );
    }
    Ok(())
}

/// Drive `caller` until storage has committed its write inside the commit gate, then drop it there
/// and let storage return. Fails if dropping `caller` also dropped the commit it began.
async fn drop_inside_the_gate(node: &TestNode, caller: impl Future) -> anyhow::Result<()> {
    let (committed, storage_committed) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    *node.storage.hold_after_commit.lock().unwrap() = Some((committed, resume));
    {
        let mut caller = std::pin::pin!(caller);
        tokio::select! {
            _ = &mut caller => anyhow::bail!("the caller finished while storage held its commit"),
            stored = storage_committed => stored?,
        }
    }
    release.send(()).map_err(|()| anyhow::anyhow!("dropping the caller dropped the commit it began"))
}

async fn node() -> anyhow::Result<TestNode> {
    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    Ok(node)
}

/// Load the resident entity for `id`; holding it keeps it resident.
async fn load_resident(node: &TestNode, id: EntityId) -> anyhow::Result<Entity> {
    let states = LocalStateGetter::new(node.storage.clone());
    let events = LocalEventGetter::new(node.storage.clone(), true);
    Ok(node.entities.get_or_retrieve(&states, &events, &id).await?.expect("the entity is stored"))
}

/// Apply `delta` from a peer, staging `history` in the caller's getter for the applier's causal checks.
async fn apply(node: &TestNode, delta: proto::EntityDelta, history: Vec<Event>) -> anyhow::Result<()> {
    let events = LocalEventGetter::new(node.storage.clone(), true);
    for event in history {
        events.stage_event(event);
    }
    let states = LocalStateGetter::new(node.storage.clone());
    NodeApplier::apply_deltas(node, &EntityId::from_bytes([9; 32]), vec![delta], &events, &states).await?;
    Ok(())
}

fn snapshot(head: &Event, models: impl IntoIterator<Item = u8>) -> proto::EntityDelta {
    let state = proto::State { memberships: models.into_iter().map(model).collect(), head: Clock::singleton(head), ..Default::default() };
    proto::EntityDelta {
        entity_id: head.entity_id,
        content: proto::DeltaContent::StateSnapshot {
            state: Attested::opt(proto::EntityState { entity_id: head.entity_id, state }, None).into(),
        },
    }
}

fn bridge(event: &Event) -> proto::EntityDelta {
    proto::EntityDelta {
        entity_id: event.entity_id,
        content: proto::DeltaContent::EventBridge { events: vec![Attested::from(event.clone()).into()] },
    }
}

fn heads(entities: &[&Entity]) -> anyhow::Result<Vec<ProjectedChange<(EntityId, Clock)>>> {
    Ok(entities.iter().map(|entity| ProjectedChange { value: (entity.id(), entity.head()), member: true, bytes: 0 }).collect())
}

fn adding_model(seed: u8) -> OperationSet { OperationSet(vec![Operation::Membership(Membership::Add(model(seed)))]) }

fn model(seed: u8) -> ModelId { ModelId::EntityId(EntityId::from_bytes([seed; 32])) }
