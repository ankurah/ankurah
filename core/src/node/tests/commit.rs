use crate::node::handler::commit_transaction;
use crate::{
    entity::{Entity, LocalTrxEntity, RemoteTrxEntity},
    reactor::ChangeNotification,
    error::{MutationError, ValidationError},
    node::Node,
    storage::{StorageCommitOutcome, StorageEngine, StorageTransaction},
    policy::{AccessDenied, DefaultContext, PolicyAgent, DEFAULT_CONTEXT},
    property::backend::{LWWBackend, PropertyBackend},
    selection::filter::Filterable,
    retrieval::{LocalEventGetter, SuspenseEvents},
    test_utils::TestStorage,
    util::Iterable,
    value::Value,
};
use ankql::ast::{Predicate, Resolved};
use ankurah_proto::{self as proto, Attested, AuthorId, EntityId, EntityState, Event, Membership, ModelId, Operation, OperationSet, PropertyId};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct OwnerOnlyAgent {
    owner: PropertyId,
    checked_owners: Arc<Mutex<Vec<Option<Value>>>>,
}

#[async_trait::async_trait]
impl PolicyAgent for OwnerOnlyAgent {
    type ContextData = &'static DefaultContext;

    fn check_write_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &Self::ContextData,
        before: &Entity,
        after: &Entity,
        event: &Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied> {
        assert_eq!(before.id(), event.entity_id);
        assert_eq!(after.id(), event.entity_id);
        assert!(after.head().contains(&event.id()), "the policy must see the event applied to the working fork");
        let owner = before.value(&self.owner);
        self.checked_owners.lock().unwrap().push(owner.clone());
        if owner != Some(Value::String("Alice".into())) {
            return Err(AccessDenied::ByPolicy("Alice no longer owns this entity"));
        }
        Ok(None)
    }

    fn sign_request<SE: StorageEngine, C: Iterable<Self::ContextData>>(
        &self,
        _: &crate::node::NodeInner<SE, Self>,
        _: &C,
        _: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied> {
        Ok(vec![])
    }

    async fn check_request<SE: StorageEngine, A: Iterable<proto::AuthData> + Send + Sync>(
        &self,
        _: &Node<SE, Self>,
        _: &A,
        _: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ValidationError> {
        Ok(vec![])
    }

    fn query_predicate<C: Iterable<Self::ContextData>>(&self, _: &C) -> Result<Predicate<Resolved>, AccessDenied> { Ok(Predicate::True) }

    fn check_write(&self, _: &Self::ContextData, _: &Entity, _: Option<&Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn attest_state<SE: StorageEngine>(&self, _: &Node<SE, Self>, _: &EntityState) -> Option<proto::Attestation> { None }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &EntityId,
        _: &Attested<Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &EntityId,
        _: &Attested<EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &EntityId,
        _: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

fn set(property: PropertyId, value: &str) -> anyhow::Result<Operation> {
    let backend = LWWBackend::new();
    backend.set(property, Some(Value::String(value.into())));
    Ok(Operation::Backend { backend: "lww".into(), operations: backend.to_operations()?.unwrap() })
}

#[tokio::test]
async fn local_creation_commits_after_its_events_arrive_remotely() -> anyhow::Result<()> {
    let storage = Arc::new(TestStorage::default());
    let node = Node::new_durable(storage.clone(), crate::policy::PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    let context = node.context(DEFAULT_CONTEXT)?;
    let [a, b] = [1, 2].map(|byte| ModelId::EntityId(EntityId::from_bytes([byte; 32])));

    for echoed in [1, 2] {
        let trx = context.begin();
        let entity = trx.add_entity(LocalTrxEntity::new(
            node.system.root_id(), AuthorId::Unknown, node.entities.system_epoch(), trx.alive.clone(),
        ));
        entity.add_membership(a)?;
        let id = entity.id();
        entity.add_membership(b)?;
        let view = entity.read();
        let events = entity.prepare_events()?;

        // A subscription can deliver some or all of our events before our local commit finishes.
        commit_transaction(&node, &DEFAULT_CONTEXT, proto::TransactionId::new(), events[..echoed].to_vec()).await?;
        trx.commit().await?;

        assert_eq!(view.memberships(), [a, b].into_iter().collect());
        assert_eq!(storage.get_state(id).await?.payload.state.head, events[1].payload.id().into());
        assert_eq!(storage.dump_entity_events(id).await?.len(), 2);
    }
    Ok(())
}

#[tokio::test]
async fn failed_add_event_poisons_the_transaction_even_if_the_error_is_caught() -> anyhow::Result<()> {
    let storage = Arc::new(TestStorage::default());
    let node = Node::new_durable(storage.clone(), crate::policy::PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    let policy = crate::policy::ContextPolicy::from_credentials(&node.policy_agent, &DEFAULT_CONTEXT);
    let valid: Attested<Event> = Event::genesis(node.system.root_id(), AuthorId::Unknown, OperationSet(vec![
        Operation::Membership(Membership::Add(ModelId::EntityId(EntityId::from_bytes([1; 32])))),
    ])).into();
    let invalid: Attested<Event> = Event::genesis(node.system.root_id(), AuthorId::Unknown, OperationSet::default()).into();
    let mut trx = crate::remote_transaction::RemoteTransaction::new(&node, &policy);
    trx.add_event(&valid).await?;
    let id = valid.payload.entity_id;
    assert!(node.entities.get(&id).is_none());
    assert!(trx.add_event(&invalid).await.is_err());
    assert!(matches!(trx.add_event(&valid).await, Err(MutationError::TransactionFailed)));
    assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));
    assert!(node.entities.get(&id).is_none());
    assert!(matches!(storage.get_state(id).await, Err(crate::error::RetrievalError::EntityNotFound(_))));
    assert!(storage.dump_entity_events(id).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn fork_commit_publishes_to_resident_without_changing_original_snapshot() -> anyhow::Result<()> {
    let storage = Arc::new(TestStorage::default());
    let getter = LocalEventGetter::new(storage.clone(), false);
    let property = PropertyId::EntityId(EntityId::from_bytes([1; 32]));
    let genesis = Event::genesis(None, AuthorId::Unknown, OperationSet(vec![set(property, "Alice")?]));

    let entities = crate::entity::WeakEntitySet::new(crate::schema::SystemEpoch::allocate());
    let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let fork = RemoteTrxEntity::new(&genesis, entities.system_epoch(), alive.clone())?;
    let before = fork.snapshot();
    fork.apply_event(&getter, &mut Attested::from(genesis.clone()), |_| Ok(None)).await?;

    let update = Event::update(genesis.entity_id, genesis.id().into(), AuthorId::Unknown, OperationSet(vec![set(property, "Bob")?]));
    fork.apply_event(&getter, &mut Attested::from(update.clone()), |_| Ok(None)).await?;
    let branch_property = PropertyId::EntityId(EntityId::from_bytes([2; 32]));
    let branch = Event::update(genesis.entity_id, genesis.id().into(), AuthorId::Unknown, OperationSet(vec![set(branch_property, "branch")?]));
    let mut branch = Attested::opt(branch, Some(proto::Attestation(vec![1, 2, 3])));
    // Divergence must find the earlier fork events without storage or explicit staging.
    fork.apply_event(&getter, &mut branch, |event| {
        assert_eq!(event.entity_id, before.id());
        assert_eq!(before.value(&branch_property), None);
        assert_eq!(fork.read().value(&branch_property), Some(Value::String("branch".into())));
        Ok(Some(proto::Attestation(vec![4, 5, 6])))
    }).await?;
    assert_eq!(*branch.attestations, vec![proto::Attestation(vec![1, 2, 3]), proto::Attestation(vec![4, 5, 6])]);
    // Redelivered events must not appear twice in the published change.
    assert!(!fork.apply_event(&getter, &mut Attested::from(genesis.clone()), |_| Ok(None)).await?);
    let invalid = Event::update(genesis.entity_id, fork.head(), AuthorId::Unknown, OperationSet(vec![
        Operation::Backend { backend: "invalid-backend".into(), operations: Vec::new() },
    ]));
    assert!(fork.apply_event(&getter, &mut Attested::from(invalid), |_| Ok(None)).await.is_err());
    assert!(entities.get(&genesis.entity_id).is_none());

    let expected_events = vec![genesis.clone().into(), update.clone().into(), branch.clone()];
    let mut transaction = storage.transaction();
    transaction.add_events(&expected_events).await?;
    transaction.commit().await?.committed()?;
    let (resident, events) = fork.commit(&entities, &getter).await?.into_parts();
    assert_eq!(entities.get(&genesis.entity_id), Some(resident.clone()));
    assert_eq!(events, expected_events);
    assert_eq!(resident.head(), proto::Clock::new([update.id(), branch.payload.id()]));
    assert_eq!(resident.value(&property), Some(Value::String("Bob".into())));
    assert_eq!(resident.value(&branch_property), Some(Value::String("branch".into())));
    assert!(before.head().is_empty());
    assert_eq!(before.value(&property), None);

    let retry = RemoteTrxEntity::edit(&resident, alive)?;
    assert!(!retry.apply_event(&getter, &mut Attested::from(update.clone()), |_| Ok(None)).await?);
    assert!(retry.commit(&entities, &getter).await?.events().is_empty(), "already-published events do not notify again");
    assert_eq!(resident.head(), proto::Clock::new([update.id(), branch.payload.id()]));
    Ok(())
}

#[tokio::test]
async fn conflict_rechecks_policy_against_the_winning_state() -> anyhow::Result<()> {
    let owner = PropertyId::EntityId(EntityId::from_bytes([1; 32]));
    let title = PropertyId::EntityId(EntityId::from_bytes([2; 32]));
    let agent = OwnerOnlyAgent { owner, checked_owners: Arc::default() };
    let storage = Arc::new(TestStorage::default());
    let node = Node::new_durable(storage.clone(), agent.clone());
    node.system.create().await?;
    node.wait_ready().await?;
    let events = LocalEventGetter::new(storage.clone(), false);
    let genesis = Event::genesis(
        Some(EntityId::from_bytes([3; 32])),
        AuthorId::Unknown,
        OperationSet(vec![Operation::Membership(Membership::Add(ModelId::EntityId(EntityId::from_bytes([4; 32])))), set(owner, "Alice")?]),
    );
    events.stage_event(genesis.clone());
    let candidate = RemoteTrxEntity::new(&genesis, node.entities.system_epoch(), Arc::new(std::sync::atomic::AtomicBool::new(true)))?;
    candidate.apply_event(&events, &mut Attested::from(genesis.clone()), |_| Ok(None)).await?;
    let mut transaction = storage.transaction();
    transaction.set_state(&proto::Clock::default(), &Attested::opt(candidate.read().to_entity_state()?, None)).await?;
    transaction.add_events(&[Attested::opt(genesis.clone(), None)]).await?;
    assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));
    let (entity, _) = candidate.commit(&node.entities, &events).await?.into_parts();

    let edit = Event::update(entity.id(), genesis.id().into(), AuthorId::Unknown, OperationSet(vec![set(title, "Alice's edit")?]));
    events.stage_event(edit.clone());
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    *storage.hold_commit.lock().unwrap() = Some((entered, release_rx));
    let pending = commit_transaction(&node, &DEFAULT_CONTEXT, proto::TransactionId::new(), vec![Attested::opt(edit.clone(), None)]);
    tokio::pin!(pending);
    assert!(futures::poll!(&mut pending).is_pending());
    entered_rx.await?;

    // Ownership changes after Alice's check but before her storage commit.
    let transfer = Event::update(entity.id(), genesis.id().into(), AuthorId::Unknown, OperationSet(vec![set(owner, "Bob")?]));
    events.stage_event(transfer.clone());
    let candidate = RemoteTrxEntity::edit(&entity, Arc::new(std::sync::atomic::AtomicBool::new(true)))?;
    candidate.apply_event(&events, &mut Attested::from(transfer.clone()), |_| Ok(None)).await?;
    let mut transaction = storage.transaction();
    transaction.set_state(&entity.head(), &Attested::opt(candidate.read().to_entity_state()?, None)).await?;
    transaction.add_events(&[Attested::opt(transfer.clone(), None)]).await?;
    assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));
    candidate.commit(&node.entities, &events).await?;
    release.send(()).unwrap();
    assert!(matches!(pending.await.unwrap_err().downcast_ref::<MutationError>(), Some(MutationError::AccessDenied(AccessDenied::ByPolicy(_)))));
    assert_eq!(*agent.checked_owners.lock().unwrap(), vec![Some(Value::String("Alice".into())), Some(Value::String("Bob".into()))]);
    assert_eq!(storage.get_state(entity.id()).await?.payload.state.head, transfer.id().into());
    assert!(storage.get_events(vec![edit.id()], &ankql::ast::Predicate::True).await?.is_empty());
    assert_eq!(entity.value(&owner), Some(Value::String("Bob".into())));
    assert_eq!(entity.value(&title), None);
    Ok(())
}
