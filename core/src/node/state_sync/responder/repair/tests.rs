//! Repair building: the current state it reads, and what it sends toward it.

use super::*;
use crate::{
    node::{applier::NodeApplier, state_sync::fixtures::*},
    property::backend::{LWWBackend, PropertyBackend},
    retrieval::{LocalEventGetter, LocalStateGetter, SuspenseEvents},
    value::Value,
};
use ankurah_proto::Attested;

/// An event that sets the label to `label`: a genesis joining a model, or an
/// update of `parent`.
fn labeled(parent: Option<&proto::Event>, label: &str) -> anyhow::Result<proto::Event> {
    let backend = LWWBackend::new();
    backend.set(proto::PropertyId::System(proto::SystemProperty::Label), Some(Value::String(label.into())));
    let set = proto::Operation::Backend { backend: "lww".into(), operations: backend.to_operations()?.unwrap() };
    Ok(match parent {
        Some(parent) => proto::Event::update(
            parent.entity_id,
            proto::Clock::singleton(parent),
            proto::AuthorId::Unknown,
            proto::OperationSet(vec![set]),
        ),
        None => {
            let model = proto::ModelId::EntityId(proto::EntityId::from_bytes([7; 32]));
            let join = proto::Operation::Membership(proto::Membership::Add(model));
            proto::Event::genesis(None, proto::AuthorId::Unknown, proto::OperationSet(vec![join, set]))
        }
    })
}

/// The repair toward entity `id` for a requester holding `known`.
async fn repair(node: &TestNode, id: proto::EntityId, known: Option<proto::Clock>, catalog: bool) -> anyhow::Result<CurrentStateRepair> {
    let credentials = vec![];
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
    let mut repairs = current_state_repairs(node, &policy, &[(id, known)], &credentials, catalog).await?;
    Ok(repairs.pop().expect("one repair per entity"))
}

/// Entity `id`'s current state, as a repair reads it.
async fn read(node: &TestNode, id: proto::EntityId) -> anyhow::Result<Option<proto::Attested<proto::EntityState>>> {
    let credentials = vec![];
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
    Ok(node_states(node, std::iter::once(id), &policy).await?.remove(&id))
}

/// Commit `event` on `node` as a peer's event bridge.
async fn apply(node: &TestNode, event: &proto::Event) -> anyhow::Result<()> {
    let events = LocalEventGetter::new(node.storage.clone(), false);
    events.stage_event(event.clone());
    NodeApplier::apply_deltas(
        node,
        &proto::EntityId::from_bytes([8; 32]),
        vec![proto::EntityDelta {
            entity_id: event.entity_id,
            content: proto::DeltaContent::EventBridge { events: vec![Attested::from(event.clone()).into()] },
        }],
        &events,
        &LocalStateGetter::new(node.storage.clone()),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn payload_reads_prefer_current_residents_and_fall_back_without_hydrating() -> anyhow::Result<()> {
    let node = node().await?;
    let current = positioned_state(1, 20, 3)?;
    let id = current.payload.entity_id;
    node.storage.set_state(current.clone());
    let held = resident(&node, id).await?;
    let mut stored = positioned_state(1, 10, 2)?;
    stored.attestations.push(proto::Attestation(vec![42]));
    node.storage.set_state(stored.clone());
    *node.storage.reject_state_read.lock().unwrap() = Some(id);

    let state = read(&node, id).await?.unwrap();
    assert_eq!(state.payload, current.payload, "the payload comes from the current resident, without a storage read");
    assert!(state.attestations.is_empty(), "PermissiveAgent freshly attests with None; stored attestations must not leak onto newer state");

    drop(held);
    *node.storage.reject_state_read.lock().unwrap() = None;
    let state = read(&node, id).await?.unwrap();
    assert_eq!(state, stored, "storage fallback retains the attestation belonging to that state");
    assert!(node.entities.get(&id).is_none(), "sending a storage payload does not make an entity resident");
    assert!(read(&node, proto::EntityId::random()).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn a_member_outside_catalog_scope_is_denied_even_while_resident() -> anyhow::Result<()> {
    let node = node().await?;
    let source = states(1, 10);
    let id = source[0].payload.entity_id;
    node.storage.set_state(source[0].clone());
    let _resident = resident(&node, id).await?;
    *node.storage.reject_state_read.lock().unwrap() = Some(id);
    assert!(matches!(repair(&node, id, None, true).await?, CurrentStateRepair::Denied));
    assert!(matches!(repair(&node, id, None, false).await?, CurrentStateRepair::Delta { .. }));
    Ok(())
}

#[tokio::test]
async fn a_repair_sends_nothing_toward_a_held_head_and_the_state_to_a_requester_holding_nothing() -> anyhow::Result<()> {
    let node = node().await?;
    let source = states(1, 10).pop().unwrap();
    let id = source.payload.entity_id;
    node.storage.set_state(source.clone());
    let held = Some(source.payload.state.head.clone());
    assert!(matches!(repair(&node, id, held, false).await?, CurrentStateRepair::Current));
    let CurrentStateRepair::Delta { head, delta } = repair(&node, id, None, false).await? else {
        panic!("a requester holding nothing gets the state")
    };
    assert_eq!(head, source.payload.state.head, "the repair names the head it brings the requester to");
    assert_eq!(delta.entity_id, id);
    assert!(matches!(delta.content, proto::DeltaContent::StateSnapshot { .. }));
    Ok(())
}

#[tokio::test]
async fn a_repair_bridges_from_an_older_head_unless_the_state_is_smaller() -> anyhow::Result<()> {
    let node = node().await?;
    let large = "x".repeat(4096);
    let genesis = labeled(None, &large)?;
    apply(&node, &genesis).await?;
    let small = labeled(Some(&genesis), "small")?;
    apply(&node, &small).await?;
    let from_genesis = Some(proto::Clock::singleton(&genesis));
    let CurrentStateRepair::Delta { delta, .. } = repair(&node, genesis.entity_id, from_genesis.clone(), false).await? else {
        panic!("the requester's copy is behind")
    };
    assert!(matches!(delta.content, proto::DeltaContent::EventBridge { .. }), "one small event is smaller than the state");

    let mut latest = small;
    for _ in 0..4 {
        latest = labeled(Some(&latest), &large)?;
        apply(&node, &latest).await?;
    }
    let CurrentStateRepair::Delta { delta, .. } = repair(&node, genesis.entity_id, from_genesis, false).await? else {
        panic!("the requester's copy is behind")
    };
    assert!(matches!(delta.content, proto::DeltaContent::StateSnapshot { .. }), "four large events outweigh the state holding the last");
    Ok(())
}

#[tokio::test]
async fn a_page_of_repairs_answers_each_entity_in_order() -> anyhow::Result<()> {
    let node = node().await?;
    let source = states(2, 10);
    for state in &source {
        node.storage.set_state(state.clone());
    }
    let (first, second) = (&source[0].payload, &source[1].payload);
    let missing = proto::EntityId::random();
    let credentials = vec![];
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
    let entities = [(second.entity_id, None), (missing, None), (first.entity_id, Some(first.state.head.clone()))];
    let repairs = current_state_repairs(&node, &policy, &entities, &credentials, false).await?;
    assert!(
        matches!(&repairs[..], [
        CurrentStateRepair::Delta { delta, .. },
        CurrentStateRepair::Denied,
        CurrentStateRepair::Current,
    ] if delta.entity_id == second.entity_id),
        "{repairs:?}"
    );
    Ok(())
}
