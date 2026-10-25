//! Repair building: the current state it reads, and what it sends toward it.

use super::*;
use crate::node::state_sync::fixtures::*;

/// The repair toward entity `id` for a requester holding `known`.
async fn repair(node: &TestNode, id: proto::EntityId, known: Option<proto::Clock>, catalog: bool) -> anyhow::Result<CurrentStateRepair> {
    let credentials = vec![];
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
    let mut repairs = current_state_repairs(node, &policy, &[(id, known)], catalog).await?;
    Ok(repairs.pop().expect("one repair per entity"))
}

/// Entity `id`'s current state, as a repair reads it.
async fn read(node: &TestNode, id: proto::EntityId) -> anyhow::Result<Option<proto::Attested<proto::EntityState>>> {
    let credentials = vec![];
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
    Ok(current_states(node, std::iter::once(id), &policy).await?.remove(&id))
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
async fn a_member_outside_catalog_scope_is_unavailable_even_while_resident() -> anyhow::Result<()> {
    let node = node().await?;
    let source = states(1, 10);
    let id = source[0].payload.entity_id;
    node.storage.set_state(source[0].clone());
    let _resident = resident(&node, id).await?;
    *node.storage.reject_state_read.lock().unwrap() = Some(id);
    assert!(matches!(repair(&node, id, None, true).await?, CurrentStateRepair::Unavailable));
    assert!(matches!(repair(&node, id, None, false).await?, CurrentStateRepair::Lacking { .. }));
    Ok(())
}

#[tokio::test]
async fn a_repair_sends_nothing_toward_a_held_head_and_the_state_to_a_requester_holding_nothing() -> anyhow::Result<()> {
    let node = node().await?;
    let source = states(1, 10).pop().unwrap();
    let id = source.payload.entity_id;
    node.storage.set_state(source.clone());
    let held = source.payload.state.head.clone();
    let CurrentStateRepair::Current { head } = repair(&node, id, Some(held.clone()), false).await? else {
        panic!("a requester holding the current head needs nothing")
    };
    assert_eq!(head, held, "the repair names the head it holds");
    let CurrentStateRepair::Lacking { head, content } = repair(&node, id, None, false).await? else {
        panic!("a requester holding nothing lacks the state")
    };
    assert_eq!(head, source.payload.state.head, "the repair names the head it brings the requester to");
    assert!(
        matches!(content, proto::UpdateContent::StateAndEvent(state, events) if state.state == source.payload.state && events.is_empty())
    );
    Ok(())
}

#[tokio::test]
async fn a_repair_bridges_from_any_older_head_it_can_and_sends_the_state_from_one_it_cannot() -> anyhow::Result<()> {
    let node = node().await?;
    let genesis = labeled(None, "small")?;
    apply(&node, &genesis).await?;
    let mut edits = Vec::new();
    for _ in 0..4 {
        let edit = labeled(Some(edits.last().unwrap_or(&genesis)), &"x".repeat(4096))?;
        apply(&node, &edit).await?;
        edits.push(edit);
    }
    // The four large edits outweigh the state, which holds only the last label.
    let from_genesis = Some(proto::Clock::singleton(&genesis));
    let CurrentStateRepair::Lacking { content, .. } = repair(&node, genesis.entity_id, from_genesis, false).await? else {
        panic!("the requester's copy is behind")
    };
    let proto::UpdateContent::EventOnly(events) = content else { panic!("an older head the events connect gets the events") };
    let sent: Vec<_> =
        events.into_iter().map(|event| proto::Attested::<proto::Event>::from_parts(genesis.entity_id, event).payload.id()).collect();
    assert_eq!(sent, edits.iter().map(proto::Event::id).collect::<Vec<_>>(), "every edit since the requester's head, parents first");

    // An edit this node never saw leaves nothing to bridge from.
    let elsewhere = labeled(Some(&genesis), "edited elsewhere")?;
    let CurrentStateRepair::Lacking { content, .. } =
        repair(&node, genesis.entity_id, Some(proto::Clock::singleton(&elsewhere)), false).await?
    else {
        panic!("the requester's copy differs")
    };
    assert!(
        matches!(content, proto::UpdateContent::StateAndEvent(_, events) if events.is_empty()),
        "a head this node never saw gets the state"
    );
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
    let repairs = current_state_repairs(&node, &policy, &entities, false).await?;
    assert!(
        matches!(&repairs[..], [
        CurrentStateRepair::Lacking { head: lacking, .. },
        CurrentStateRepair::Unavailable,
        CurrentStateRepair::Current { head: current },
    ] if *lacking == second.state.head && *current == first.state.head),
        "{repairs:?}"
    );
    Ok(())
}
