//! Real nodes and transport, with only the comparison trees kept in memory.
mod common;
use ankurah::Mutable;
use anyhow::Result;
use common::*;
use proto::{NodeMessage, NodeResponseBody, StateItem};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Default)]
struct Trace(Arc<Mutex<Vec<NodeResponseBody>>>);
impl Trace {
    fn record(&self, message: &NodeMessage) {
        if let NodeMessage::Response(response) = message {
            self.0.lock().unwrap().push(response.body.clone());
        }
    }
    fn clear(&self) { self.0.lock().unwrap().clear(); }
    fn items(&self, query: Option<proto::QueryId>) -> Vec<StateItem> {
        let messages = self.0.lock().unwrap();
        let comparisons: HashSet<_> = messages
            .iter()
            .filter_map(|message| match message {
                NodeResponseBody::Fetch(state) if query.is_none() => Some(state.page.comparison),
                NodeResponseBody::QuerySubscribed { query_id, state } if Some(*query_id) == query => Some(state.page.comparison),
                _ => None,
            })
            .collect();
        messages
            .iter()
            .filter_map(|message| match message {
                NodeResponseBody::Fetch(state) | NodeResponseBody::QuerySubscribed { state, .. } => Some(&state.page),
                NodeResponseBody::TreeCompared(page) => Some(page),
                _ => None,
            })
            .filter(|page| comparisons.contains(&page.comparison))
            .flat_map(|page| page.items.clone())
            .collect()
    }
}

type TestNode = Node<SledStorageEngine, PermissiveAgent>;
async fn nodes() -> Result<(TestNode, TestNode)> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    server.wait_ready().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    Ok((server, client))
}
async fn wait_for(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition did not become true");
}
fn bridge_for(items: &[StateItem], id: EntityId) -> bool {
    items.iter().any(|item| {
        matches!(item, StateItem::Repair { delta, .. }
        if delta.entity_id == id && matches!(delta.content, proto::DeltaContent::EventBridge { .. }))
    })
}

#[tokio::test]
async fn fetch_equal_head_and_sparse_changes_avoid_full_inventory() -> Result<()> {
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let mut pets = Vec::new();
    for i in 0..256 {
        pets.push(trx.create(&Pet { name: format!("pet-{i:03}"), age: "1".into() }).await?.read()?);
    }
    trx.commit().await?;
    let trace = Trace::default();
    let (_connection, _) = GatedConnection::new(&server, &client, {
        let trace = trace.clone();
        move |message| {
            trace.record(message);
            false
        }
    })
    .await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    assert_eq!(reader.fetch::<PetView>("true").await?.len(), 256);
    let cold = trace.items(None);
    assert!(cold.iter().filter(|item| matches!(item, StateItem::Leaves(_))).count() > 1, "cold fetch must page its leaves");
    trace.clear();
    assert_eq!(reader.fetch::<PetView>("true").await?.len(), 256);
    assert!(matches!(trace.items(None).as_slice(), [StateItem::Equal(_)]), "equal results need only a root comparison");

    let trx = writer.begin();
    pets[117].edit(&trx)?.age()?.replace("2")?;
    trx.commit().await?;
    trace.clear();
    let fetched = reader.fetch::<PetView>("true").await?;
    assert_eq!(fetched.iter().find(|pet| pet.id() == pets[117].id()).unwrap().age()?, "2");
    let items = trace.items(None);
    assert!(bridge_for(&items, pets[117].id()), "a changed leaf should use its actual clock for an event bridge");
    let leaves: usize = items.iter().map(|item| if let StateItem::Leaves(page) = item { page.leaves.len() } else { 0 }).sum();
    assert!(leaves < 64, "one changed leaf should not resend 256 result identities: {leaves}");
    assert_eq!(items.iter().filter(|item| matches!(item, StateItem::Repair { .. })).count(), 1);
    Ok(())
}

#[tokio::test]
async fn fetch_repairs_former_matches_and_uses_heads_held_outside_the_query() -> Result<()> {
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let pet = trx.create(&Pet { name: "Rex".into(), age: "1".into() }).await?.read()?;
    trx.commit().await?;
    let trace = Trace::default();
    let (_connection, _) = GatedConnection::new(&server, &client, {
        let trace = trace.clone();
        move |message| {
            trace.record(message);
            false
        }
    })
    .await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let retained = reader.fetch::<PetView>("age = '1'").await?.remove(0);
    let trx = writer.begin();
    pet.edit(&trx)?.age()?.replace("2")?;
    trx.commit().await?;
    trace.clear();
    assert_eq!(reader.fetch::<PetView>("age = '2'").await?.len(), 1);
    assert!(bridge_for(&trace.items(None), pet.id()), "the new result can be known locally under another selection");
    assert_eq!(retained.age()?, "2");

    let trx = writer.begin();
    pet.edit(&trx)?.age()?.replace("3")?;
    trx.commit().await?;
    trace.clear();
    assert!(reader.fetch::<PetView>("age = '2'").await?.is_empty());
    assert_eq!(retained.age()?, "3", "an old match still receives its readable new state");
    assert!(bridge_for(&trace.items(None), pet.id()));
    Ok(())
}

#[tokio::test]
async fn fetch_falls_back_to_snapshot_when_event_bridge_exceeds_work_budget() -> Result<()> {
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let pet = trx.create(&Pet { name: "Rex".into(), age: "0".into() }).await?.read()?;
    trx.commit().await?;
    let trace = Trace::default();
    let (_connection, _) = GatedConnection::new(&server, &client, {
        let trace = trace.clone();
        move |message| {
            trace.record(message);
            false
        }
    })
    .await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    reader.fetch::<PetView>("true").await?;
    for age in 1..=300 {
        let trx = writer.begin();
        pet.edit(&trx)?.age()?.replace(&age.to_string())?;
        trx.commit().await?;
    }
    trace.clear();
    let fetched = reader.fetch::<PetView>("true").await?;
    assert_eq!(fetched[0].age()?, "300");
    assert_eq!(fetched[0].entity().head(), pet.entity().head());
    assert!(trace.items(None).iter().any(|item| matches!(
        item,
        StateItem::Repair { delta, .. }
            if delta.entity_id == pet.id() && matches!(delta.content, proto::DeltaContent::StateSnapshot { .. })
    )));
    Ok(())
}

#[tokio::test]
async fn large_snapshot_is_chunked_and_applied_before_fetch_returns() -> Result<()> {
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let payload = "x".repeat(180_000);
    let trx = writer.begin();
    let id = trx.create(&Record { title: payload.clone(), artist: "artist".into() }).await?.id();
    trx.commit().await?;
    let trace = Trace::default();
    let (_connection, _) = GatedConnection::new(&server, &client, {
        let trace = trace.clone();
        move |message| {
            trace.record(message);
            false
        }
    })
    .await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let fetched = reader.fetch::<RecordView>("true").await?;
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].id(), id);
    assert_eq!(fetched[0].title()?, payload);
    let items = trace.items(None);
    let chunks: Vec<_> =
        items.iter().filter_map(|item| if let StateItem::RepairChunk(chunk) = item { Some(chunk) } else { None }).collect();
    assert!(chunks.len() >= 5);
    assert_eq!(chunks[0].offset, 0);
    for pair in chunks.windows(2) {
        assert_eq!(pair[1].offset, pair[0].offset + pair[0].bytes.len() as u64);
    }
    assert!(chunks.last().unwrap().last);
    Ok(())
}

#[tokio::test]
async fn live_catchup_holds_edits_until_activation_then_reconnects_from_retained_state() -> Result<()> {
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let pet = trx.create(&Pet { name: "Rex".into(), age: "1".into() }).await?.read()?;
    trx.commit().await?;
    let held_query = Arc::new(Mutex::new(None));
    let held = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let trace = Trace::default();
    let (connection, gate) = GatedConnection::new(&server, &client, {
        let query = held_query.clone(); let held = held.clone(); let trace = trace.clone();
        move |message| {
            trace.record(message);
            let park = matches!(message, NodeMessage::Response(response)
                if matches!(&response.body, NodeResponseBody::QuerySubscribed { query_id, .. } if Some(*query_id) == *query.lock().unwrap()));
            if park { held.store(true, std::sync::atomic::Ordering::Release); }
            park
        }
    }).await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    reader.fetch::<PetView>("true").await?;
    let query = reader.query::<PetView>(nocache("true")?)?;
    *held_query.lock().unwrap() = Some(query.query_id());
    wait_for(|| held.load(std::sync::atomic::Ordering::Acquire)).await;
    let watcher = TestWatcher::changeset_with_event_ids();
    let _guard = query.subscribe(&watcher);
    let trx = writer.begin();
    pet.edit(&trx)?.age()?.replace("2")?;
    let new_id = trx.create(&Pet { name: "Nori".into(), age: "3".into() }).await?.id();
    trx.commit().await?;
    assert!(!query.loaded(), "the held initial comparison is not applied");
    assert_eq!(watcher.count(), 0);
    *held_query.lock().unwrap() = None;
    gate.release_held(&client).await;
    query.wait_durable_answered().await?;
    wait_for(|| {
        query.peek().iter().any(|row| row.id() == new_id)
            && query.peek().iter().any(|row| row.id() == pet.id() && row.age().unwrap() == "2")
    })
    .await;
    let changes = watcher.quiesce_drain().await;
    assert!(changes.iter().flatten().any(|(id, _, events)| *id == pet.id() && !events.is_empty()), "the held edit must retain its events");

    drop(connection);
    server.deregister_peer(client.id);
    client.deregister_peer(server.id);
    trace.clear();
    let (_connection, _) = GatedConnection::new(&server, &client, {
        let trace = trace.clone();
        move |message| {
            trace.record(message);
            false
        }
    })
    .await;
    wait_for(|| !trace.items(Some(query.query_id())).is_empty()).await;
    assert!(matches!(trace.items(Some(query.query_id())).as_slice(), [StateItem::Equal(_)]));
    let trx = writer.begin();
    pet.edit(&trx)?.age()?.replace("4")?;
    trx.commit().await?;
    wait_for(|| query.peek().iter().any(|row| row.id() == pet.id() && row.age().unwrap() == "4")).await;
    Ok(())
}

#[tokio::test]
async fn ordered_limit_with_ties_preserves_membership_and_fills_evictions() -> Result<()> {
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    for (i, age) in ["1", "1", "2", "3", "4"].into_iter().enumerate() {
        trx.create(&Pet { name: format!("pet-{i}"), age: age.into() }).await?;
    }
    trx.commit().await?;
    let _connection = LocalProcessConnection::new(&server, &client).await?;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let selection = "age >= '1' ORDER BY age ASC LIMIT 3";
    let expected = writer.fetch::<PetView>(selection).await?;
    let ids = |rows: Vec<PetView>| rows.into_iter().map(|row| row.id()).collect::<Vec<_>>();
    assert_eq!(ids(reader.fetch(selection).await?), ids(expected.clone()));
    let query = reader.query_wait::<PetView>(nocache(selection)?).await?;
    assert_eq!(query.ids(), ids(expected.clone()));
    let trx = writer.begin();
    expected[0].edit(&trx)?.age()?.replace("0")?;
    trx.commit().await?;
    let expected = ids(writer.fetch(selection).await?);
    wait_for(|| query.ids() == expected).await;
    assert_eq!(ids(reader.fetch(selection).await?), expected);
    Ok(())
}

#[tokio::test]
async fn superseded_comparison_cannot_activate_or_close_the_new_query_version() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    trx.create(&Pet { name: "old".into(), age: "1".into() }).await?;
    let next = trx.create(&Pet { name: "new".into(), age: "1".into() }).await?.read()?;
    trx.commit().await?;
    let target = Arc::new(Mutex::new(None));
    let parked = Arc::new(AtomicBool::new(false));
    let (_connection, gate) = GatedConnection::new(&server, &client, {
        let target = target.clone();
        let parked = parked.clone();
        move |message| {
            matches!(message, NodeMessage::Response(response)
            if matches!(&response.body, NodeResponseBody::QuerySubscribed { query_id, .. }
                if Some(*query_id) == *target.lock().unwrap() && !parked.swap(true, Ordering::AcqRel)))
        }
    })
    .await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    reader.resolve_model_id::<Pet>().await?;
    let query = reader.query::<PetView>(nocache("name = 'old'")?)?;
    *target.lock().unwrap() = Some(query.query_id());
    wait_for(|| parked.load(Ordering::Acquire)).await;
    query.update_selection("name = 'new'")?;
    query.wait_durable_answered().await?;
    assert_eq!(query.ids(), [next.id()]);
    gate.release_held(&client).await;
    let trx = writer.begin();
    next.edit(&trx)?.age()?.replace("2")?;
    trx.commit().await?;
    wait_for(|| query.peek().first().is_some_and(|row| row.age().unwrap() == "2")).await;
    assert_eq!(query.ids(), [next.id()]);
    Ok(())
}

#[tokio::test]
async fn comparisons_reject_wrong_credentials_unsolicited_answers_and_early_applied() -> Result<()> {
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    trx.create(&Pet { name: "Rex".into(), age: "1".into() }).await?;
    trx.commit().await?;
    let _connection = LocalProcessConnection::new(&server, &client).await?;
    client.wait_ready().await?;
    let model = writer.resolve_model_id::<Pet>().await?;
    let response = client
        .request(
            server.id,
            &DEFAULT_CONTEXT,
            proto::NodeRequestBody::Fetch { selection: ankql::ast::Predicate::MemberOf(model).into(), known_state: None },
        )
        .await?;
    let NodeResponseBody::Fetch(state) = response else { panic!("{response:?}") };
    let comparison = state.page.comparison;
    assert!(matches!(
        client
            .request(
                server.id,
                &DEFAULT_CONTEXT,
                proto::NodeRequestBody::CloseComparison { comparison, outcome: proto::ComparisonOutcome::Applied }
            )
            .await?,
        NodeResponseBody::Error(_)
    ));
    let no_credentials = Vec::<&'static ankurah::policy::DefaultContext>::new();
    assert!(matches!(
        client.request(server.id, &no_credentials, proto::NodeRequestBody::CompareTree { comparison, answers: vec![] }).await?,
        NodeResponseBody::Error(_)
    ));
    assert!(matches!(
        client
            .request(
                server.id,
                &DEFAULT_CONTEXT,
                proto::NodeRequestBody::CompareTree {
                    comparison,
                    answers: vec![proto::StateAnswer::Heads { known: vec![], unknown: vec![EntityId::random()] }],
                }
            )
            .await?,
        NodeResponseBody::Error(_)
    ));
    assert!(matches!(
        client
            .request(
                server.id,
                &DEFAULT_CONTEXT,
                proto::NodeRequestBody::CloseComparison { comparison, outcome: proto::ComparisonOutcome::Abandoned }
            )
            .await?,
        NodeResponseBody::Success
    ));
    assert!(matches!(
        client.request(server.id, &DEFAULT_CONTEXT, proto::NodeRequestBody::CompareTree { comparison, answers: vec![] }).await?,
        NodeResponseBody::ComparisonUnavailable { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn former_match_reentering_during_catchup_is_not_lost_to_a_noop_live_update() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let pet = trx.create(&Pet { name: "Rex".into(), age: "1".into() }).await?.read()?;
    trx.commit().await?;
    let target = Arc::new(Mutex::new(None));
    let parked = Arc::new(AtomicBool::new(false));
    let (_connection, gate) = GatedConnection::new(&server, &client, {
        let target = target.clone();
        let parked = parked.clone();
        move |message| {
            matches!(message, NodeMessage::Response(response)
            if matches!(&response.body, NodeResponseBody::QuerySubscribed { query_id, .. }
                if Some(*query_id) == *target.lock().unwrap() && !parked.swap(true, Ordering::AcqRel)))
        }
    })
    .await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    reader.fetch::<PetView>("age = '1'").await?;
    let trx = writer.begin();
    pet.edit(&trx)?.age()?.replace("2")?;
    trx.commit().await?;
    let query = reader.query::<PetView>(nocache("age = '1'")?)?;
    *target.lock().unwrap() = Some(query.query_id());
    wait_for(|| parked.load(Ordering::Acquire)).await;
    // The captured result is empty. The selective former-match repair will
    // now read this newer state; the queued Add carries that same head.
    let trx = writer.begin();
    pet.edit(&trx)?.age()?.replace("1")?;
    trx.commit().await?;
    gate.release_held(&client).await;
    query.wait_durable_answered().await?;
    wait_for(|| query.ids() == [pet.id()]).await;
    Ok(())
}

#[tokio::test]
async fn retry_replays_the_same_snapshot_and_followup_without_advancing_twice() -> Result<()> {
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    trx.create(&Pet { name: "Rex".into(), age: "1".into() }).await?;
    trx.commit().await?;
    let trace = Trace::default();
    let (_connection, _) = GatedConnection::new(&server, &client, {
        let trace = trace.clone();
        move |message| {
            trace.record(message);
            false
        }
    })
    .await;
    client.wait_ready().await?;
    let model = writer.resolve_model_id::<Pet>().await?;
    let body = proto::NodeRequestBody::Fetch { selection: ankql::ast::Predicate::MemberOf(model).into(), known_state: None };
    let initial_request = proto::RequestId::new();
    for _ in 0..2 {
        server
            .handle_message(NodeMessage::Request {
                auth: vec![proto::AuthData(vec![])],
                request: proto::NodeRequest {
                    id: initial_request.clone(),
                    from: client.id,
                    to: server.id,
                    body: bincode::deserialize(&bincode::serialize(&body)?)?,
                },
            })
            .await?;
    }
    let initial = || {
        trace
            .0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|body| match body {
                NodeResponseBody::Fetch(state) => Some(state.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    wait_for(|| initial().len() == 2).await;
    let first = initial().remove(0);
    assert_eq!(bincode::serialize(&first)?, bincode::serialize(&initial()[1])?);
    let comparison = first.page.comparison;
    let StateItem::Summary(mut head) = first.page.items[0].clone() else { panic!("expected initial summary") };
    head.digest = Default::default();
    let body = proto::NodeRequestBody::CompareTree { comparison, answers: vec![proto::StateAnswer::Summary(head)] };
    let followup = proto::RequestId::new();
    for _ in 0..2 {
        server
            .handle_message(NodeMessage::Request {
                auth: vec![proto::AuthData(vec![])],
                request: proto::NodeRequest {
                    id: followup.clone(),
                    from: client.id,
                    to: server.id,
                    body: bincode::deserialize(&bincode::serialize(&body)?)?,
                },
            })
            .await?;
    }
    let pages = || {
        trace
            .0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|body| match body {
                NodeResponseBody::TreeCompared(page) if page.comparison == comparison => Some(page.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    wait_for(|| pages().len() == 2).await;
    assert_eq!(bincode::serialize(&pages()[0])?, bincode::serialize(&pages()[1])?);
    let StateItem::NeedLeaves(bounds) = pages()[0].items[0].clone() else { panic!("expected leaf request") };
    let advanced = client
        .request(
            server.id,
            &DEFAULT_CONTEXT,
            proto::NodeRequestBody::CompareTree {
                comparison,
                answers: vec![proto::StateAnswer::Leaves(proto::LeafPage { bounds, leaves: vec![] })],
            },
        )
        .await?;
    assert!(matches!(advanced, NodeResponseBody::TreeCompared(_)));
    server
        .handle_message(NodeMessage::Request {
            auth: vec![proto::AuthData(vec![])],
            request: proto::NodeRequest { id: followup, from: client.id, to: server.id, body },
        })
        .await?;
    wait_for(|| {
        trace
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|body| matches!(body, NodeResponseBody::ComparisonUnavailable { comparison: id } if *id == comparison))
    })
    .await;
    client
        .request(
            server.id,
            &DEFAULT_CONTEXT,
            proto::NodeRequestBody::CloseComparison { comparison, outcome: proto::ComparisonOutcome::Abandoned },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn fetch_restarts_when_the_responder_discards_its_comparison() -> Result<()> {
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let pet = trx.create(&Pet { name: "Rex".into(), age: "1".into() }).await?.read()?;
    trx.commit().await?;
    let held = Arc::new(Mutex::new(None));
    let (_connection, gate) = GatedConnection::new(&server, &client, {
        let held = held.clone();
        move |message| {
            if let NodeMessage::Response(response) = message {
                if let NodeResponseBody::Fetch(state) = &response.body {
                    let mut comparison = held.lock().unwrap();
                    if comparison.is_none() {
                        *comparison = Some(state.page.comparison);
                        return true;
                    }
                }
            }
            false
        }
    })
    .await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    reader.resolve_model_id::<Pet>().await?;
    let fetch = tokio::spawn(async move { reader.fetch::<PetView>("true").await });
    wait_for(|| held.lock().unwrap().is_some()).await;
    let comparison = held.lock().unwrap().unwrap();
    client
        .request(
            server.id,
            &DEFAULT_CONTEXT,
            proto::NodeRequestBody::CloseComparison { comparison, outcome: proto::ComparisonOutcome::Abandoned },
        )
        .await?;
    let trx = writer.begin();
    pet.edit(&trx)?.age()?.replace("2")?;
    trx.commit().await?;
    gate.release_held(&client).await;
    let rows = tokio::time::timeout(Duration::from_secs(10), fetch).await???;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].age()?, "2");
    Ok(())
}

#[tokio::test]
async fn catchup_batches_many_edits_with_their_connecting_events() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let pet = trx.create(&Pet { name: "Rex".into(), age: "0".into() }).await?.read()?;
    trx.commit().await?;
    let target = Arc::new(Mutex::new(None));
    let parked = Arc::new(AtomicBool::new(false));
    let (_connection, gate) = GatedConnection::new(&server, &client, {
        let target = target.clone();
        let parked = parked.clone();
        move |message| {
            matches!(message, NodeMessage::Response(response)
            if matches!(&response.body, NodeResponseBody::QuerySubscribed { query_id, .. }
                if Some(*query_id) == *target.lock().unwrap() && !parked.swap(true, Ordering::AcqRel)))
        }
    })
    .await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    reader.fetch::<PetView>("true").await?;
    let query = reader.query::<PetView>(nocache("true")?)?;
    *target.lock().unwrap() = Some(query.query_id());
    wait_for(|| parked.load(Ordering::Acquire)).await;
    let watcher = TestWatcher::changeset_with_event_ids();
    let _guard = query.subscribe(&watcher);
    let mut events = HashSet::new();
    for age in 1..=40 {
        let trx = writer.begin();
        pet.edit(&trx)?.age()?.replace(&age.to_string())?;
        trx.commit().await?;
        events.extend(pet.entity().head().ids().cloned());
    }
    gate.release_held(&client).await;
    query.wait_durable_answered().await?;
    wait_for(|| query.peek().first().is_some_and(|row| row.age().unwrap() == "40")).await;
    let received: HashSet<_> = watcher.quiesce_drain().await.into_iter().flatten().flat_map(|(_, _, events)| events).collect();
    assert_eq!(received, events, "coalescing must preserve the entire connecting bridge");
    Ok(())
}

#[tokio::test]
async fn catchup_buffer_overflow_restarts_even_after_a_complete_reply_was_sent() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let record = trx.create(&Record { title: "old".into(), artist: "artist".into() }).await?.read()?;
    trx.commit().await?;
    let target = Arc::new(Mutex::new(None));
    let parked = Arc::new(AtomicBool::new(false));
    let trace = Trace::default();
    let (_connection, gate) = GatedConnection::new(&server, &client, {
        let target = target.clone();
        let parked = parked.clone();
        let trace = trace.clone();
        move |message| {
            trace.record(message);
            matches!(message, NodeMessage::Response(response)
                if matches!(&response.body, NodeResponseBody::QuerySubscribed { query_id, .. }
                    if Some(*query_id) == *target.lock().unwrap() && !parked.swap(true, Ordering::AcqRel)))
        }
    })
    .await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let retained = reader.fetch::<RecordView>("true").await?.remove(0);
    let query = reader.query::<RecordView>(nocache("true")?)?;
    *target.lock().unwrap() = Some(query.query_id());
    wait_for(|| parked.load(Ordering::Acquire)).await;
    assert!(matches!(trace.items(Some(query.query_id())).as_slice(), [StateItem::Equal(_)]));
    let payload = "x".repeat(5 * 1024 * 1024); // State plus event exceed the 8 MiB handoff buffer.
    let trx = writer.begin();
    record.edit(&trx)?.title()?.set(&payload)?;
    trx.commit().await?;
    wait_for(|| retained.entity().head() == record.entity().head()).await;
    gate.release_held(&client).await;
    tokio::time::timeout(Duration::from_secs(30), query.wait_durable_answered()).await??;
    assert_eq!(query.peek()[0].title()?, payload);
    assert!(trace.0.lock().unwrap().iter().any(|body| matches!(body, NodeResponseBody::ComparisonUnavailable { .. })));
    assert!(trace.items(Some(query.query_id())).len() >= 2, "the query must obtain a fresh comparison");
    Ok(())
}
