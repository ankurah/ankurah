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
struct Trace(Arc<Mutex<Vec<NodeResponseBody>>>, Arc<Mutex<Vec<proto::StateFrame>>>);
impl Trace {
    fn record(&self, message: &NodeMessage) {
        if let NodeMessage::Response(response) = message {
            self.0.lock().unwrap().push(response.body.clone());
        } else if let NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame { frame, .. }, .. }) = message {
            self.1.lock().unwrap().push(frame.clone());
        }
    }
    fn clear(&self) {
        self.0.lock().unwrap().clear();
        self.1.lock().unwrap().clear();
    }
    fn items(&self, query: Option<proto::QueryId>) -> Vec<StateItem> {
        let messages = self.0.lock().unwrap();
        let comparisons: HashSet<_> = messages
            .iter()
            .filter_map(|message| match message {
                NodeResponseBody::Fetch(state) if query.is_none() => Some(state.frame.stream),
                NodeResponseBody::QuerySubscribed { query_id, state } if Some(*query_id) == query => Some(state.frame.stream),
                _ => None,
            })
            .collect();
        let pushed = self.1.lock().unwrap();
        messages
            .iter()
            .filter_map(|message| match message {
                NodeResponseBody::Fetch(state) | NodeResponseBody::QuerySubscribed { state, .. } => Some(&state.frame),
                _ => None,
            })
            .chain(pushed.iter())
            .filter(|frame| comparisons.contains(&frame.stream))
            .flat_map(|frame| match &frame.body {
                proto::StateFrameBody::State { items, .. } => items.clone(),
                _ => vec![],
            })
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
async fn wait_for(mut condition: impl FnMut() -> bool) { wait_for_named("condition", &mut condition).await; }
async fn wait_for_named(description: &str, mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{description} did not become true"));
}
fn bridge_for(items: &[StateItem], id: EntityId) -> bool {
    items.iter().any(|item| {
        matches!(item, StateItem::Repair { delta, .. }
        if delta.entity_id == id && matches!(delta.content, proto::DeltaContent::EventBridge { .. }))
    })
}
fn offers_target(items: &[StateItem], id: EntityId, head: &proto::Clock) -> bool {
    items.iter().any(|item| match item {
        StateItem::Repair { target, delta } => delta.entity_id == id && target == head,
        _ => false,
    })
}

#[tokio::test]
async fn fetch_and_subscription_send_one_complete_initial_head_inventory() -> Result<()> {
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    for i in 0..256 {
        trx.create(&Pet { name: format!("pet-{i:03}"), age: "1".into() }).await?;
    }
    trx.commit().await?;
    let offers = Arc::new(Mutex::new(Vec::new()));
    let (_connection, _) = GatedConnection::new(&client, &server, {
        let offers = offers.clone();
        move |message| {
            if let NodeMessage::Request { request, .. } = message {
                let (subscribe, known) = match &request.body {
                    proto::NodeRequestBody::Fetch { known_state, .. } => (false, known_state),
                    proto::NodeRequestBody::SubscribeQuery { known_state, .. } => (true, known_state),
                    _ => return false,
                };
                if let [proto::SpanContent::Heads(inventory)] = known.spans.as_slice() {
                    offers.lock().unwrap().push((subscribe, inventory.heads.len()));
                }
            }
            false
        }
    })
    .await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    assert_eq!(reader.fetch::<PetView>("true").await?.len(), 256);
    assert_eq!(reader.fetch::<PetView>("true").await?.len(), 256);
    let query = reader.query_wait::<PetView>("true").await?;
    query.wait_durable_answered().await?;
    let offers = offers.lock().unwrap();
    assert!(offers.contains(&(false, 256)), "Fetch sends all heads in one region");
    assert!(offers.contains(&(true, 256)), "SubscribeQuery sends all heads in one region");
    Ok(())
}

#[tokio::test]
async fn fetch_initial_inventory_omits_equal_heads_and_sends_only_the_changed_target() -> Result<()> {
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
    assert!(cold.iter().filter(|item| matches!(item, StateItem::Heads(_))).count() > 1, "cold fetch must page membership metadata");
    assert_eq!(cold.iter().map(|item| if let StateItem::Heads(page) = item { page.heads.len() } else { 0 }).sum::<usize>(), 256);
    assert_eq!(
        cold.iter()
            .filter(|item| matches!(item, StateItem::Repair { delta, .. }
        if matches!(delta.content, proto::DeltaContent::StateSnapshot { .. })))
            .count(),
        256
    );
    trace.clear();
    assert_eq!(reader.fetch::<PetView>("true").await?.len(), 256);
    let equal = trace.items(None);
    assert!(equal.is_empty(), "the equal initial inventory needs no response items: {equal:?}");
    assert!(
        trace.0.lock().unwrap().iter().any(|body| matches!(body,
        NodeResponseBody::Fetch(state) if matches!(state.frame.body,
            proto::StateFrameBody::State { progress: proto::ComparisonProgress::Complete, .. }))),
        "empty comparison output must still finish with Complete"
    );

    let trx = writer.begin();
    pets[117].edit(&trx)?.age()?.replace("2")?;
    trx.commit().await?;
    trace.clear();
    let fetched = reader.fetch::<PetView>("true").await?;
    let changed = fetched.iter().find(|pet| pet.id() == pets[117].id()).unwrap();
    assert_eq!(changed.age()?, "2");
    assert_eq!(changed.entity().head(), pets[117].entity().head());
    let items = trace.items(None);
    assert!(bridge_for(&items, pets[117].id()), "a changed leaf should use its actual clock for an event bridge");
    assert!(offers_target(&items, pets[117].id(), &pets[117].entity().head()), "the one repair must cover the captured target");
    // Captured membership is independent of the current repair target. Only
    // the changed member needs its head repeated; equal members remain omitted.
    let heads = items
        .iter()
        .filter_map(|item| if let StateItem::Heads(page) = item { Some(page.heads.to_vec()) } else { None })
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(heads.len(), 1);
    assert_eq!(heads[0].entity.entity_id, pets[117].id());
    assert_eq!(heads[0].entity.head, pets[117].entity().head());
    assert_eq!(items.iter().filter(|item| matches!(item, StateItem::Repair { .. })).count(), 1);
    assert!(
        items.iter().all(|item| matches!(item, StateItem::Heads(_) | StateItem::Repair { .. })),
        "only the changed target needs output: {items:?}"
    );
    Ok(())
}

#[tokio::test]
async fn fetch_repairs_former_matches_and_preserves_state_held_outside_the_query() -> Result<()> {
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
    assert!(
        offers_target(&trace.items(None), pet.id(), &pet.entity().head()),
        "a new result offers the current target even when state was held under another selection"
    );
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
async fn empty_query_inventory_sends_snapshots_without_asserting_global_entity_absence() -> Result<()> {
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let mut known = Vec::new();
    for i in 0..96 {
        known.push(trx.create(&Pet { name: format!("known-{i}"), age: "1".into() }).await?.read()?);
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
    assert_eq!(reader.fetch::<PetView>("age = '1'").await?.len(), 96);
    let trx = writer.begin();
    for pet in &known {
        pet.edit(&trx)?.age()?.replace("2")?;
    }
    for i in 0..96 {
        trx.create(&Pet { name: format!("new-{i}"), age: "2".into() }).await?;
    }
    trx.commit().await?;
    trace.clear();
    assert_eq!(reader.fetch::<PetView>("age = '2'").await?.len(), 192);
    let items = trace.items(None);
    assert!(items.iter().all(|item| !matches!(item, StateItem::Digest(_) | StateItem::NeedHeads(_))));
    assert!(known.iter().all(|pet| offers_target(&items, pet.id(), &pet.entity().head())));
    assert_eq!(
        items
            .iter()
            .filter(
                |item| matches!(item, StateItem::Repair { delta, .. } if matches!(delta.content, proto::DeltaContent::StateSnapshot { .. }))
            )
            .count(),
        192
    );
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
        items.iter().filter_map(|item| if let StateItem::RepairChunk { chunk, .. } = item { Some(chunk) } else { None }).collect();
    assert!(chunks.len() >= 5);
    assert_eq!(chunks[0].offset, 0);
    for pair in chunks.windows(2) {
        assert_eq!(pair[1].offset, pair[0].offset + pair[0].bytes.len() as u64);
    }
    assert!(chunks.last().unwrap().last);
    Ok(())
}

#[tokio::test]
async fn live_edits_cross_unfinished_comparison_then_reconnect_from_retained_state() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let mut pets = Vec::new();
    for i in 0..64 {
        // Wide canonical keys exceed the bounded initial inventory. The query
        // must exchange refinement, which the gate can hold without leaving
        // a hole in the ordered source-to-receiver frame sequence.
        pets.push(trx.create(&Pet { name: format!("{i:03}-{}", "x".repeat(8192)), age: "1".into() }).await?.read()?);
    }
    trx.commit().await?;
    let armed = Arc::new(AtomicBool::new(false));
    let held = Arc::new(Mutex::new(None));
    let (connection, gate) = GatedConnection::new(&client, &server, {
        let armed = armed.clone();
        let held = held.clone();
        move |message| {
            if !armed.load(Ordering::Acquire) {
                return false;
            }
            if let NodeMessage::Request {
                request:
                    proto::NodeRequest {
                        body: proto::NodeRequestBody::ContinueState { stream: comparison, round: Some(0), answers, .. }, ..
                    },
                ..
            } = message
            {
                if !answers.is_empty() {
                    *held.lock().unwrap() = Some(*comparison);
                    return true;
                }
            }
            false
        }
    })
    .await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let retained = reader.fetch::<PetView>("true").await?;
    // Diverge from the offer before subscribing, so refinement cannot finish
    // with an equal root digest.
    let trx = writer.begin();
    pets[0].edit(&trx)?.age()?.replace("2")?;
    trx.commit().await?;
    armed.store(true, Ordering::Release);
    let query = reader.query::<PetView>(nocache("name >= '' AND age >= '1' ORDER BY name ASC")?)?;
    wait_for_named("round feedback held", || held.lock().unwrap().is_some()).await;
    assert!(!query.loaded(), "the refinement answer is still held");
    let watcher = TestWatcher::changeset_with_event_ids();
    let _guard = query.subscribe(&watcher);
    let trx = writer.begin();
    pets[1].edit(&trx)?.age()?.replace("3")?;
    trx.commit().await?;
    wait_for_named("live edit applied before feedback release", || {
        query.peek().iter().any(|row| row.id() == pets[1].id() && row.age().as_deref() == Ok("3"))
    })
    .await;
    assert!(!query.loaded(), "unrelated live application must not claim the unfinished round is ready");
    assert_eq!(query.peek().iter().find(|row| row.id() == pets[1].id()).unwrap().entity().head(), pets[1].entity().head());
    let changes = watcher.quiesce_drain().await;
    assert!(
        changes.iter().flatten().any(|(id, _, events)| *id == pets[1].id() && !events.is_empty()),
        "the live edit must publish its events before comparison feedback is released"
    );
    // R includes this member. A later absolute Absent must survive the older
    // baseline membership that will arrive after refinement resumes.
    let trx = writer.begin();
    pets[2].edit(&trx)?.age()?.replace("0")?;
    trx.commit().await?;
    let former = retained.iter().find(|row| row.id() == pets[2].id()).unwrap();
    wait_for_named("former member state applied before feedback release", || former.age().as_deref() == Ok("0")).await;
    assert!(!query.loaded());
    armed.store(false, Ordering::Release);
    gate.release_held(&server).await;
    tokio::time::timeout(Duration::from_secs(10), query.wait_durable_answered()).await??;
    let expected_ids = pets.iter().filter(|pet| pet.id() != pets[2].id()).map(|pet| pet.id()).collect::<Vec<_>>();
    assert_eq!(query.ids(), expected_ids, "an older baseline must not resurrect a later absent member");
    for (actual, expected) in query.peek().iter().zip(pets.iter().filter(|pet| pet.id() != pets[2].id())) {
        assert_eq!(actual.age()?, expected.age()?);
        assert_eq!(actual.entity().head(), expected.entity().head());
    }

    drop(connection);
    server.deregister_peer(client.id);
    client.deregister_peer(server.id);
    let trace = Trace::default();
    let (_connection, _) = GatedConnection::new(&server, &client, {
        let trace = trace.clone();
        move |message| {
            trace.record(message);
            false
        }
    })
    .await;
    wait_for(|| {
        trace.0.lock().unwrap().iter().any(|body| {
            matches!(body,
        NodeResponseBody::QuerySubscribed { query_id, .. } if *query_id == query.query_id())
        })
    })
    .await;
    let trx = writer.begin();
    pets[1].edit(&trx)?.age()?.replace("4")?;
    trx.commit().await?;
    wait_for(|| query.peek().iter().any(|row| row.id() == pets[1].id() && row.age().as_deref() == Ok("4"))).await;
    assert_eq!(query.ids(), expected_ids);
    Ok(())
}

#[tokio::test]
async fn repeated_refresh_reuses_the_standing_stream_and_keeps_live_delivery() -> Result<()> {
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
    let query = reader.query_wait::<PetView>(nocache("true")?).await?;
    query.wait_durable_answered().await?;
    let initial = || {
        trace
            .0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|body| match body {
                NodeResponseBody::QuerySubscribed { query_id, state } if *query_id == query.query_id() => Some(state.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    let replies = initial();
    assert_eq!(replies.len(), 1);
    let stream = replies[0].frame.stream;

    for round in 1..=10 {
        let age = round.to_string();
        let trx = writer.begin();
        pet.edit(&trx)?.age()?.replace(&age)?;
        trx.commit().await?;
        wait_for(|| query.peek().first().is_some_and(|row| row.age().as_deref() == Ok(age.as_str()))).await;
        tokio::time::timeout(Duration::from_secs(10), query.refresh()).await??;
        assert!(query.loaded(), "refresh keeps the established result usable");
        assert_eq!(query.ids(), [pet.id()]);
        assert_eq!(query.peek()[0].age()?, age);
        assert_eq!(query.peek()[0].entity().head(), pet.entity().head());
        let rounds: Vec<_> = trace
            .1
            .lock()
            .unwrap()
            .iter()
            .filter_map(|frame| match frame.body {
                proto::StateFrameBody::BeginRound(start) if frame.stream == stream => Some(start.number),
                _ => None,
            })
            .collect();
        assert_eq!(rounds, (1..=round).collect::<Vec<_>>(), "each refresh starts exactly one round on the same stream");
        assert_eq!(initial().len(), 1, "refresh must not replace the query subscription");
    }

    // The last Applied releases round resources, not the standing stream.
    let trx = writer.begin();
    pet.edit(&trx)?.age()?.replace("after-refresh")?;
    trx.commit().await?;
    wait_for(|| query.peek().first().is_some_and(|row| row.age().as_deref() == Ok("after-refresh"))).await;
    assert_eq!(query.peek()[0].entity().head(), pet.entity().head());
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
            proto::NodeRequestBody::Fetch {
                selection: ankql::ast::Predicate::MemberOf(model).into(),
                known_state: proto::KnownState {
                    spec: proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), ankql::ast::Predicate::MemberOf(model)),
                    spans: vec![],
                },
            },
        )
        .await?;
    let NodeResponseBody::Fetch(state) = response else { panic!("{response:?}") };
    let comparison = state.frame.stream;
    assert!(matches!(
        client
            .request(
                server.id,
                &DEFAULT_CONTEXT,
                proto::NodeRequestBody::FinishStateRound {
                    stream: comparison,
                    round: 0,
                    outcome: proto::StateRoundOutcome::Applied { processed_through: 1 }
                }
            )
            .await?,
        NodeResponseBody::Error(_)
    ));
    let no_credentials = Vec::<&'static ankurah::policy::DefaultContext>::new();
    assert!(matches!(
        client
            .request(
                server.id,
                &no_credentials,
                proto::NodeRequestBody::ContinueState {
                    stream: comparison,
                    round: None,
                    processed_through: 0,
                    applied_through: None,
                    answers: vec![]
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
                proto::NodeRequestBody::ContinueState {
                    stream: comparison,
                    round: Some(0),
                    applied_through: None,
                    processed_through: 0,
                    answers: vec![proto::StateAnswer::Heads(proto::TreeSpanInventory {
                        bounds: proto::TreeSpan::new(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
                        heads: vec![].into()
                    })],
                }
            )
            .await?,
        NodeResponseBody::Error(_)
    ));
    server.handle_message(proto::NodeMessage::AbandonStateStream { from: client.id, to: server.id, stream: comparison }).await?;
    assert!(matches!(
        client
            .request(
                server.id,
                &DEFAULT_CONTEXT,
                proto::NodeRequestBody::ContinueState {
                    stream: comparison,
                    round: None,
                    processed_through: 0,
                    applied_through: None,
                    answers: vec![]
                }
            )
            .await?,
        NodeResponseBody::Success
    ));
    assert!(matches!(
        client
            .request(
                server.id,
                &DEFAULT_CONTEXT,
                proto::NodeRequestBody::ContinueState {
                    stream: comparison,
                    round: Some(0),
                    applied_through: None,
                    processed_through: 0,
                    answers: vec![proto::StateAnswer::Heads(proto::TreeSpanInventory {
                        bounds: proto::TreeSpan::new(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
                        heads: vec![].into()
                    })],
                }
            )
            .await?,
        NodeResponseBody::StateStreamUnavailable { .. }
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
async fn retry_replays_frame_zero_and_feedback_without_advancing_twice() -> Result<()> {
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
    let body = proto::NodeRequestBody::Fetch {
        selection: ankql::ast::Predicate::MemberOf(model).into(),
        known_state: proto::KnownState {
            spec: proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), ankql::ast::Predicate::MemberOf(model)),
            spans: vec![],
        },
    };
    let origin = proto::RequestId::new();
    for _ in 0..2 {
        server
            .handle_message(NodeMessage::Request {
                auth: vec![proto::AuthData(vec![])],
                request: proto::NodeRequest {
                    id: origin.clone(),
                    from: client.id,
                    to: server.id,
                    body: bincode::deserialize(&bincode::serialize(&body)?)?,
                },
            })
            .await?;
    }
    let replies = || {
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
    wait_for(|| replies().len() == 2).await;
    let initial = replies().remove(0);
    assert_eq!(bincode::serialize(&initial)?, bincode::serialize(&replies()[1])?);
    let comparison = initial.frame.stream;
    let feedback = proto::NodeRequestBody::ContinueState {
        stream: comparison,
        round: None,
        processed_through: 0,
        applied_through: None,
        answers: vec![],
    };
    let request = proto::RequestId::new();
    for _ in 0..2 {
        server
            .handle_message(NodeMessage::Request {
                auth: vec![proto::AuthData(vec![])],
                request: proto::NodeRequest {
                    id: request.clone(),
                    from: client.id,
                    to: server.id,
                    body: bincode::deserialize(&bincode::serialize(&feedback)?)?,
                },
            })
            .await?;
    }
    assert!(trace.1.lock().unwrap().iter().all(|frame| frame.stream != comparison), "receipt retries cannot create extra state output");
    server
        .handle_message(NodeMessage::Request {
            auth: vec![proto::AuthData(vec![])],
            request: proto::NodeRequest { id: origin, from: client.id, to: server.id, body },
        })
        .await?;
    wait_for(|| {
        trace
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|body| matches!(body, NodeResponseBody::StateStreamUnavailable { stream: id } if *id == comparison))
    })
    .await;
    server.handle_message(proto::NodeMessage::AbandonStateStream { from: client.id, to: server.id, stream: comparison }).await?;
    Ok(())
}

#[tokio::test]
async fn fetch_restarts_when_unfinished_chunk_transfer_expires() -> Result<()> {
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let pet = trx.create(&Pet { name: "x".repeat(5 * 1024 * 1024), age: "1".into() }).await?.read()?;
    trx.commit().await?;
    let held = Arc::new(Mutex::new(None));
    let (_connection, gate) = GatedConnection::new(&server, &client, {
        let held = held.clone();
        move |message| {
            if let NodeMessage::Response(response) = message {
                if let NodeResponseBody::Fetch(state) = &response.body {
                    let mut comparison = held.lock().unwrap();
                    if comparison.is_none() {
                        *comparison = Some(state.frame.stream);
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
    ankurah::core::test_helpers::expire_state_stream(&server, client.id, comparison).await?;
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
async fn large_payload_does_not_overflow_metadata_maintenance_and_streams_in_chunks() -> Result<()> {
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
    assert!(
        trace.items(Some(query.query_id())).iter().all(|item| matches!(item, StateItem::Heads(_))),
        "cached initialization supplies membership without repair"
    );
    let initial = trace
        .0
        .lock()
        .unwrap()
        .iter()
        .find_map(|body| match body {
            NodeResponseBody::QuerySubscribed { query_id, state } if *query_id == query.query_id() => Some(state.frame.stream),
            _ => None,
        })
        .unwrap();
    let payload = "x".repeat(9 * 1024 * 1024);
    let trx = writer.begin();
    record.edit(&trx)?.title()?.set(&payload)?;
    trx.commit().await?;
    assert!(
        bincode::serialized_size(&record.entity().to_entity_state()?)? > 8 * 1024 * 1024,
        "the entity payload exceeds the maintenance queue budget, but the queue retains only metadata"
    );
    assert_eq!(retained.title()?, "old", "the held stream metadata prevents receiver application");
    gate.release_held(&client).await;
    // Payload transport has its own chunk and assembly bounds. Its size must
    // not invalidate the resident tree's compact maintenance subscription.
    tokio::time::timeout(Duration::from_secs(120), async {
        query.wait_durable_answered().await?;
        while !query.peek().first().is_some_and(|row| row.title().as_deref() == Ok(payload.as_str())) {
            tokio::task::yield_now().await;
        }
        query.refresh().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    assert_eq!(query.peek()[0].entity().head(), record.entity().head(), "chunked delivery must incorporate the final target");
    assert_eq!(query.ids(), [record.id()]);
    assert_eq!(query.peek()[0].artist()?, "artist");
    assert!(
        !trace.1.lock().unwrap().iter().any(|frame| frame.stream == initial
            && matches!(frame.body, proto::StateFrameBody::RestartRequired | proto::StateFrameBody::SourceUnavailable)),
        "payload size must not overflow the metadata queue or restart the stream"
    );
    Ok(())
}

#[tokio::test]
async fn expiring_unfinished_generation_signals_restart_while_established_queries_keep_flowing() -> Result<()> {
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
    let established = reader.query_wait::<PetView>("true").await?;
    established.wait_durable_answered().await?;
    let model = writer.resolve_model_id::<Pet>().await?;
    let response = client
        .request(
            server.id,
            &DEFAULT_CONTEXT,
            proto::NodeRequestBody::SubscribeQuery {
                query_id: proto::QueryId::new(),
                version: 1,
                selection: ankql::ast::Predicate::MemberOf(model).into(),
                known_state: proto::KnownState {
                    spec: proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), ankql::ast::Predicate::MemberOf(model)),
                    spans: vec![],
                },
            },
        )
        .await?;
    let NodeResponseBody::QuerySubscribed { state, .. } = response else { panic!("{response:?}") };
    let comparison = state.frame.stream;
    let trx = writer.begin();
    pet.edit(&trx)?.age()?.replace("2")?;
    trx.commit().await?;
    wait_for(|| established.peek()[0].age().unwrap() == "2").await;
    ankurah::core::test_helpers::expire_state_stream(&server, client.id, comparison).await?;
    wait_for(|| {
        trace
            .1
            .lock()
            .unwrap()
            .iter()
            .any(|frame| frame.stream == comparison && matches!(frame.body, proto::StateFrameBody::RestartRequired))
    })
    .await;
    assert!(matches!(
        client
            .request(
                server.id,
                &DEFAULT_CONTEXT,
                proto::NodeRequestBody::FinishStateRound {
                    stream: comparison,
                    round: 0,
                    outcome: proto::StateRoundOutcome::Applied { processed_through: 0 }
                }
            )
            .await?,
        NodeResponseBody::StateStreamUnavailable { .. }
    ));
    server.handle_message(proto::NodeMessage::AbandonStateStream { from: client.id, to: server.id, stream: comparison }).await?;
    let trx = writer.begin();
    pet.edit(&trx)?.age()?.replace("3")?;
    trx.commit().await?;
    wait_for(|| established.peek()[0].age().unwrap() == "3").await;
    Ok(())
}

#[tokio::test]
async fn pushed_chunks_can_outrun_metadata_and_arrive_out_of_order_without_early_activation() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let payload = "x".repeat(180_000);
    let trx = writer.begin();
    let record = trx.create(&Record { title: payload.clone(), artist: "artist".into() }).await?.read()?;
    trx.commit().await?;
    let target = Arc::new(Mutex::new(None));
    let comparison = Arc::new(Mutex::new(None));
    let parked = Arc::new(AtomicBool::new(false));
    let trace = Trace::default();
    let (_connection, gate) = GatedConnection::new(&server, &client, {
        let target = target.clone();
        let comparison = comparison.clone();
        let parked = parked.clone();
        let trace = trace.clone();
        move |message| {
            trace.record(message);
            match message {
                NodeMessage::Response(proto::NodeResponse { body: NodeResponseBody::QuerySubscribed { query_id, state }, .. })
                    if Some(*query_id) == *target.lock().unwrap() =>
                {
                    *comparison.lock().unwrap() = Some(state.frame.stream);
                    parked.store(true, Ordering::Release);
                    true
                }
                NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame { frame, .. }, .. }) => {
                    Some(frame.stream) == *comparison.lock().unwrap()
                }
                _ => false,
            }
        }
    })
    .await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    reader.resolve_model_id::<Record>().await?;
    let query = reader.query::<RecordView>(nocache("true")?)?;
    *target.lock().unwrap() = Some(query.query_id());
    wait_for(|| {
        parked.load(Ordering::Acquire)
            && trace.items(Some(query.query_id())).iter().filter(|item| matches!(item, StateItem::RepairChunk { .. })).count() >= 5
    })
    .await;
    gate.release_last(&client, 1).await;
    assert!(!query.loaded(), "a final chunk cannot activate before its metadata and earlier frames");
    gate.release_first(&client, 1).await;
    assert!(!query.loaded(), "metadata cannot activate an incomplete chunk assembly");
    gate.release_held(&client).await;
    query.wait_durable_answered().await?;
    assert_eq!(query.ids(), [record.id()]);
    assert_eq!(query.peek()[0].title()?, payload);
    assert_eq!(query.peek()[0].entity().head(), record.entity().head());
    Ok(())
}

#[tokio::test]
async fn byte_smaller_snapshot_advances_a_retained_older_entity_truthfully() -> Result<()> {
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let record = trx.create(&Record { title: "old".into(), artist: "retained".into() }).await?.read()?;
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
    let retained = reader.fetch::<RecordView>("true").await?.remove(0);
    let mut value = String::new();
    for edit in 0..8 {
        value = format!("{}-{edit}", "x".repeat(8192));
        let trx = writer.begin();
        record.edit(&trx)?.title()?.set(&value)?;
        trx.commit().await?;
    }
    trace.clear();
    let fetched = reader.fetch::<RecordView>("true").await?;
    assert_eq!(fetched[0].title()?, value);
    assert_eq!(retained.title()?, value);
    assert_eq!(retained.artist()?, "retained");
    assert_eq!(retained.entity().head(), record.entity().head());
    assert!(trace.items(None).iter().any(|item| matches!(item, StateItem::Repair { delta, .. }
        if delta.entity_id == record.id() && matches!(delta.content, proto::DeltaContent::StateSnapshot { .. }))));
    Ok(())
}

#[tokio::test]
async fn head_inventory_preserves_captured_keys_while_repair_joins_a_concurrent_key_change() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let record = trx.create(&Record { title: "a-000".into(), artist: "outside".into() }).await?.read()?;
    let mut expected = HashSet::from([record.id()]);
    for i in 1..200 {
        expected.insert(trx.create(&Record { title: format!("a-{i:03}"), artist: "remote".into() }).await?.id());
    }
    trx.commit().await?;
    let (connection, _) = GatedConnection::new(&server, &client, |_| false).await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let retained = reader.get::<RecordView>(record.id()).await?;
    let local = reader.begin();
    retained.edit(&local)?.title()?.set(&"zz-local".to_owned())?;
    // Commit offline so a legitimate merged Live echo cannot substitute for
    // the connecting-history repair this fixture specifically exercises.
    drop(connection);
    server.deregister_peer(client.id);
    client.deregister_peer(server.id);
    let remote = writer.begin();
    record.edit(&remote)?.artist()?.set(&"remote".to_owned())?;
    remote.commit().await?;
    let captured = record.entity().head();
    local.commit().await?;
    let local_head = retained.entity().head();
    let target = Arc::new(Mutex::new(None));
    let parked = Arc::new(AtomicBool::new(false));
    let repair_parked = Arc::new(AtomicBool::new(false));
    let trace = Trace::default();
    let (_connection, gate) = GatedConnection::new(&server, &client, {
        let target = target.clone();
        let parked = parked.clone();
        let trace = trace.clone();
        let repair_parked = repair_parked.clone();
        let record_id = record.id();
        move |message| {
            trace.record(message);
            if let NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame { frame, .. }, .. }) = message {
                if matches!(&frame.body, proto::StateFrameBody::State { items, .. } if bridge_for(items, record_id)) {
                    repair_parked.store(true, Ordering::Release);
                    return true;
                }
            }
            matches!(message, NodeMessage::Response(response)
                if matches!(&response.body, NodeResponseBody::QuerySubscribed { query_id, .. }
                    if Some(*query_id) == *target.lock().unwrap() && !parked.swap(true, Ordering::AcqRel)))
        }
    })
    .await;
    let query = reader.query::<RecordView>(nocache("artist = 'remote' ORDER BY title ASC LIMIT 200")?)?;
    *target.lock().unwrap() = Some(query.query_id());
    wait_for(|| parked.load(Ordering::Acquire)).await;
    let offered = trace.items(Some(query.query_id()));
    let page = offered
        .iter()
        .find_map(|item| match item {
            StateItem::Heads(page) if page.heads.to_vec().iter().any(|head| head.entity.entity_id == record.id()) => Some(page),
            _ => None,
        })
        .expect("the outside-query entity must have explicit captured membership");
    assert!(matches!(page.bounds.end, std::ops::Bound::Included(_)), "the first page must have a finite key boundary");
    assert!(page.heads.to_vec().iter().any(|head| head.entity.entity_id == record.id() && head.entity.head == captured));
    assert_eq!(retained.title()?, "zz-local");
    assert_eq!(retained.artist()?, "outside", "the held sender branch has not reached the receiver");
    assert!(
        captured.entries().iter().all(|event| !local_head.entries().contains(event)),
        "the local branch must be concurrent with the offered target"
    );
    assert!(!query.loaded());
    gate.release_held(&client).await;
    wait_for(|| repair_parked.load(Ordering::Acquire)).await;
    assert!(
        snapshot_offer(&trace.items(Some(query.query_id())), record.id(), &captured),
        "current payload is sent separately from captured membership, possibly in a later frame"
    );
    assert!(!query.loaded(), "the concurrent branch cannot become ready while its connecting history is withheld");
    assert_eq!(retained.artist()?, "outside", "the snapshot alone did not incorporate the remote branch");
    gate.release_held(&client).await;
    query.wait_durable_answered().await?;
    assert_eq!(query.ids().into_iter().collect::<HashSet<_>>(), expected, "captured page keys must preserve every result identity");
    assert_eq!(retained.title()?, "zz-local", "snapshot application must preserve the local key edit");
    assert_eq!(retained.artist()?, "remote", "connecting history must incorporate the sender branch");
    let merged = retained.entity().head();
    assert!(captured.entries().iter().all(|event| merged.entries().contains(event)));
    assert!(local_head.entries().iter().all(|event| merged.entries().contains(event)));
    assert_eq!(
        client.storage.get_state(record.id()).await?.payload.state.head,
        merged,
        "covered branches must be persisted before readiness"
    );
    assert!(
        bridge_for(&trace.items(Some(query.query_id())), record.id()),
        "the uncovered snapshot target must request and apply connecting events"
    );
    Ok(())
}

#[tokio::test]
async fn initial_inventory_snapshot_first_joins_preserve_globally_cached_branches_before_ready() -> Result<()> {
    snapshot_first_joins_preserve_cached_branches(256, true).await
}

#[tokio::test]
async fn retained_frontier_snapshot_first_joins_preserve_globally_cached_branches_before_ready() -> Result<()> {
    snapshot_first_joins_preserve_cached_branches(2049, false).await
}

fn snapshot_offer(items: &[StateItem], id: EntityId, head: &proto::Clock) -> bool {
    items.iter().any(|item| match item {
        StateItem::Repair { target, delta } => {
            delta.entity_id == id && target == head && matches!(delta.content, proto::DeltaContent::StateSnapshot { .. })
        }
        _ => false,
    })
}

async fn snapshot_first_joins_preserve_cached_branches(count: usize, initial_inventory: bool) -> Result<()> {
    use futures_util::FutureExt;
    use std::sync::atomic::{AtomicBool, Ordering};
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    // Moderate results offer complete initial Leaves; above 2048 rows uses
    // a bounded retained Digest frontier. Padded keys preserve lexical order
    // in both cases; unchanged payload favors the former match's event bridge.
    let unchanged = "unchanged-".to_owned() + &"p".repeat(1024);
    let mut rows = Vec::new();
    for i in 0..count {
        rows.push(trx.create(&Record { title: format!("a-{i:06}"), artist: unchanged.clone() }).await?.read()?);
    }
    let older = trx.create(&Record { title: "z-older".into(), artist: unchanged.clone() }).await?.read()?;
    let concurrent = trx.create(&Record { title: "z-concurrent".into(), artist: unchanged.clone() }).await?.read()?;
    let newer = trx.create(&Record { title: "z-newer".into(), artist: unchanged.clone() }).await?.read()?;
    trx.commit().await?;
    let former_index = count / 6;
    let former = rows[former_index].clone();
    let older_title = format!("a-{:06}5", count / 4);
    let concurrent_title = format!("a-{:06}5", count / 2);
    let newer_title = format!("a-{:06}5", count * 3 / 4);
    let selection = format!("true ORDER BY title ASC LIMIT {count}");
    let (connection, _) = GatedConnection::new(&server, &client, |_| false).await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let cached_older = reader.get::<RecordView>(older.id()).await?;
    let cached_concurrent = reader.get::<RecordView>(concurrent.id()).await?;
    let cached_newer = reader.get::<RecordView>(newer.id()).await?;
    let query = reader.query::<RecordView>(nocache(selection.as_str())?)?;
    query.wait_durable_answered().await?;
    assert_eq!(query.ids().into_iter().collect::<HashSet<_>>(), rows.iter().map(|row| row.id()).collect());
    let cached_former = reader.get::<RecordView>(former.id()).await?;

    // Receive this sender head before going offline, then make the receiver
    // strictly newer while moving its key outside the local ordered result.
    let trx = writer.begin();
    newer.edit(&trx)?.title()?.set(&newer_title)?;
    trx.commit().await?;
    let newer_target = newer.entity().head();
    // Materialize a fresh answer here instead of making setup depend on
    // ordinary live ORDER BY/LIMIT insertion behavior.
    query.update_selection_wait(selection.as_str()).await?;
    query.wait_durable_answered().await?;
    assert_eq!(cached_newer.entity().head(), newer_target);
    assert!(query.ids().contains(&newer.id()));
    drop(connection);
    server.deregister_peer(client.id);
    client.deregister_peer(server.id);
    let trx = reader.begin();
    cached_newer.edit(&trx)?.title()?.set(&"zz-local-newer".to_owned())?;
    cached_newer.edit(&trx)?.artist()?.set(&"local-newer".to_owned())?;
    cached_concurrent.edit(&trx)?.artist()?.set(&"local-concurrent".to_owned())?;
    trx.commit().await?;
    let newer_local = cached_newer.entity().head();
    let concurrent_local = cached_concurrent.entity().head();
    let older_before = cached_older.entity().head();
    let trx = writer.begin();
    older.edit(&trx)?.title()?.set(&older_title)?;
    concurrent.edit(&trx)?.title()?.set(&concurrent_title)?;
    former.edit(&trx)?.title()?.set(&"zz-former".to_owned())?;
    trx.commit().await?;
    let older_target = older.entity().head();
    let concurrent_target = concurrent.entity().head();
    assert_ne!(older_before, older_target);
    assert!(concurrent_target.entries().iter().all(|entry| !concurrent_local.entries().contains(entry)));
    // The three inserted keys evict the two highest old keys after the
    // explicit former match leaves. Do not use another query as the oracle.
    let expected: HashSet<_> = rows
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != former_index && *index < count - 2)
        .map(|(_, row)| row.id())
        .chain([older.id(), concurrent.id(), newer.id()])
        .collect();
    assert_eq!(expected.len(), count);
    assert_eq!(writer.fetch::<RecordView>(selection.as_str()).await?.into_iter().map(|row| row.id()).collect::<HashSet<_>>(), expected);
    assert!([older.id(), concurrent.id(), newer.id()].iter().all(|id| expected.contains(id)));
    assert!(!expected.contains(&former.id()));

    // A new version makes readiness assertions independent of the previous
    // answer's flags while retaining the query's comparison tree.
    query.update_selection(selection.as_str())?;
    let history_held = Arc::new(AtomicBool::new(false));
    let trace = Trace::default();
    let (_connection, gate) = GatedConnection::new(&server, &client, {
        let held = history_held.clone();
        let trace = trace.clone();
        let concurrent_id = concurrent.id();
        move |message| {
            trace.record(message);
            let frame = match message {
                NodeMessage::Response(response) => match &response.body {
                    NodeResponseBody::QuerySubscribed { state, .. } => Some(&state.frame),
                    _ => None,
                },
                NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame { frame, .. }, .. }) => Some(frame),
                _ => None,
            };
            let repair = frame.is_some_and(|frame| match &frame.body {
                proto::StateFrameBody::State { items, .. } => bridge_for(items, concurrent_id),
                _ => false,
            });
            if repair {
                held.store(true, Ordering::Release);
            }
            repair
        }
    })
    .await;
    wait_for(|| {
        let items = trace.items(Some(query.query_id()));
        history_held.load(Ordering::Acquire)
            && [(older.id(), &older_target), (concurrent.id(), &concurrent_target), (newer.id(), &newer_target)]
                .iter()
                .all(|(id, head)| snapshot_offer(&items, *id, head))
    })
    .await;
    let offered = trace.items(Some(query.query_id()));
    for (id, target) in [(older.id(), &older_target), (concurrent.id(), &concurrent_target), (newer.id(), &newer_target)] {
        assert!(snapshot_offer(&offered, id, target), "an absent query member must offer its exact captured snapshot target first: {id}");
    }
    // The gate observes inbound responses, not the outgoing KnownState. A
    // frontier necessarily asks for refinement/leaves here; the complete
    // initial inventory handles these sparse changes without that feedback.
    let asks_for_inventory = offered.iter().any(|item| matches!(item, StateItem::Digest(_) | StateItem::NeedHeads(_)));
    assert_eq!(asks_for_inventory, !initial_inventory, "fixture must exercise the intended initial knowledge path");
    assert!(!query.loaded(), "Complete cannot activate membership while a snapshot target remains uncovered");
    assert!(query.wait_durable_answered().now_or_never().is_none(), "uncovered causal repair must block readiness");
    assert_eq!(cached_concurrent.artist()?, "local-concurrent");
    assert_ne!(cached_concurrent.entity().head(), concurrent_target);
    gate.release_held(&client).await;
    tokio::time::timeout(Duration::from_secs(10), query.wait_durable_answered()).await??;
    assert_eq!(query.ids().into_iter().collect::<HashSet<_>>(), expected, "membership comes from captured sender keys");
    assert_eq!(cached_older.title()?, older_title);
    assert_eq!(cached_older.entity().head(), older_target);
    assert_eq!(cached_concurrent.title()?, concurrent_title);
    assert_eq!(cached_concurrent.artist()?, "local-concurrent", "connecting history must retain the independent local edit");
    let merged = cached_concurrent.entity().head();
    assert!(concurrent_target.entries().iter().all(|entry| merged.entries().contains(entry)));
    assert!(concurrent_local.entries().iter().all(|entry| merged.entries().contains(entry)));
    assert_eq!(cached_newer.title()?, "zz-local-newer", "an older sender snapshot must preserve the newer local key");
    assert_eq!(cached_newer.artist()?, "local-newer");
    assert_eq!(cached_newer.entity().head(), newer_local);
    assert_eq!(cached_former.title()?, "zz-former", "a former match must still receive its current readable state");
    assert_eq!(cached_former.entity().head(), former.entity().head());
    let items = trace.items(Some(query.query_id()));
    assert!(bridge_for(&items, concurrent.id()), "uncovered snapshot must trigger connecting history");
    assert!(bridge_for(&items, former.id()), "requester-only former matches retain their supplied-clock repair");
    for (id, head) in [(older.id(), older_target), (concurrent.id(), merged), (newer.id(), newer_local)] {
        assert_eq!(client.storage.get_state(id).await?.payload.state.head, head, "covered heads are persisted before readiness");
    }
    Ok(())
}

#[tokio::test]
async fn cold_query_with_2049_interleaved_cached_rows_uses_a_summary_frontier() -> Result<()> {
    use std::collections::{BTreeMap, BTreeSet};

    // 4096 half-cache is exactly the 2048-leaf inventory cap. This extra
    // pair makes the genuinely partial requester offer a Digest frontier.
    const COUNT: usize = 4098;
    let (server, client) = nodes().await?;
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let mut rows = Vec::new();
    for i in 0..COUNT {
        rows.push(trx.create(&Record { title: format!("row-{i:06}"), artist: format!("artist-{i:06}") }).await?.read()?);
    }
    trx.commit().await?;
    let expected = rows.iter().map(|row| Ok((row.id(), row.entity().to_state()?))).collect::<Result<BTreeMap<_, _>>>()?;
    assert_eq!(expected.len(), COUNT);

    // Gate the server side only to observe actual outgoing requests. No
    // response is held or synthesized, and catalog offers are excluded by id.
    let offers = Arc::new(Mutex::new(Vec::<(proto::QueryId, proto::KnownState)>::new()));
    let (_connection, _) = GatedConnection::new(&client, &server, {
        let offers = offers.clone();
        move |message| {
            if let NodeMessage::Request { request, .. } = message {
                if let proto::NodeRequestBody::SubscribeQuery { query_id, known_state: known, .. } = &request.body {
                    offers.lock().unwrap().push((*query_id, known.clone()));
                }
            }
            false
        }
    })
    .await;
    client.wait_ready().await?;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let wanted_cache = (0..COUNT).step_by(2).map(|i| rows[i].id()).collect::<BTreeSet<_>>();
    let predicates = (0..COUNT).step_by(2).map(|i| format!("title = 'row-{i:06}'")).collect::<Vec<_>>();
    let mut prefetched = BTreeSet::new();
    for batch in predicates.chunks(32) {
        prefetched.extend(reader.fetch::<RecordView>(batch.join(" OR ")).await?.into_iter().map(|row| row.id()));
    }
    assert_eq!(prefetched, wanted_cache, "public Fetch batches must cache exactly the alternating rows");
    assert_eq!(prefetched.len(), 2049);
    let model = reader.resolve_model_id::<Record>().await?;
    let cached = client.storage.fetch_states(&ankql::ast::Predicate::MemberOf(model).into()).await?;
    assert_eq!(
        cached.iter().map(|state| state.payload.entity_id).collect::<BTreeSet<_>>(),
        wanted_cache,
        "the missing rows must still be absent before the first full query"
    );

    // There is no earlier full query/refresh to hydrate the omitted rows.
    let query = reader.query::<RecordView>(nocache("title >= 'row-' ORDER BY title ASC")?)?;
    tokio::time::timeout(Duration::from_secs(30), query.wait_durable_answered()).await??;
    // Capture synchronously immediately after the versioned ready wait. Each
    // full state/head is atomic; no async verifier or drain can rescue it.
    // The fixture has no writes after server setup, so comparison is stable.
    let installed =
        query.resultset().read().iter_entities().map(|(id, entity)| Ok((id, entity.to_state()?))).collect::<Result<BTreeMap<_, _>>>()?;
    assert_eq!(installed, expected, "ready must expose every exact entity, full state and head");
    let own_offers =
        offers.lock().unwrap().iter().filter(|(id, _)| *id == query.query_id()).map(|(_, known)| known.clone()).collect::<Vec<_>>();
    assert_eq!(own_offers.len(), 1, "the cold query must have one actual initial offer, excluding catalog requests");
    let known = &own_offers[0];
    assert!(known.spans.iter().all(|region| matches!(region, proto::SpanContent::Digest(_))), "the partial cache must offer Digest-only");
    let offered_count = known
        .spans
        .iter()
        .map(|region| match region {
            proto::SpanContent::Digest(head) => head.digest.count(),
            _ => unreachable!(),
        })
        .sum::<i64>();
    assert_eq!(offered_count, 2049, "the frontier must describe only the genuinely cached alternating membership");
    assert_eq!(known.spec.key_spec().keyparts.len(), 1, "the real ordered title selection must retain its planner key");
    assert_eq!(
        known.spec.key_spec().keyparts[0].key,
        resolved_prop(&client, Record::descriptor(), "title"),
        "the frontier must use the actual resolved title property, not an arbitrary one-part key"
    );

    // Persistence is deliberately checked only after the immutable ready
    // capture passed; subsequent reads cannot conceal incomplete activation.
    for (id, state) in &installed {
        assert_eq!(client.storage.get_state(*id).await?.payload.state, *state);
    }
    Ok(())
}
