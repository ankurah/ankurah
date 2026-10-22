use super::*;
use crate::{
    error::RequestError,
    livequery::{EntityLiveQuery, QueryResolution},
    node::{source_publications::MAX_PENDING_BATCHES, CachePolicy},
    peer_subscription::Status,
};

type Messages = Arc<std::sync::Mutex<Vec<proto::NodeMessage>>>;

#[derive(Clone)]
struct WireSender {
    target: WeakNode<OrderedStorage, PermissiveAgent>,
    peer: proto::EntityId,
    messages: Messages,
}

impl PeerSender for WireSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        let encoded = bincode::serialize(&message).unwrap();
        self.messages.lock().unwrap().push(bincode::deserialize(&encoded).unwrap());
        let message = bincode::deserialize(&encoded).unwrap();
        let target = self.target.clone();
        crate::task::spawn(async move {
            if let Some(target) = target.upgrade() {
                target.handle_message(message).await.unwrap();
            }
        });
        Ok(())
    }
    fn recipient_node_id(&self) -> proto::EntityId { self.peer }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

async fn pair() -> anyhow::Result<(TestNode, TestNode, Messages)> {
    let server = node().await?;
    let client = Node::new(Arc::new(OrderedStorage::default()), PermissiveAgent::new());
    let messages = Messages::default();
    server
        .register_peer(
            client.presence().await?,
            Box::new(WireSender { target: client.weak(), peer: client.id, messages: messages.clone() }),
        )
        .await?;
    client
        .register_peer(
            server.presence().await?,
            Box::new(WireSender { target: server.weak(), peer: server.id, messages: messages.clone() }),
        )
        .await?;
    client.wait_ready().await?;
    Ok((server, client, messages))
}

async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition did not settle");
}

fn responses(messages: &Messages, predicate: impl Fn(&proto::NodeResponseBody) -> bool) -> usize {
    messages
        .lock()
        .unwrap()
        .iter()
        .filter(|message| matches!(message, proto::NodeMessage::Response(response) if predicate(&response.body)))
        .count()
}

fn query(client: &TestNode, selection: Selection<Resolved>) -> anyhow::Result<EntityLiveQuery> {
    let context = crate::context::Context::new(client.clone(), DEFAULT_CONTEXT);
    Ok(EntityLiveQuery::new(context.0, CachePolicy::Durable, QueryResolution::Resolved(selection))?)
}

async fn add_catalog_model(node: &TestNode, label: &str) -> anyhow::Result<()> {
    let context = node.privileged_context();
    let transaction = context.begin();
    transaction.create(&crate::schema::catalog::SysModelRow { label: label.into(), name: "Capture retry fixture".into() }).await?;
    transaction.commit().await?;
    Ok(())
}

#[tokio::test]
async fn catalog_change_during_fetch_capture_reopens_the_source() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let scans = node.storage.2.load(std::sync::atomic::Ordering::Relaxed);
    let (entered, release) = hold_fetch(&node);
    let capture = node.resident_trees.capture(&node, selection(None), None, vec![DEFAULT_CONTEXT]);
    tokio::pin!(capture);
    assert!(futures::poll!(&mut capture).is_pending());
    entered.await?;
    add_catalog_model(&node, "fetch_capture_fixture").await?;
    release.send(()).unwrap();
    let captured = capture.await?;
    assert_eq!(captured.snapshot.states.len(), 1);
    assert_eq!(captured.snapshot.states[&initial.entity_id].payload.state.head, proto::Clock::singleton(&initial));
    assert_eq!(node.storage.2.load(std::sync::atomic::Ordering::Relaxed) - scans, 2, "the raced admission must be read again");
    assert_eq!(node.resident_trees.len(), 1, "only the replacement admission may be reusable");
    Ok(())
}

#[tokio::test]
async fn catalog_change_during_subscription_capture_reopens_the_source() -> anyhow::Result<()> {
    let (server, client, messages) = pair().await?;
    let initial = genesis("01")?;
    apply(&server, &initial).await?;
    let (entered, release) = hold_fetch(&server);
    let query = query(&client, selection(None))?;
    tokio::time::timeout(std::time::Duration::from_secs(3), entered).await??;
    add_catalog_model(&server, "subscribe_capture_fixture").await?;
    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), query.wait_durable_answered()).await??;
    let rows = query.resultset().read().iter_entities().map(|(_, entity)| entity.clone()).collect::<Vec<_>>();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id(), initial.entity_id);
    assert_eq!(rows[0].head(), proto::Clock::singleton(&initial));
    assert_eq!(responses(&messages, |body| matches!(body, proto::NodeResponseBody::StateCaptureBusy)), 0);
    drop(query);
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}

#[tokio::test]
async fn catalog_churn_bounds_capture_retries_and_later_capture_recovers() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let scans = node.storage.2.load(std::sync::atomic::Ordering::Relaxed);
    let (mut entered, mut release) = hold_fetch(&node);
    let capture = node.resident_trees.capture(&node, selection(None), None, vec![DEFAULT_CONTEXT]);
    tokio::pin!(capture);
    for attempt in 0..2 {
        assert!(futures::poll!(&mut capture).is_pending());
        entered.await?;
        add_catalog_model(&node, &format!("catalog_churn_{attempt}")).await?;
        if attempt == 1 {
            release.send(()).unwrap();
            break;
        }
        let next = hold_fetch(&node);
        release.send(()).unwrap();
        (entered, release) = next;
    }
    let error = capture.await.err().expect("continued catalog churn must exhaust the bounded retry");
    assert!(matches!(error.downcast_ref::<SourceError>(), Some(SourceError::CatalogChanged)));
    assert!(crate::node::state_sync::ResidentTrees::<&'static crate::policy::DefaultContext>::can_retry(&error));
    assert_eq!(node.storage.2.load(std::sync::atomic::Ordering::Relaxed) - scans, 2);
    assert_eq!(node.resident_trees.len(), 0);
    let current = node.resident_trees.capture(&node, selection(None), None, vec![DEFAULT_CONTEXT]).await?;
    assert_eq!(current.snapshot.states.len(), 1);
    assert!(current.snapshot.states.contains_key(&initial.entity_id));
    Ok(())
}

#[tokio::test]
async fn catalog_change_invalidates_an_existing_source_with_a_retryable_error() -> anyhow::Result<()> {
    let node = node().await?;
    let selected = selection(None);
    let captured = capture_query(
        &node,
        selected.clone(),
        QueryTreeCache::spec(&selected, &node.catalog),
        vec![DEFAULT_CONTEXT],
        Arc::new(QueryTreeCache::default()),
    )
    .await?;
    let mut source = QuerySource::from_capture(vec![DEFAULT_CONTEXT], captured);
    add_catalog_model(&node, "existing_capture_fixture").await?;
    let error = source.refresh(&node).await.err().expect("an existing source cannot cross a catalog change");
    assert!(matches!(error.downcast_ref::<SourceError>(), Some(SourceError::CatalogChanged)));
    assert!(crate::node::state_sync::ResidentTrees::<&'static crate::policy::DefaultContext>::can_retry(&error));
    Ok(())
}

async fn assert_recovers(client: &TestNode, query: &EntityLiveQuery, messages: &Messages, expected: &proto::Event) -> anyhow::Result<()> {
    let relay = client.subscription_relay.as_ref().unwrap();
    until(|| matches!(relay.get_status(query.query_id()), Some(Status::PendingRemote))).await;
    assert_eq!(responses(messages, |body| matches!(body, proto::NodeResponseBody::StateCaptureBusy)), 1);
    assert!(query.error().value().is_none(), "a busy capture cannot permanently fail the query");
    assert!(!query.resultset().is_loaded());
    // Exercise the existing five-second retry task, not a manual new registration.
    tokio::time::timeout(std::time::Duration::from_secs(7), query.wait_durable_answered()).await??;
    let rows = query.resultset().read().iter_entities().map(|(_, entity)| entity.clone()).collect::<Vec<_>>();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id(), expected.entity_id);
    assert_eq!(rows[0].head(), proto::Clock::singleton(expected));
    assert!(matches!(relay.get_status(query.query_id()), Some(Status::Established(_, 1))));
    Ok(())
}

#[tokio::test]
async fn limited_query_retries_a_raced_bounded_capture() -> anyhow::Result<()> {
    let (server, client, messages) = pair().await?;
    let mut latest = genesis("01")?;
    apply(&server, &latest).await?;
    let (entered, release) = hold_fetch(&server);
    let query = query(&client, selection(Some(1)))?;
    tokio::time::timeout(std::time::Duration::from_secs(3), entered).await??;
    for attempt in 0..8 {
        latest = update(&latest, &format!("01-{attempt}"))?;
        apply(&server, &latest).await?;
    }
    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), query.wait_durable_answered()).await??;
    let rows = query.resultset().read().iter_entities().map(|(_, entity)| entity.clone()).collect::<Vec<_>>();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].head(), proto::Clock::singleton(&latest));
    assert_eq!(responses(&messages, |body| matches!(body, proto::NodeResponseBody::StateCaptureBusy)), 0);
    drop(query);
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}

#[tokio::test]
async fn lagged_initial_capture_retries_with_a_fresh_source_window() -> anyhow::Result<()> {
    let (server, client, messages) = pair().await?;
    let mut latest = genesis("01")?;
    apply(&server, &latest).await?;
    let (entered, release) = hold_fetch(&server);
    let query = query(&client, selection(None))?;
    tokio::time::timeout(std::time::Duration::from_secs(3), entered).await??;
    for n in 0..=MAX_PENDING_BATCHES {
        latest = update(&latest, &format!("01-{n}"))?;
        apply(&server, &latest).await?;
    }
    release.send(()).unwrap();
    assert_recovers(&client, &query, &messages, &latest).await?;
    drop(query);
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}

#[tokio::test]
async fn fetch_bounds_busy_retries_and_can_be_retried_after_quiescence() -> anyhow::Result<()> {
    let (server, client, messages) = pair().await?;
    let mut latest = genesis("01")?;
    apply(&server, &latest).await?;
    let (mut entered, mut release) = hold_fetch(&server);
    let fetcher = client.clone();
    let fetch = tokio::spawn(async move { fetcher.fetch_from_peer(selection(Some(1)), &vec![DEFAULT_CONTEXT]).await });
    for attempt in 0..2 {
        tokio::time::timeout(std::time::Duration::from_secs(3), entered).await??;
        for change in 0..=MAX_PENDING_BATCHES {
            latest = update(&latest, &format!("01-{attempt}-{change}"))?;
            apply(&server, &latest).await?;
        }
        if attempt + 1 == 2 {
            release.send(()).unwrap();
            break;
        }
        let next = hold_fetch(&server);
        release.send(()).unwrap();
        (entered, release) = next;
    }
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), fetch).await??;
    assert!(matches!(result, Err(RetrievalError::RequestError(RequestError::StateCaptureBusy))));
    assert_eq!(responses(&messages, |body| matches!(body, proto::NodeResponseBody::StateCaptureBusy)), 2);
    let rows = client.fetch_from_peer(selection(Some(1)), &vec![DEFAULT_CONTEXT]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].head(), proto::Clock::singleton(&latest));
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}

#[tokio::test]
async fn unavailable_source_is_terminal_for_initial_query_and_fetch() -> anyhow::Result<()> {
    let (server, client, messages) = pair().await?;
    // An unknown commit outcome cannot be repaired by retrying a capture.
    drop(server.source_publications.begin_commit().await);
    let query = query(&client, selection(None))?;
    let error = tokio::time::timeout(std::time::Duration::from_secs(2), query.wait_durable_answered()).await?.unwrap_err();
    assert!(matches!(error, RetrievalError::RequestError(RequestError::ServerError(_))));
    assert!(matches!(client.subscription_relay.as_ref().unwrap().get_status(query.query_id()), Some(Status::Failed)));
    let error = client.fetch_from_peer(selection(None), &vec![DEFAULT_CONTEXT]).await.unwrap_err();
    assert!(matches!(error, RetrievalError::RequestError(RequestError::ServerError(_))));
    assert_eq!(responses(&messages, |body| matches!(body, proto::NodeResponseBody::StateCaptureBusy)), 0);
    drop(query);
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}

#[tokio::test]
async fn malformed_initial_offer_is_not_a_busy_capture() -> anyhow::Result<()> {
    let (server, client, messages) = pair().await?;
    let known_state =
        proto::KnownState { spec: proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), Predicate::False), spans: vec![] };
    for body in [
        proto::NodeRequestBody::Fetch { selection: selection(None), known_state: known_state.clone() },
        proto::NodeRequestBody::SubscribeQuery { query_id: proto::QueryId::new(), selection: selection(None), version: 1, known_state },
    ] {
        let response = client.request(server.id, &vec![DEFAULT_CONTEXT], body).await?;
        assert!(matches!(response, proto::NodeResponseBody::Error(_)));
    }
    assert_eq!(responses(&messages, |body| matches!(body, proto::NodeResponseBody::StateCaptureBusy)), 0);
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}
