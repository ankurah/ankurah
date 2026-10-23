//! Two real browser nodes, with serialized messages and IndexedDB entity storage.
use super::*;
use ankurah::{
    core::connector::{PeerSender, SendError},
    proto, Mutable, View,
};
use ankurah_signals::Peek;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

type TestNode = Node<IndexedDBStorageEngine, PermissiveAgent>;
type Responses = Arc<Mutex<Vec<StateTraffic>>>;

enum StateTraffic {
    Response(proto::NodeResponseBody),
    Frame(proto::StateFrame),
}

#[derive(Clone)]
struct BrowserSender {
    target: ankurah::core::node::WeakNode<IndexedDBStorageEngine, PermissiveAgent>,
    target_id: proto::EntityId,
    responses: Responses,
    gate: Option<Arc<FeedbackGate>>,
}

#[derive(Default)]
struct FeedbackGate {
    armed: AtomicBool,
    held: Mutex<Vec<proto::NodeMessage>>,
}

impl FeedbackGate {
    fn should_hold(&self, message: &proto::NodeMessage) -> bool {
        self.armed.load(Ordering::Acquire)
            && matches!(message,
            proto::NodeMessage::Request { request: proto::NodeRequest {
                body: proto::NodeRequestBody::StateFeedback { answers: Some(proto::FeedbackAnswers::Round { round: 0, answers }), .. }, ..
            }, .. } if !answers.is_empty())
    }

    async fn release(&self, target: &TestNode) -> anyhow::Result<()> {
        self.armed.store(false, Ordering::Release);
        let held = std::mem::take(&mut *self.held.lock().unwrap());
        for message in held {
            target.handle_message(message).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl PeerSender for BrowserSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        let target = self.target.upgrade().ok_or(SendError::ConnectionClosed)?;
        let bytes = bincode::serialize(&message).map_err(anyhow::Error::from)?;
        let message = bincode::deserialize(&bytes).map_err(anyhow::Error::from)?;
        if let proto::NodeMessage::Response(response) = &message {
            self.responses.lock().unwrap().push(StateTraffic::Response(response.body.clone()));
        } else if let proto::NodeMessage::StateFrame { frame, .. } = &message {
            self.responses.lock().unwrap().push(StateTraffic::Frame(frame.clone()));
        }
        if let Some(gate) = &self.gate {
            if gate.should_hold(&message) {
                gate.held.lock().unwrap().push(message);
                return Ok(());
            }
        }
        wasm_bindgen_futures::spawn_local(async move {
            target.handle_message(message).await.expect("browser peer accepts serialized message");
        });
        Ok(())
    }
    fn recipient_node_id(&self) -> proto::EntityId { self.target_id }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

async fn connect(server: &TestNode, client: &TestNode, responses: &Responses) -> anyhow::Result<()> {
    connect_gated(server, client, responses, None).await
}

async fn connect_gated(server: &TestNode, client: &TestNode, responses: &Responses, gate: Option<Arc<FeedbackGate>>) -> anyhow::Result<()> {
    client
        .register_peer(
            server.presence().await?,
            Box::new(BrowserSender { target: server.weak(), target_id: server.id, responses: responses.clone(), gate: gate.clone() }),
        )
        .await?;
    server
        .register_peer(
            client.presence().await?,
            Box::new(BrowserSender { target: client.weak(), target_id: client.id, responses: responses.clone(), gate: gate.clone() }),
        )
        .await?;
    client.wait_ready().await?;
    Ok(())
}

fn frames(responses: &Responses, query: Option<proto::QueryId>) -> Vec<proto::StateFrame> {
    let replies = responses.lock().unwrap();
    let ids: Vec<_> = replies
        .iter()
        .filter_map(|reply| match reply {
            StateTraffic::Response(proto::NodeResponseBody::Fetch(state)) if query.is_none() => Some(state.frame.stream),
            StateTraffic::Response(proto::NodeResponseBody::QuerySubscribed { query_id, state }) if Some(*query_id) == query => {
                Some(state.frame.stream)
            }
            _ => None,
        })
        .collect();
    replies
        .iter()
        .filter_map(|reply| match reply {
            StateTraffic::Response(proto::NodeResponseBody::Fetch(state) | proto::NodeResponseBody::QuerySubscribed { state, .. }) => {
                Some(&state.frame)
            }
            StateTraffic::Frame(frame) => Some(frame),
            _ => None,
        })
        .filter(|page| ids.contains(&page.stream))
        .cloned()
        .collect()
}

fn completed(responses: &Responses, query: Option<proto::QueryId>) -> bool {
    frames(responses, query)
        .iter()
        .any(|frame| matches!(&frame.body, proto::StateFrameBody::Round { progress: proto::ComparisonProgress::Complete, .. }))
}

fn items(responses: &Responses, query: Option<proto::QueryId>) -> Vec<proto::StateItem> {
    frames(responses, query)
        .into_iter()
        .flat_map(|frame| match frame.body {
            proto::StateFrameBody::Round { items, .. } => items,
            _ => vec![],
        })
        .collect()
}

#[wasm_bindgen]
pub async fn digest_state_browser_roundtrip(db_prefix: &str) -> Result<(), JsValue> {
    run(db_prefix).await.map_err(|error| JsValue::from_str(&format!("{error:#}")))
}

async fn run(db_prefix: &str) -> anyhow::Result<()> {
    let server_name = format!("{db_prefix}_server");
    let client_name = format!("{db_prefix}_client");
    let server = Node::new_durable(Arc::new(IndexedDBStorageEngine::open(&server_name).await?), PermissiveAgent::new());
    server.system.create().await?;
    let writer = server.context_async(DEFAULT_CONTEXT).await?;
    let trx = writer.begin();
    let mut users = Vec::new();
    for i in 0..64 {
        users.push(trx.create(&TestUser { name: format!("user-{i:02}") }).await?.read()?);
    }
    trx.commit().await?;
    let client = Node::new(Arc::new(IndexedDBStorageEngine::open(&client_name).await?), PermissiveAgent::new());
    let responses = Responses::default();
    connect(&server, &client, &responses).await?;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    anyhow::ensure!(reader.fetch::<TestUserView>("true").await?.len() == 64, "cold Fetch membership");
    responses.lock().unwrap().clear();
    anyhow::ensure!(reader.fetch::<TestUserView>("true").await?.len() == 64, "warm Fetch membership");
    anyhow::ensure!(completed(&responses, None), "warm Fetch must complete its comparison");
    anyhow::ensure!(items(&responses, None).is_empty(), "equal warm Fetch must return no changes");
    let trx = writer.begin();
    users[17].edit(&trx)?.name()?.replace("changed")?;
    trx.commit().await?;
    responses.lock().unwrap().clear();
    let fetched = reader.fetch::<TestUserView>("true").await?;
    anyhow::ensure!(fetched.iter().any(|user| user.name().as_deref() == Ok("changed")), "sparse Fetch repair");
    anyhow::ensure!(items(&responses, None).iter().any(|item| matches!(item,
        proto::StateItem::Repair(proto::RepairItem::Delta { delta, .. }) if delta.entity_id == users[17].id() && matches!(delta.content, proto::DeltaContent::EventBridge { .. })
    )), "sparse Fetch should bridge the known clock");

    let query = reader.query_wait::<TestUserView>("true").await?;
    query.wait_durable_answered().await?;
    let trx = writer.begin();
    trx.create(&TestUser { name: "live".into() }).await?;
    trx.commit().await?;
    for _ in 0..500 {
        if query.ids().len() == 65 {
            break;
        }
        gloo_timers::future::TimeoutFuture::new(10).await;
    }
    anyhow::ensure!(query.ids().len() == 65, "live update after comparison activation");
    server.deregister_peer(client.id);
    client.deregister_peer(server.id);
    responses.lock().unwrap().clear();
    connect(&server, &client, &responses).await?;
    for _ in 0..500 {
        if completed(&responses, Some(query.query_id())) {
            break;
        }
        gloo_timers::future::TimeoutFuture::new(10).await;
    }
    anyhow::ensure!(query.ids().len() == 65, "reconnect membership");
    anyhow::ensure!(completed(&responses, Some(query.query_id())), "reconnect must complete its own comparison");
    anyhow::ensure!(items(&responses, Some(query.query_id())).is_empty(), "equal reconnect must return no changes");
    let trx = writer.begin();
    trx.create(&TestUser { name: "after-reconnect".into() }).await?;
    trx.commit().await?;
    for _ in 0..500 {
        if query.ids().len() == 66 {
            break;
        }
        gloo_timers::future::TimeoutFuture::new(10).await;
    }
    anyhow::ensure!(query.ids().len() == 66, "live delivery resumes after reconnect");
    let stream = frames(&responses, Some(query.query_id())).first().expect("reconnect frame").stream;
    let subscribed_before = responses
        .lock()
        .unwrap()
        .iter()
        .filter(|item| {
            matches!(item,
        StateTraffic::Response(proto::NodeResponseBody::QuerySubscribed { query_id, .. }) if *query_id == query.query_id())
        })
        .count();
    for number in 1..=3 {
        query.refresh().await?;
        anyhow::ensure!(query.ids().len() == 66, "refresh preserves exact membership");
        anyhow::ensure!(
            frames(&responses, Some(query.query_id()))
                .iter()
                .any(|frame| frame.stream == stream
                    && matches!(frame.body, proto::StateFrameBody::BeginRound(round) if round.number == number)),
            "refresh begins a round on the existing stream"
        );
    }
    let subscribed_after = responses
        .lock()
        .unwrap()
        .iter()
        .filter(|item| {
            matches!(item,
        StateTraffic::Response(proto::NodeResponseBody::QuerySubscribed { query_id, .. }) if *query_id == query.query_id())
        })
        .count();
    anyhow::ensure!(subscribed_after == subscribed_before, "refresh must not replace SubscribeQuery");
    drop(query);
    server.deregister_peer(client.id);
    client.deregister_peer(server.id);
    drop(reader);
    drop(writer);
    // Delete while the connection callbacks are alive to handle versionchange.
    IndexedDBStorageEngine::cleanup(&client_name).await?;
    IndexedDBStorageEngine::cleanup(&server_name).await?;
    drop(client);
    drop(server);
    Ok(())
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct DigestRow {
    pub name: String,
    pub status: String,
}

async fn wait_until(mut ready: impl FnMut() -> bool, description: &str) -> anyhow::Result<()> {
    for _ in 0..500 {
        if ready() {
            return Ok(());
        }
        gloo_timers::future::TimeoutFuture::new(10).await;
    }
    anyhow::bail!("timed out waiting for {description}")
}

#[wasm_bindgen]
pub async fn digest_state_browser_interleaved(db_prefix: &str) -> Result<(), JsValue> {
    interleaved(db_prefix).await.map_err(|error| JsValue::from_str(&format!("{error:#}")))
}

async fn interleaved(db_prefix: &str) -> anyhow::Result<()> {
    let server_name = format!("{db_prefix}_server");
    let client_name = format!("{db_prefix}_client");
    let server = Node::new_durable(Arc::new(IndexedDBStorageEngine::open(&server_name).await?), PermissiveAgent::new());
    server.system.create().await?;
    let writer = server.context_async(DEFAULT_CONTEXT).await?;
    let trx = writer.begin();
    let mut rows = Vec::new();
    for i in 0..64 {
        // These keys force refinement instead of fitting the initial inventory.
        rows.push(trx.create(&DigestRow { name: format!("{i:03}-{}", "x".repeat(8192)), status: "1".into() }).await?.read()?);
    }
    trx.commit().await?;
    let client = Node::new(Arc::new(IndexedDBStorageEngine::open(&client_name).await?), PermissiveAgent::new());
    let responses = Responses::default();
    let gate = Arc::new(FeedbackGate::default());
    connect_gated(&server, &client, &responses, Some(gate.clone())).await?;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let retained = reader.fetch::<DigestRowView>("true").await?;
    let trx = writer.begin();
    rows[0].edit(&trx)?.status()?.replace("2")?;
    trx.commit().await?;
    gate.armed.store(true, Ordering::Release);
    let query = reader.query::<DigestRowView>(ankurah::core::node::nocache("name >= '' AND status >= '1' ORDER BY name ASC")?)?;
    wait_until(|| !gate.held.lock().unwrap().is_empty(), "held round feedback").await?;
    anyhow::ensure!(!query.loaded(), "held comparison must remain unfinished");
    let trx = writer.begin();
    rows[1].edit(&trx)?.status()?.replace("3")?;
    trx.commit().await?;
    wait_until(
        || query.peek().iter().any(|row| row.id() == rows[1].id() && row.status().as_deref() == Ok("3")),
        "live application before comparison release",
    )
    .await?;
    anyhow::ensure!(!query.loaded(), "live delivery must not complete unfinished comparison");
    let trx = writer.begin();
    rows[2].edit(&trx)?.status()?.replace("0")?;
    trx.commit().await?;
    let former = retained.iter().find(|row| row.id() == rows[2].id()).unwrap();
    wait_until(|| former.status().as_deref() == Ok("0"), "readable state for absent member").await?;
    gate.release(&server).await?;
    query.wait_durable_answered().await?;
    let expected = rows.iter().filter(|row| row.id() != rows[2].id()).collect::<Vec<_>>();
    anyhow::ensure!(
        query.ids() == expected.iter().map(|row| row.id()).collect::<Vec<_>>(),
        "late baseline must not resurrect absent member"
    );
    for (actual, expected) in query.peek().iter().zip(expected) {
        anyhow::ensure!(
            actual.status()? == expected.status()? && actual.entity().head() == expected.entity().head(),
            "final applied state/head must match source"
        );
    }
    query.refresh().await?;
    anyhow::ensure!(query.ids().len() == 63, "subsequent round preserves membership");
    drop(query);
    server.deregister_peer(client.id);
    client.deregister_peer(server.id);
    drop(reader);
    drop(writer);
    IndexedDBStorageEngine::cleanup(&client_name).await?;
    IndexedDBStorageEngine::cleanup(&server_name).await?;
    drop(client);
    drop(server);
    Ok(())
}
