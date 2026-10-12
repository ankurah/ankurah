//! Two real browser nodes, with serialized messages and IndexedDB entity storage.
use super::*;
use ankurah::{
    core::connector::{PeerSender, SendError},
    proto, Mutable,
};
use std::sync::Mutex;

type TestNode = Node<IndexedDBStorageEngine, PermissiveAgent>;
type Responses = Arc<Mutex<Vec<proto::NodeResponseBody>>>;

#[derive(Clone)]
struct BrowserSender {
    target: ankurah::core::node::WeakNode<IndexedDBStorageEngine, PermissiveAgent>,
    target_id: proto::EntityId,
    responses: Responses,
}

#[async_trait::async_trait]
impl PeerSender for BrowserSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        let target = self.target.upgrade().ok_or(SendError::ConnectionClosed)?;
        let bytes = bincode::serialize(&message).map_err(anyhow::Error::from)?;
        let message = bincode::deserialize(&bytes).map_err(anyhow::Error::from)?;
        if let proto::NodeMessage::Response(response) = &message {
            self.responses.lock().unwrap().push(response.body.clone());
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
    client
        .register_peer(
            server.presence().await?,
            Box::new(BrowserSender { target: server.weak(), target_id: server.id, responses: responses.clone() }),
        )
        .await?;
    server
        .register_peer(
            client.presence().await?,
            Box::new(BrowserSender { target: client.weak(), target_id: client.id, responses: responses.clone() }),
        )
        .await?;
    client.wait_ready().await?;
    Ok(())
}

fn items(responses: &Responses, query: Option<proto::QueryId>) -> Vec<proto::StateItem> {
    let replies = responses.lock().unwrap();
    let ids: Vec<_> = replies
        .iter()
        .filter_map(|reply| match reply {
            proto::NodeResponseBody::Fetch(state) if query.is_none() => Some(state.page.comparison),
            proto::NodeResponseBody::QuerySubscribed { query_id, state } if Some(*query_id) == query => Some(state.page.comparison),
            _ => None,
        })
        .collect();
    replies
        .iter()
        .filter_map(|reply| match reply {
            proto::NodeResponseBody::Fetch(state) | proto::NodeResponseBody::QuerySubscribed { state, .. } => Some(&state.page),
            proto::NodeResponseBody::TreeCompared(page) => Some(page),
            _ => None,
        })
        .filter(|page| ids.contains(&page.comparison))
        .flat_map(|page| page.items.clone())
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
    anyhow::ensure!(matches!(items(&responses, None).as_slice(), [proto::StateItem::Equal(_)]), "warm Fetch must compare equal");
    let trx = writer.begin();
    users[17].edit(&trx)?.name()?.replace("changed")?;
    trx.commit().await?;
    responses.lock().unwrap().clear();
    let fetched = reader.fetch::<TestUserView>("true").await?;
    anyhow::ensure!(fetched.iter().any(|user| user.name().as_deref() == Ok("changed")), "sparse Fetch repair");
    anyhow::ensure!(items(&responses, None).iter().any(|item| matches!(item,
        proto::StateItem::Repair { delta, .. } if delta.entity_id == users[17].id() && matches!(delta.content, proto::DeltaContent::EventBridge { .. })
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
        if !items(&responses, Some(query.query_id())).is_empty() {
            break;
        }
        gloo_timers::future::TimeoutFuture::new(10).await;
    }
    anyhow::ensure!(query.ids().len() == 65, "reconnect membership");
    anyhow::ensure!(
        matches!(items(&responses, Some(query.query_id())).as_slice(), [proto::StateItem::Equal(_)]),
        "reconnect must compare retained state"
    );
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
