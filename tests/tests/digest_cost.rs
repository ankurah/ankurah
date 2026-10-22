//! Payload/row measurements, optionally with simulated per-message network latency.
use ankql::ast::{Predicate, Resolved, Selection};
use ankurah::{
    core::{
        connector::{PeerSender, SendError},
        storage::{CatalogResolver, GetStateResult, StorageEngine},
    },
    error::{MutationError, RetrievalError},
    policy::DEFAULT_CONTEXT,
    proto, Model, Mutable, Node, PermissiveAgent,
};
use ankurah_storage_sled::SledStorageEngine;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct CostRow {
    #[active_type(LWW)]
    value: String,
}

#[derive(Default, Debug, Clone, Serialize)]
struct Reads {
    fetches: usize,
    rows: usize,
}
struct Counted {
    inner: SledStorageEngine,
    model: Mutex<Option<proto::ModelId>>,
    reads: Mutex<Reads>,
}
impl Counted {
    fn new() -> anyhow::Result<Self> {
        Ok(Self { inner: SledStorageEngine::new_test()?, model: Mutex::new(None), reads: Mutex::new(Reads::default()) })
    }
    fn take(&self) -> Reads { std::mem::take(&mut *self.reads.lock().unwrap()) }
}
#[async_trait]
impl StorageEngine for Counted {
    type Value = <SledStorageEngine as StorageEngine>::Value;
    type Transaction<'a> = <SledStorageEngine as StorageEngine>::Transaction<'a>;
    fn transaction(&self) -> Self::Transaction<'_> { self.inner.transaction() }
    async fn get_state(&self, id: proto::EntityId) -> Result<proto::Attested<proto::EntityState>, RetrievalError> {
        self.inner.get_state(id).await
    }
    async fn fetch_states(&self, selection: &Selection<Resolved>) -> Result<Vec<proto::Attested<proto::EntityState>>, RetrievalError> {
        let states = self.inner.fetch_states(selection).await?;
        if self.model.lock().unwrap().is_some_and(|model| selection.predicate.referenced_models().contains(&model)) {
            let mut reads = self.reads.lock().unwrap();
            reads.fetches += 1;
            reads.rows += states.len();
        }
        Ok(states)
    }
    async fn get_states(&self, ids: Vec<proto::EntityId>, predicate: &Predicate<Resolved>) -> Result<Vec<GetStateResult>, RetrievalError> {
        self.inner.get_states(ids, predicate).await
    }
    async fn get_events(&self, ids: Vec<proto::EventId>) -> Result<Vec<proto::Attested<proto::Event>>, RetrievalError> {
        self.inner.get_events(ids).await
    }
    async fn dump_entity_events(&self, id: proto::EntityId) -> Result<Vec<proto::Attested<proto::Event>>, RetrievalError> {
        self.inner.dump_entity_events(id).await
    }
    async fn delete_all(&self) -> Result<bool, MutationError> { self.inner.delete_all().await }
    async fn list_materializations(&self) -> Result<Vec<proto::ModelId>, RetrievalError> { self.inner.list_materializations().await }
    fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn CatalogResolver>) { self.inner.set_catalog_resolver(resolver); }
}

#[derive(Default, Debug, Clone, Serialize)]
struct Traffic {
    request_bytes: usize,
    response_bytes: usize,
    update_bytes: usize,
    ack_bytes: usize,
    frames: usize,
    requests: usize,
    leaves: usize,
    repairs: usize,
    snapshots: usize,
    completed: usize,
    summaries: usize,
    leaf_questions: usize,
    max_reply_items: usize,
    max_reply_item_bytes: usize,
    elapsed_ms: u128,
}
#[derive(Default)]
struct Meter {
    model: Option<proto::ModelId>,
    requests: HashSet<proto::RequestId>,
    comparisons: HashSet<proto::StateStreamId>,
    updates: HashSet<proto::UpdateId>,
    traffic: Traffic,
    started: Option<Instant>,
    one_way_ms: u64,
    generation: usize,
    errors: Vec<String>,
}
impl Meter {
    fn observe(&mut self, message: &proto::NodeMessage) {
        match message {
            proto::NodeMessage::Request { request, .. } => {
                let include = match &request.body {
                    proto::NodeRequestBody::Fetch { selection, .. } | proto::NodeRequestBody::SubscribeQuery { selection, .. } => {
                        self.model.is_some_and(|model| selection.predicate.referenced_models().contains(&model))
                    }
                    proto::NodeRequestBody::ContinueState { stream: comparison, .. }
                    | proto::NodeRequestBody::FinishStateRound { stream: comparison, .. } => self.comparisons.contains(comparison),
                    _ => false,
                };
                if include {
                    if let proto::NodeRequestBody::FinishStateRound { outcome: proto::StateRoundOutcome::Applied { .. }, .. } =
                        &request.body
                    {
                        self.traffic.completed += 1;
                    }
                    self.requests.insert(request.id.clone());
                    self.traffic.requests += 1;
                    self.traffic.request_bytes += bincode::serialized_size(message).unwrap() as usize;
                }
            }
            proto::NodeMessage::Response(response) if self.requests.contains(&response.request_id) => {
                self.traffic.response_bytes += bincode::serialized_size(message).unwrap() as usize;
                let page = match &response.body {
                    proto::NodeResponseBody::Fetch(state) | proto::NodeResponseBody::QuerySubscribed { state, .. } => {
                        self.comparisons.insert(state.frame.stream);
                        Some(&state.frame)
                    }
                    _ => None,
                };
                if let Some(page) = page {
                    self.observe_frame(page);
                }
            }
            proto::NodeMessage::Update(update) => {
                if let proto::NodeUpdateBody::StateFrame { request_id, frame } = &update.body {
                    if self.requests.contains(request_id) {
                        self.updates.insert(update.id.clone());
                        let bytes = bincode::serialized_size(message).unwrap() as usize;
                        self.traffic.update_bytes += bytes;
                        self.traffic.response_bytes += bytes;
                        self.observe_frame(frame);
                    }
                }
            }
            proto::NodeMessage::AbandonStateStream { stream, .. } if self.comparisons.contains(stream) => {
                self.traffic.request_bytes += bincode::serialized_size(message).unwrap() as usize;
            }
            proto::NodeMessage::UpdateAck(ack) if self.updates.contains(&ack.id) => {
                let bytes = bincode::serialized_size(message).unwrap() as usize;
                self.traffic.ack_bytes += bytes;
                self.traffic.request_bytes += bytes;
            }
            _ => {}
        }
    }
    fn observe_frame(&mut self, frame: &proto::StateFrame) {
        self.traffic.frames += 1;
        if let proto::StateFrameBody::State { items, .. } = &frame.body {
            self.traffic.max_reply_items = self.traffic.max_reply_items.max(items.len());
            let bytes = items.iter().map(|item| bincode::serialized_size(item).unwrap() as usize).sum();
            self.traffic.max_reply_item_bytes = self.traffic.max_reply_item_bytes.max(bytes);
            for item in items {
                match item {
                    proto::StateItem::Heads(page) => self.traffic.leaves += page.heads.len(),
                    proto::StateItem::Repair { delta, .. } => {
                        self.traffic.repairs += 1;
                        if matches!(delta.content, proto::DeltaContent::StateSnapshot { .. }) {
                            self.traffic.snapshots += 1;
                        }
                    }
                    proto::StateItem::Digest(_) => self.traffic.summaries += 1,
                    proto::StateItem::NeedHeads(_) => self.traffic.leaf_questions += 1,
                    _ => {}
                }
            }
        }
    }
    fn take(&mut self) -> Traffic {
        self.traffic.elapsed_ms = self.started.replace(Instant::now()).map_or(0, |start| start.elapsed().as_millis());
        std::mem::take(&mut self.traffic)
    }
}

type TestNode = Node<Counted, PermissiveAgent>;
#[derive(Clone)]
struct Sender {
    target: ankurah::core::node::WeakNode<Counted, PermissiveAgent>,
    id: proto::EntityId,
    meter: Arc<Mutex<Meter>>,
    generation: usize,
}
#[async_trait]
impl PeerSender for Sender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        let node = self.target.upgrade().ok_or(SendError::ConnectionClosed)?;
        let one_way_ms = {
            let mut meter = self.meter.lock().unwrap();
            meter.observe(&message);
            meter.one_way_ms
        };
        let meter = self.meter.clone();
        let generation = self.generation;
        tokio::spawn(async move {
            if one_way_ms > 0 {
                tokio::time::sleep(Duration::from_millis(one_way_ms)).await;
            }
            // Disconnect discards packets still in flight, including a final
            // unsubscribe. A later connection must not receive those packets.
            if meter.lock().unwrap().generation != generation {
                return;
            }
            if let Err(error) = node.handle_message(message).await {
                let mut meter = meter.lock().unwrap();
                if meter.generation == generation {
                    meter.errors.push(error.to_string());
                }
            }
        });
        Ok(())
    }
    fn recipient_node_id(&self) -> proto::EntityId { self.id }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}
async fn connect(server: &TestNode, client: &TestNode, meter: &Arc<Mutex<Meter>>) -> anyhow::Result<()> {
    let generation = {
        let mut meter = meter.lock().unwrap();
        meter.generation += 1;
        meter.generation
    };
    client
        .register_peer(
            server.presence().await?,
            Box::new(Sender { target: server.weak(), id: server.id, meter: meter.clone(), generation }),
        )
        .await?;
    server
        .register_peer(
            client.presence().await?,
            Box::new(Sender { target: client.weak(), id: client.id, meter: meter.clone(), generation }),
        )
        .await?;
    client.wait_ready().await?;
    Ok(())
}
fn disconnect(server: &TestNode, client: &TestNode, meter: &Arc<Mutex<Meter>>) {
    meter.lock().unwrap().generation += 1;
    server.deregister_peer(client.id);
    client.deregister_peer(server.id);
}
fn reset(server: &TestNode, client: &TestNode, meter: &Arc<Mutex<Meter>>) {
    server.storage.take();
    client.storage.take();
    meter.lock().unwrap().take();
}
fn record(label: &str, n: usize, server: &TestNode, client: &TestNode, meter: &Arc<Mutex<Meter>>) -> Traffic {
    let traffic = {
        let mut meter = meter.lock().unwrap();
        assert!(meter.errors.is_empty(), "peer errors: {:?}", meter.errors);
        meter.take()
    };
    assert!(traffic.max_reply_items <= 128);
    assert!(traffic.max_reply_item_bytes <= 64 * 1024);
    println!(
        "DIGEST_COST {}",
        serde_json::json!({"case":label,"entities":n,"one_way_ms":meter.lock().unwrap().one_way_ms,"wire":traffic,"server_materialization":server.storage.take(),"client_materialization":client.storage.take()})
    );
    traffic
}

#[tokio::test]
async fn measure_cold_equal_sparse_fetch_and_live_reconnect() -> anyhow::Result<()> {
    let one_way_ms = std::env::var("DIGEST_COST_ONE_WAY_MS").ok().map(|value| value.parse()).transpose()?.unwrap_or(0);
    for n in [256, 4096] {
        let server = Node::new_durable(Arc::new(Counted::new()?), PermissiveAgent::new());
        server.system.create().await?;
        let writer = server.context_async(DEFAULT_CONTEXT).await?;
        let trx = writer.begin();
        let mut rows = Vec::new();
        for i in 0..n {
            rows.push(trx.create(&CostRow { value: format!("value-{i:04}") }).await?.read()?);
        }
        trx.commit().await?;
        let model = writer.resolve_model_id::<CostRow>().await?;
        *server.storage.model.lock().unwrap() = Some(model);
        let client = Node::new(Arc::new(Counted::new()?), PermissiveAgent::new());
        *client.storage.model.lock().unwrap() = Some(model);
        let meter = Arc::new(Mutex::new(Meter { model: Some(model), one_way_ms, ..Default::default() }));
        connect(&server, &client, &meter).await?;
        let reader = client.context_async(DEFAULT_CONTEXT).await?;
        reader.resolve_model_id::<CostRow>().await?;
        reset(&server, &client, &meter);
        assert_eq!(reader.fetch::<CostRowView>("true").await?.len(), n);
        let cold = record("fetch_cold", n, &server, &client, &meter);
        assert_eq!((cold.snapshots, cold.repairs, cold.leaves), (n, n, n));
        assert_eq!((cold.summaries, cold.leaf_questions), (0, 0));
        // Require roughly 96 small rows per exchange, with six exchanges of
        // startup/tail slack. This is a regression target, not a protocol limit.
        assert!(cold.requests <= n.div_ceil(96) + 6, "cold transfer takes too many exchanges: {cold:?}");
        assert_eq!(reader.fetch::<CostRowView>("true").await?.len(), n);
        let equal = record("fetch_equal", n, &server, &client, &meter);
        assert_eq!((equal.leaves, equal.repairs, equal.snapshots), (0, 0, 0));
        let trx = writer.begin();
        rows[n / 2].edit(&trx)?.value()?.set(&"changed".into())?;
        trx.commit().await?;
        reset(&server, &client, &meter);
        assert_eq!(reader.fetch::<CostRowView>("true").await?.len(), n);
        let sparse = record("fetch_sparse", n, &server, &client, &meter);
        assert_eq!(sparse.repairs, 1);
        assert!(sparse.response_bytes < cold.response_bytes / 2);
        // The actual wire budget rejects walking every terminal bucket
        // before comparing the intermediate branch summaries.
        assert!(sparse.request_bytes + sparse.response_bytes < 32_768);
        let query = reader.query_wait::<CostRowView>("true").await?;
        query.wait_durable_answered().await?;
        record("subscribe_cached", n, &server, &client, &meter);
        disconnect(&server, &client, &meter);
        reset(&server, &client, &meter);
        connect(&server, &client, &meter).await?;
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while meter.lock().unwrap().traffic.completed == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let equal = record("reconnect_equal", n, &server, &client, &meter);
        assert_eq!((equal.leaves, equal.repairs, equal.snapshots), (0, 0, 0));
        assert_eq!((equal.summaries, equal.leaf_questions), (0, 0));
        disconnect(&server, &client, &meter);
        let trx = writer.begin();
        rows[n / 2].edit(&trx)?.value()?.set(&"offline-edit".into())?;
        trx.commit().await?;
        reset(&server, &client, &meter);
        connect(&server, &client, &meter).await?;
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while meter.lock().unwrap().traffic.completed == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let sparse = record("reconnect_sparse", n, &server, &client, &meter);
        assert_eq!(sparse.repairs, 1);
        assert!(sparse.request_bytes + sparse.response_bytes < 32_768);
        assert_eq!(query.ids().len(), n);
        drop(query);
        disconnect(&server, &client, &meter);
    }
    Ok(())
}
