//! Repeatable payload/row measurements, not a wall-clock benchmark.
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
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
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
    requests: usize,
    leaves: usize,
    repairs: usize,
    equal: usize,
    completed: usize,
}
#[derive(Default)]
struct Meter {
    model: Option<proto::ModelId>,
    requests: HashSet<proto::RequestId>,
    closes: HashMap<proto::RequestId, proto::ComparisonId>,
    comparisons: HashSet<proto::ComparisonId>,
    traffic: Traffic,
}
impl Meter {
    fn observe(&mut self, message: &proto::NodeMessage) {
        match message {
            proto::NodeMessage::Request { request, .. } => {
                let include = match &request.body {
                    proto::NodeRequestBody::Fetch { selection, .. } | proto::NodeRequestBody::SubscribeQuery { selection, .. } => {
                        self.model.is_some_and(|model| selection.predicate.referenced_models().contains(&model))
                    }
                    proto::NodeRequestBody::CompareTree { comparison, .. } | proto::NodeRequestBody::CloseComparison { comparison, .. } => {
                        self.comparisons.contains(comparison)
                    }
                    _ => false,
                };
                if include {
                    if let proto::NodeRequestBody::CloseComparison { comparison, outcome: proto::ComparisonOutcome::Applied } =
                        &request.body
                    {
                        self.closes.insert(request.id.clone(), *comparison);
                    }
                    self.requests.insert(request.id.clone());
                    self.traffic.requests += 1;
                    self.traffic.request_bytes += bincode::serialized_size(message).unwrap() as usize;
                }
            }
            proto::NodeMessage::Response(response) if self.requests.contains(&response.request_id) => {
                if self.closes.contains_key(&response.request_id) && matches!(response.body, proto::NodeResponseBody::Success) {
                    self.traffic.completed += 1;
                }
                self.traffic.response_bytes += bincode::serialized_size(message).unwrap() as usize;
                let page = match &response.body {
                    proto::NodeResponseBody::Fetch(state) | proto::NodeResponseBody::QuerySubscribed { state, .. } => {
                        self.comparisons.insert(state.page.comparison);
                        Some(&state.page)
                    }
                    proto::NodeResponseBody::TreeCompared(page) => Some(page),
                    _ => None,
                };
                if let Some(page) = page {
                    for item in &page.items {
                        match item {
                            proto::StateItem::Leaves(page) => self.traffic.leaves += page.leaves.len(),
                            proto::StateItem::Repair { .. } => self.traffic.repairs += 1,
                            proto::StateItem::Equal(_) => self.traffic.equal += 1,
                            _ => {}
                        }
                    }
                }
            }
            _ => {}
        }
    }
    fn take(&mut self) -> Traffic { std::mem::take(&mut self.traffic) }
}

type TestNode = Node<Counted, PermissiveAgent>;
#[derive(Clone)]
struct Sender {
    target: ankurah::core::node::WeakNode<Counted, PermissiveAgent>,
    id: proto::EntityId,
    meter: Arc<Mutex<Meter>>,
}
#[async_trait]
impl PeerSender for Sender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        let node = self.target.upgrade().ok_or(SendError::ConnectionClosed)?;
        self.meter.lock().unwrap().observe(&message);
        tokio::spawn(async move {
            node.handle_message(message).await.expect("cost peer handles message");
        });
        Ok(())
    }
    fn recipient_node_id(&self) -> proto::EntityId { self.id }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}
async fn connect(server: &TestNode, client: &TestNode, meter: &Arc<Mutex<Meter>>) -> anyhow::Result<()> {
    client.register_peer(server.presence().await?, Box::new(Sender { target: server.weak(), id: server.id, meter: meter.clone() })).await?;
    server.register_peer(client.presence().await?, Box::new(Sender { target: client.weak(), id: client.id, meter: meter.clone() })).await?;
    client.wait_ready().await?;
    Ok(())
}
fn reset(server: &TestNode, client: &TestNode, meter: &Arc<Mutex<Meter>>) {
    server.storage.take();
    client.storage.take();
    meter.lock().unwrap().take();
}
fn record(label: &str, n: usize, server: &TestNode, client: &TestNode, meter: &Arc<Mutex<Meter>>) -> Traffic {
    let traffic = meter.lock().unwrap().take();
    println!(
        "DIGEST_COST {}",
        serde_json::json!({"case":label,"entities":n,"wire":traffic,"server_materialization":server.storage.take(),"client_materialization":client.storage.take()})
    );
    traffic
}

#[tokio::test]
async fn measure_cold_equal_sparse_fetch_and_live_reconnect() -> anyhow::Result<()> {
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
        let meter = Arc::new(Mutex::new(Meter { model: Some(model), ..Default::default() }));
        connect(&server, &client, &meter).await?;
        let reader = client.context_async(DEFAULT_CONTEXT).await?;
        reader.resolve_model_id::<CostRow>().await?;
        reset(&server, &client, &meter);
        assert_eq!(reader.fetch::<CostRowView>("true").await?.len(), n);
        let cold = record("fetch_cold", n, &server, &client, &meter);
        assert_eq!(cold.repairs, n);
        assert_eq!(reader.fetch::<CostRowView>("true").await?.len(), n);
        let equal = record("fetch_equal", n, &server, &client, &meter);
        assert_eq!((equal.equal, equal.leaves, equal.repairs), (1, 0, 0));
        let trx = writer.begin();
        rows[n / 2].edit(&trx)?.value()?.set(&"changed".into())?;
        trx.commit().await?;
        reset(&server, &client, &meter);
        assert_eq!(reader.fetch::<CostRowView>("true").await?.len(), n);
        let sparse = record("fetch_sparse", n, &server, &client, &meter);
        assert_eq!(sparse.repairs, 1);
        assert!(sparse.response_bytes < cold.response_bytes / 2);
        // Allow variation from random entity IDs, but reject walking every
        // terminal bucket before comparing the intermediate branch summaries.
        assert!(sparse.equal < 64);
        assert!(sparse.request_bytes + sparse.response_bytes < 32_768);
        let query = reader.query_wait::<CostRowView>("true").await?;
        query.wait_durable_answered().await?;
        record("subscribe_cached", n, &server, &client, &meter);
        server.deregister_peer(client.id);
        client.deregister_peer(server.id);
        reset(&server, &client, &meter);
        connect(&server, &client, &meter).await?;
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while meter.lock().unwrap().traffic.completed == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let equal = record("reconnect_equal", n, &server, &client, &meter);
        assert_eq!((equal.equal, equal.leaves, equal.repairs), (1, 0, 0));
        server.deregister_peer(client.id);
        client.deregister_peer(server.id);
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
        assert!(sparse.equal < 64);
        assert!(sparse.request_bytes + sparse.response_bytes < 32_768);
        assert_eq!(query.ids().len(), n);
        drop(query);
        server.deregister_peer(client.id);
        client.deregister_peer(server.id);
    }
    Ok(())
}
