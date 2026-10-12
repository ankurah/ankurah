//! Cold SubscribeQuery measurements through public APIs, portable to the
//! pre-digest baseline. Run with `cargo bench -p ankurah-tests --bench live_query_startup`.
//! Environment: STARTUP_SIZES=0,8,32,256,4096 STARTUP_RTT_MS=0,50
//! STARTUP_REPEATS=3 STARTUP_CACHE=empty,half,all STARTUP_LABEL=candidate.
//!
//! A new client/query is used for every sample. Catalog negotiation and optional
//! entity prefetch are excluded; initialization through durable answer and local
//! application is included. Both directions actually encode/decode the messages.
//! Delay is injected per message on a real runtime; it models latency, not a
//! bandwidth limit. Bytes exclude transport framing. No wall-clock CI threshold.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use ankql::ast::{Predicate, Resolved, Selection};
use ankurah::{
    core::{
        connector::{PeerSender, SendError},
        storage::{CatalogResolver, GetStateResult, StorageEngine},
    },
    error::{MutationError, RetrievalError},
    policy::DEFAULT_CONTEXT,
    proto, Model, Node, PermissiveAgent,
};
use ankurah_storage_sled::SledStorageEngine;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct StartupRow {
    pub position: i64,
    pub body: String,
}

#[derive(Default, Serialize)]
struct Reads {
    selections: usize,
    returned_states: usize,
    returned_state_bytes: usize,
    point_reads: usize,
    event_calls: usize,
    requested_events: usize,
}

struct Counted {
    inner: SledStorageEngine,
    reads: Mutex<Reads>,
}

impl Counted {
    fn new() -> anyhow::Result<Self> { Ok(Self { inner: SledStorageEngine::new_test()?, reads: Mutex::new(Reads::default()) }) }
    fn take(&self) -> Reads { std::mem::take(&mut *self.reads.lock().unwrap()) }
}

#[async_trait]
impl StorageEngine for Counted {
    type Value = <SledStorageEngine as StorageEngine>::Value;
    type Transaction<'a> = <SledStorageEngine as StorageEngine>::Transaction<'a>;
    fn transaction(&self) -> Self::Transaction<'_> { self.inner.transaction() }
    async fn get_state(&self, id: proto::EntityId) -> Result<proto::Attested<proto::EntityState>, RetrievalError> {
        self.reads.lock().unwrap().point_reads += 1;
        self.inner.get_state(id).await
    }
    async fn fetch_states(&self, selection: &Selection<Resolved>) -> Result<Vec<proto::Attested<proto::EntityState>>, RetrievalError> {
        let states = self.inner.fetch_states(selection).await?;
        let mut reads = self.reads.lock().unwrap();
        reads.selections += 1;
        reads.returned_states += states.len();
        reads.returned_state_bytes += states.iter().map(|state| bincode::serialized_size(state).unwrap() as usize).sum::<usize>();
        Ok(states)
    }
    async fn get_states(&self, ids: Vec<proto::EntityId>, predicate: &Predicate<Resolved>) -> Result<Vec<GetStateResult>, RetrievalError> {
        self.reads.lock().unwrap().point_reads += ids.len();
        self.inner.get_states(ids, predicate).await
    }
    async fn get_events(&self, ids: Vec<proto::EventId>) -> Result<Vec<proto::Attested<proto::Event>>, RetrievalError> {
        {
            let mut reads = self.reads.lock().unwrap();
            reads.event_calls += 1;
            reads.requested_events += ids.len();
        }
        self.inner.get_events(ids).await
    }
    async fn dump_entity_events(&self, id: proto::EntityId) -> Result<Vec<proto::Attested<proto::Event>>, RetrievalError> {
        self.inner.dump_entity_events(id).await
    }
    async fn delete_all(&self) -> Result<bool, MutationError> { self.inner.delete_all().await }
    async fn list_materializations(&self) -> Result<Vec<proto::ModelId>, RetrievalError> { self.inner.list_materializations().await }
    fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn CatalogResolver>) { self.inner.set_catalog_resolver(resolver); }
}

#[derive(Default, Serialize)]
struct Traffic {
    requester_messages: usize,
    responder_messages: usize,
    requester_bytes: usize,
    responder_bytes: usize,
    max_message_bytes: usize,
}

#[derive(Default)]
struct Meter {
    measuring: bool,
    connected: bool,
    traffic: Traffic,
    errors: Vec<String>,
}

type TestNode = Node<Counted, PermissiveAgent>;

#[derive(Clone)]
struct Sender {
    target: ankurah::core::node::WeakNode<Counted, PermissiveAgent>,
    recipient: proto::EntityId,
    requester: bool,
    one_way: Duration,
    meter: Arc<Mutex<Meter>>,
}

#[async_trait]
impl PeerSender for Sender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        let node = self.target.upgrade().ok_or(SendError::ConnectionClosed)?;
        let bytes = bincode::serialize(&message).expect("encode node message");
        {
            let mut meter = self.meter.lock().unwrap();
            if !meter.connected {
                return Err(SendError::ConnectionClosed);
            }
            if meter.measuring {
                let traffic = &mut meter.traffic;
                if self.requester {
                    traffic.requester_messages += 1;
                    traffic.requester_bytes += bytes.len();
                } else {
                    traffic.responder_messages += 1;
                    traffic.responder_bytes += bytes.len();
                }
                traffic.max_message_bytes = traffic.max_message_bytes.max(bytes.len());
            }
        }
        let meter = self.meter.clone();
        let one_way = self.one_way;
        tokio::spawn(async move {
            if !one_way.is_zero() {
                tokio::time::sleep(one_way).await;
            }
            if !meter.lock().unwrap().connected {
                return;
            }
            let message = bincode::deserialize(&bytes).expect("decode node message");
            if let Err(error) = node.handle_message(message).await {
                let mut meter = meter.lock().unwrap();
                if meter.connected {
                    meter.errors.push(error.to_string());
                }
            }
        });
        Ok(())
    }
    fn recipient_node_id(&self) -> proto::EntityId { self.recipient }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

async fn connect(server: &TestNode, client: &TestNode, rtt_ms: u64, meter: &Arc<Mutex<Meter>>) -> anyhow::Result<()> {
    let one_way = Duration::from_micros(rtt_ms * 500);
    meter.lock().unwrap().connected = true;
    client
        .register_peer(
            server.presence().await?,
            Box::new(Sender { target: server.weak(), recipient: server.id, requester: true, one_way, meter: meter.clone() }),
        )
        .await?;
    server
        .register_peer(
            client.presence().await?,
            Box::new(Sender { target: client.weak(), recipient: client.id, requester: false, one_way, meter: meter.clone() }),
        )
        .await?;
    client.wait_ready().await?;
    Ok(())
}

async fn sample(n: usize, cache: &str, rtt_ms: u64, repeat: usize, label: &str) -> anyhow::Result<()> {
    let server = Node::new_durable(Arc::new(Counted::new()?), PermissiveAgent::new());
    server.system.create().await?;
    let writer = server.context_async(DEFAULT_CONTEXT).await?;
    let trx = writer.begin();
    let mut expected = BTreeSet::new();
    for position in 0..n {
        let row = trx.create(&StartupRow { position: position as i64, body: format!("row-{position:08}") }).await?;
        expected.insert(ankurah::Mutable::id(&*row));
    }
    trx.commit().await?;
    // Also register the model in the empty-result case.
    writer.resolve_model_id::<StartupRow>().await?;
    let client = Node::new(Arc::new(Counted::new()?), PermissiveAgent::new());
    let meter = Arc::new(Mutex::new(Meter::default()));
    connect(&server, &client, rtt_ms, &meter).await?;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    reader.resolve_model_id::<StartupRow>().await?;
    let cached = match cache {
        "empty" => 0,
        "half" => reader.fetch::<StartupRowView>(format!("position < {}", n / 2)).await?.len(),
        "all" => reader.fetch::<StartupRowView>("position >= 0").await?.len(),
        _ => anyhow::bail!("unknown cache mode: {cache}"),
    };
    assert_eq!(
        cached,
        match cache {
            "empty" => 0,
            "half" => n / 2,
            _ => n,
        },
        "prefetch coverage"
    );
    server.storage.take();
    client.storage.take();
    {
        let mut meter = meter.lock().unwrap();
        anyhow::ensure!(meter.errors.is_empty(), "setup peer errors: {:?}", meter.errors);
        meter.measuring = true;
    }
    let start = Instant::now();
    let query = reader.query_wait::<StartupRowView>("position >= 0").await?;
    query.wait_durable_answered().await?;
    let elapsed_us = start.elapsed().as_micros();
    let traffic = {
        let mut meter = meter.lock().unwrap();
        meter.measuring = false;
        anyhow::ensure!(meter.errors.is_empty(), "measured peer errors: {:?}", meter.errors);
        std::mem::take(&mut meter.traffic)
    };
    let server_reads = server.storage.take();
    let client_reads = client.storage.take();
    assert_eq!(query.ids().into_iter().collect::<BTreeSet<_>>(), expected, "authoritative membership");
    // Validate all payloads outside the timed region, not just their count.
    for id in expected {
        let local = client.storage.get_state(id).await?;
        let remote = server.storage.get_state(id).await?;
        assert_eq!(local.payload.state, remote.payload.state, "applied entity state");
    }
    println!(
        "STARTUP {}",
        serde_json::json!({
            "label":label, "entities":n, "cache":cache, "cached_entities":cached,
            "rtt_ms":rtt_ms, "repeat":repeat, "elapsed_us":elapsed_us,
            "traffic":traffic, "server_reads":server_reads, "client_reads":client_reads,
        })
    );
    meter.lock().unwrap().connected = false;
    server.deregister_peer(client.id);
    client.deregister_peer(server.id);
    drop(query);
    Ok(())
}

fn numbers(name: &str, default: &str) -> anyhow::Result<Vec<usize>> {
    std::env::var(name).unwrap_or_else(|_| default.into()).split(',').map(|n| Ok(n.parse()?)).collect()
}

fn main() -> anyhow::Result<()> {
    let sizes = numbers("STARTUP_SIZES", "0,8,32,256,4096")?;
    let rtts = numbers("STARTUP_RTT_MS", "0,50")?;
    let repeats: usize = std::env::var("STARTUP_REPEATS").unwrap_or_else(|_| "3".into()).parse()?;
    let caches = std::env::var("STARTUP_CACHE").unwrap_or_else(|_| "empty,half,all".into());
    let label = std::env::var("STARTUP_LABEL").unwrap_or_else(|_| "candidate".into());
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?;
    rt.block_on(async {
        for repeat in 0..repeats {
            for &n in &sizes {
                for &rtt in &rtts {
                    for cache in caches.split(',') {
                        if n == 0 && cache != "empty" {
                            continue;
                        }
                        tokio::time::timeout(Duration::from_secs(180), sample(n, cache, rtt as u64, repeat, &label)).await??;
                    }
                }
            }
        }
        Ok(())
    })
}
