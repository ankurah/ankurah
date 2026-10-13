//! Cold SubscribeQuery and retained-query reconnect measurements through public APIs, portable to the
//! pre-digest baseline. Run with `cargo bench -p ankurah-tests --bench live_query_startup`.
//! Environment: STARTUP_SIZES=0,8,32,256,4096 STARTUP_RTT_MS=0,50
//! STARTUP_REPEATS=3 STARTUP_CACHE=empty,half,all STARTUP_LABEL=candidate.
//!
//! A new client/query is used for every sample. Catalog negotiation and optional
//! entity prefetch are excluded; initialization through durable answer and local
//! application is included. Both directions actually encode/decode the messages.
//! Propagation delay and optional full-duplex bandwidth limits are injected.
//! STARTUP_MODES=cold,warm_equal,warm_edit,warm_membership STARTUP_BPS=0,1250000. Bytes exclude transport framing. No wall-clock CI threshold.

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
    proto, Model, Mutable, Node, PermissiveAgent,
};
use ankurah_storage_sled::SledStorageEngine;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct StartupRow {
    pub position: i64,
    pub body: String,
}

// Process CPU includes both nodes and every background thread during the phase.
// Non-Unix hosts report null; external runner resources still cover the process.
#[derive(Clone, Copy, Serialize)]
struct ProcessCpu {
    user_us: u64,
    system_us: u64,
}

impl ProcessCpu {
    fn now() -> anyhow::Result<Option<Self>> {
        #[cfg(unix)]
        {
            let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
            // getrusage initializes the output structure only on success.
            if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let usage = unsafe { usage.assume_init() };
            let micros = |time: libc::timeval| time.tv_sec as u64 * 1_000_000 + time.tv_usec as u64;
            Ok(Some(Self { user_us: micros(usage.ru_utime), system_us: micros(usage.ru_stime) }))
        }
        #[cfg(not(unix))]
        {
            Ok(None)
        }
    }
}

fn cpu_delta(start: Option<ProcessCpu>, end: Option<ProcessCpu>) -> Option<ProcessCpu> {
    start.zip(end).map(|(start, end)| ProcessCpu { user_us: end.user_us - start.user_us, system_us: end.system_us - start.system_us })
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

impl Reads {
    fn add(&mut self, other: Self) {
        self.selections += other.selections;
        self.returned_states += other.returned_states;
        self.returned_state_bytes += other.returned_state_bytes;
        self.point_reads += other.point_reads;
        self.event_calls += other.event_calls;
        self.requested_events += other.requested_events;
    }
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

#[derive(Clone, Default, Serialize)]
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
    generation: usize,
    pending: usize,
    subscriptions_applied: usize,
    last_activity: Option<Instant>,
    uplink_free: Option<Instant>,
    downlink_free: Option<Instant>,
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
    bytes_per_second: u64,
}

#[async_trait]
impl PeerSender for Sender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        let node = self.target.upgrade().ok_or(SendError::ConnectionClosed)?;
        let bytes = bincode::serialize(&message).expect("encode node message");
        let (generation, arrival) = {
            let mut meter = self.meter.lock().unwrap();
            if !meter.connected {
                return Err(SendError::ConnectionClosed);
            }
            let now = Instant::now();
            let lane = if self.requester { &mut meter.uplink_free } else { &mut meter.downlink_free };
            let begin = lane.unwrap_or(now).max(now);
            let transmission = if self.bytes_per_second == 0 {
                Duration::ZERO
            } else {
                Duration::from_secs_f64(bytes.len() as f64 / self.bytes_per_second as f64)
            };
            // Full-duplex FIFO link: serialization is shared by messages in each
            // direction; propagation overlaps subsequent serialization.
            let departure = begin + transmission;
            *lane = Some(departure);
            meter.pending += 1;
            meter.last_activity = Some(now);
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
            (meter.generation, departure + self.one_way)
        };
        let meter = self.meter.clone();
        tokio::spawn(async move {
            if arrival > Instant::now() {
                tokio::time::sleep_until(tokio::time::Instant::from_std(arrival)).await;
            }
            let current = {
                let meter = meter.lock().unwrap();
                meter.connected && meter.generation == generation
            };
            if current {
                let message = bincode::deserialize(&bytes).expect("decode node message");
                let subscribed = matches!(&message, proto::NodeMessage::Response(response)
                    if matches!(response.body, proto::NodeResponseBody::QuerySubscribed { .. }));
                let result = node.handle_message(message).await;
                let mut meter = meter.lock().unwrap();
                if meter.connected && meter.generation == generation {
                    if let Err(error) = result {
                        meter.errors.push(error.to_string());
                    } else if subscribed {
                        meter.subscriptions_applied += 1;
                    }
                }
            }
            let mut meter = meter.lock().unwrap();
            meter.pending -= 1;
            meter.last_activity = Some(Instant::now());
        });
        Ok(())
    }
    fn recipient_node_id(&self) -> proto::EntityId { self.recipient }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

async fn connect(
    server: &TestNode,
    client: &TestNode,
    rtt_ms: u64,
    bytes_per_second: u64,
    meter: &Arc<Mutex<Meter>>,
) -> anyhow::Result<()> {
    let one_way = Duration::from_micros(rtt_ms * 500);
    {
        let mut meter = meter.lock().unwrap();
        meter.connected = true;
        meter.generation += 1;
        meter.uplink_free = None;
        meter.downlink_free = None;
    }
    // Match a completed presence handshake before relay startup can send.
    server
        .register_peer(
            client.presence().await?,
            Box::new(Sender {
                target: client.weak(),
                recipient: client.id,
                requester: false,
                one_way,
                meter: meter.clone(),
                bytes_per_second,
            }),
        )
        .await?;
    client
        .register_peer(
            server.presence().await?,
            Box::new(Sender {
                target: server.weak(),
                recipient: server.id,
                requester: true,
                one_way,
                meter: meter.clone(),
                bytes_per_second,
            }),
        )
        .await?;
    client.wait_ready().await?;
    Ok(())
}

async fn sample(n: usize, cache: &str, mode: &str, rtt_ms: u64, bytes_per_second: u64, repeat: usize, label: &str) -> anyhow::Result<()> {
    anyhow::ensure!(["cold", "warm_equal", "warm_edit", "warm_membership", "warm_refresh"].contains(&mode), "unknown mode: {mode}");
    let sample_cpu_start = ProcessCpu::now()?;
    phase("server-create");
    let server = Node::new_durable(Arc::new(Counted::new()?), PermissiveAgent::new());
    server.system.create().await?;
    let writer = server.context_async(DEFAULT_CONTEXT).await?;
    let trx = writer.begin();
    let mut expected = BTreeSet::new();
    let mut rows = Vec::new();
    for position in 0..n {
        let row = trx.create(&StartupRow { position: position as i64, body: format!("row-{position:08}") }).await?;
        expected.insert(ankurah::Mutable::id(&*row));
        rows.push(row.read()?);
    }
    trx.commit().await?;
    phase("server-populated");
    // Also register the model in the empty-result case.
    writer.resolve_model_id::<StartupRow>().await?;
    let client = Node::new(Arc::new(Counted::new()?), PermissiveAgent::new());
    let meter = Arc::new(Mutex::new(Meter::default()));
    phase("connect-catalog");
    connect(&server, &client, rtt_ms, bytes_per_second, &meter).await?;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    reader.resolve_model_id::<StartupRow>().await?;
    phase("prefetch");
    let cached = match cache {
        "empty" => 0,
        "half" => reader.fetch::<StartupRowView>(format!("position < {}", n / 2)).await?.len(),
        "half_blocks" => {
            let intervals = (0..n)
                .step_by(512)
                .map(|start| format!("(position >= {start} AND position < {})", (start + 256).min(n)))
                .collect::<Vec<_>>();
            let wanted: BTreeSet<_> =
                rows.iter().enumerate().filter(|(position, _)| position / 256 % 2 == 0).map(|(_, row)| row.id()).collect();
            let mut cached_ids = BTreeSet::new();
            for batch in intervals.chunks(16) {
                cached_ids.extend(reader.fetch::<StartupRowView>(batch.join(" OR ")).await?.into_iter().map(|row| row.id()));
            }
            assert_eq!(cached_ids, wanted, "gapped prefetch membership");
            cached_ids.len()
        }
        "half_interleaved" => {
            let positions = (0..n).step_by(2).map(|position| format!("position = {position}")).collect::<Vec<_>>();
            let wanted: BTreeSet<_> = rows.iter().enumerate().filter(|(position, _)| position % 2 == 0).map(|(_, row)| row.id()).collect();
            let mut cached_ids = BTreeSet::new();
            for batch in positions.chunks(32) {
                cached_ids.extend(reader.fetch::<StartupRowView>(batch.join(" OR ")).await?.into_iter().map(|row| row.id()));
            }
            assert_eq!(cached_ids, wanted, "gapped prefetch membership");
            cached_ids.len()
        }
        "all" => reader.fetch::<StartupRowView>("position >= 0").await?.len(),
        _ => anyhow::bail!("unknown cache mode: {cache}"),
    };
    assert_eq!(
        cached,
        match cache {
            "empty" => 0,
            "half" => n / 2,
            "half_blocks" => (0..n).filter(|position| position / 256 % 2 == 0).count(),
            "half_interleaved" => n.div_ceil(2),
            _ => n,
        },
        "prefetch coverage"
    );
    settle(&meter, 0).await?;
    let retained_query = if mode == "cold" {
        None
    } else {
        phase("warm-tree-initialize");
        let query = reader.query_wait::<StartupRowView>("position >= 0").await?;
        query.wait_durable_answered().await?;
        settle(&meter, 1).await?;
        if mode != "warm_refresh" {
            disconnect(&server, &client, &meter);
            settle(&meter, 0).await?;
        }
        if mode == "warm_edit" || mode == "warm_membership" {
            let trx = writer.begin();
            rows[n / 2].edit(&trx)?.body()?.replace("offline-edit")?;
            if mode == "warm_membership" {
                rows[0].edit(&trx)?.position()?.set(&-1)?;
                expected.remove(&rows[0].id());
                let added = trx.create(&StartupRow { position: n as i64, body: "offline-added".into() }).await?;
                expected.insert(ankurah::Mutable::id(&*added));
            }
            trx.commit().await?;
        }
        Some(query)
    };
    server.storage.take();
    client.storage.take();
    let subscriptions_before = {
        let mut meter = meter.lock().unwrap();
        anyhow::ensure!(meter.errors.is_empty(), "setup peer errors: {:?}", meter.errors);
        meter.traffic = Traffic::default();
        meter.measuring = true;
        meter.subscriptions_applied
    };
    phase("measure");
    let measurement_cpu_start = ProcessCpu::now()?;
    let start = Instant::now();
    let (query, ready_us, ready_cpu) = if let Some(query) = retained_query {
        if mode == "warm_refresh" {
            query.update_selection_wait("position >= 0").await?;
            query.wait_durable_answered().await?;
            let ready_us = start.elapsed().as_micros();
            (query, Some(ready_us), ProcessCpu::now()?)
        } else {
            connect(&server, &client, rtt_ms, bytes_per_second, &meter).await?;
            (query, None, None)
        }
    } else {
        let query = reader.query_wait::<StartupRowView>("position >= 0").await?;
        query.wait_durable_answered().await?;
        let ready_us = start.elapsed().as_micros();
        (query, Some(ready_us), ProcessCpu::now()?)
    };
    let settled = settle(&meter, subscriptions_before + 1).await?;
    let initial_drained_us = settled.duration_since(start).as_micros();
    let initial_drain_cpu = ProcessCpu::now()?;
    let settled_traffic = meter.lock().unwrap().traffic.clone();
    let mut server_reads = server.storage.take();
    let mut client_reads = client.storage.take();
    phase("verify-applied");
    let verification_cpu_start = ProcessCpu::now()?;
    let verification_start = Instant::now();
    let mut verification_attempts = 0;
    let mut verification_client_point_reads = 0;
    while query.ids().into_iter().collect::<BTreeSet<_>>() != expected {
        anyhow::ensure!(ready_us.is_none(), "ready query has incorrect authoritative membership");
        verification_attempts += 1;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    // Reconnect has no portable re-answer generation API on the baseline. Await
    // exact final state rather than mistaking a drained response for application.
    for id in &expected {
        let remote = server.storage.get_state(*id).await?;
        loop {
            verification_attempts += 1;
            verification_client_point_reads += 1;
            match client.storage.get_state(*id).await {
                Ok(local) if local.payload.state == remote.payload.state => break,
                Ok(_) | Err(_) if ready_us.is_none() => {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Ok(_) => anyhow::bail!("ready query has incorrect applied state for {id}"),
                Err(error) => return Err(error.into()),
            }
        }
    }
    let verification_us = verification_start.elapsed().as_micros();
    let verified_us = start.elapsed().as_micros();
    let verified_cpu = ProcessCpu::now()?;
    let elapsed_us = ready_us.unwrap_or(verified_us);
    let final_settled = settle(&meter, subscriptions_before + 1).await?;
    let transport_settled_us = final_settled.duration_since(start).as_micros();
    let final_drain_cpu = ProcessCpu::now()?;
    let settled_us = transport_settled_us.max(ready_us.unwrap_or(verified_us));
    let mut server_tail_reads = server.storage.take();
    let mut client_tail_reads = client.storage.take();
    // Application can continue asynchronously after transport drains. Retain
    // its work while subtracting only the verifier's known direct point reads.
    server_tail_reads.point_reads -= expected.len();
    client_tail_reads.point_reads -= verification_client_point_reads;
    server_reads.add(server_tail_reads);
    client_reads.add(client_tail_reads);

    let traffic = {
        let mut meter = meter.lock().unwrap();
        meter.measuring = false;
        anyhow::ensure!(meter.errors.is_empty(), "measured peer errors: {:?}", meter.errors);
        std::mem::take(&mut meter.traffic)
    };
    println!(
        "STARTUP {}",
        serde_json::json!({
            "label":label, "entities":n, "cache":cache, "cached_entities":cached,
            "mode":mode, "bandwidth_bytes_per_second":bytes_per_second,
            "rtt_ms":rtt_ms, "repeat":repeat, "elapsed_us":elapsed_us,
            "ready_us":ready_us, "initial_drained_us":initial_drained_us,
            "settled_us":settled_us, "transport_settled_us":transport_settled_us,
            "verified_us":verified_us, "verification_us":verification_us, "verification_attempts":verification_attempts, "verified":true,
            "traffic":traffic, "settled_traffic":settled_traffic, "server_reads":server_reads, "client_reads":client_reads,
            "phase_process_cpu": {
                "scope":"process CPU during each interval: server/client combined, all background threads included; phases can overlap; excludes teardown",
                "setup":cpu_delta(sample_cpu_start, measurement_cpu_start),
                "through_ready":cpu_delta(measurement_cpu_start, ready_cpu),
                "through_initial_drain":cpu_delta(measurement_cpu_start, initial_drain_cpu),
                "verification":cpu_delta(verification_cpu_start, verified_cpu),
                "through_verified":cpu_delta(measurement_cpu_start, verified_cpu),
                "after_verification_drain":cpu_delta(verified_cpu, final_drain_cpu),
                "through_final_drain":cpu_delta(measurement_cpu_start, final_drain_cpu),
            },
        })
    );
    disconnect(&server, &client, &meter);
    drop(query);
    Ok(())
}

fn phase(name: &str) {
    eprintln!("PHASE {name}");
}

fn disconnect(server: &TestNode, client: &TestNode, meter: &Arc<Mutex<Meter>>) {
    meter.lock().unwrap().connected = false;
    server.deregister_peer(client.id);
    client.deregister_peer(server.id);
}

/// Public durable-answer flags are sticky on the old baseline across reconnect.
/// A new QuerySubscribed application plus a drained link is the portable fully
/// settled landmark. The 2 ms confirmation is excluded from the recorded time.
async fn settle(meter: &Arc<Mutex<Meter>>, subscriptions: usize) -> anyhow::Result<Instant> {
    let quiet_begin = Instant::now();
    loop {
        {
            let meter = meter.lock().unwrap();
            anyhow::ensure!(meter.errors.is_empty(), "peer errors: {:?}", meter.errors);
            if meter.pending == 0 && meter.subscriptions_applied >= subscriptions {
                let last = meter.last_activity.unwrap_or_else(Instant::now);
                if last.max(quiet_begin).elapsed() >= Duration::from_millis(2) {
                    return Ok(last);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn numbers(name: &str, default: &str) -> anyhow::Result<Vec<usize>> {
    std::env::var(name).unwrap_or_else(|_| default.into()).split(',').map(|n| Ok(n.parse()?)).collect()
}

fn main() -> anyhow::Result<()> {
    let sizes = numbers("STARTUP_SIZES", "0,8,32,256,4096")?;
    let rtts = numbers("STARTUP_RTT_MS", "0,50")?;
    let bandwidths = numbers("STARTUP_BPS", "0")?;
    let modes = std::env::var("STARTUP_MODES").unwrap_or_else(|_| "cold".into());
    let repeats: usize = std::env::var("STARTUP_REPEATS").unwrap_or_else(|_| "3".into()).parse()?;
    let caches = std::env::var("STARTUP_CACHE").unwrap_or_else(|_| "empty,half,all".into());
    let label = std::env::var("STARTUP_LABEL").unwrap_or_else(|_| "candidate".into());
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?;
    rt.block_on(async {
        for repeat in 0..repeats {
            for &n in &sizes {
                for &rtt in &rtts {
                    for &bps in &bandwidths {
                        for mode in modes.split(',') {
                            for cache in caches.split(',') {
                                if (n == 0 && cache != "empty")
                                    || (mode != "cold" && n == 0)
                                    || (mode != "cold" && mode != "warm_refresh" && cache != "all")
                                {
                                    continue;
                                }
                                tokio::time::timeout(
                                    Duration::from_secs(180),
                                    sample(n, cache, mode, rtt as u64, bps as u64, repeat, &label),
                                )
                                .await??;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    })
}
