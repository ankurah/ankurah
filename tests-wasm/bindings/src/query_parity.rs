//! Identical public-API cold and retained-query fixtures for baseline/candidate browsers.
use ankurah::core::storage::StorageEngine;
use ankurah::{
    core::connector::{PeerSender, SendError},
    policy::DEFAULT_CONTEXT,
    proto, Model, Mutable, Node, PermissiveAgent,
};
use ankurah_signals::Signal;
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = performance, js_name = now)]
    fn monotonic_ms() -> f64;
}

#[derive(Clone, Copy, PartialEq, PartialOrd)]
struct Instant(f64);
impl Instant {
    fn now() -> Self { Self(monotonic_ms()) }
    fn elapsed(self) -> Duration { Self::now().duration_since(self) }
    fn duration_since(self, earlier: Self) -> Duration { Duration::from_secs_f64(((self.0 - earlier.0) / 1000.0).max(0.0)) }
    fn max(self, other: Self) -> Self {
        if self > other {
            self
        } else {
            other
        }
    }
}
impl std::ops::Add<Duration> for Instant {
    type Output = Self;
    fn add(self, duration: Duration) -> Self { Self(self.0 + duration.as_secs_f64() * 1000.0) }
}
async fn sleep(duration: Duration) {
    gloo_timers::future::TimeoutFuture::new(duration.as_micros().div_ceil(1000).min(u32::MAX as u128) as u32).await;
}

#[wasm_bindgen]
pub async fn query_parity_browser(
    db_prefix: &str,
    n: usize,
    cache: &str,
    mode: &str,
    rtt_ms: u32,
    bytes_per_second: u32,
    label: &str,
) -> Result<String, JsValue> {
    sample(db_prefix, n, cache, mode, rtt_ms as u64, bytes_per_second as u64, label)
        .await
        .map_err(|error| JsValue::from_str(&format!("{error:#}")))
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct StartupRow {
    pub position: i64,
    pub body: String,
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
    query_subscriptions_applied: HashMap<proto::QueryId, usize>,
    local_result_installed_at: Option<Instant>,
    last_activity: Option<Instant>,
    uplink_free: Option<Instant>,
    downlink_free: Option<Instant>,
    traffic: Traffic,
    errors: Vec<String>,
}

type TestNode = Node<IndexedDBStorageEngine, PermissiveAgent>;

#[derive(Clone)]
struct Sender {
    target: ankurah::core::node::WeakNode<IndexedDBStorageEngine, PermissiveAgent>,
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
        wasm_bindgen_futures::spawn_local(async move {
            if arrival > Instant::now() {
                sleep(arrival.duration_since(Instant::now())).await;
            }
            let current = {
                let meter = meter.lock().unwrap();
                meter.connected && meter.generation == generation
            };
            if current {
                let message = bincode::deserialize(&bytes).expect("decode node message");
                let subscribed = match &message {
                    proto::NodeMessage::Response(response) => match &response.body {
                        proto::NodeResponseBody::QuerySubscribed { query_id, .. } => Some(*query_id),
                        _ => None,
                    },
                    _ => None,
                };
                let result = node.handle_message(message).await;
                let mut meter = meter.lock().unwrap();
                if meter.connected && meter.generation == generation {
                    if let Err(error) = result {
                        meter.errors.push(error.to_string());
                    } else if let Some(query_id) = subscribed {
                        meter.subscriptions_applied += 1;
                        *meter.query_subscriptions_applied.entry(query_id).or_default() += 1;
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

async fn sample(
    db_prefix: &str,
    n: usize,
    cache: &str,
    mode: &str,
    rtt_ms: u64,
    bytes_per_second: u64,
    label: &str,
) -> anyhow::Result<String> {
    anyhow::ensure!(["cold", "warm_equal", "warm_edit", "warm_membership", "warm_refresh"].contains(&mode), "unknown mode: {mode}");
    let server_name = format!("{db_prefix}_server");
    let client_name = format!("{db_prefix}_client");
    phase("server-create");
    let server = Node::new_durable(Arc::new(IndexedDBStorageEngine::open(&server_name).await?), PermissiveAgent::new());
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
    let client = Node::new(Arc::new(IndexedDBStorageEngine::open(&client_name).await?), PermissiveAgent::new());
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
    settle(&meter, 0, None).await?;
    let retained_query = if mode == "cold" {
        None
    } else {
        phase("warm-tree-initialize");
        let query = reader.query_wait::<StartupRowView>("position >= 0").await?;
        query.wait_durable_answered().await?;
        settle(&meter, 1, None).await?;
        if mode != "warm_refresh" {
            disconnect(&server, &client, &meter);
            settle(&meter, 0, None).await?;
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
    // Reset public loading metadata without changing selection, version, state or tree.
    // A first loaded=true resultset notification then observes local installation;
    // dirty/membership broadcasts before final installation still see loaded=false.
    let _installation_guard = if mode != "cold" && mode != "warm_refresh" {
        let resultset = retained_query.as_ref().expect("retained reconnect query").resultset();
        resultset.set_loaded(false);
        let observed = retained_query.as_ref().unwrap().resultset();
        let meter = meter.clone();
        Some(resultset.listen(Arc::new(move |_| {
            if observed.is_loaded() {
                let mut meter = meter.lock().unwrap();
                if meter.measuring && meter.local_result_installed_at.is_none() {
                    meter.local_result_installed_at = Some(Instant::now());
                }
            }
        })))
    } else {
        None
    };
    let retained_query_id = retained_query.as_ref().map(|query| query.query_id());
    let (subscriptions_before, query_subscriptions_before) = {
        let mut meter = meter.lock().unwrap();
        anyhow::ensure!(meter.errors.is_empty(), "setup peer errors: {:?}", meter.errors);
        meter.traffic = Traffic::default();
        meter.measuring = true;
        let query_count = retained_query_id.map(|id| meter.query_subscriptions_applied.get(&id).copied().unwrap_or(0));
        (meter.subscriptions_applied, query_count)
    };
    let required_query_subscription = retained_query_id.zip(query_subscriptions_before).map(|(id, count)| (id, count + 1));
    phase("measure");
    let start = Instant::now();
    let (query, ready_us) = if let Some(query) = retained_query {
        if mode == "warm_refresh" {
            query.update_selection_wait("position >= 0").await?;
            query.wait_durable_answered().await?;
            let ready_us = start.elapsed().as_micros();
            (query, Some(ready_us))
        } else {
            connect(&server, &client, rtt_ms, bytes_per_second, &meter).await?;
            (query, None)
        }
    } else {
        let query = reader.query_wait::<StartupRowView>("position >= 0").await?;
        query.wait_durable_answered().await?;
        let ready_us = start.elapsed().as_micros();
        (query, Some(ready_us))
    };
    let settled = settle(&meter, subscriptions_before + 1, required_query_subscription).await?;
    let initial_drained_us = settled.duration_since(start).as_micros();
    let settled_traffic = meter.lock().unwrap().traffic.clone();
    let local_result_installed_us = if ready_us.is_none() {
        phase("wait-local-result-installed");
        loop {
            if let Some(error) = query.error().value() {
                anyhow::bail!("reconnect initialization failed: {error}");
            }
            let installed = {
                let meter = meter.lock().unwrap();
                anyhow::ensure!(meter.errors.is_empty(), "peer errors: {:?}", meter.errors);
                meter.local_result_installed_at
            };
            if let Some(installed) = installed {
                break Some(installed.duration_since(start).as_micros());
            }
            sleep(Duration::from_millis(1)).await;
        }
    } else {
        None
    };
    phase("verify-applied");
    let verification_start = Instant::now();
    let verification_attempts = expected.len();

    // Both landmarks precede verification: versioned durable-ready for primary
    // cases and a fresh loaded=true local installation notification for reconnect.
    // A mismatched set or state at that landmark fails; verification never waits
    // for a later repair that would make the recorded installation too early.
    anyhow::ensure!(query.ids().into_iter().collect::<BTreeSet<_>>() == expected, "installed query has incorrect authoritative membership");
    for id in &expected {
        let remote = server.storage.get_state(*id).await?;
        let local = client.storage.get_state(*id).await?;
        anyhow::ensure!(local.payload.state == remote.payload.state, "installed query has incorrect applied state for {id}");
    }
    let verification_us = verification_start.elapsed().as_micros();
    let verified_us = start.elapsed().as_micros();
    let elapsed_us = ready_us.or(local_result_installed_us).expect("ready or installed landmark");
    let final_settled = settle(&meter, subscriptions_before + 1, required_query_subscription).await?;
    let transport_settled_us = final_settled.duration_since(start).as_micros();
    let settled_us = transport_settled_us.max(ready_us.unwrap_or(verified_us));

    let traffic = {
        let mut meter = meter.lock().unwrap();
        meter.measuring = false;
        anyhow::ensure!(meter.errors.is_empty(), "measured peer errors: {:?}", meter.errors);
        std::mem::take(&mut meter.traffic)
    };
    let record = serde_json::json!({
        "label":label, "entities":n, "cache":cache, "cached_entities":cached,
        "mode":mode, "bandwidth_bytes_per_second":bytes_per_second,
        "rtt_ms":rtt_ms, "elapsed_us":elapsed_us,
        "ready_us":ready_us, "local_result_installed_us":local_result_installed_us, "initial_drained_us":initial_drained_us,
            "settled_us":settled_us, "transport_settled_us":transport_settled_us,
        "verified_us":verified_us, "verification_us":verification_us, "verification_attempts":verification_attempts, "verified":true,
        "traffic":traffic, "settled_traffic":settled_traffic, "storage":"IndexedDB", "timer":"performance.now",
    })
    .to_string();
    disconnect(&server, &client, &meter);
    drop(query);
    drop(reader);
    drop(writer);
    IndexedDBStorageEngine::cleanup(&client_name).await?;
    IndexedDBStorageEngine::cleanup(&server_name).await?;
    drop(client);
    drop(server);
    Ok(record)
}

fn phase(name: &str) { web_sys::console::debug_1(&format!("PARITY_PHASE {name}").into()); }

fn disconnect(server: &TestNode, client: &TestNode, meter: &Arc<Mutex<Meter>>) {
    meter.lock().unwrap().connected = false;
    server.deregister_peer(client.id);
    client.deregister_peer(server.id);
}

/// Public durable-answer flags are sticky on the old baseline across reconnect.
/// A handled QuerySubscribed response and drained link form a transport landmark.
/// Fresh local installation is observed separately; response delivery does not
/// prove activation. The 2 ms confirmation is excluded from the recorded time.
async fn settle(
    meter: &Arc<Mutex<Meter>>,
    subscriptions: usize,
    query_subscription: Option<(proto::QueryId, usize)>,
) -> anyhow::Result<Instant> {
    let quiet_begin = Instant::now();
    loop {
        {
            let meter = meter.lock().unwrap();
            anyhow::ensure!(meter.errors.is_empty(), "peer errors: {:?}", meter.errors);
            let query_handled =
                query_subscription.is_none_or(|(id, count)| meter.query_subscriptions_applied.get(&id).copied().unwrap_or(0) >= count);
            if meter.pending == 0 && meter.subscriptions_applied >= subscriptions && query_handled {
                let last = meter.last_activity.unwrap_or_else(Instant::now);
                if last.max(quiet_begin).elapsed() >= Duration::from_millis(2) {
                    return Ok(last);
                }
            }
        }
        sleep(Duration::from_millis(1)).await;
    }
}
