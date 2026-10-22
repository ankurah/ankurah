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
    collections::{BTreeMap, BTreeSet, HashMap},
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
    limit: Option<usize>,
) -> Result<String, JsValue> {
    sample(db_prefix, n, cache, mode, rtt_ms as u64, bytes_per_second as u64, label, limit)
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
    subscriptions_handled: usize,
    query_subscriptions_handled: HashMap<proto::QueryId, usize>,
    local_result_installed_at: Option<Instant>,
    local_result_states: Option<BTreeMap<proto::EntityId, proto::State>>,
    local_result_order: Option<Vec<proto::EntityId>>,
    local_result_capture_us: Option<u128>,
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
                        meter.subscriptions_handled += 1;
                        *meter.query_subscriptions_handled.entry(query_id).or_default() += 1;
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
    limit: Option<usize>,
) -> anyhow::Result<String> {
    anyhow::ensure!(["cold", "warm_equal", "warm_edit", "warm_membership", "warm_refresh"].contains(&mode), "unknown mode: {mode}");
    // Numeric ORDER BY is a known baseline defect; bounded cases use one string predicate/order.
    let predicate = if limit.is_some() { "body >= 'row-'" } else { "position >= 0" };
    let selection = limit.map_or_else(|| predicate.to_owned(), |limit| format!("{predicate} ORDER BY body ASC LIMIT {limit}"));
    let result_count = limit.unwrap_or(n).min(n);
    anyhow::ensure!(
        !matches!(mode, "warm_edit" | "warm_membership") || result_count > 0,
        "warm mutation fixtures require at least one result"
    );
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
        if position < result_count {
            expected.insert(ankurah::Mutable::id(&*row));
        }
        rows.push(row.read()?);
    }
    trx.commit().await?;
    let mut expected_order = limit.map(|_| rows.iter().take(result_count).map(|row| row.id()).collect::<Vec<_>>());
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
        "half" => {
            let selection = if limit.is_some() {
                format!("body >= 'row-' AND body < 'row-{:08}'", result_count / 2)
            } else {
                format!("position < {}", result_count / 2)
            };
            reader.fetch::<StartupRowView>(selection).await?.len()
        }
        "half_blocks" => {
            let intervals = (0..result_count)
                .step_by(512)
                .map(|start| {
                    let end = (start + 256).min(result_count);
                    if limit.is_some() {
                        format!("(body >= 'row-{start:08}' AND body < 'row-{end:08}')")
                    } else {
                        format!("(position >= {start} AND position < {end})")
                    }
                })
                .collect::<Vec<_>>();
            let wanted: BTreeSet<_> = rows
                .iter()
                .take(result_count)
                .enumerate()
                .filter(|(position, _)| position / 256 % 2 == 0)
                .map(|(_, row)| row.id())
                .collect();
            let mut cached_ids = BTreeSet::new();
            for batch in intervals.chunks(16) {
                cached_ids.extend(reader.fetch::<StartupRowView>(batch.join(" OR ")).await?.into_iter().map(|row| row.id()));
            }
            assert_eq!(cached_ids, wanted, "gapped prefetch membership");
            cached_ids.len()
        }
        "half_interleaved" => {
            let positions = (0..result_count)
                .step_by(2)
                .map(|position| if limit.is_some() { format!("body = 'row-{position:08}'") } else { format!("position = {position}") })
                .collect::<Vec<_>>();
            let wanted: BTreeSet<_> =
                rows.iter().take(result_count).enumerate().filter(|(position, _)| position % 2 == 0).map(|(_, row)| row.id()).collect();
            let mut cached_ids = BTreeSet::new();
            for batch in positions.chunks(32) {
                cached_ids.extend(reader.fetch::<StartupRowView>(batch.join(" OR ")).await?.into_iter().map(|row| row.id()));
            }
            assert_eq!(cached_ids, wanted, "gapped prefetch membership");
            cached_ids.len()
        }
        "all" => {
            let fetched = reader.fetch::<StartupRowView>(selection.as_str()).await?;
            if let Some(order) = &expected_order {
                anyhow::ensure!(
                    fetched.iter().map(|row| row.id()).collect::<Vec<_>>() == *order,
                    "prefetch has incorrect ordered membership"
                );
            }
            fetched.len()
        }
        // Separate exploratory case: an unlimited resident tree precedes a bounded query.
        "full" => reader.fetch::<StartupRowView>(predicate).await?.len(),
        _ => anyhow::bail!("unknown cache mode: {cache}"),
    };
    assert_eq!(
        cached,
        match cache {
            "empty" => 0,
            "half" => result_count / 2,
            "half_blocks" => (0..result_count).filter(|position| position / 256 % 2 == 0).count(),
            "half_interleaved" => result_count.div_ceil(2),
            "full" => n,
            _ => result_count,
        },
        "prefetch coverage"
    );
    settle(&meter, 0, None).await?;
    let retained_query = if mode == "cold" {
        None
    } else {
        phase("initial-query");
        let query = reader.query_wait::<StartupRowView>(selection.as_str()).await?;
        query.wait_durable_answered().await?;
        settle(&meter, 1, None).await?;
        if mode != "warm_refresh" {
            disconnect(&server, &client, &meter);
            settle(&meter, 0, None).await?;
        }
        if mode == "warm_edit" || mode == "warm_membership" {
            let trx = writer.begin();
            let edited = result_count / 2;
            if limit.is_some() {
                // Mutate payload without touching the bounded selection's string sort key.
                rows[edited].edit(&trx)?.position()?.set(&(n as i64 + 1))?;
            } else {
                rows[edited].edit(&trx)?.body()?.replace("offline-edit")?;
            }
            if mode == "warm_membership" {
                if limit.is_some() {
                    rows[0].edit(&trx)?.body()?.replace("excluded")?;
                } else {
                    rows[0].edit(&trx)?.position()?.set(&-1)?;
                }
                let body = if limit.is_some() { format!("row-{n:08}") } else { "offline-added".to_owned() };
                let added = trx.create(&StartupRow { position: n as i64, body }).await?;
                let added_id = ankurah::Mutable::id(&*added);
                if let Some(limit) = limit {
                    // Removing the first result pulls the next original row into the window;
                    // the new row at position n stays outside unless the window reaches it.
                    let order = rows.iter().skip(1).map(|row| row.id()).chain(std::iter::once(added_id)).take(limit).collect::<Vec<_>>();
                    expected = order.iter().copied().collect();
                    expected_order = Some(order);
                } else {
                    expected.remove(&rows[0].id());
                    expected.insert(added_id);
                }
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
                    // Capture before any transport drain or asynchronous verifier can
                    // allow a later repair to rescue an early installation landmark.
                    let capture_start = Instant::now();
                    let result = observed.read();
                    meter.local_result_order = limit.map(|_| result.iter_entities().map(|(id, _)| id).collect());
                    let states =
                        result.iter_entities().map(|(id, entity)| Ok((id, entity.to_state()?))).collect::<anyhow::Result<BTreeMap<_, _>>>();
                    meter.local_result_capture_us = Some(capture_start.elapsed().as_micros());
                    match states {
                        Ok(states) => meter.local_result_states = Some(states),
                        Err(error) => meter.errors.push(format!("installation state capture: {error}")),
                    }
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
        let query_count = retained_query_id.map(|id| meter.query_subscriptions_handled.get(&id).copied().unwrap_or(0));
        (meter.subscriptions_handled, query_count)
    };
    let required_query_subscription = retained_query_id.zip(query_subscriptions_before).map(|(id, count)| (id, count + 1));
    phase("measure");
    let start = Instant::now();
    let (query, ready_us) = if let Some(query) = retained_query {
        if mode == "warm_refresh" {
            query.update_selection_wait(selection.as_str()).await?;
            query.wait_durable_answered().await?;
            let ready_us = start.elapsed().as_micros();
            (query, Some(ready_us))
        } else {
            connect(&server, &client, rtt_ms, bytes_per_second, &meter).await?;
            (query, None)
        }
    } else {
        let query = reader.query_wait::<StartupRowView>(selection.as_str()).await?;
        query.wait_durable_answered().await?;
        let ready_us = start.elapsed().as_micros();
        (query, Some(ready_us))
    };
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

    // Reconnect captures synchronously in the first loaded=true callback after
    // recording its timestamp. Primary cases capture immediately after the
    // versioned wait, without awaiting response bookkeeping or a transport drain.
    // Each entity's full state is atomic; the collection is not one global
    // snapshot. This fixture performs no writes after measurement begins.
    let (captured_states, captured_order) = if ready_us.is_none() {
        let mut meter = meter.lock().unwrap();
        (meter.local_result_states.take().expect("installation callback captured states"), meter.local_result_order.take())
    } else {
        let resultset = query.resultset();
        let result = resultset.read();
        let order = limit.map(|_| result.iter_entities().map(|(id, _)| id).collect::<Vec<_>>());
        let states = result.iter_entities().map(|(id, entity)| Ok((id, entity.to_state()?))).collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        (states, order)
    };
    if let Some(order) = &expected_order {
        anyhow::ensure!(
            captured_order.as_ref() == Some(order),
            "installed query has incorrect authoritative ordering: expected seeded positions {:?}, captured {:?}; expected {} states, captured {}",
            order.iter().map(|id| rows.iter().position(|row| row.id() == *id)).collect::<Vec<_>>(),
            captured_order.iter().flatten().map(|id| rows.iter().position(|row| row.id() == *id)).collect::<Vec<_>>(),
            expected.len(),
            captured_states.len()
        );
    }
    anyhow::ensure!(
        captured_states.keys().copied().collect::<BTreeSet<_>>() == expected,
        "installed query has incorrect authoritative membership"
    );
    for id in &expected {
        let remote = server.storage.get_state(*id).await?;
        anyhow::ensure!(captured_states[id] == remote.payload.state, "installation capture has incorrect applied state for {id}");
        // Persistence is a post-landmark check; these async reads do not claim
        // to observe a globally atomic state at the recorded timestamp.
        let local = client.storage.get_state(*id).await?;
        anyhow::ensure!(local.payload.state == remote.payload.state, "installed query has incorrect persisted state for {id}");
    }
    let verification_us = verification_start.elapsed().as_micros();
    let verified_us = start.elapsed().as_micros();
    let elapsed_us = ready_us.or(local_result_installed_us).expect("ready or installed landmark");
    // Own QuerySubscribed and transport quiescence validate this activation
    // after its captured membership/state have already been checked.
    let drained_at = settle(&meter, subscriptions_before + 1, required_query_subscription).await?;
    let transport_settled_us = drained_at.duration_since(start).as_micros();
    let settled_traffic = meter.lock().unwrap().traffic.clone();
    // Legacy initial/final fields refer to this single post-verification drain.
    let settled_us = transport_settled_us.max(ready_us.unwrap_or(verified_us));

    let (traffic, local_result_capture_us) = {
        let mut meter = meter.lock().unwrap();
        meter.measuring = false;
        anyhow::ensure!(meter.errors.is_empty(), "measured peer errors: {:?}", meter.errors);
        (std::mem::take(&mut meter.traffic), meter.local_result_capture_us)
    };
    let record = serde_json::json!({
        "label":label, "entities":n, "result_limit":limit, "expected_results":expected.len(), "selection":selection,
            "cache":cache, "cached_entities":cached,
        "mode":mode, "bandwidth_bytes_per_second":bytes_per_second,
        "rtt_ms":rtt_ms, "elapsed_us":elapsed_us,
        "ready_us":ready_us, "local_result_installed_us":local_result_installed_us,
            "local_result_capture_us":local_result_capture_us, "initial_drained_us":transport_settled_us,
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
                query_subscription.is_none_or(|(id, count)| meter.query_subscriptions_handled.get(&id).copied().unwrap_or(0) >= count);
            if meter.pending == 0 && meter.subscriptions_handled >= subscriptions && query_handled {
                let last = meter.last_activity.unwrap_or_else(Instant::now);
                if last.max(quiet_begin).elapsed() >= Duration::from_millis(2) {
                    return Ok(last);
                }
            }
        }
        sleep(Duration::from_millis(1)).await;
    }
}
