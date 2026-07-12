// Differential measurement for the D1 evidence block: the per-item cost of a
// no-op state delivery, which the ef68e081 persist fix made write
// unconditionally where the M2/M4 elision skipped the write. Run at HEAD and
// at ef68e081~1 (aa2e5cd4) with --nocapture; the per-item delta is the
// reintroduced no-op-delta write cost.

mod common;

use ankurah::core::node_applier::NodeApplier;
use ankurah::proto::{self};
use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent, View};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use std::sync::Arc;

use common::{Record, RecordView};

#[tokio::test]
#[ignore = "measurement harness for the D1 evidence block, run manually with --nocapture"]
async fn measure_noop_state_delivery_cost() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let ctx_s = server.context(c)?;
    let ctx_c = client.context(c)?;
    let _relay_context = ctx_c.query_wait::<RecordView>("title = 'no-such-title'").await?;

    let rec_id = {
        let trx = ctx_s.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        trx.commit().await?;
        id
    };
    let view = ctx_c.get::<RecordView>(rec_id).await?;
    assert_eq!(view.title().unwrap(), "t0");

    // The client's own current state, redelivered as a StateAndEvent item
    // with no events: the shared state-apply compares Equal (advanced =
    // false), which is exactly the no-op shape whose persist the fix made
    // unconditional.
    let collection = ctx_c.collection(&Record::collection()).await?;
    let stored = collection.get_state(rec_id).await?;
    let fragment = proto::StateFragment { state: stored.payload.state.clone(), attestations: stored.attestations.clone() };
    let item = proto::SubscriptionUpdateItem {
        entity_id: rec_id,
        model: stored.payload.model,
        content: proto::UpdateContent::StateAndEvent(fragment, vec![]),
        predicate_relevance: vec![],
    };

    const WARMUP: usize = 200;
    const N: usize = 2000;
    for _ in 0..WARMUP {
        NodeApplier::apply_updates_for_test(&client, &server.id, vec![item.clone()]).await.expect("no-op item applies");
    }
    let start = std::time::Instant::now();
    for _ in 0..N {
        NodeApplier::apply_updates_for_test(&client, &server.id, vec![item.clone()]).await.expect("no-op item applies");
    }
    let elapsed = start.elapsed();
    println!("NOOP-DELTA n={N} total={elapsed:?} per_item={:?}", elapsed / N as u32);

    Ok(())
}
