//! A client's copy of a query result synchronizing with its durable server's:
//! the server pushes the initial data on the update channel, the query is
//! ready once the copy matches or exceeds the server's account of its result,
//! and the client repairs what is lost.

mod busy;
mod catchup;
#[path = "../common.rs"]
mod common;
mod credentials;
mod fetch;
mod large;
mod limits;
mod live;
mod loss;
mod pushes;
mod readiness;
mod reconnect;
mod sharing;
mod wire;

use common::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use wire::*;

type Sled = Node<SledStorageEngine, PermissiveAgent>;

/// A durable server and an ephemeral client, connected.
async fn connected(
) -> anyhow::Result<(Sled, Sled, LocalProcessConnection<SledStorageEngine, PermissiveAgent, SledStorageEngine, PermissiveAgent>)> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let connection = LocalProcessConnection::new(&server, &client).await?;
    client.wait_ready().await?;
    Ok((server, client, connection))
}

/// A durable server and an ephemeral client, connected through a gate that
/// holds each message to the client that `hold` matches.
async fn gated(
    hold: impl Fn(&proto::NodeMessage) -> bool + Send + Sync + 'static,
) -> anyhow::Result<(Sled, Sled, GatedConnection, MessageGate)> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let (connection, gate) = GatedConnection::new(&server, &client, hold).await;
    client.wait_ready().await?;
    Ok((server, client, connection, gate))
}

/// Pets named `names` and aged `age`, created on `context` in one transaction.
async fn create_pets(context: &Context, age: &str, names: impl IntoIterator<Item = String>) -> anyhow::Result<Vec<EntityId>> {
    let trx = context.begin();
    let mut ids = Vec::new();
    for name in names {
        ids.push(trx.create(&Pet { name, age: age.to_string() }).await?.id());
    }
    trx.commit().await?;
    Ok(ids)
}

fn sorted(mut ids: Vec<EntityId>) -> Vec<EntityId> {
    ids.sort();
    ids
}

/// The head `node` holds of each of `ids`.
async fn heads(node: &Sled, ids: &[EntityId]) -> anyhow::Result<BTreeMap<EntityId, proto::Clock>> {
    let mut heads = BTreeMap::new();
    for id in ids {
        heads.insert(*id, node.storage.get_state(*id).await?.payload.state.head);
    }
    Ok(heads)
}

/// Wait until `node` stores each entity in `heads` at the head beside it.
async fn wait_for_heads(node: &Sled, heads: &BTreeMap<EntityId, proto::Clock>) -> anyhow::Result<()> {
    let ids: Vec<_> = heads.keys().copied().collect();
    tokio::time::timeout(Duration::from_secs(10), async {
        while self::heads(node, &ids).await.ok().as_ref() != Some(heads) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("the node stores {heads:?}"))
}

/// Wait until `condition` holds, failing with `what` after ten seconds.
async fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    let held = tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(held.is_ok(), "{what}");
}
