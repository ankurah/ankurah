//! A client's copy of a query result synchronizing with its durable server's:
//! the server pushes the initial data on the update channel, the query is
//! ready once the copy matches or exceeds the server's account of its result,
//! and the client repairs what is lost.

mod busy;
#[path = "../common.rs"]
mod common;
mod fetch;
mod live;
mod loss;

use common::*;

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

/// Whether `message` is an update that pushes `entity`.
fn pushes(message: &proto::NodeMessage, entity: EntityId) -> bool {
    match message {
        proto::NodeMessage::Update(update) => {
            let proto::NodeUpdateBody::SubscriptionUpdate { items } = &update.body;
            items.iter().any(|item| item.entity_id == entity)
        }
        _ => false,
    }
}

fn sorted(mut ids: Vec<EntityId>) -> Vec<EntityId> {
    ids.sort();
    ids
}
