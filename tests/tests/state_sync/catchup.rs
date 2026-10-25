//! A copy catching up with a server that changed it while the client was
//! away or changes it while the sync runs: what the copy holds once ready.

use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

/// A server and a client whose gate, once `armed`, holds the answer to each
/// subscribe, which keeps that subscribe's sync unfinished.
async fn answers_held() -> anyhow::Result<(Sled, Sled, GatedConnection, MessageGate, Arc<AtomicBool>)> {
    let armed = Arc::new(AtomicBool::new(false));
    let (server, client, connection, gate) = {
        let armed = armed.clone();
        gated(move |message| armed.load(Ordering::SeqCst) && answers_subscribe(message)).await?
    };
    Ok((server, client, connection, gate, armed))
}

#[tokio::test]
async fn live_changes_that_cross_an_unfinished_sync_stand_once_it_is_ready() -> anyhow::Result<()> {
    let (server_node, client_node, _connection, gate, armed) = answers_held().await?;
    let (server, client) = (server_node.context_async(DEFAULT_CONTEXT).await?, client_node.context_async(DEFAULT_CONTEXT).await?);
    let ids = create_pets(&server, "1", (0..8).map(|n| format!("pet {n}"))).await?;
    armed.store(true, Ordering::SeqCst);
    let query = client.query::<PetView>(nocache("age >= '1'")?)?;
    wait_until("the answer is held", || gate.held() == 1).await;

    // While the sync waits for the server's account, one member changes and
    // another leaves; both changes reach the copy live.
    let trx = server.begin();
    trx.get::<Pet>(&ids[1]).await?.age()?.replace("3")?;
    trx.get::<Pet>(&ids[2]).await?.age()?.replace("0")?;
    trx.commit().await?;
    let changed = heads(&server_node, &ids[1..3]).await?;
    wait_for_heads(&client_node, &changed).await?;
    assert!(!query.loaded());

    gate.release_held(&client_node).await;
    tokio::time::timeout(Duration::from_secs(5), query.wait_initialized()).await??;
    let stayed: Vec<_> = ids.iter().copied().filter(|id| *id != ids[2]).collect();
    assert_eq!(sorted(query.ids()), sorted(stayed), "the account, older than the departure, does not bring the member back");
    let member = query.peek().into_iter().find(|pet| pet.id() == ids[1]).unwrap();
    assert_eq!(member.entity().head(), changed[&ids[1]], "the changed member keeps its newer head");
    Ok(())
}

#[tokio::test]
async fn a_member_that_returns_while_the_client_catches_up_is_a_member_once_ready() -> anyhow::Result<()> {
    let (server_node, client_node, _connection, gate, armed) = answers_held().await?;
    let (server, client) = (server_node.context_async(DEFAULT_CONTEXT).await?, client_node.context_async(DEFAULT_CONTEXT).await?);
    let id = create_pets(&server, "1", ["rex".to_string()]).await?[0];
    assert_eq!(client.fetch::<PetView>("age = '1'").await?.len(), 1);
    // The client holds no live query, so only its subscribe finds rex gone.
    let trx = server.begin();
    trx.get::<Pet>(&id).await?.age()?.replace("2")?;
    trx.commit().await?;

    armed.store(true, Ordering::SeqCst);
    let query = client.query::<PetView>(nocache("age = '1'")?)?;
    wait_until("the answer, which lists no member, is held", || gate.held() == 1).await;
    // While the sync waits for that answer, rex returns, and reaches the copy live.
    let trx = server.begin();
    trx.get::<Pet>(&id).await?.age()?.replace("1")?;
    trx.commit().await?;
    let returned = heads(&server_node, &[id]).await?;
    wait_for_heads(&client_node, &returned).await?;

    gate.release_held(&client_node).await;
    tokio::time::timeout(Duration::from_secs(5), query.wait_initialized()).await??;
    assert_eq!(query.ids(), [id], "rex is a member, though the account that answered lists none");
    assert_eq!(query.peek()[0].entity().head(), returned[&id]);
    Ok(())
}

#[tokio::test]
async fn a_subscribe_catches_up_many_edits_as_bridges_spread_over_updates_by_size() -> anyhow::Result<()> {
    let received = Received::default();
    let (server_node, client, _connection, _gate) = {
        let received = received.clone();
        gated(move |message| {
            received.record(message);
            false
        })
        .await?
    };
    let (server, client) = (server_node.context_async(DEFAULT_CONTEXT).await?, client.context_async(DEFAULT_CONTEXT).await?);
    let trx = server.begin();
    let mut ids = Vec::new();
    for n in 0..4 {
        ids.push(trx.create(&Record { title: format!("record {n}"), artist: "artist".into() }).await?.id());
    }
    trx.commit().await?;
    assert_eq!(client.fetch::<RecordView>("true").await?.len(), 4);

    // The client holds no live query, so these edits reach it only as its
    // subscribe's catch-up. Each carries a large title, so all four bridges
    // together outgrow one update's data.
    let edits = 10;
    for edit in 1..=edits {
        let trx = server.begin();
        for id in &ids {
            trx.get::<Record>(id).await?.title()?.set(&format!("{edit} {}", "x".repeat(32 * 1024)))?;
        }
        trx.commit().await?;
    }
    received.clear();
    let query = client.query_wait::<RecordView>(nocache("true")?).await?;

    let items = received.items_for(&ids);
    assert_eq!(sorted(items.iter().map(|item| item.entity_id).collect()), sorted(ids.clone()), "each record is pushed once");
    assert!(
        items.iter().all(|item| matches!(&item.content, proto::UpdateContent::EventOnly(events) if events.len() == edits)),
        "as a bridge of every edit the copy missed"
    );
    let updates = received.updates().into_iter().filter(|update| update.iter().any(|item| ids.contains(&item.entity_id))).count();
    assert!(updates > 1, "the bridges are spread over updates by their size");
    let current = heads(&server_node, &ids).await?;
    for record in query.peek() {
        assert_eq!(record.entity().head(), current[&record.id()]);
    }
    Ok(())
}

#[tokio::test]
async fn a_member_whose_key_moves_while_its_repair_is_under_way_arrives_at_its_new_key() -> anyhow::Result<()> {
    // Once armed, the gate holds the answer to the next Fetch.
    let armed = Arc::new(AtomicBool::new(false));
    let (server_node, client_node, _connection, gate) = {
        let armed = armed.clone();
        gated(move |message| {
            let fetch_answer =
                matches!(message, proto::NodeMessage::Response(response) if matches!(response.body, proto::NodeResponseBody::Fetch(_)));
            fetch_answer && armed.swap(false, Ordering::SeqCst)
        })
        .await?
    };
    let (server, client) = (server_node.context_async(DEFAULT_CONTEXT).await?, client_node.context_async(DEFAULT_CONTEXT).await?);
    // Members keyed by long names: listing their heads takes more work than
    // an account may carry, so the server's account describes the result by
    // span digests, within which it pushes nothing, and the client repairs
    // what differs.
    let pad = "x".repeat(1024);
    let ids = create_pets(&server, "1", (0..320).map(|n| format!("{n:04} {pad}"))).await?;
    let query = "name >= '' ORDER BY name ASC";
    assert_eq!(client.fetch::<PetView>(query).await?.len(), ids.len());

    // The copy falls behind on one member, so the next sync repairs it.
    let trx = server.begin();
    trx.get::<Pet>(&ids[7]).await?.age()?.replace("2")?;
    trx.commit().await?;
    armed.store(true, Ordering::SeqCst);
    let fetch = tokio::spawn({
        let client = client.clone();
        async move { client.fetch::<PetView>(query).await }
    });
    wait_until("the Fetch's answer is held", || gate.held() == 1).await;
    // After the answer and before the repair, the member's key moves to the
    // far end of the order.
    let trx = server.begin();
    trx.get::<Pet>(&ids[7]).await?.name()?.replace(&format!("9999 {pad}"))?;
    trx.commit().await?;
    let moved = heads(&server_node, &[ids[7]]).await?;
    gate.release_held(&client_node).await;

    let fetched = tokio::time::timeout(Duration::from_secs(10), fetch).await???;
    assert_eq!(sorted(fetched.iter().map(|pet| pet.id()).collect()), sorted(ids.clone()), "every member is held");
    let last = fetched.last().unwrap();
    assert_eq!((last.id(), last.entity().head()), (ids[7], moved[&ids[7]].clone()), "the moved member arrives at its new key");
    Ok(())
}
