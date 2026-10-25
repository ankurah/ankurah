//! Offline edits: a client that edited a few members of a large result while
//! away from its server syncs the result again within the Fetch's own round
//! trip, keeping its newer heads.

use super::*;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn a_warm_fetch_of_a_large_result_over_offline_edits_is_ready_without_compare_spans_or_sync_entities() -> anyhow::Result<()> {
    let (server_node, client_node, connection) = connected().await?;
    let (server, client) = (server_node.context_async(DEFAULT_CONTEXT).await?, client_node.context_async(DEFAULT_CONTEXT).await?);
    // More members than one account's heads, so both accounts are digest frontiers.
    let mut ids = Vec::new();
    for batch in 0..5 {
        ids.extend(create_pets(&server, "1", (0..450).map(|n| format!("pet {batch}-{n}"))).await?);
    }
    let held = client.fetch::<PetView>("age = '1'").await?;
    assert_eq!(held.len(), ids.len());

    // Away from its server the client has no durable peer to relay its
    // edits to, so they commit only locally.
    drop(connection);
    let edited = [0, 1000, 2000].map(|n| &held[n]);
    let trx = client.begin();
    for pet in edited {
        trx.edit::<Pet>(pet.entity())?.name()?.replace("edited offline")?;
    }
    trx.commit().await?;

    // Back on a connection whose gate records, and never holds, each query
    // request the client sends, with the query it concerns.
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (_connection, _gate) = {
        let requests = requests.clone();
        GatedConnection::new(&client_node, &server_node, move |message| {
            if let proto::NodeMessage::Request { request, .. } = message {
                if let proto::NodeRequestBody::Fetch { selection, .. }
                | proto::NodeRequestBody::CompareSpans { selection, .. }
                | proto::NodeRequestBody::SyncEntities { selection, .. } = &request.body
                {
                    requests.lock().unwrap().push((request.body.to_string(), selection.clone()));
                }
            }
            false
        })
        .await
    };
    let fetched = client.fetch::<PetView>("age = '1'").await?;

    assert_eq!(sorted(fetched.iter().map(|pet| pet.id()).collect()), sorted(ids.clone()));
    let edited = edited.map(|pet| pet.id());
    let names: Vec<_> = fetched.iter().filter(|pet| edited.contains(&pet.id())).map(|pet| pet.name().unwrap()).collect();
    assert_eq!(names, ["edited offline"; 3], "the copy keeps its newer heads");
    // The catalog's own queries resubscribe on reconnecting; only the
    // fetched query's requests count.
    let requests = requests.lock().unwrap().clone();
    let (_, query) = requests.iter().find(|(request, _)| request.starts_with("Fetch")).expect("the Fetch went out");
    let repairs: Vec<_> = requests
        .iter()
        .filter(|(request, selection)| selection == query && !request.starts_with("Fetch"))
        .map(|(request, _)| request)
        .collect();
    assert!(
        repairs.is_empty(),
        "the target listed the server's heads over the spans the edits changed, which the copy covers: {repairs:?}"
    );
    Ok(())
}
