//! Query reconciliation transfers differences through bounded peer updates.
mod common;

use common::*;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Traffic {
    repairs: Vec<Vec<EntityId>>,
    equal_targets: usize,
    compared_pages: usize,
}

#[tokio::test]
async fn cold_equal_and_sparse_fetches_transfer_only_the_required_entities() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let source = server.context_async(DEFAULT_CONTEXT).await?;
    let transaction = source.begin();
    let mut ids = HashSet::new();
    for index in 0..300 {
        ids.insert(transaction.create(&Album { name: format!("album-{index:03}"), year: "2020".into() }).await?.id());
    }
    transaction.commit().await?;
    let client = ephemeral_sled_setup().await?;
    let traffic = Arc::new(Mutex::new(Traffic::default()));
    let observed = traffic.clone();
    let data_ids = ids.clone();
    let (_connection, _) = GatedConnection::new(&server, &client, move |message| {
        let mut traffic = observed.lock().unwrap();
        match message {
            proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::Comparison { deltas, .. }, .. }) => {
                let ids: Vec<_> = deltas.iter().map(|delta| delta.entity_id).filter(|id| data_ids.contains(id)).collect();
                if !ids.is_empty() {
                    traffic.repairs.push(ids);
                }
            }
            proto::NodeMessage::Response(proto::NodeResponse { body: proto::NodeResponseBody::Fetch(target), .. })
                if target.known_state.digest.count() == 300 && target.page.is_none() =>
            {
                traffic.equal_targets += 1
            }
            proto::NodeMessage::Response(proto::NodeResponse {
                body: proto::NodeResponseBody::Compared(Ok(proto::ComparisonReply::Pages(_))),
                ..
            }) => traffic.compared_pages += 1,
            _ => {}
        }
        false
    })
    .await;
    let target = client.context_async(DEFAULT_CONTEXT).await?;
    target.resolve_model_id::<Album>().await?;
    assert_eq!(target.fetch::<AlbumView>(nocache("true")?).await?.len(), 300);
    {
        let mut traffic = traffic.lock().unwrap();
        assert!(traffic.repairs.iter().all(|batch| batch.len() <= 256));
        assert_eq!(traffic.repairs.iter().flatten().copied().collect::<HashSet<_>>(), ids);
        assert_eq!(traffic.repairs.iter().map(Vec::len).sum::<usize>(), 300);
        *traffic = Traffic::default();
    }

    assert_eq!(target.fetch::<AlbumView>(nocache("true")?).await?.len(), 300);
    {
        let mut traffic = traffic.lock().unwrap();
        assert_eq!(traffic.equal_targets, 1);
        assert!(traffic.repairs.is_empty());
        assert_eq!(traffic.compared_pages, 0);
        *traffic = Traffic::default();
    }

    let changed = *ids.iter().min().unwrap();
    let transaction = source.begin();
    transaction.get::<Album>(&changed).await?.year()?.replace("2024")?;
    transaction.commit().await?;
    let fetched = target.fetch::<AlbumView>(nocache("true")?).await?;
    assert_eq!(fetched.iter().find(|album| album.id() == changed).unwrap().year()?, "2024");
    let traffic = traffic.lock().unwrap();
    assert_eq!(traffic.repairs.iter().flatten().copied().collect::<Vec<_>>(), vec![changed]);
    Ok(())
}

#[tokio::test]
async fn limited_fetch_repairs_departures_and_refills_after_order_changes() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let source = server.context_async(DEFAULT_CONTEXT).await?;
    let ids = create_albums(&source, 2020..=2025).await?;
    let client = ephemeral_sled_setup().await?;
    let _connection = LocalProcessConnection::new(&server, &client).await?;
    let target = client.context_async(DEFAULT_CONTEXT).await?;
    let selection = "year >= '2020' ORDER BY year ASC LIMIT 3";
    let fetch = || async {
        Ok::<_, anyhow::Error>(
            target.fetch::<AlbumView>(nocache(selection)?).await?.into_iter().map(|album| album.year().unwrap()).collect::<Vec<_>>(),
        )
    };
    assert_eq!(fetch().await?, ["2020", "2021", "2022"]);
    let transaction = source.begin();
    transaction.get::<Album>(&ids[1]).await?.year()?.replace("2010")?;
    transaction.get::<Album>(&ids[0]).await?.year()?.replace("2030")?;
    transaction.commit().await?;
    assert_eq!(fetch().await?, ["2022", "2023", "2024"]);
    assert_eq!(client.storage.get_state(ids[1]).await?.payload.state.head, server.storage.get_state(ids[1]).await?.payload.state.head);
    assert_eq!(client.storage.get_state(ids[0]).await?.payload.state.head, server.storage.get_state(ids[0]).await?.payload.state.head);
    assert_eq!(fetch().await?, ["2022", "2023", "2024"]);
    Ok(())
}

async fn next_repair(receiver: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>) -> anyhow::Result<Vec<u8>> {
    tokio::time::timeout(std::time::Duration::from_secs(5), receiver.recv()).await?.ok_or_else(|| anyhow::anyhow!("repair recorder closed"))
}

fn repair_ids(message: &[u8]) -> anyhow::Result<Vec<EntityId>> {
    let proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::Comparison { deltas, .. }, .. }) =
        bincode::deserialize(message)?
    else {
        anyhow::bail!("expected comparison delivery")
    };
    Ok(deltas.into_iter().map(|delta| delta.entity_id).collect())
}

#[tokio::test]
async fn delayed_and_duplicate_repairs_preserve_concurrent_offline_edits() -> anyhow::Result<()> {
    use ankurah::core::storage::StateScan;

    let server = durable_sled_setup().await?;
    let source = server.context_async(DEFAULT_CONTEXT).await?;
    let transaction = source.begin();
    let id = transaction.create(&Record { title: "original title".into(), artist: "original artist".into() }).await?.id();
    let genesis = transaction.commit_and_return_events().await?.into_iter().find(|event| event.entity_id == id).unwrap();
    let server_record = source.get::<RecordView>(id).await?;
    let client = ephemeral_sled_setup().await?;
    let connection = LocalProcessConnection::new(&server, &client).await?;
    let target = client.context_async(DEFAULT_CONTEXT).await?;
    let client_record = target.fetch::<RecordView>(nocache("true")?).await?.remove(0);
    assert!(client.storage.dump_entity_events(id).await?.is_empty(), "cold repair adopts state without its event history");
    drop(connection);

    // Keep both views resident while making genuinely concurrent offline edits.
    let transaction = target.begin();
    client_record.edit(&transaction)?.artist()?.set(&"local artist".to_owned())?;
    let local = transaction.commit_and_return_events().await?.remove(0);
    let transaction = source.begin();
    server_record.edit(&transaction)?.title()?.set(&"remote title".to_owned())?;
    let remote = transaction.commit_and_return_events().await?.remove(0);
    assert_eq!(local.parent, remote.parent);
    assert_eq!(local.parent, proto::Clock::singleton(&genesis));

    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let (_connection, gate) = GatedConnection::new(&server, &client, move |message| {
        if matches!(message, proto::NodeMessage::Update(proto::NodeUpdate {
            body: proto::NodeUpdateBody::Comparison { deltas, .. }, ..
        }) if deltas.iter().any(|delta| delta.entity_id == id))
        {
            sent.send(bincode::serialize(message).unwrap()).unwrap();
            return true;
        }
        false
    })
    .await;
    let fetch = {
        let target = target.clone();
        tokio::spawn(async move { target.fetch::<RecordView>(nocache("true").unwrap()).await })
    };
    let delivery = next_repair(&mut received).await?;
    assert_eq!(repair_ids(&delivery)?, vec![id]);
    assert!(!fetch.is_finished(), "a repair reply cannot complete fetch before its delivery");
    assert_eq!(client_record.title()?, "original title");
    assert_eq!(client_record.artist()?, "local artist");
    assert_eq!(client_record.entity().head(), proto::Clock::singleton(&local));
    gate.release_held(&client).await;
    let fetched = tokio::time::timeout(std::time::Duration::from_secs(5), fetch).await???;
    assert_eq!(fetched.len(), 1);
    assert_eq!(client_record.title()?, "remote title");
    assert_eq!(client_record.artist()?, "local artist");
    let merged = proto::Clock::new(vec![(local.generation(), local.id()), (remote.generation(), remote.id())])?;
    assert_eq!(client_record.entity().head(), merged);
    assert_eq!(client.storage.get_state(id).await?.payload.state.head, merged);
    assert_eq!(server.storage.get_state(id).await?.payload.state.head, proto::Clock::singleton(&remote));
    let expected: HashSet<_> = [genesis.id(), local.id(), remote.id()].into_iter().collect();
    assert_eq!(client.storage.dump_entity_events(id).await?.into_iter().map(|event| event.payload.id()).collect::<HashSet<_>>(), expected);

    let before_duplicate = client.storage.state_snapshot(StateScan::Changes(None)).await?.finish().await?;
    client.handle_message(client.connections_of(server.id().unwrap())[0], bincode::deserialize(&delivery)?).await?;
    assert_eq!(
        client.storage.state_snapshot(StateScan::Changes(None)).await?.finish().await?,
        before_duplicate,
        "replaying a consumed repair delivery must not publish another canonical change"
    );
    assert_eq!(client_record.entity().head(), merged);
    assert_eq!(client_record.artist()?, "local artist");

    // The local merged frontier covers the server's captured head. A repeated
    // fetch must preserve the extra local edit without retransmitting ancestry.
    let fetched = tokio::time::timeout(std::time::Duration::from_secs(5), target.fetch::<RecordView>(nocache("true")?)).await??;
    assert_eq!(fetched[0].entity().head(), merged);
    assert!(received.try_recv().is_err(), "a causally newer local member needs no repair");
    assert_eq!(client.storage.dump_entity_events(id).await?.len(), expected.len());
    Ok(())
}

#[tokio::test]
async fn cancelled_delivery_keeps_completed_batches_for_the_next_fetch() -> anyhow::Result<()> {
    use ankurah::core::{error::RetrievalError, storage::StateScan};
    use std::sync::atomic::{AtomicBool, Ordering};

    let server = durable_sled_setup().await?;
    let source = server.context_async(DEFAULT_CONTEXT).await?;
    let ids: HashSet<_> = create_albums(&source, 1900..2000).await?.into_iter().collect();
    let client = ephemeral_sled_setup().await?;
    let hold = Arc::new(AtomicBool::new(true));
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let data_ids = ids.clone();
    let paused = hold.clone();
    let (_connection, gate) = GatedConnection::new(&server, &client, move |message| {
        if matches!(message, proto::NodeMessage::Update(proto::NodeUpdate {
            body: proto::NodeUpdateBody::Comparison { deltas, .. }, ..
        }) if deltas.iter().any(|delta| data_ids.contains(&delta.entity_id)))
        {
            sent.send(bincode::serialize(message).unwrap()).unwrap();
            return paused.load(Ordering::SeqCst);
        }
        false
    })
    .await;
    let target = client.context_async(DEFAULT_CONTEXT).await?;
    target.resolve_model_id::<Album>().await?;
    let fetch = {
        let target = target.clone();
        tokio::spawn(async move { target.fetch::<AlbumView>(nocache("true").unwrap()).await })
    };
    let first = next_repair(&mut received).await?;
    let completed: HashSet<_> = repair_ids(&first)?.into_iter().collect();
    assert!(!completed.is_empty() && completed.len() < ids.len());
    gate.release_first(&client, 1).await;
    let lost = next_repair(&mut received).await?;
    let missing = repair_ids(&lost)?[0];
    assert!(completed.iter().all(|id| ids.contains(id)));
    assert!(matches!(client.storage.get_state(missing).await, Err(RetrievalError::EntityNotFound(_))));
    for id in &completed {
        assert_eq!(client.storage.get_state(*id).await?.payload.state.head, server.storage.get_state(*id).await?.payload.state.head);
    }
    fetch.abort();
    assert!(fetch.await.unwrap_err().is_cancelled());
    gate.release_held(&client).await;
    assert!(
        matches!(client.storage.get_state(missing).await, Err(RetrievalError::EntityNotFound(_))),
        "a late delivery from the cancelled operation is no longer admissible"
    );

    // An intervening write makes one completed member stale. The renewed fetch
    // must repair it and the outstanding members, retaining all other progress.
    let changed = *completed.iter().min().unwrap();
    let transaction = source.begin();
    transaction.get::<Album>(&changed).await?.year()?.replace("2025")?;
    let update = transaction.commit_and_return_events().await?.into_iter().find(|event| event.entity_id == changed).unwrap();
    hold.store(false, Ordering::SeqCst);
    let fetched = tokio::time::timeout(std::time::Duration::from_secs(5), target.fetch::<AlbumView>(nocache("true")?)).await??;
    assert_eq!(fetched.len(), ids.len());
    assert_eq!(fetched.iter().find(|album| album.id() == changed).unwrap().year()?, "2025");
    let mut repaired = Vec::new();
    while let Ok(message) = received.try_recv() {
        repaired.extend(repair_ids(&message)?);
    }
    let expected: HashSet<_> = ids.difference(&completed).copied().chain([changed]).collect();
    assert_eq!(repaired.len(), expected.len(), "completed current members must not be transferred again");
    assert_eq!(repaired.into_iter().collect::<HashSet<_>>(), expected);
    assert_eq!(client.storage.get_state(changed).await?.payload.state.head, proto::Clock::singleton(&update));
    assert!(client.storage.get_events(vec![update.id()]).await?.iter().any(|event| event.payload == update));

    let before_late_delivery = client.storage.state_snapshot(StateScan::Changes(None)).await?.finish().await?;
    for old in [first, lost] {
        client.handle_message(client.connections_of(server.id().unwrap())[0], bincode::deserialize(&old)?).await?;
    }
    assert_eq!(client.storage.state_snapshot(StateScan::Changes(None)).await?.finish().await?, before_late_delivery);
    assert_eq!(client.storage.get_state(changed).await?.payload.state.head, proto::Clock::singleton(&update));
    assert_eq!(fetched.iter().find(|album| album.id() == changed).unwrap().year()?, "2025");
    Ok(())
}
