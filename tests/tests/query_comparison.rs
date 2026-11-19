//! Query reconciliation transfers differences through bounded peer updates.
mod common;

use common::*;
use std::{collections::HashSet, sync::{Arc, Mutex}};

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
                if !ids.is_empty() { traffic.repairs.push(ids); }
            }
            proto::NodeMessage::Response(proto::NodeResponse { body: proto::NodeResponseBody::Fetch(target), .. })
                if target.known_state.digest.count() == 300 && target.page.is_none() => traffic.equal_targets += 1,
            proto::NodeMessage::Response(proto::NodeResponse { body: proto::NodeResponseBody::Compared(Ok(proto::ComparisonReply::Pages(_))), .. }) => traffic.compared_pages += 1,
            _ => {},
        }
        false
    }).await;
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
        Ok::<_, anyhow::Error>(target.fetch::<AlbumView>(nocache(selection)?).await?.into_iter().map(|album| album.year().unwrap()).collect::<Vec<_>>())
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
