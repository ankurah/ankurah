//! Large entities and large results: an entity larger than an update's data
//! travels in an update of its own, and a result with more members than an
//! account lists heads for is described by span digests.

use super::*;

#[tokio::test]
async fn an_entity_larger_than_an_update_is_pushed_alone_and_applied_before_the_fetch_returns() -> anyhow::Result<()> {
    let received = Received::default();
    let (server, client, _connection, _gate) = {
        let received = received.clone();
        gated(move |message| {
            received.record(message);
            false
        })
        .await?
    };
    let (server, client) = (server.context_async(DEFAULT_CONTEXT).await?, client.context_async(DEFAULT_CONTEXT).await?);
    // A title about a tenth larger than the item data one update carries.
    let payload = "x".repeat(1126 * 1024);
    let trx = server.begin();
    let large = trx.create(&Record { title: payload.clone(), artist: "large".into() }).await?.id();
    let mut ids = vec![large];
    for n in 0..3 {
        ids.push(trx.create(&Record { title: format!("small {n}"), artist: "small".into() }).await?.id());
    }
    trx.commit().await?;

    let fetched = client.fetch::<RecordView>("true").await?;
    assert_eq!(sorted(fetched.iter().map(|record| record.id()).collect()), sorted(ids.clone()));
    assert_eq!(
        fetched.iter().find(|record| record.id() == large).unwrap().title()?,
        payload,
        "the large entity applied before the Fetch returned"
    );
    let updates: Vec<_> = received.updates().into_iter().filter(|update| update.iter().any(|item| ids.contains(&item.entity_id))).collect();
    let carrying = updates.iter().find(|update| update.iter().any(|item| item.entity_id == large)).unwrap();
    assert_eq!(carrying.len(), 1, "the large entity travels alone");
    assert!(updates.len() > 1, "and the small ones in another update");
    Ok(())
}

#[tokio::test]
async fn a_result_with_more_members_than_an_account_lists_is_described_and_synced_by_span_digests() -> anyhow::Result<()> {
    let received = Received::default();
    let (server_node, client_node, _connection, _gate) = {
        let received = received.clone();
        gated(move |message| {
            received.record(message);
            false
        })
        .await?
    };
    let (server, client) = (server_node.context_async(DEFAULT_CONTEXT).await?, client_node.context_async(DEFAULT_CONTEXT).await?);
    // The rows the copy will hold, one more than an account lists heads for,
    // and every 33rd row among them, which it will not.
    let trx = server.begin();
    let mut ids = Vec::new();
    for n in 0..2113 {
        let artist = if n % 33 == 32 { "missed" } else { "held" };
        ids.push(trx.create(&Record { title: format!("row {n:06}"), artist: artist.into() }).await?.id());
    }
    trx.commit().await?;

    // A cold Fetch of the held rows, whose account is a frontier of digests.
    assert_eq!(client.fetch::<RecordView>("artist = 'held'").await?.len(), 2049);
    let account = received
        .responses()
        .into_iter()
        .find_map(|body| match body {
            proto::NodeResponseBody::Fetch(account)
                if !account.spans.is_empty() && account.spans.iter().all(|span| matches!(span, proto::SpanContent::Digest(_))) =>
            {
                Some(account)
            }
            _ => None,
        })
        .expect("the held rows' account describes them by span digests");
    let described: i64 =
        account.spans.iter().map(|span| if let proto::SpanContent::Digest(digest) = span { digest.digest.count() } else { 0 }).sum();
    assert_eq!(described, 2049, "its digests sum to the result");

    // The copy holds all but the interleaved rows of the whole result, so its
    // offer is a frontier of digests within which the server pushes nothing:
    // the client repairs each span to every row.
    let query = client.query::<RecordView>(nocache("title >= 'row ' ORDER BY title ASC")?)?;
    tokio::time::timeout(Duration::from_secs(30), query.wait_initialized()).await??;
    assert_eq!(sorted(query.ids()), sorted(ids.clone()));
    assert_eq!(heads(&client_node, &ids).await?, heads(&server_node, &ids).await?, "each row is stored at the server's head");
    Ok(())
}
