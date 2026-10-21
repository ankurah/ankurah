//! Selection updates must offer a tree layout the responder accepts.
mod common;
use anyhow::Result;
use common::*;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

async fn selection_update(initial: &str, updated: &str, expected_keys: &[&str], expected_titles: &[&str]) -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    server.wait_ready().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    for (title, artist) in [("row-a", "artist-c"), ("row-b", "artist-b"), ("row-c", "artist-a")] {
        trx.create(&Record { title: title.into(), artist: artist.into() }).await?;
    }
    trx.commit().await?;

    let offers = Arc::new(Mutex::new(Vec::new()));
    let (_connection, _) = GatedConnection::new(&client, &server, {
        let offers = offers.clone();
        move |message| {
            if let proto::NodeMessage::Request { request, .. } = message {
                if let proto::NodeRequestBody::SubscribeQuery { query_id, known_state, .. } = &request.body {
                    offers.lock().unwrap().push((*query_id, known_state.spec.clone()));
                }
            }
            false
        }
    })
    .await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let query = reader.query::<RecordView>(nocache(initial)?)?;
    tokio::time::timeout(Duration::from_secs(10), query.wait_durable_answered()).await??;
    let first = offers.lock().unwrap().iter().find(|(id, _)| *id == query.query_id()).unwrap().1.clone();
    assert_eq!(first.key_spec().keyparts.len(), 1);
    assert_eq!(first.key_spec().keyparts[0].key, resolved_prop(&client, Record::descriptor(), "title"));

    query.update_selection(updated)?;
    tokio::time::timeout(Duration::from_secs(10), query.wait_durable_answered()).await??;
    let actual = query.peek().iter().map(|row| row.title()).collect::<Result<Vec<_>, _>>()?;
    assert_eq!(actual, expected_titles);
    let offers = offers.lock().unwrap();
    let current = &offers.iter().rev().find(|(id, _)| *id == query.query_id()).unwrap().1;
    assert_eq!(current.predicate(), first.predicate(), "only order/limit changed");
    let keys = current.key_spec().keyparts.iter().map(|part| part.key).collect::<Vec<_>>();
    let expected = expected_keys.iter().map(|name| resolved_prop(&client, Record::descriptor(), name)).collect::<Vec<_>>();
    assert_eq!(keys, expected, "the cached layout must change with negotiation policy");
    assert_ne!(current, &first);
    Ok(())
}

#[tokio::test]
async fn order_update_replaces_incompatible_cached_tree() -> Result<()> {
    selection_update(
        "title >= 'row-' AND artist >= 'artist-' ORDER BY title",
        "title >= 'row-' AND artist >= 'artist-' ORDER BY artist",
        &["artist"],
        &["row-c", "row-b", "row-a"],
    )
    .await
}

#[tokio::test]
async fn adding_limit_replaces_incompatible_cached_tree() -> Result<()> {
    selection_update(
        "title >= 'row-' ORDER BY title, artist",
        "title >= 'row-' ORDER BY title, artist LIMIT 2",
        &["title", "artist"],
        &["row-a", "row-b"],
    )
    .await
}
