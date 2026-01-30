use ankurah::signals::With;
use ankurah::{policy::DEFAULT_CONTEXT as c, Mutable, Node, PermissiveAgent};
use ankurah_connector_local_process::{LocalProcessConnection, MessageTransform};
use ankurah_storage_sled::SledStorageEngine;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

mod common;
use crate::common::*;

#[tokio::test]
async fn rt_missing_events_from_state_snapshot() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Server (durable) and client (ephemeral)
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client_storage = Arc::new(SledStorageEngine::new_test().unwrap());
    let client = Node::new(client_storage.clone(), PermissiveAgent::new());

    let strip_known_matches = Arc::new(AtomicBool::new(false));
    let empty_getevents = Arc::new(AtomicBool::new(false));
    let to_server: MessageTransform = {
        let strip_known_matches = Arc::clone(&strip_known_matches);
        Arc::new(move |message| {
            if !strip_known_matches.load(Ordering::SeqCst) {
                return message;
            }
            match message {
                ankurah::proto::NodeMessage::Request { auth, mut request } => {
                    if let ankurah::proto::NodeRequestBody::SubscribeQuery { query_id, collection, selection, version, .. } = request.body {
                        request.body = ankurah::proto::NodeRequestBody::SubscribeQuery {
                            query_id,
                            collection,
                            selection,
                            version,
                            known_matches: Vec::new(),
                        };
                    }
                    ankurah::proto::NodeMessage::Request { auth, request }
                }
                _ => message,
            }
        })
    };
    let to_client: MessageTransform = {
        let empty_getevents = Arc::clone(&empty_getevents);
        Arc::new(move |message| {
            if !empty_getevents.load(Ordering::SeqCst) {
                return message;
            }
            match message {
                ankurah::proto::NodeMessage::Response(mut response) => {
                    if matches!(response.body, ankurah::proto::NodeResponseBody::GetEvents(_)) {
                        response.body = ankurah::proto::NodeResponseBody::GetEvents(Vec::new());
                    }
                    ankurah::proto::NodeMessage::Response(response)
                }
                _ => message,
            }
        })
    };
    let _conn = LocalProcessConnection::new_with_transform(&server, &client, to_server, to_client).await?;
    client.system.wait_system_ready().await;

    let server_ctx = server.context(c)?;
    let client_ctx = client.context(c)?;

    // Seed initial state on the client (state snapshot with no events stored).
    {
        let trx = server_ctx.begin();
        let album = trx.create(&Album { name: "Test Album".into(), year: "2020".into() }).await?;
        let album: AlbumView = album.read();
        trx.commit().await?;

        let fetched = client_ctx.fetch::<AlbumView>(nocache("year >= '2020'")?).await?;
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].id(), album.id());
    }

    // Mutate server state to advance the head.
    {
        let trx = server_ctx.begin();
        let album = server_ctx.fetch_one::<AlbumView>(nocache("year >= '2020'")?).await?.expect("album exists");
        album.edit(&trx)?.year().overwrite(0, 4, "2021")?;
        trx.commit().await?;
    }

    // Adversarial behavior: strip known_matches so server sends StateSnapshot,
    // and return empty GetEvents so lineage comparison can't resolve missing events.
    strip_known_matches.store(true, Ordering::SeqCst);
    empty_getevents.store(true, Ordering::SeqCst);

    let livequery = client_ctx.query::<AlbumView>(nocache("year >= '2020'")?)?;

    let err_string = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(err) = livequery.error().with(|e| e.as_ref().map(|e| e.to_string())) {
                break err;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await?;

    assert!(err_string.contains("Events not found"), "unexpected error: {err_string}");
    Ok(())
}
