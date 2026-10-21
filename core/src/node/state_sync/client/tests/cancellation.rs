use super::*;
use ankurah_signals::Peek;
use std::sync::atomic::{AtomicBool, Ordering};

fn fetch_reply(messages: &[proto::NodeMessage]) -> Option<(proto::RequestId, proto::StateReply)> {
    messages.iter().find_map(|message| match message {
        proto::NodeMessage::Response(proto::NodeResponse { request_id, body: proto::NodeResponseBody::Fetch(state), .. }) => {
            Some((request_id.clone(), state.clone()))
        }
        _ => None,
    })
}

fn abandoned(messages: &[proto::NodeMessage], comparison: proto::StateStreamId) -> usize {
    messages
        .iter()
        .filter(|message| {
            matches!(message,
                proto::NodeMessage::AbandonStateStream { stream: id, .. } if *id == comparison
            )
        })
        .count()
}

async fn assert_released_with_live_query(
    server: &StreamNode,
    client: &StreamNode,
    query: &crate::livequery::LiveQuery<StreamRowView>,
    messages: &Arc<std::sync::Mutex<Vec<proto::NodeMessage>>>,
    origin: proto::RequestId,
    state: proto::StateReply,
) -> anyhow::Result<()> {
    let comparison = state.frame.stream;
    let server_peer = server.peer_connections.get(&client.id).unwrap();
    let client_peer = client.peer_connections.get(&server.id).unwrap();
    until(|| server_peer.state_streams.was_closed(&comparison)).await;
    until(|| !client_peer.outgoing_state_streams.contains_key(&comparison)).await;
    assert!(server_peer.state_streams.get(&comparison).is_none());
    assert!(!client_peer.incoming_states.is_current(&origin));
    assert_eq!(abandoned(&messages.lock().unwrap(), comparison), 1);

    // Delayed duplicate metadata and an already scheduled expiry cannot revive
    // the Fetch or interfere with another stream on this connection.
    client
        .handle_message(proto::NodeMessage::Response(proto::NodeResponse {
            request_id: origin,
            from: server.id,
            to: client.id,
            body: proto::NodeResponseBody::Fetch(state),
        }))
        .await?;
    super::super::super::expire_state_stream(server, client.id, comparison).await?;
    let (live, revision) = messages
        .lock()
        .unwrap()
        .iter()
        .find_map(|message| match message {
            proto::NodeMessage::Response(proto::NodeResponse {
                body: proto::NodeResponseBody::QuerySubscribed { query_id, state },
                ..
            }) if *query_id == query.query_id() => Some((state.frame.stream, state.round.revision)),
            _ => None,
        })
        .unwrap();
    super::super::super::queue_live(
        server,
        client.id,
        live,
        revision + 1,
        vec![proto::QueryChange { entity_id: id(0), member: None, delta: None }],
    )?;
    until(|| query.resultset().len() == 63).await;
    assert!(query.resultset().is_loaded());
    assert!(Arc::ptr_eq(&server_peer, &server.peer_connections.get(&client.id).unwrap()));
    assert!(Arc::ptr_eq(&client_peer, &client.peer_connections.get(&server.id).unwrap()));
    assert_eq!(abandoned(&messages.lock().unwrap(), comparison), 1);
    Ok(())
}

#[tokio::test]
async fn cancelled_fetch_before_metadata_abandons_the_late_reply() -> anyhow::Result<()> {
    let (server, client, query, messages) = stream_fixture().await?;
    let mut selection = query.selection().peek().unwrap().0;
    // Use a distinct selection so the existing standing tree is not a cache hit.
    selection.predicate = ankql::ast::Predicate::And(Box::new(selection.predicate), Box::new(ankql::ast::Predicate::True));
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    server.storage.hold_fetch.lock().unwrap().insert(proto::ModelId::EntityId(id(99)), (entered, release_rx));
    let credentials = vec![crate::policy::DEFAULT_CONTEXT];
    let mut fetch = Box::pin(client.fetch_from_peer(selection, &credentials));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::select! {
            entered = entered_rx => entered.unwrap(),
            result = &mut fetch => panic!("Fetch finished before the capture gate: {result:?}"),
        }
    })
    .await?;
    assert!(fetch_reply(&messages.lock().unwrap()).is_none());
    drop(fetch);
    release.send(()).unwrap();
    until(|| fetch_reply(&messages.lock().unwrap()).is_some()).await;
    let (origin, state) = fetch_reply(&messages.lock().unwrap()).unwrap();
    assert_released_with_live_query(&server, &client, &query, &messages, origin, state).await?;
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}

#[tokio::test]
async fn cancelled_fetch_with_metadata_queued_in_its_waiter_abandons_the_stream() -> anyhow::Result<()> {
    let (server, client, query, messages) = stream_fixture().await?;
    let credentials = vec![crate::policy::DEFAULT_CONTEXT];
    let mut fetch = Box::pin(client.fetch_from_peer(query.selection().peek().unwrap().0, &credentials));
    futures::future::poll_fn(|cx| {
        use std::{future::Future, task::Poll};
        assert!(fetch.as_mut().poll(cx).is_pending());
        if messages.lock().unwrap().iter().any(|message| {
            matches!(
                message,
                proto::NodeMessage::Request { request: proto::NodeRequest { body: proto::NodeRequestBody::Fetch { .. }, .. }, .. }
            )
        }) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    until(|| fetch_reply(&messages.lock().unwrap()).is_some()).await;
    let (origin, state) = fetch_reply(&messages.lock().unwrap()).unwrap();
    let connection = client.peer_connections.get(&server.id).unwrap();
    until(|| !connection.pending_requests.contains_key(&origin)).await;
    assert!(matches!(state.frame.body, proto::StateFrameBody::State { progress: proto::ComparisonProgress::Complete, .. }));
    assert!(!connection.outgoing_state_streams.contains_key(&state.frame.stream), "metadata has not been consumed");
    drop(fetch);
    assert_released_with_live_query(&server, &client, &query, &messages, origin, state).await?;
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}

#[tokio::test]
async fn cancelled_fetch_during_bound_transfer_releases_the_remote_stream() -> anyhow::Result<()> {
    let gate = Arc::new(AtomicBool::new(false));
    let (server, client, query, messages) = stream_fixture_with_gate(gate.clone()).await?;
    let selection = query.selection().peek().unwrap().0;
    let states = client.storage.fetch_states(&selection).await?;
    let mut snapshot = super::super::super::QueryTreeCache::default()
        .refresh(super::super::super::QueryTreeCache::spec(&selection, &client.catalog), states)
        .await?;
    // Force real refinement work beyond frame zero while holding its continuations.
    let mut start = super::super::super::whole().start;
    let mut spans = Vec::new();
    for n in 1..=32 {
        let bounds = proto::TreeSpan::new(start, Excluded(address(n)));
        spans.push(if n == 1 {
            proto::SpanContent::Heads(snapshot.page(bounds)?)
        } else {
            proto::SpanContent::Digest(snapshot.span_digest(bounds).await?)
        });
        start = Included(address(n));
    }
    spans.push(proto::SpanContent::Digest(snapshot.span_digest(proto::TreeSpan::new(start, super::super::super::whole().end)).await?));
    gate.store(true, Ordering::SeqCst);
    let credentials = vec![crate::policy::DEFAULT_CONTEXT];
    let (stream, response) = request_state(
        &client,
        server.id,
        &credentials,
        proto::NodeRequestBody::Fetch { selection, known_state: proto::KnownState { spec: snapshot.spec.clone(), spans } },
    )
    .await?;
    let proto::NodeResponseBody::Fetch(state) = response else { panic!("Fetch metadata") };
    assert!(matches!(state.frame.body, proto::StateFrameBody::State { progress: proto::ComparisonProgress::Continue, .. }));
    let origin = stream.request.clone();
    let comparison = state.frame.stream;
    let mut receive = Box::pin(receive_state(&client, server.id, &credentials, snapshot, state.clone(), false, None, stream));
    assert!(futures::poll!(&mut receive).is_pending());
    assert!(client.peer_connections.get(&server.id).unwrap().outgoing_state_streams.contains_key(&comparison));
    drop(receive);
    gate.store(false, Ordering::SeqCst);
    assert_released_with_live_query(&server, &client, &query, &messages, origin, state).await?;
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}

#[tokio::test]
async fn cancelled_fetch_during_final_local_hydration_releases_the_remote_stream() -> anyhow::Result<()> {
    let (server, client, query, messages) = stream_fixture().await?;
    let credentials = vec![crate::policy::DEFAULT_CONTEXT];
    let mut fetch = Box::pin(client.fetch_from_peer(query.selection().peek().unwrap().0, &credentials));
    futures::future::poll_fn(|cx| {
        use std::{future::Future, task::Poll};
        assert!(fetch.as_mut().poll(cx).is_pending());
        if messages.lock().unwrap().iter().any(|message| {
            matches!(
                message,
                proto::NodeMessage::Request { request: proto::NodeRequest { body: proto::NodeRequestBody::Fetch { .. }, .. }, .. }
            )
        }) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    until(|| fetch_reply(&messages.lock().unwrap()).is_some()).await;
    let (origin, state) = fetch_reply(&messages.lock().unwrap()).unwrap();
    until(|| !client.peer_connections.get(&server.id).unwrap().pending_requests.contains_key(&origin)).await;
    // The first local scan has finished. This gate catches the scan performed
    // after receive_state has installed the complete authoritative result.
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    client.storage.hold_fetch.lock().unwrap().insert(proto::ModelId::EntityId(id(99)), (entered, release_rx));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::select! {
            entered = entered_rx => entered.unwrap(),
            result = &mut fetch => panic!("Fetch finished before the hydration gate: {result:?}"),
        }
    })
    .await?;
    assert!(client.peer_connections.get(&server.id).unwrap().outgoing_state_streams.contains_key(&state.frame.stream));
    drop(fetch);
    assert!(release.send(()).is_err(), "the pending hydration must have been cancelled");
    assert_released_with_live_query(&server, &client, &query, &messages, origin, state).await?;
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}

#[tokio::test]
async fn successful_fetch_closes_with_applied_without_abandoning() -> anyhow::Result<()> {
    let (server, client, query, messages) = stream_fixture().await?;
    let rows = client.fetch_from_peer(query.selection().peek().unwrap().0, &vec![crate::policy::DEFAULT_CONTEXT]).await?;
    assert_eq!(rows.len(), 64);
    let (origin, state) = fetch_reply(&messages.lock().unwrap()).unwrap();
    let comparison = state.frame.stream;
    until(|| server.peer_connections.get(&client.id).unwrap().state_streams.was_closed(&comparison)).await;
    until(|| !client.peer_connections.get(&server.id).unwrap().incoming_states.is_current(&origin)).await;
    assert_eq!(abandoned(&messages.lock().unwrap(), comparison), 0);
    assert!(query.resultset().is_loaded());
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}

#[tokio::test]
async fn fetch_validation_error_abandons_once() -> anyhow::Result<()> {
    let (server, client, query, messages) = stream_fixture().await?;
    let selection = query.selection().peek().unwrap().0;
    let states = client.storage.fetch_states(&selection).await?;
    let mut snapshot = super::super::super::QueryTreeCache::default()
        .refresh(super::super::super::QueryTreeCache::spec(&selection, &client.catalog), states)
        .await?;
    let credentials = vec![crate::policy::DEFAULT_CONTEXT];
    let (stream, response) =
        request_state(&client, server.id, &credentials, proto::NodeRequestBody::Fetch { selection, known_state: snapshot.offer().await? })
            .await?;
    let proto::NodeResponseBody::Fetch(state) = response else { panic!("Fetch metadata") };
    let origin = stream.request.clone();
    let mut invalid = state.clone();
    invalid.frame.sequence = 1;
    assert!(receive_state(&client, server.id, &credentials, snapshot, invalid, false, None, stream).await.is_err());
    assert_released_with_live_query(&server, &client, &query, &messages, origin, state).await?;
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}

#[tokio::test]
async fn queued_fetch_cleanup_cannot_send_on_a_replacement_connection() -> anyhow::Result<()> {
    let (server, client, query, messages) = stream_fixture().await?;
    let selection = query.selection().peek().unwrap().0;
    let states = client.storage.fetch_states(&selection).await?;
    let mut snapshot = super::super::super::QueryTreeCache::default()
        .refresh(super::super::super::QueryTreeCache::spec(&selection, &client.catalog), states)
        .await?;
    let credentials = vec![crate::policy::DEFAULT_CONTEXT];
    let (mut stream, response) =
        request_state(&client, server.id, &credentials, proto::NodeRequestBody::Fetch { selection, known_state: snapshot.offer().await? })
            .await?;
    let proto::NodeResponseBody::Fetch(state) = response else { panic!("Fetch metadata") };
    let comparison = state.frame.stream;
    let original = client.peer_connections.get(&server.id).unwrap();
    until(|| original.pending_requests.is_empty()).await;
    let before_cleanup = Arc::weak_count(&original);
    stream.bind(comparison, false, original.outgoing_state_streams.clone());
    drop(stream);
    // Swap the connection before the queued drop cleanup runs, retaining the
    // old Arc so the weak pointer alone cannot hide a missing generation check.
    let replacement = Arc::new(crate::node::peer_state::PeerState::new(
        original.sender.cloned(),
        true,
        crate::peer_subscription::SubscriptionHandler::new(server.id),
    ));
    client.peer_connections.insert(server.id, replacement.clone());
    // Wait for the cleanup closure to release its weak connection reference,
    // rather than checking for absent messages before the spawned task ran.
    until(|| Arc::weak_count(&original) < before_cleanup).await;
    assert_eq!(abandoned(&messages.lock().unwrap(), comparison), 0);
    assert!(!original.outgoing_state_streams.contains_key(&comparison));
    assert!(!replacement.outgoing_state_streams.contains_key(&comparison));
    assert!(Arc::ptr_eq(&replacement, &client.peer_connections.get(&server.id).unwrap()));
    original.incoming_states.close();
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}
