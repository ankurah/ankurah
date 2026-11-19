//! Queries opened between a canonical commit and its publication wait for publication.
//! Existing live queries receive the commit exactly once, with its full events.
//! ANKURAH_TEST_PUBLICATION_DELAY_MS widens this window for local and peer writes.
mod common;
use ankurah::{
    changes::{ChangeKind, ChangeSet},
    policy::DEFAULT_CONTEXT,
    proto::EventId,
    signals::{Peek, Subscribe, SubscriptionGuard},
    EntityId, LiveQuery, Node, PermissiveAgent,
};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use common::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Which node writes the album: the ephemeral peer, whose transactions the durable node commits in
/// transaction/remote.rs, or the durable node itself, whose local transactions commit in
/// transaction/local.rs.
#[derive(Clone, Copy)]
enum Writer {
    Peer,
    Local,
}

/// One change a live query told its subscriber of: its kind, its album, and the ids of the events
/// it carried.
type Change = (ChangeKind, EntityId, Vec<EventId>);

/// What a live query on the durable node was told of one commit to `album`, after its initial
/// rows, beside the ids of the events that commit created.
struct Heard {
    album: EntityId,
    committed: Vec<EventId>,
    changes: Vec<Change>,
}

impl Heard {
    /// Whether the live query was told of the album by a change of `kind`.
    fn told(&self, kind: ChangeKind) -> bool { self.changes.iter().any(|(told, album, _)| *told == kind && *album == self.album) }
}

/// Record every change `query` tells its subscriber of after its initial rows.
fn record(query: &LiveQuery<AlbumView>) -> (Arc<Mutex<Vec<Change>>>, SubscriptionGuard) {
    let changes = Arc::new(Mutex::new(Vec::new()));
    let guard = {
        let changes = changes.clone();
        query.subscribe(move |cs: ChangeSet<AlbumView>| {
            changes.lock().unwrap().extend(
                cs.changes
                    .iter()
                    .filter(|change| change.kind() != ChangeKind::Initial)
                    .map(|change| (change.kind(), change.entity().id(), change.events().iter().map(|event| event.payload.id()).collect())),
            )
        })
    };
    (changes, guard)
}

/// The names of the albums `query` holds.
fn names(query: &LiveQuery<AlbumView>) -> Result<Vec<String>> {
    Ok(query.peek().iter().map(|album| album.name()).collect::<Result<_, _>>()?)
}

/// What a live query on the durable node hears of an album `writer` creates, alone or with another
/// query opened while publication waits. Opening its tree must wait for that publication.
async fn heard(writer: Writer, concurrent_query: bool) -> Result<Heard> {
    let durable = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    durable.system.create().await?;
    let ephemeral = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let _conn = LocalProcessConnection::new(&ephemeral, &durable).await?;
    ephemeral.wait_ready().await?;
    let ctx_d = durable.context_async(DEFAULT_CONTEXT).await?;
    let ctx_e = ephemeral.context_async(DEFAULT_CONTEXT).await?;
    let ctx_w = match writer {
        Writer::Peer => ctx_e,
        Writer::Local => ctx_d.clone(),
    };

    let watching = ctx_d.query_wait::<AlbumView>("year = '2020'").await?;
    let (changes, _guard) = record(&watching);

    let create = tokio::spawn(async move {
        let trx = ctx_w.begin();
        let id = trx.create(&Album { name: "the question".into(), year: "2020".into() }).await?.id();
        let events = trx.commit_and_return_events().await?;
        Ok::<_, anyhow::Error>((id, events))
    });
    // Held until the publication, so that the entity it loaded stays resident.
    let _loaded = if concurrent_query {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let loading = ctx_d.query_wait::<AlbumView>("name = 'the question'");
        futures_util::pin_mut!(loading);
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut loading).await.is_err(), "tree opening waits for publication");
        assert!(changes.lock().unwrap().is_empty(), "the test entered the publication window");
        let loaded = loading.await?;
        assert_eq!(names(&loaded)?, ["the question"]);
        Some(loaded)
    } else {
        None
    };
    let (album, events) = create.await??;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let changes = changes.lock().unwrap().clone();
    Ok(Heard { album, committed: events.iter().map(|event| event.id()).collect(), changes })
}

/// What a live query on the durable node hears of an edit `writer` commits to an album the query
/// holds, alone or with another query on the durable node opened while the edit's publication
/// waits. The second query observes the edit only after publication completes.
async fn heard_edit(writer: Writer, concurrent_query: bool) -> Result<Heard> {
    let durable = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    durable.system.create().await?;
    let ephemeral = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let _conn = LocalProcessConnection::new(&ephemeral, &durable).await?;
    ephemeral.wait_ready().await?;
    let ctx_d = durable.context_async(DEFAULT_CONTEXT).await?;
    let ctx_e = ephemeral.context_async(DEFAULT_CONTEXT).await?;
    let ctx_w = match writer {
        Writer::Peer => ctx_e,
        Writer::Local => ctx_d.clone(),
    };
    let id = {
        let trx = ctx_w.begin();
        let id = trx.create(&Album { name: "the question".into(), year: "2020".into() }).await?.id();
        trx.commit().await?;
        id
    };

    let watching = ctx_d.query_wait::<AlbumView>("year = '2020'").await?;
    let (changes, _guard) = record(&watching);

    let edit = tokio::spawn(async move {
        let album = ctx_w.get::<AlbumView>(id).await?;
        let trx = ctx_w.begin();
        album.edit(&trx)?.name()?.replace("the answer")?;
        Ok::<_, anyhow::Error>(trx.commit_and_return_events().await?)
    });
    let _loaded = if concurrent_query {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let loading = ctx_d.query_wait::<AlbumView>("name = 'the answer'");
        futures_util::pin_mut!(loading);
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut loading).await.is_err(), "tree opening waits for publication");
        assert!(changes.lock().unwrap().is_empty(), "the test entered the publication window");
        let loaded = loading.await?;
        assert_eq!(names(&loaded)?, ["the answer"]);
        Some(loaded)
    } else {
        None
    };
    let events = edit.await??;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let changes = changes.lock().unwrap().clone();
    Ok(Heard { album: id, committed: events.iter().map(|event| event.id()).collect(), changes })
}

/// A live query on the durable node hears of the album `writer` creates and then edits, alone and
/// with a query opening during each publication, which ANKURAH_TEST_PUBLICATION_DELAY_MS holds
/// back; with that query, it hears of each commit exactly once, carrying the commit's events.
async fn hears_each_commit(writer: Writer) -> Result<()> {
    std::env::set_var("ANKURAH_TEST_PUBLICATION_DELAY_MS", "500");
    assert!(heard(writer, false).await?.told(ChangeKind::Add), "alone, the live query hears of the creation");
    let creation = heard(writer, true).await?;
    assert_eq!(
        creation.changes,
        [(ChangeKind::Add, creation.album, creation.committed.clone())],
        "with a query waiting for publication, the live query hears of the creation exactly once"
    );
    assert!(heard_edit(writer, false).await?.told(ChangeKind::Update), "alone, the live query hears of the edit");
    let edit = heard_edit(writer, true).await?;
    assert_eq!(
        edit.changes,
        [(ChangeKind::Update, edit.album, edit.committed.clone())],
        "with a query waiting for publication, the live query hears of the edit exactly once"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_commit_publishes_before_a_concurrent_query_initializes() -> Result<()> { hears_each_commit(Writer::Peer).await }

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_local_commit_publishes_before_a_concurrent_query_initializes() -> Result<()> { hears_each_commit(Writer::Local).await }
