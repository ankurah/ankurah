//! Queries opened between a canonical commit and publication wait for publication.
//! Direct local reads can hydrate residents in that window. Neither path may lose
//! the original live query's notification or any of its committed events.
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
use anyhow::{Context as _, Result};
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

#[derive(Clone, Copy)]
enum Interleave {
    None,
    Query,
    Hydrate,
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

/// Hydrate directly before publication, or open a query that must wait for it.
/// Keep both kinds of reader alive until the original notification is checked.
async fn read_during_publication(
    node: &Node<SledStorageEngine, PermissiveAgent>,
    context: &ankurah::Context,
    interleave: Interleave,
    name: &str,
    changes: &Mutex<Vec<Change>>,
) -> Result<(Option<LiveQuery<AlbumView>>, Vec<AlbumView>)> {
    if matches!(interleave, Interleave::None) {
        return Ok((None, Vec::new()));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let expression = format!("name = '{name}'");
    let (query, views) = if matches!(interleave, Interleave::Hydrate) {
        let model = Album::descriptor().model_id(node.epoch())?;
        let selection = node.catalog.resolve_selection(&model, ankql::parser::parse_selection(&expression)?)?.and_member_of(model);
        let entities = node.fetch_entities_from_local(&selection).await?;
        assert!(changes.lock().unwrap().is_empty(), "hydration must happen before the original query hears publication");
        (None, entities.into_iter().map(AlbumView::from_entity).collect::<Result<Vec<_>, _>>()?)
    } else {
        let loading = context.query_wait::<AlbumView>(expression.as_str());
        futures_util::pin_mut!(loading);
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut loading).await.is_err(), "tree opening waits for publication");
        assert!(changes.lock().unwrap().is_empty(), "the test entered the publication window");
        let query = loading.await?;
        let views = query.peek();
        (Some(query), views)
    };
    assert_eq!(
        views.iter().map(|album| album.name()).collect::<Result<Vec<_>, _>>()?,
        [name],
        "the concurrent read must observe the canonical commit"
    );
    Ok((query, views))
}

/// What the original query hears of a creation, with the chosen read interleaved.
async fn heard(writer: Writer, interleave: Interleave) -> Result<Heard> {
    let durable = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    durable.wait_ready().await?;
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
    let _loaded = read_during_publication(&durable, &ctx_d, interleave, "the question", &changes).await?;
    let (album, events) = create.await??;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let changes = changes.lock().unwrap().clone();
    Ok(Heard { album, committed: events.iter().map(|event| event.id()).collect(), changes })
}

/// What the original query hears of an edit, with the chosen read interleaved.
async fn heard_edit(writer: Writer, interleave: Interleave) -> Result<Heard> {
    let durable = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    durable.wait_ready().await?;
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
    let _loaded = read_during_publication(&durable, &ctx_d, interleave, "the answer", &changes).await?;
    let events = edit.await??;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let changes = changes.lock().unwrap().clone();
    Ok(Heard { album: id, committed: events.iter().map(|event| event.id()).collect(), changes })
}

/// Both creation and edit carry every committed event exactly once, whether
/// the concurrent reader hydrates early or waits for publication.
async fn hears_each_commit(writer: Writer, interleave: Interleave) -> Result<()> {
    std::env::set_var("ANKURAH_TEST_PUBLICATION_DELAY_MS", "500");
    let baseline =
        tokio::time::timeout(Duration::from_secs(10), heard(writer, Interleave::None)).await.context("baseline creation timed out")??;
    assert!(baseline.told(ChangeKind::Add), "alone, the live query hears of the creation");
    let creation =
        tokio::time::timeout(Duration::from_secs(10), heard(writer, interleave)).await.context("interleaved creation timed out")??;
    assert_eq!(
        creation.changes,
        [(ChangeKind::Add, creation.album, creation.committed.clone())],
        "the original live query hears the creation exactly once with all committed events"
    );
    let baseline =
        tokio::time::timeout(Duration::from_secs(10), heard_edit(writer, Interleave::None)).await.context("baseline edit timed out")??;
    assert!(baseline.told(ChangeKind::Update), "alone, the live query hears of the edit");
    let edit =
        tokio::time::timeout(Duration::from_secs(10), heard_edit(writer, interleave)).await.context("interleaved edit timed out")??;
    assert_eq!(
        edit.changes,
        [(ChangeKind::Update, edit.album, edit.committed.clone())],
        "the original live query hears the edit exactly once with all committed events"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_commit_publishes_before_a_concurrent_query_initializes() -> Result<()> {
    hears_each_commit(Writer::Peer, Interleave::Query).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_local_commit_publishes_before_a_concurrent_query_initializes() -> Result<()> {
    hears_each_commit(Writer::Local, Interleave::Query).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_commit_hydrated_before_publication_still_reaches_live_queries() -> Result<()> {
    hears_each_commit(Writer::Peer, Interleave::Hydrate).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_local_commit_hydrated_before_publication_still_reaches_live_queries() -> Result<()> {
    hears_each_commit(Writer::Local, Interleave::Hydrate).await
}
