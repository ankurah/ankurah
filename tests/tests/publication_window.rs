//! A creation or an edit reaches the durable node's live queries even when another query loads it
//! from storage between its storage commit and its publication, whether an ephemeral peer wrote it
//! through the durable node or the durable node wrote it in a local transaction. Before a durable
//! node published every event of a commit, such a load made the entity resident first; the
//! publication then found the creation's event applied, carried no events, and was dropped, so no
//! live query heard of it (found by fwdcentaur, whose routing service is such a query and never
//! routed a question the VS Code extension's new live query had loaded).
//! ANKURAH_TEST_PUBLICATION_DELAY_MS (feature test-helpers, read in remote_transaction.rs and
//! transaction/commit.rs) widens the window so that the second query lands in it.
mod common;
use ankurah::{changes::ChangeSet, policy::DEFAULT_CONTEXT, signals::Subscribe, Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use common::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Which node writes the album: the ephemeral peer, whose transactions the durable node commits in
/// remote_transaction.rs, or the durable node itself, whose local transactions commit in
/// transaction/commit.rs.
#[derive(Clone, Copy)]
enum Writer {
    Peer,
    Local,
}

/// Whether a live query on the durable node hears of an album `writer` creates, with another query
/// on the durable node opened while the creation's publication waits, or none.
async fn heard(writer: Writer, concurrent_query: bool) -> Result<bool> {
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
    let added = Arc::new(Mutex::new(Vec::new()));
    let _guard = {
        let added = added.clone();
        watching.subscribe(move |cs: ChangeSet<AlbumView>| added.lock().unwrap().extend(cs.added().iter().map(|a| a.id())))
    };

    let create = tokio::spawn(async move {
        let trx = ctx_w.begin();
        let id = trx.create(&Album { name: "the question".into(), year: "2020".into() }).await?.id();
        trx.commit().await?;
        Ok::<_, anyhow::Error>(id)
    });
    let _loaded = if concurrent_query {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Some(ctx_d.query_wait::<AlbumView>("name = 'the question'").await?)
    } else {
        None
    };
    let id = create.await??;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let heard = added.lock().unwrap().contains(&id);
    Ok(heard)
}

/// Whether a live query on the durable node hears of an edit `writer` commits to an album the
/// query holds, with another query on the durable node opened while the edit's publication waits,
/// or none.
async fn heard_edit(writer: Writer, concurrent_query: bool) -> Result<bool> {
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
    let updated = Arc::new(Mutex::new(Vec::new()));
    let _guard = {
        let updated = updated.clone();
        watching.subscribe(move |cs: ChangeSet<AlbumView>| updated.lock().unwrap().extend(cs.updated().iter().map(|a| a.id())))
    };

    let edit = tokio::spawn(async move {
        let album = ctx_w.get::<AlbumView>(id).await?;
        let trx = ctx_w.begin();
        album.edit(&trx)?.name()?.replace("the answer")?;
        trx.commit().await?;
        Ok::<_, anyhow::Error>(())
    });
    let _loaded = if concurrent_query {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Some(ctx_d.query_wait::<AlbumView>("name = 'the answer'").await?)
    } else {
        None
    };
    edit.await??;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let heard = updated.lock().unwrap().contains(&id);
    Ok(heard)
}

/// A live query on the durable node hears of the album `writer` creates and then edits, alone and
/// with a query loading it before each publication, which ANKURAH_TEST_PUBLICATION_DELAY_MS holds
/// back.
async fn hears_each_commit(writer: Writer) -> Result<()> {
    std::env::set_var("ANKURAH_TEST_PUBLICATION_DELAY_MS", "500");
    assert!(heard(writer, false).await?, "alone, the live query hears of the creation");
    assert!(heard(writer, true).await?, "with a query loading it before its publication, the live query hears of it too");
    assert!(heard_edit(writer, false).await?, "alone, the live query hears of the edit");
    assert!(heard_edit(writer, true).await?, "with a query loading it before its publication, the live query hears of the edit too");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_commit_loaded_before_its_publication_still_reaches_live_queries() -> Result<()> { hears_each_commit(Writer::Peer).await }

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_local_commit_loaded_before_its_publication_still_reaches_live_queries() -> Result<()> { hears_each_commit(Writer::Local).await }
