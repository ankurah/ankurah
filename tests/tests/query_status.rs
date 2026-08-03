//! The livequery status signal: two legs, each reporting where the
//! CURRENT selection stands. The local leg says what this node's reactor
//! serves; the remote leg follows the relay's transitions (pending,
//! requested, established, refused) and un-fails as conditions change.
//! Nothing latches.

mod common;
use common::*;

use ankurah::core::livequery::{LocalStatus, QueryStatus, RemoteStatus};
use ankurah::signals::Peek;

async fn eventually(mut condition: impl FnMut() -> bool) -> bool {
    for _ in 0..100 {
        if condition() {
            return true;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    condition()
}

/// A relayed query's remote leg walks pending/requested to established,
/// and a selection update walks it there again at the new version.
#[tokio::test]
async fn remote_leg_reaches_established_and_follows_updates() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let server_ctx = server.context(DEFAULT_CONTEXT)?;
    {
        let trx = server_ctx.begin();
        trx.create(&Album { name: "first".into(), year: "1999".into() }).await?;
        trx.commit().await?;
    }

    let client_ctx = client.context(DEFAULT_CONTEXT)?;
    let lq = client_ctx.query_wait::<AlbumView>(nocache("true")?).await?;

    assert!(
        eventually(|| lq.status().peek()
            == QueryStatus { local: LocalStatus::Active { version: 1 }, remote: RemoteStatus::Established { version: 1 } })
        .await,
        "the initial subscription establishes at version 1, got {:?}",
        lq.status().peek()
    );

    lq.update_selection_wait("name = 'first'").await?;
    assert!(
        eventually(|| lq.status().peek()
            == QueryStatus { local: LocalStatus::Active { version: 2 }, remote: RemoteStatus::Established { version: 2 } })
        .await,
        "a selection update re-establishes at version 2, got {:?}",
        lq.status().peek()
    );
    Ok(())
}

/// A durable node's own query has no remote leg: its status reports
/// RemoteStatus::None for life, and the local leg tracks its versions.
#[tokio::test]
async fn durable_local_query_has_no_remote_leg() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let ctx = server.context(DEFAULT_CONTEXT)?;

    let lq = ctx.query_wait::<AlbumView>("true").await?;
    assert_eq!(
        lq.status().peek(),
        QueryStatus { local: LocalStatus::Active { version: 1 }, remote: RemoteStatus::None },
        "a durable-local query serves locally with no remote leg"
    );

    lq.update_selection_wait("name = 'anything'").await?;
    assert_eq!(
        lq.status().peek(),
        QueryStatus { local: LocalStatus::Active { version: 2 }, remote: RemoteStatus::None },
        "updates advance the local leg; the remote leg stays None"
    );
    Ok(())
}

/// The status signal is safe to re-enter: a subscriber may call
/// update_selection from inside its callback (the classic reactive
/// pattern) without deadlocking — dispatch never runs under the query's
/// locks, and a re-entrant enqueue is picked up by the active drainer.
#[tokio::test(flavor = "multi_thread")]
async fn status_subscriber_may_reenter_update_selection() -> anyhow::Result<()> {
    use ankurah::signals::Subscribe;

    let server = durable_sled_setup().await?;
    let ctx = server.context(DEFAULT_CONTEXT)?;

    let lq = ctx.query_wait::<AlbumView>("true").await?;

    let reentered = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _guard = {
        let lq2 = lq.clone();
        let reentered = reentered.clone();
        lq.status().subscribe(move |_status: QueryStatus| {
            // Re-enter exactly once, synchronously, from inside dispatch.
            if !reentered.swap(true, std::sync::atomic::Ordering::SeqCst) {
                lq2.update_selection("name = 'reentrant'").expect("re-entrant update_selection must not fail");
            }
        })
    };

    // Drive one transition so the subscriber fires (version 2), whose
    // callback issues version 3.
    lq.update_selection("name = 'outer'")?;

    assert!(
        eventually(|| lq.status().peek().local == (LocalStatus::Active { version: 3 })).await,
        "the re-entrant update must complete and activate, got {:?}",
        lq.status().peek()
    );
    assert!(reentered.load(std::sync::atomic::Ordering::SeqCst), "the subscriber must have re-entered");
    Ok(())
}

/// Before any registration lands, both legs report Pending: Active is
/// only ever reported by a registration that actually completed, so a
/// disconnected ephemeral node's query says so instead of claiming
/// service.
#[tokio::test]
async fn unestablished_query_reports_pending_on_both_legs() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    {
        let conn = LocalProcessConnection::new(&server, &client).await?;
        client.system.wait_system_ready().await;
        drop(conn);
    }
    let ctx = client.context(DEFAULT_CONTEXT)?;

    // The peer is gone: the remote leg cannot establish and the local
    // activation waits on it, so nothing ever lands and neither leg may
    // claim service.
    let lq = ctx.query::<AlbumView>(nocache("true")?)?;
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    assert_eq!(
        lq.status().peek(),
        QueryStatus { local: LocalStatus::Pending, remote: RemoteStatus::Pending },
        "an unestablished relayed query reports Pending on both legs"
    );
    Ok(())
}
