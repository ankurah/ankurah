//! Live credential sessions: a Context's CData can be replaced in place
//! (`Context::update_cdata`), holders read the current value per
//! operation, and the relay re-permissions remote subscriptions by
//! re-sending them under the new credential through the existing
//! versioned update flow. The node's SessionSet tracks live sessions,
//! extended through livequeries and culled by RAII.

mod common;
use common::*;

use ankurah::core::node::ContextData;
use ankurah::signals::With;

/// A second ContextData type, for the wrong-type rejection path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OtherCd;
impl ContextData for OtherCd {}

/// An update whose value compares equal to the current credential is a
/// complete no-op at the context surface: the subscription keeps its
/// version, nothing tears down, and later commits still arrive. (The
/// value-different path — a real re-login re-permissioning in place —
/// is driven end to end under real credentials in
/// extensions/jwt-auth/tests/session_refresh_tests.rs.)
#[tokio::test]
async fn identical_update_cdata_is_a_noop() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let server_ctx = server.context(DEFAULT_CONTEXT)?;
    let client_ctx = client.context(DEFAULT_CONTEXT)?;

    {
        let trx = server_ctx.begin();
        trx.create(&Album { name: "first".into(), year: "1999".into() }).await?;
        trx.commit().await?;
    }

    let lq = client_ctx.query_wait::<AlbumView>(nocache("true")?).await?;
    assert_eq!(lq.resultset().len(), 1, "initial subscription delivers the existing album");
    assert_eq!(lq.selection().value().1, 1, "initial subscription is version 1");

    // Every DEFAULT_CONTEXT value compares equal, so this update is
    // gated as a no-op: Eq is operational identity, and nothing
    // observable changed.
    client_ctx.update_cdata(DEFAULT_CONTEXT)?;
    lq.wait_initialized().await;
    assert_eq!(lq.selection().value().1, 1, "an identical update leaves the subscription version untouched");
    assert!(lq.error().with(|e| e.is_none()), "the no-op leaves no error behind");

    // The subscription is untouched and still live end to end.
    {
        let trx = server_ctx.begin();
        trx.create(&Album { name: "second".into(), year: "2001".into() }).await?;
        trx.commit().await?;
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while lq.resultset().len() < 2 {
        if std::time::Instant::now() > deadline {
            panic!("commit after re-permission never arrived (resultset len {})", lq.resultset().len());
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    Ok(())
}

/// The SessionSet tracks context liveness, extended through livequeries:
/// a query keeps its credential session live after the user drops the
/// Context handle, and the last drop culls the slot.
#[tokio::test]
async fn sessions_extend_through_livequeries() -> anyhow::Result<()> {
    let node = durable_sled_setup().await?;
    assert!(node.sessions.sessions().is_empty());

    let ctx = node.context(DEFAULT_CONTEXT)?;
    assert_eq!(node.sessions.sessions().len(), 1, "context construction registers a session");

    let second = node.context(DEFAULT_CONTEXT)?;
    assert_eq!(node.sessions.sessions().len(), 2, "sessions are per-context, never deduplicated");
    drop(second);
    assert_eq!(node.sessions.sessions().len(), 1);

    ctx.register_model::<Album>().await?;
    let lq = ctx.query_wait::<AlbumView>("true").await?;
    drop(ctx);
    assert_eq!(node.sessions.sessions().len(), 1, "a live query extends its session past the context drop");
    drop(lq);
    assert!(node.sessions.sessions().is_empty(), "the last holder culls the session");
    Ok(())
}

/// `update_cdata` with a different ContextData type than the node's
/// policy agent uses is rejected across the type-erased boundary.
#[tokio::test]
async fn update_cdata_rejects_wrong_type() -> anyhow::Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context(DEFAULT_CONTEXT)?;
    assert!(ctx.update_cdata(OtherCd).is_err(), "a mismatched ContextData type is refused");
    assert!(ctx.update_cdata(DEFAULT_CONTEXT).is_ok(), "the node's own type is accepted");
    Ok(())
}
