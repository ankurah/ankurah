//! Live credential sessions, the vocabulary layer: a Context holds a
//! Session registered in the node's SessionSet, and
//! `Context::update_cdata` replaces the credential in place for
//! subsequent operations. Standing queries keep their creation
//! credential until the liveness PR wires re-permission.

mod common;
use common::*;

use ankurah::core::node::ContextData;

/// A second ContextData type, for the wrong-type rejection path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OtherCd;
impl ContextData for OtherCd {}

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
