//! Live credential sessions, the vocabulary layer: a Context owns its
//! Session in a private set attached to the node's registry, whose
//! union reflects the source's members for as long as they live.

mod common;
use common::*;

/// The SessionSet tracks context liveness, extended through livequeries:
/// a query keeps its credential session live after the user drops the
/// Context handle, and the last drop culls the slot.
#[tokio::test]
async fn sessions_extend_through_livequeries() -> anyhow::Result<()> {
    let node = durable_sled_setup().await?;
    assert!(node.session_registry().is_empty());

    let ctx = node.context(DEFAULT_CONTEXT)?;
    assert_eq!(node.session_registry().len(), 1, "context construction attaches its source to the registry");

    let second = node.context(DEFAULT_CONTEXT)?;
    assert_eq!(node.session_registry().len(), 2, "sessions are per-context: an equal credential still gets its own session");
    drop(second);
    assert_eq!(node.session_registry().len(), 1);

    ctx.register_model::<Album>().await?;
    let lq = ctx.query_wait::<AlbumView>("true").await?;
    drop(ctx);
    assert_eq!(node.session_registry().len(), 1, "a live query extends its session past the context drop");
    drop(lq);
    assert!(node.session_registry().is_empty(), "the last holder's drop removes the source from the union");
    Ok(())
}
