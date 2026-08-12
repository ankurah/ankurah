use super::common::*;
use ankurah::{policy::DEFAULT_CONTEXT, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use std::sync::Arc;

#[tokio::test]
async fn test_collection_boundary_excludes_other_collections() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

    // Insert albums and books with overlapping names to ensure sorted adjacency
    create_albums(
        &ctx,
        vec![("Album1", "1965"), ("Album2", "1966"), ("Album3", "1967"), ("Album4", "1968"), ("Album5", "1969"), ("Album6", "1970")],
    )
    .await?;

    create_books(&ctx, vec![("Book1", "2001"), ("Book2", "2002")]).await?;

    // Guard is implicit in sled: per-collection index trees
    assert_eq!(names(&fetch(&ctx, "year >= '1900' ORDER BY name LIMIT 5").await?), vec!["Album1", "Album2", "Album3", "Album4", "Album5"]);

    assert_eq!(
        names(&fetch(&ctx, "year >= '1900' ORDER BY name LIMIT 100").await?),
        vec!["Album1", "Album2", "Album3", "Album4", "Album5", "Album6"]
    );

    Ok(())
}

#[tokio::test]
async fn test_prefix_guard_toggle_effect() -> Result<(), anyhow::Error> {
    let engine = Arc::new(SledStorageEngine::new_test()?);
    let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;

    // Insert albums across multiple years and a couple of books (different collection)
    create_albums(
        &ctx,
        vec![("Album1", "1965"), ("Album2", "1966"), ("Album3", "1967"), ("Album4", "1968"), ("Album5", "1969"), ("Album6", "1970")],
    )
    .await?;
    create_books(&ctx, vec![("Book1", "2001"), ("Book2", "2002")]).await?;

    // 1) Guard enabled (default): equality prefix constrains the scan to year=1969
    assert_eq!(names(&fetch(&ctx, "year = '1969' ORDER BY name LIMIT 100").await?), vec!["Album5"]);

    // 2) Disable guard: the index scan overshoots the equality prefix, but the
    //    residual predicate re-checks every row, so the answer does not change.
    //    (This used to pin the overshoot showing up in the results — that was
    //    only observable while the planner dropped the equality term from the
    //    residual. The guard remains a scan-narrowing optimization; the
    //    residual is what keeps the answer right without it.)
    #[cfg(debug_assertions)]
    engine.set_prefix_guard_disabled(true);
    assert_eq!(names(&fetch(&ctx, "year = '1969' ORDER BY name LIMIT 100").await?), vec!["Album5"]);

    // 3) Re-enable guard: same answer, narrower scan.
    #[cfg(debug_assertions)]
    engine.set_prefix_guard_disabled(false);
    assert_eq!(names(&fetch(&ctx, "year = '1969' ORDER BY name LIMIT 100").await?), vec!["Album5"]);

    Ok(())
}
