use super::common::*;

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

/// An equality on a leading part of the index scans exactly that prefix's
/// key range: the keys of the next year follow it in the tree and must not
/// be reached, with no guard to stop the scan.
#[tokio::test]
async fn test_equality_scan_ends_at_the_prefix_range_end() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

    create_albums(
        &ctx,
        vec![("Album1", "1965"), ("Album2", "1966"), ("Album3", "1967"), ("Album4", "1968"), ("Album5", "1969"), ("Album6", "1970")],
    )
    .await?;
    create_books(&ctx, vec![("Book1", "2001"), ("Book2", "2002")]).await?;

    assert_eq!(names(&fetch(&ctx, "year = '1969' ORDER BY name LIMIT 100").await?), vec!["Album5"]);

    Ok(())
}
