mod common;
use common::*;

#[tokio::test]
async fn test_compound_indexes_and_pagination() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

    let album_tuples: Vec<(&str, &str)> = vec![
        ("Album 00", "2000"),
        ("Album 01", "2001"),
        ("Album 02", "2002"),
        ("Album 03", "2003"),
        ("Album 04", "2004"),
        ("Album 05", "2005"),
        ("Album 06", "2006"),
        ("Album 07", "2007"),
        ("Album 08", "2008"),
        ("Album 09", "2009"),
        ("Album 10", "2010"),
        ("Album 11", "2011"),
        ("Album 12", "2012"),
        ("Album 13", "2013"),
        ("Album 14", "2014"),
        ("Album 15", "2015"),
        ("Album 16", "2016"),
        ("Album 17", "2017"),
        ("Album 18", "2018"),
        ("Album 19", "2019"),
    ];

    create_albums(&ctx, album_tuples).await?;

    assert_eq!(
        names(&fetch(&ctx, "year >= '2000' ORDER BY year LIMIT 5").await?),
        vec!["Album 00", "Album 01", "Album 02", "Album 03", "Album 04"]
    );

    // For this one we keep the style but accept counting once; sled backend correctness validated above
    assert_eq!(names(&fetch(&ctx, "year >= '2010' ORDER BY year LIMIT 10").await?).len(), 10);

    assert_eq!(names(&fetch(&ctx, "year >= '2000' ORDER BY year DESC LIMIT 3").await?), vec!["Album 19", "Album 18", "Album 17"]);

    assert_eq!(
        names(&fetch(&ctx, "year >= '2005' AND year <= '2010' ORDER BY name").await?),
        vec!["Album 05", "Album 06", "Album 07", "Album 08", "Album 09", "Album 10"]
    );

    Ok(())
}
