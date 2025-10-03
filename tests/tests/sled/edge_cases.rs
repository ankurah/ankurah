use super::common::*;
use tracing::info;

#[tokio::test]
async fn test_edge_cases() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

    let albums = create_albums(
        &ctx,
        vec![
            ("", "2000"),                       // Empty name
            ("Album with spaces", "2001"),      // Spaces
            ("Album-with-dashes", "2002"),      // Dashes
            ("Album_with_underscores", "2003"), // Underscores
            ("UPPERCASE", "2004"),              // Case variations
            ("lowercase", "2005"),
            ("MixedCase", "2006"),
            ("Special!@#$%", "2007"),                                    // Special characters
            ("Unicode: 你好", "2008"),                                   // Unicode
            ("Very Long Album Name That Goes On And On And On", "2009"), // Long name
        ],
    )
    .await?;

    println!(
        "albums: {}",
        albums.iter().map(|a| format!("{}: \"{}\"", a.id().to_base64_short(), a.name().unwrap())).collect::<Vec<_>>().join(", ")
    );

    assert_eq!(names(&fetch(&ctx, "name = ''").await?), vec![""]);

    assert_eq!(names(&fetch(&ctx, "name = 'Special!@#$%'").await?), vec!["Special!@#$%"]);

    assert_eq!(names(&fetch(&ctx, "name = 'Unicode: 你好'").await?), vec!["Unicode: 你好"]);

    assert_eq!(names(&fetch(&ctx, "name = 'UPPERCASE'").await?), vec!["UPPERCASE"]);
    assert_eq!(names(&fetch(&ctx, "name = 'lowercase'").await?), vec!["lowercase"]);

    assert_eq!(
        sort_names(&fetch(&ctx, "(name = 'UPPERCASE' OR name = 'lowercase') AND year >= '2004'").await?),
        vec!["UPPERCASE", "lowercase"]
    );

    assert_eq!(years(&fetch(&ctx, "year > '2005' AND year < '2008'").await?), vec!["2006", "2007"]);
    assert_eq!(names(&fetch(&ctx, "year > '2010' AND year < '2005'").await?), Vec::<String>::new());
    assert_eq!(
        names(&fetch(&ctx, "year >= '2001' ORDER BY name LIMIT 5").await?),
        vec!["Album with spaces", "Album-with-dashes", "Album_with_underscores", "MixedCase", "Special!@#$%"]
    );

    Ok(())
}
