//! Workload phase implementations for sprout benchmarks.

use crate::sprout::common::{Album, AlbumView, Artist, ArtistView, BenchWatcher};
use ankurah::{Context, changes::ChangeSet, fetch};
use anyhow::Result;

/// Seeds the database with a specified number of Album entities.
pub async fn seed_albums(ctx: &Context, count: usize) -> Result<()> {
    let trx = ctx.begin();
    for i in 0..count {
        trx.create(&Album::with_index(i)).await?;
    }
    trx.commit().await?;
    Ok(())
}

/// Seeds Artist entities to expand predicate coverage.
pub async fn seed_artists(ctx: &Context, count: usize) -> Result<()> {
    let trx = ctx.begin();
    for i in 0..count {
        trx.create(&Artist::with_index(i)).await?;
    }
    trx.commit().await?;
    Ok(())
}

/// Performs sequential fetch rounds using a variety of predicates inspired by the planner tests.
pub async fn workload_readonly_fetch(ctx: &Context, rounds: usize) -> Result<()> {
    let sample_album: Option<AlbumView> = fetch!(ctx, "true LIMIT 1").await?.into_iter().next();
    let sample_id = sample_album.as_ref().map(|album| album.id());

    for _ in 0..rounds {
        let names = vec!["Album 1".to_string(), "Album 2".to_string()];
        let years = vec!["1995".to_string(), "2005".to_string()];
        let _: Vec<AlbumView> = fetch!(ctx, "true ORDER BY foo, bar LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "foo > 10 ORDER BY foo, bar LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "age = 30 ORDER BY foo, bar LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "age = 30 ORDER BY foo, bar LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "true ORDER BY name DESC LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "true ORDER BY name DESC, year DESC LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "true ORDER BY name ASC, year DESC LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "true ORDER BY name DESC, year ASC LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "true ORDER BY foo ASC, bar DESC, score DESC LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "status = 'active' ORDER BY name DESC LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "age > 25 ORDER BY age DESC LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "age > 25 AND age < 50 LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "age > 25 AND score < 100 LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "name = 'Album 10' LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "name = 'Album 10' AND age = 30 LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "name = 'Album 10' AND age > 25 LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "score > 50 AND age = 30 ORDER BY score LIMIT 100").await?;
        if let Some(id) = sample_id {
            let _: Vec<AlbumView> = fetch!(ctx, "id = {id}").await?;
        }
        let _: Vec<AlbumView> = fetch!(ctx, "name IN {names} LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "year IN {years} LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "name != 'Album 1' LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "(age > 25 OR name = 'Album 3') LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "status = 'active' AND (age > 25 OR name = 'Album 5') LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "year >= '2001' ORDER BY name LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "timestamp > 1600000010 LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "age >= 25 AND age <= 50 AND age > 20 LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "name = '' AND year = '2000' LIMIT 100").await?;
        let _: Vec<AlbumView> = fetch!(ctx, "year >= '2001' ORDER BY name DESC, score ASC LIMIT 100").await?;
        let _: Vec<ArtistView> = fetch!(ctx, "popularity > 50 LIMIT 100").await?;
    }
    Ok(())
}

/// Placeholder until concurrent mutations are re-enabled.
pub async fn concurrent_mutations(_ctx: &Context, _concurrency: usize) -> Result<()> { Ok(()) }

/// Performs subscription churn cycles, measuring notification latency.
pub async fn subscription_churn(ctx: &Context, cycles: usize) -> Result<()> {
    use ankurah::{LiveQuery, signals::Subscribe};

    for i in 0..cycles {
        let watcher = BenchWatcher::new();
        let predicate = ankql::parser::parse_selection("year > '2015'").unwrap();
        let query: LiveQuery<AlbumView> = ctx.query_wait::<AlbumView>(predicate).await?;

        let _handle = query.subscribe(&watcher);

        // Perform a mutation to trigger notification
        let trx = ctx.begin();
        trx.create(&Album::with_index(9999 + i)).await?;
        trx.commit().await?;

        // Wait for notification
        watcher.wait_for(1).await;
    }
    Ok(())
}

/// Drives live query subscriptions that mirror the planner predicate coverage.
pub async fn subscribe_rounds(ctx: &Context, cycles: usize) -> Result<()> {
    use ankurah::signals::Subscribe;

    for i in 0..cycles {
        let album_watcher: BenchWatcher<ChangeSet<AlbumView>> = BenchWatcher::new();
        let query = ctx.query_wait::<AlbumView>("status = 'active' ORDER BY name DESC LIMIT 100").await?;
        let _album_guard = query.subscribe(&album_watcher);
        let trx = ctx.begin();
        let mut album = Album::with_index(50 + i);
        album.status = "active".into();
        trx.create(&album).await?;
        trx.commit().await?;
        album_watcher.wait_for(1).await;
        drop(_album_guard);

        let artist_watcher: BenchWatcher<ChangeSet<ArtistView>> = BenchWatcher::new();
        let query2 = ctx.query_wait::<ArtistView>("popularity > 40 LIMIT 100").await?;
        let _artist_guard = query2.subscribe(&artist_watcher);
        let trx2 = ctx.begin();
        trx2.create(&Artist::with_index(200 + i)).await?;
        trx2.commit().await?;
        artist_watcher.wait_for(1).await;
        drop(_artist_guard);
    }

    Ok(())
}

/// Performs ORDER BY + LIMIT queries to exercise gap filling.
pub async fn limit_gap_queries(ctx: &Context, rounds: usize) -> Result<()> {
    use ankurah::{LiveQuery, signals::Peek};

    for i in 0..rounds {
        let predicate = ankql::parser::parse_selection("year >= '1995' ORDER BY year LIMIT 100").unwrap();
        let query: LiveQuery<AlbumView> = ctx.query_wait::<AlbumView>(predicate).await?;
        let current = query.peek();

        for album in current.into_iter().take(3) {
            let trx = ctx.begin();
            let editable = album.edit(&trx)?;
            editable.year().replace("1900")?;
            editable.score().set(&(i as i32))?;
            trx.commit().await?;
        }

        let _ = query.peek();
    }
    Ok(())
}
