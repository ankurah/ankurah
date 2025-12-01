//! Workload phase implementations for sprout benchmarks.

use crate::sprout::common::{Album, AlbumView, BenchWatcher};
use ankurah::Context;
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

/// Performs sequential fetch rounds, querying all albums.
pub async fn fetch_rounds(ctx: &Context, rounds: usize) -> Result<()> {
    use ankurah::{LiveQuery, signals::Peek};

    for _ in 0..rounds {
        let predicate = ankql::parser::parse_selection("year > '1999'").unwrap();
        let query: LiveQuery<AlbumView> = ctx.query_wait::<AlbumView>(predicate).await?;
        let _results = query.peek();
        // Benchmark will measure the time to execute this
    }
    Ok(())
}

/// Performs concurrent mutations on existing albums.
pub async fn concurrent_mutations(ctx: &Context, concurrency: usize) -> Result<()> {
    use ankurah::{LiveQuery, signals::Peek};

    let predicate = ankql::parser::parse_selection("year > '1999'").unwrap();
    let query: LiveQuery<AlbumView> = ctx.query_wait::<AlbumView>(predicate).await?;
    let albums = query.peek();

    let mut handles = vec![];
    for (i, album) in albums.into_iter().take(concurrency).enumerate() {
        let ctx = ctx.clone();
        let album: AlbumView = album;
        let handle = tokio::spawn(async move {
            let trx = ctx.begin();
            let album_mut = album.edit(&trx)?;
            album_mut.year().replace(&format!("{}", 2000 + i))?;
            trx.commit().await?;
            Ok::<_, anyhow::Error>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }
    Ok(())
}

/// Performs subscription churn cycles, measuring notification latency.
pub async fn subscription_churn(ctx: &Context, cycles: usize) -> Result<()> {
    use ankurah::{LiveQuery, signals::Subscribe};

    for _ in 0..cycles {
        let watcher = BenchWatcher::new();
        let predicate = ankql::parser::parse_selection("year > '2015'").unwrap();
        let query: LiveQuery<AlbumView> = ctx.query_wait::<AlbumView>(predicate).await?;

        let _handle = query.subscribe(&watcher);

        // Perform a mutation to trigger notification
        let trx = ctx.begin();
        trx.create(&Album::with_index(9999)).await?;
        trx.commit().await?;

        // Wait for notification
        watcher.wait_for(1).await;
    }
    Ok(())
}

/// Performs ORDER BY + LIMIT queries to exercise gap filling.
pub async fn limit_gap_queries(ctx: &Context, rounds: usize) -> Result<()> {
    use ankurah::{LiveQuery, signals::Peek};

    for _ in 0..rounds {
        let predicate = ankql::parser::parse_selection("year > '1999' ORDER BY year LIMIT 10").unwrap();
        let query: LiveQuery<AlbumView> = ctx.query_wait::<AlbumView>(predicate).await?;
        let _results = query.peek();
    }
    Ok(())
}
