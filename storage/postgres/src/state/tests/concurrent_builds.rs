//! Independent managers may race to build the same durable Selection.

use std::sync::atomic::{AtomicBool, Ordering};

use ankurah_core::sync::{CacheSettings, TreeManager};
use ankurah_digest_tree::Ristretto255;

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn competing_initial_builds_postgres_11() -> anyhow::Result<()> { competing_initial_builds("11-alpine").await }

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn competing_initial_builds_postgres_17() -> anyhow::Result<()> { competing_initial_builds("17").await }

async fn competing_initial_builds(tag: &str) -> anyhow::Result<()> {
    let container = postgres::Postgres::default().with_tag(tag).start().await?;
    let connection = PostgresConnectionManager::new_from_stringlike(
        format!(
            "host={} port={} user=postgres password=postgres dbname=postgres",
            container.get_host().await?,
            container.get_host_port_ipv4(5432).await?,
        ),
        NoTls,
    )?;
    let pool = bb8::Pool::builder().max_size(2).build(connection).await?;
    let first = Arc::new(Postgres::new(pool.clone()).await?);
    let second = Arc::new(Postgres::new(pool).await?);
    let inspector = first.pool.dedicated_connection().await?;
    first.ensure_shared_tables(&inspector).await?;
    drop(first.map_snapshot(&[]).await?);
    inspector
        .execute(
            &format!(
                r#"INSERT INTO "{ENTITY_TABLE}" ("id", "state_buffer", "head", "attestations")
            SELECT translate(rtrim(encode(decode(lpad(to_hex(n), 64, '0'), 'hex'), 'base64'), '='), '+/', '-_'), $1, $2, $3
            FROM generate_series(1, 5000) n"#
            ),
            &[&bincode::serialize(&StateBuffers::default())?, &Clock::genesis(EventId::from_bytes([1; 32])), &Vec::<Vec<u8>>::new()],
        )
        .await?;
    let first =
        Arc::new(TreeManager::<_, Ristretto255>::new(first, Arc::new(tokio::sync::Mutex::new(())), CacheSettings { bytes: 1 << 20 }, 1));
    let second =
        Arc::new(TreeManager::<_, Ristretto255>::new(second, Arc::new(tokio::sync::Mutex::new(())), CacheSettings { bytes: 1 << 20 }, 1));
    let selection = Selection { predicate: Predicate::True, order_by: None, limit: None };
    let started = std::time::Instant::now();
    let first_open = tokio::spawn({
        let first = first.clone();
        let selection = selection.clone();
        async move { retry(|| first.open(selection.clone())).await }
    });

    // Start the competing first open while the first builder's incomplete
    // generation is durable. Neither builder is paused or delayed afterwards.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some((false, _, members)) = progress(&inspector).await? {
                if members == 0 {
                    continue;
                }
                return anyhow::Ok(());
            }
            assert!(!first_open.is_finished(), "the first build completed before its incomplete batch was observed");
            tokio::task::yield_now().await;
        }
    })
    .await??;
    let done = Arc::new(AtomicBool::new(false));
    let monitor = tokio::spawn({
        let done = done.clone();
        async move {
            let mut changes = Vec::new();
            while !done.load(Ordering::Acquire) {
                if let Some(progress) = progress(&inspector).await? {
                    if changes.last().is_none_or(|(_, previous)| *previous != progress) {
                        changes.push((started.elapsed(), progress));
                    }
                }
                tokio::task::yield_now().await;
            }
            anyhow::Ok(changes)
        }
    });
    let second_open = retry(|| second.open(selection.clone()));
    let (first_result, second_result) = tokio::time::timeout(Duration::from_secs(60), async {
        let (first_result, second_result) = tokio::join!(first_open, second_open);
        anyhow::Ok((first_result?, second_result))
    })
    .await??;
    done.store(true, Ordering::Release);
    let progress = monitor.await??;
    eprintln!(
        "PG {tag} initial builds: first={:?}, second={:?}, elapsed={:?}, observed generations={}",
        first_result.as_ref().map(|_| ()).map_err(|error| error.to_string()),
        second_result.as_ref().map(|_| ()).map_err(|error| error.to_string()),
        started.elapsed(),
        progress.len(),
    );
    assert!(
        progress.windows(2).all(|pair| pair[0].1 .2 <= pair[1].1 .2),
        "competing builders must not erase completed batches: {progress:?}"
    );
    for result in [first_result, second_result] {
        if let Err(error) = result {
            assert!(
                matches!(error, RetrievalError::MutationError(ref error) if matches!(**error, MutationError::WriteConflict)),
                "competing maintenance must remain retryable: {error:?}"
            );
        }
    }
    // Once the competition has ended, both managers can converge on the
    // complete source. Existing successful handles must remain recoverable too.
    let first_tree = tokio::time::timeout(Duration::from_secs(30), first.open(selection.clone())).await??;
    let second_tree = tokio::time::timeout(Duration::from_secs(30), second.open(selection)).await??;
    assert_eq!(first_tree.audit().await?.members, 5000);
    assert_eq!(second_tree.audit().await?.members, 5000);
    assert_eq!(
        first_tree.view().span_digest(ankurah_digest_tree::Span::all()).await?.digest,
        second_tree.view().span_digest(ankurah_digest_tree::Span::all()).await?.digest
    );
    Ok(())
}

async fn progress(client: &Client) -> anyhow::Result<Option<(bool, u64, i64)>> {
    let Some(row) = client
        .query_opt(
            &format!(
                r#"SELECT "value", (SELECT count(*) FROM "{}" WHERE get_byte("map", 0) = 109 AND octet_length("key") > 0) AS members
            FROM "{}" WHERE "map" = $1 LIMIT 1"#,
                crate::MAP_TABLE,
                crate::MAP_TABLE
            ),
            &[&vec![b't']],
        )
        .await?
    else {
        return Ok(None);
    };
    let value: Vec<u8> = row.get("value");
    Ok(Some((value[0] == 0, u64::from_be_bytes(value[9..17].try_into()?), row.get("members"))))
}

async fn retry<F, Fut, T>(mut open: F) -> Result<T, RetrievalError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, RetrievalError>>,
{
    for attempt in 0..8 {
        let result = open().await;
        if matches!(&result, Err(RetrievalError::MutationError(error)) if matches!(**error, MutationError::WriteConflict)) && attempt < 7 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        } else {
            return result;
        }
    }
    unreachable!()
}
