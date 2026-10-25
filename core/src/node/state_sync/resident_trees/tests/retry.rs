//! Admission retries after catalog changes, races and lag, and a commit whose outcome is in doubt.
use super::*;
use crate::reactor::maintenance::MAX_PENDING_BYTES;

/// The unlimited fixture selection, narrowed by a comparison on its ordering property.
fn keyed_selection() -> Selection<Resolved> {
    let mut selected = selection(None);
    selected.predicate = Predicate::And(
        Box::new(selected.predicate),
        Box::new(Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Path(PropertyPath::from(property()))),
            operator: ankql::ast::ComparisonOperator::GreaterThanOrEqual,
            right: Box::new(ankql::ast::Expr::Literal(Value::String("0".into()))),
        }),
    );
    selected
}

async fn overflow_maintenance_bytes(node: &TestNode, latest: &mut proto::Event) -> anyhow::Result<()> {
    // A few large indexed keys exceed the unchanged byte budget regardless of
    // commit grouping. Restore a small value before reopening the held scan.
    let large = format!("01-{}", "x".repeat(MAX_PENDING_BYTES / 4));
    for _ in 0..4 {
        *latest = update(latest, &large)?;
        apply(node, latest).await?;
    }
    *latest = update(latest, "01")?;
    apply(node, latest).await?;
    Ok(())
}

async fn add_catalog_model(node: &TestNode, label: &str) -> anyhow::Result<()> {
    let context = node.privileged_context();
    let transaction = context.begin();
    transaction.create(&crate::schema::catalog::SysModelRow { label: label.into(), name: "Admission retry fixture".into() }).await?;
    transaction.commit().await?;
    Ok(())
}

#[tokio::test]
async fn a_catalog_change_during_an_admission_reopens_the_source() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let scans = node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed);
    let (entered, release) = hold_fetch(&node);
    let admission = node.resident_trees.snapshot(&node, selection(None), None, vec![DEFAULT_CONTEXT]);
    tokio::pin!(admission);
    assert!(futures::poll!(&mut admission).is_pending());
    entered.await?;
    add_catalog_model(&node, "fetch_admission_fixture").await?;
    release.send(()).unwrap();
    let admitted = admission.await?;
    assert_eq!(admitted.snapshot.entries.len(), 1);
    assert_eq!(admitted.snapshot.entries[&initial.entity_id].head, proto::Clock::singleton(&initial));
    assert_eq!(node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed) - scans, 2, "the raced admission must be read again");
    assert_eq!(node.resident_trees.len(), 1, "only the replacement admission may be reusable");
    Ok(())
}

#[tokio::test]
async fn catalog_churn_across_every_admission_fails_a_snapshot_retryably_and_a_later_snapshot_recovers() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let scans = node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed);
    let (mut entered, mut release) = hold_fetch(&node);
    let admission = node.resident_trees.snapshot(&node, selection(None), None, vec![DEFAULT_CONTEXT]);
    tokio::pin!(admission);
    let admissions = super::super::MAX_SNAPSHOT_ATTEMPTS;
    for attempt in 1..=admissions {
        tokio::select! {
            _ = &mut admission => anyhow::bail!("the snapshot ended while its admission's read was held"),
            read = &mut entered => read?,
        }
        add_catalog_model(&node, &format!("catalog_churn_{attempt}")).await?;
        if attempt == admissions {
            release.send(()).unwrap();
            break;
        }
        let next = hold_fetch(&node);
        release.send(()).unwrap();
        (entered, release) = next;
    }
    let error = admission.await.err().expect("catalog churn across every admission exhausts the snapshot's retries");
    assert!(matches!(error.downcast_ref::<SourceError>(), Some(SourceError::CatalogChanged)));
    assert!(SourceError::is_retryable(&error));
    assert_eq!(node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed) - scans, admissions as usize, "one read per admission");
    assert_eq!(node.resident_trees.len(), 0);
    let current = node.resident_trees.snapshot(&node, selection(None), None, vec![DEFAULT_CONTEXT]).await?;
    assert_eq!(current.snapshot.entries.len(), 1);
    assert!(current.snapshot.entries.contains_key(&initial.entity_id));
    Ok(())
}

#[tokio::test]
async fn catalog_change_invalidates_an_existing_source_with_a_retryable_error() -> anyhow::Result<()> {
    let node = node().await?;
    let selected = selection(None);
    let mut source = complete_tree(&node, spec_for(&selected, &node.catalog), vec![DEFAULT_CONTEXT]).await?;
    add_catalog_model(&node, "existing_admission_fixture").await?;
    let error = source.refresh(&node).await.err().expect("an existing source cannot cross a catalog change");
    assert!(matches!(error.downcast_ref::<SourceError>(), Some(SourceError::CatalogChanged)));
    assert!(SourceError::is_retryable(&error));
    Ok(())
}

#[tokio::test]
async fn a_lagged_admission_is_admitted_again_from_a_fresh_window() -> anyhow::Result<()> {
    let node = node().await?;
    let mut latest = genesis("01")?;
    apply(&node, &latest).await?;
    let scans = node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed);
    let (entered, release) = hold_fetch(&node);
    let admission = node.resident_trees.snapshot(&node, keyed_selection(), None, vec![DEFAULT_CONTEXT]);
    tokio::pin!(admission);
    tokio::select! {
        _ = &mut admission => anyhow::bail!("the snapshot ended while its admission's read was held"),
        read = entered => read?,
    }
    overflow_maintenance_bytes(&node, &mut latest).await?;
    release.send(()).unwrap();
    let current = admission.await?;
    assert_eq!(current.snapshot.entries[&latest.entity_id].head, proto::Clock::singleton(&latest));
    assert_eq!(node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed) - scans, 2, "the lagged admission's window was read again");
    assert_eq!(node.resident_trees.len(), 1);
    Ok(())
}

#[tokio::test]
async fn a_commit_in_doubt_stops_the_trees_that_exist_and_a_new_admission_reads_storage() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let mut existing = node.resident_trees.snapshot(&node, selection(None), None, vec![DEFAULT_CONTEXT]).await?;
    // A commit that took the gate and stopped before publishing.
    drop(node.reactor.begin_commit().await);
    let stopped = tokio::time::timeout(std::time::Duration::from_secs(5), existing.changes.changed()).await?;
    assert!(stopped.is_err(), "the tree that existed stops");
    let admitted = node.resident_trees.snapshot(&node, selection(None), None, vec![DEFAULT_CONTEXT]).await?;
    assert_eq!(admitted.snapshot.entries[&initial.entity_id].head, proto::Clock::singleton(&initial), "a new admission reads storage");
    Ok(())
}
