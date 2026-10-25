//! Admission: the storage read, the commits that cross it, limited refills and maintenance.
use super::*;

async fn limited(node: &TestNode, selection: Selection<Resolved>) -> anyhow::Result<ResidentActor<&'static crate::policy::DefaultContext>> {
    let spec = spec_for(&selection, &node.catalog);
    resident(node, selection, spec).await
}

#[tokio::test]
async fn an_unlimited_admission_folds_an_entrant_and_a_move_after_its_storage_read() -> anyhow::Result<()> {
    let node = node().await?;
    let a = genesis("01")?;
    apply(&node, &a).await?;
    let selection = selection(None);
    let spec = proto::TreeSpec::new(
        ankurah_core_types::KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(property(), crate::value::ValueType::String)]),
        selection.predicate.clone(),
    );
    let (entered, release) = hold_fetch(&node);
    let admission = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]);
    tokio::pin!(admission);
    assert!(futures::poll!(&mut admission).is_pending());
    entered.await?;
    let moved = update(&a, "09")?;
    let b = genesis("02")?;
    apply(&node, &moved).await?;
    apply(&node, &b).await?;
    release.send(()).unwrap();
    let actor = admission.await?;
    let admitted = view(&actor, &selection, &node.catalog)?;
    assert_eq!(actor.tree.entries().len(), 2);
    assert_eq!(admitted.entries[&a.entity_id].head, proto::Clock::singleton(&moved));
    assert!(admitted.entries.contains_key(&b.entity_id));
    assert!(admitted.entries[&b.entity_id].key() < admitted.entries[&a.entity_id].key());
    Ok(())
}

#[tokio::test]
async fn a_complete_id_keyed_tree_cannot_select_an_unindexed_ordered_limit() -> anyhow::Result<()> {
    let node = node().await?;
    let a = genesis("01")?;
    let b = genesis("02")?;
    apply(&node, &a).await?;
    apply(&node, &b).await?;
    // Deliberately oppose ID order and requested ascending label order.
    let (first, last) = if a.entity_id > b.entity_id { (&a, &b) } else { (&b, &a) };
    apply(&node, &update(first, "01")?).await?;
    apply(&node, &update(last, "02")?).await?;
    let selection = selection(Some(1));
    let by_id = proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), selection.predicate.clone());
    let actor = complete_resident(&node, by_id, vec![DEFAULT_CONTEXT]).await?;
    assert!(view(&actor, &selection, &node.catalog).is_err(), "a complete tree cannot rank properties absent from its keys");
    assert_eq!(actor.tree.entries().len(), 2, "a failed scope never truncates the resident admission");
    assert_eq!(
        actor.tree.snapshot().member_map().values().copied().collect::<Vec<_>>(),
        vec![last.entity_id, first.entity_id],
        "physical digest address order remains by ID"
    );
    Ok(())
}

#[tokio::test]
async fn a_complete_admission_folds_changes_before_resolving_a_limit_with_unchanged_entrants() -> anyhow::Result<()> {
    let node = node().await?;
    let [a, b, c]: [proto::Event; 3] = ["01", "02", "03"].map(genesis).into_iter().collect::<anyhow::Result<Vec<_>>>()?.try_into().unwrap();
    for event in [&a, &b, &c] {
        apply(&node, event).await?;
    }
    let selection = selection(Some(2));
    let spec = spec_for(&selection, &node.catalog);
    let scans = node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed);
    let (entered, release) = hold_fetch(&node);
    let admission = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]);
    tokio::pin!(admission);
    assert!(futures::poll!(&mut admission).is_pending());
    entered.await?;
    apply(&node, &update(&a, "09")?).await?;
    release.send(()).unwrap();
    let actor = admission.await?;
    assert_eq!(actor.tree.entries().len(), 3, "the unchanged replacement is already resident");
    let admitted = view(&actor, &selection, &node.catalog)?;
    assert_eq!(admitted.entries.keys().copied().collect::<BTreeSet<_>>(), BTreeSet::from([b.entity_id, c.entity_id]));
    assert_eq!(
        node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed),
        scans + 1,
        "a concurrent update does not cause a refill read"
    );
    Ok(())
}

#[tokio::test]
async fn limited_scopes_follow_concurrent_movements_without_refetching_or_mutating_old_snapshots() -> anyhow::Result<()> {
    let node = node().await?;
    let mut events = Vec::new();
    for value in ["01", "02", "03"] {
        let event = genesis(value)?;
        apply(&node, &event).await?;
        events.push(event);
    }
    let selection = selection(Some(2));
    let spec = spec_for(&selection, &node.catalog);
    let mut actor = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]).await?;
    let frozen = view(&actor, &selection, &node.catalog)?;
    let scans = node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed);
    let moved_a = update(&events[0], "09")?;
    apply(&node, &moved_a).await?;
    let intermediate = actor.refresh(&node).await?.select(&selection, &node.catalog)?;
    assert_eq!(intermediate.entries.keys().copied().collect::<BTreeSet<_>>(), BTreeSet::from([events[1].entity_id, events[2].entity_id]));
    // C now moves past A. The next scope must re-admit unchanged A from the
    // same resident tree, while both previous comparisons stay frozen.
    apply(&node, &update(&events[2], "10")?).await?;
    let current = actor.refresh(&node).await?.select(&selection, &node.catalog)?;
    assert_eq!(current.entries.keys().copied().collect::<BTreeSet<_>>(), BTreeSet::from([events[0].entity_id, events[1].entity_id]));
    assert_eq!(current.entries[&events[0].entity_id].head, proto::Clock::singleton(&moved_a));
    assert_eq!(frozen.entries[&events[0].entity_id].head, proto::Clock::singleton(&events[0]));
    assert_eq!(intermediate.entries[&events[2].entity_id].head, proto::Clock::singleton(&events[2]));
    assert_eq!(actor.tree.entries().len(), 3);
    assert_eq!(
        node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed),
        scans,
        "supported limited scopes never refill from storage"
    );
    Ok(())
}

#[tokio::test]
async fn a_storage_read_ahead_of_admission_cannot_be_replaced_by_an_older_window_record() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let selection = selection(None);
    let spec = spec_for(&selection, &node.catalog);
    let (entered, observed) = oneshot::channel();
    let (read, resume) = oneshot::channel();
    *node.storage.fetch_pause.lock().unwrap() = Some((entered, resume));
    let admitting = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]);
    tokio::pin!(admitting);
    assert!(futures::poll!(&mut admitting).is_pending());
    observed.await?;
    let first = update(&initial, "02")?;
    apply(&node, &first).await?;
    let second = update(&first, "03")?;
    let (committed, stored) = oneshot::channel();
    let (admit, release) = oneshot::channel();
    *node.storage.inner.hold_after_commit.lock().unwrap() = Some((committed, release));
    let publishing = apply(&node, &second);
    tokio::pin!(publishing);
    assert!(futures::poll!(&mut publishing).is_pending());
    stored.await?;
    read.send(()).unwrap();
    // The raw storage read now sees "03" while the window still contains
    // only "02". Its closing fence must wait for the second admission.
    assert!(futures::poll!(&mut admitting).is_pending());
    admit.send(()).unwrap();
    let (published, admitted) = tokio::join!(publishing, admitting);
    published?;
    let actor = admitted?;
    assert_eq!(actor.tree.entries()[&initial.entity_id].head, proto::Clock::singleton(&second));
    Ok(())
}

#[tokio::test]
async fn a_complete_resident_applies_a_published_batch_while_another_commit_is_in_flight() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let mut actor = complete_resident(&node, spec_for(&selection(None), &node.catalog), vec![DEFAULT_CONTEXT]).await?;
    let first = update(&initial, "02")?;
    apply(&node, &first).await?;
    let batch = actor.cursor.next().await?;
    // Hold the next commit after its storage write, while it holds the commit gate.
    let (committed, stored) = oneshot::channel();
    let (admit, admission) = oneshot::channel();
    *node.storage.inner.hold_after_commit.lock().unwrap() = Some((committed, admission));
    let second = update(&first, "03")?;
    let publishing = apply(&node, &second);
    tokio::pin!(publishing);
    assert!(futures::poll!(&mut publishing).is_pending());
    stored.await?;
    tokio::time::timeout(std::time::Duration::from_secs(2), actor.advance(&node, Some(batch)))
        .await
        .expect("a published batch does not wait for a commit in flight")?;
    assert_eq!(actor.tree.entries()[&initial.entity_id].head, proto::Clock::singleton(&first));
    admit.send(()).unwrap();
    publishing.await?;
    Ok(())
}

#[tokio::test]
async fn maintenance_and_snapshots_retain_only_keys_and_cloned_heads() -> anyhow::Result<()> {
    let node = node().await?;
    let original = genesis("01")?;
    apply(&node, &original).await?;
    let selected = selection(None);
    let spec = spec_for(&selected, &node.catalog);
    let mut actor = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]).await?;
    let id = original.entity_id;
    let frozen = view(&actor, &selected, &node.catalog)?;
    assert_eq!(frozen.entries[&id].head, proto::Clock::singleton(&original));
    assert!(node.entities.get(&id).is_none(), "a resident tree must not pin an entity");

    let moved = update(&original, "02")?;
    apply(&node, &moved).await?;
    let batch = actor.cursor.next().await?;
    let published = batch.changes.iter().find(|change| change.entity_id == id).unwrap();
    assert_eq!(published.entry.as_ref().unwrap().head, proto::Clock::singleton(&moved));
    actor.apply(batch.changes).await?;
    let current = actor.tree.snapshot();
    assert_eq!(current.entries[&id].head, proto::Clock::singleton(&moved));
    assert_eq!(frozen.entries[&id].head, proto::Clock::singleton(&original));
    assert!(node.entities.get(&id).is_none(), "maintenance must not retain an entity handle");
    Ok(())
}

#[tokio::test]
async fn a_limited_admission_retries_to_find_an_unchanged_replacement() -> anyhow::Result<()> {
    let node = node().await?;
    let first = genesis("01")?;
    let second = genesis("02")?;
    apply(&node, &first).await?;
    apply(&node, &second).await?;
    let scans = node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed);
    let (entered, release) = hold_fetch(&node);
    let actor = {
        let admission = limited(&node, selection(Some(1)));
        tokio::pin!(admission);
        assert!(futures::poll!(&mut admission).is_pending());
        entered.await?;
        apply(&node, &update(&first, "09")?).await?;
        release.send(()).unwrap();
        admission.await?
    };
    assert_eq!(actor.tree.entries().keys().copied().collect::<Vec<_>>(), vec![second.entity_id]);
    assert_eq!(actor.candidates.len(), 1);
    assert_eq!(node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed), scans + 2, "the raced bounded scan must be repeated");
    let mut different = selection(Some(2));
    assert!(view(&actor, &different, &node.catalog).is_err());
    different.limit = None;
    assert!(view(&actor, &different, &node.catalog).is_err());
    Ok(())
}

#[tokio::test]
async fn a_limited_refill_raced_by_a_commit_takes_it_and_leaves_the_next_refresh_nothing_to_read() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = [genesis("01")?, genesis("02")?, genesis("03")?];
    for event in &initial {
        apply(&node, event).await?;
    }
    let mut actor = limited(&node, selection(Some(2))).await?;
    let frozen = actor.tree.snapshot();

    let first = update(&initial[0], "09")?;
    let other = update(&initial[1], "05")?;
    apply(&node, &first).await?;
    apply(&node, &other).await?;
    let (entered, release) = hold_fetch(&node);
    let latest = update(&first, "10")?;
    let current = {
        let refresh = actor.refresh(&node);
        tokio::pin!(refresh);
        assert!(futures::poll!(&mut refresh).is_pending());
        entered.await?;
        apply(&node, &latest).await?;
        release.send(()).unwrap();
        refresh.await?
    };
    assert_eq!(current.entries.keys().copied().collect::<BTreeSet<_>>(), [initial[1].entity_id, initial[2].entity_id].into());
    assert_eq!(frozen.entries[&initial[0].entity_id].head, proto::Clock::singleton(&initial[0]));
    assert_eq!(actor.tree.snapshot().entries.len(), 2);
    assert_eq!(actor.candidates.len(), 2);
    let scans = node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed);
    actor.refresh(&node).await?;
    assert_eq!(node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed), scans, "consumed publications must not be replayed");

    Ok(())
}

#[tokio::test]
async fn zero_limit_ignores_application_publications_without_reads_or_retention() -> anyhow::Result<()> {
    let node = node().await?;
    let scans = node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed);
    let mut actor = limited(&node, selection(Some(0))).await?;
    apply(&node, &genesis("01")?).await?;
    let current = actor.refresh(&node).await?;
    assert!(current.entries.is_empty());
    assert!(actor.tree.entries().is_empty());
    assert!(actor.candidates.is_empty());
    assert_eq!(node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed), scans);
    Ok(())
}
