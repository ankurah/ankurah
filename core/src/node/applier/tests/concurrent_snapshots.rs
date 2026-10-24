//! Snapshots for an entity whose state moved on: concurrent snapshots with and without their
//! connecting events, equal and older snapshots, and a snapshot a reader loaded between the storage
//! commit and the resident entity's advance.
use super::*;

#[tokio::test]
async fn a_concurrent_snapshot_without_events_leaves_the_entity_unchanged() -> anyhow::Result<()> {
    for delivery in [Delivery::Initial, Delivery::Live] {
        let fixture = Fixture::new().await?;
        let remote =
            Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![set(property(), "remote")?]));
        // The comparison reads the sender's history through the getter, as a peer-backed getter would fetch it.
        fixture.events.stage_event(remote.clone());
        let snapshot = fixture.remote_snapshot(&remote).await?;
        let local = Event::update(
            fixture.entity.id(),
            fixture.entity.head(),
            AuthorId::Unknown,
            OperationSet(vec![set(other_property(), "local")?]),
        );
        fixture.commit_event(local.clone()).await?;
        let before = fixture.entity.to_state()?;
        let (notifications, observed) = mpsc::channel();
        let _listener = fixture.entity.broadcast().reference().listen(notifications);

        let outcome = fixture.apply(proto::DeltaContent::StateSnapshot { state: fragment(snapshot.clone()) }, delivery).await?;
        assert!(outcome.change.is_none(), "{delivery:?}");

        assert_eq!(outcome.entity, Some(fixture.entity.clone()));
        assert_eq!(fixture.entity.to_state()?, before);
        assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, before);
        assert!(fixture.storage.get_events(vec![remote.id()]).await?.is_empty());
        assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)));
    }
    Ok(())
}

/// A concurrent snapshot whose connecting events are carried merges those events: the local
/// edit survives and the sender's branch is incorporated.
#[tokio::test]
async fn a_concurrent_snapshot_with_its_events_merges_and_keeps_the_local_edit() -> anyhow::Result<()> {
    for delivery in [Delivery::Initial, Delivery::Live] {
        let fixture = Fixture::new().await?;
        let remote =
            Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![set(property(), "remote")?]));
        let snapshot = fixture.remote_snapshot(&remote).await?;
        let local = Event::update(
            fixture.entity.id(),
            fixture.entity.head(),
            AuthorId::Unknown,
            OperationSet(vec![set(other_property(), "local")?]),
        );
        fixture.commit_event(local.clone()).await?;
        let (notifications, observed) = mpsc::channel();
        let _listener = fixture.entity.broadcast().reference().listen(notifications);

        let outcome = fixture
            .apply(proto::DeltaContent::StateAndEvents { state: fragment(snapshot.clone()), events: fragments([remote.clone()]) }, delivery)
            .await?;

        let change = outcome.change.expect("the merged branch is published");
        let published: &[Attested<Event>] = match delivery {
            Delivery::Initial => &[],
            Delivery::Live => &[Attested::from(remote.clone())],
        };
        assert_eq!(change.events(), published, "{delivery:?}");
        assert_eq!(fixture.entity.head(), Clock::from_events([&local, &remote]));
        assert_eq!(
            values(&fixture.entity)?,
            BTreeMap::from([(property(), Some(Value::String("remote".into()))), (other_property(), Some(Value::String("local".into()))),])
        );
        assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, fixture.entity.to_state()?);
        assert_eq!(fixture.storage.get_events(vec![remote.id()]).await?.len(), 1);
        assert_eq!(observed.try_recv()?, ());
        assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)));
    }
    Ok(())
}

/// Equal and older snapshots leave state unchanged and publish nothing;
/// events carried beside them that the entity has applied are not applied again.
#[tokio::test]
async fn equal_and_older_snapshots_leave_the_entity_unchanged() -> anyhow::Result<()> {
    for delivery in [Delivery::Initial, Delivery::Live] {
        let fixture = Fixture::new().await?;
        let older = fixture.entity.to_state()?;
        let next =
            Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("next")?]));
        fixture.commit_event(next.clone()).await?;
        let current = fixture.entity.to_state()?;
        let (notifications, observed) = mpsc::channel();
        let _listener = fixture.entity.broadcast().reference().listen(notifications);

        let shapes = [
            proto::DeltaContent::StateSnapshot { state: fragment(current.clone()) },
            proto::DeltaContent::StateSnapshot { state: fragment(older.clone()) },
            proto::DeltaContent::StateAndEvents { state: fragment(current.clone()), events: fragments([next.clone()]) },
            proto::DeltaContent::StateAndEvents { state: fragment(older.clone()), events: fragments([fixture.genesis.clone()]) },
        ];
        for content in shapes {
            let description = format!("{delivery:?}: {content:?}");
            let outcome = fixture.apply(content, delivery).await?;
            assert!(outcome.change.is_none(), "{description}");

            assert_eq!(outcome.entity, Some(fixture.entity.clone()), "{description}");
            assert_eq!(fixture.entity.to_state()?, current, "{description}");
            assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)), "{description}");
        }
        assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, current);
    }
    Ok(())
}

#[tokio::test]
async fn adopted_snapshot_reports_change_when_a_reader_loaded_the_committed_state() -> anyhow::Result<()> {
    for delivery in [Delivery::Initial, Delivery::Live] {
        let fixture = Fixture::new().await?;
        let incoming = Event::update(
            fixture.entity.id(),
            fixture.entity.head(),
            AuthorId::Unknown,
            OperationSet(vec![value_operation("incoming")?, Operation::Membership(Membership::Add(model_b()))]),
        );
        fixture.events.stage_event(incoming.clone());
        let snapshot = fixture.remote_snapshot(&incoming).await?;
        let content = match delivery {
            Delivery::Initial => proto::DeltaContent::StateSnapshot { state: fragment(snapshot.clone()) },
            Delivery::Live => {
                proto::DeltaContent::StateAndEvents { state: fragment(snapshot.clone()), events: fragments([incoming.clone()]) }
            }
        };
        let (committed, committed_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        *fixture.storage.hold_after_commit.lock().unwrap() = Some((committed, release_rx));
        let mut applying = Box::pin(fixture.apply(content, delivery));
        assert!(futures::poll!(&mut applying).is_pending());
        committed_rx.await?;

        // Storage is committed, but the applier has not published. A reader can
        // load that state into the resident without notifying query membership.
        let stored = fixture.storage.get_state(fixture.entity.id()).await?.payload.state;
        assert_eq!(stored, snapshot);
        let (advanced, _) = fixture
            .node
            .entities
            .with_state(&LocalStateGetter::new(fixture.storage.clone()), &fixture.events, fixture.entity.id(), stored)
            .await?;
        assert_eq!(advanced, Some(true));
        assert!(fixture.entity.has_membership(&model_b()));
        release.send(()).unwrap();

        let outcome = applying.await?;
        let change = outcome.change.expect("the committed snapshot still needs a reactor notification");
        assert_eq!(change.entity().to_state()?, snapshot);
        assert_eq!(
            change.events(),
            match delivery {
                Delivery::Initial => Vec::new(),
                Delivery::Live => vec![Attested::from(incoming)],
            }
        );
    }
    Ok(())
}
