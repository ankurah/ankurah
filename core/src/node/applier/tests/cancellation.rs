//! A caller dropping an application, as a failed state stream drops its in-flight applications.
use super::*;

/// An application dropped after it dispatches its storage commit still advances the resident entity
/// to the state storage committed, whether a snapshot or a bridge carried the change.
#[tokio::test]
async fn an_application_dropped_after_dispatching_its_commit_still_advances_the_resident() -> anyhow::Result<()> {
    #[derive(Clone, Copy, Debug)]
    enum Shape {
        StateAndEvents,
        Bridge,
    }
    for shape in [Shape::StateAndEvents, Shape::Bridge] {
        let fixture = Fixture::new().await?;
        let incoming =
            Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("incoming")?]));
        let snapshot = fixture.remote_snapshot(&incoming).await?;
        let content = match shape {
            Shape::StateAndEvents => {
                proto::DeltaContent::StateAndEvents { state: fragment(snapshot.clone()), events: fragments([incoming.clone()]) }
            }
            Shape::Bridge => proto::DeltaContent::EventBridge { events: fragments([incoming.clone()]) },
        };
        let (committed, committed_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        *fixture.storage.hold_after_commit.lock().unwrap() = Some((committed, release_rx));
        let (notifications, mut advanced) = tokio::sync::mpsc::unbounded_channel();
        let _listener = fixture.entity.broadcast().reference().listen(notifications);

        let mut applying = Box::pin(fixture.apply(content, Delivery::Live));
        assert!(futures::poll!(&mut applying).is_pending());
        committed_rx.await?;
        assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, snapshot, "{shape:?}");
        assert_ne!(fixture.entity.head(), snapshot.head, "{shape:?}: the resident has not advanced yet");
        drop(applying);
        assert!(release.send(()).is_ok(), "{shape:?}: dropping the application dropped the commit it dispatched");

        tokio::time::timeout(std::time::Duration::from_secs(10), advanced.recv()).await?;
        assert_eq!(fixture.entity.to_state()?, snapshot, "{shape:?}");
        assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, fixture.entity.to_state()?, "{shape:?}");
    }
    Ok(())
}
