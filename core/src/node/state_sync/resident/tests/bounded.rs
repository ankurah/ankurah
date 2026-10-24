use super::*;

async fn limited(node: &TestNode, selection: Selection<Resolved>) -> anyhow::Result<ResidentActor<&'static crate::policy::DefaultContext>> {
    let spec = spec_for(&selection, &node.catalog);
    resident(node, selection, spec).await
}

#[tokio::test]
async fn bounded_capture_retries_to_find_an_unchanged_replacement() -> anyhow::Result<()> {
    let node = node().await?;
    let first = genesis("01")?;
    let second = genesis("02")?;
    apply(&node, &first).await?;
    apply(&node, &second).await?;
    let scans = node.storage.2.load(std::sync::atomic::Ordering::Relaxed);
    let (entered, release) = hold_fetch(&node);
    let actor = {
        let capture = limited(&node, selection(Some(1)));
        tokio::pin!(capture);
        assert!(futures::poll!(&mut capture).is_pending());
        entered.await?;
        apply(&node, &update(&first, "09")?).await?;
        release.send(()).unwrap();
        capture.await?
    };
    assert_eq!(actor.tree.entries().keys().copied().collect::<Vec<_>>(), vec![second.entity_id]);
    assert_eq!(actor.candidates.len(), 1);
    assert_eq!(node.storage.2.load(std::sync::atomic::Ordering::Relaxed), scans + 2, "the raced bounded scan must be repeated");
    let mut different = selection(Some(2));
    assert!(view(&actor, &different, &node.catalog).await.is_err());
    different.limit = None;
    assert!(view(&actor, &different, &node.catalog).await.is_err());
    Ok(())
}

#[derive(Clone)]
struct CaptureSender {
    peer: proto::EntityId,
    messages: Arc<std::sync::Mutex<Vec<proto::NodeMessage>>>,
}
impl PeerSender for CaptureSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.messages.lock().unwrap().push(message);
        Ok(())
    }
    fn recipient_node_id(&self) -> proto::EntityId { self.peer }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

#[tokio::test]
async fn bounded_refill_keeps_all_events_and_a_newer_revision_across_refresh_races() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = [genesis("01")?, genesis("02")?, genesis("03")?];
    for event in &initial {
        apply(&node, event).await?;
    }
    let selected = selection(Some(2));
    let mut actor = limited(&node, selected.clone()).await?;
    let frozen = actor.tree.snapshot();
    let mut snapshot = actor.tree.snapshot();
    let members = snapshot.query_members();
    let peer = proto::EntityId::from_bytes([91; 32]);
    let messages = Arc::new(std::sync::Mutex::new(Vec::new()));
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(CaptureSender { peer, messages: messages.clone() }),
    )
    .await?;
    let known = snapshot.offer().await?;
    let pending = crate::node::state_sync::prepare_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![DEFAULT_CONTEXT],
        Some((proto::QueryId::new(), 1)),
        false,
        actor.revision,
        snapshot,
        known,
    )?;
    let stream = pending.id();
    pending.initialize(&node).await?;
    let owner = Arc::new(());
    actor.routes.push(Route { peer, stream, owner: Arc::downgrade(&owner), selection: selected.clone(), filtered: selected, members });

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
    let revision = actor.revision;
    assert_eq!(current.entries.keys().copied().collect::<BTreeSet<_>>(), [initial[1].entity_id, initial[2].entity_id].into());
    assert_eq!(frozen.entries[&initial[0].entity_id].head, proto::Clock::singleton(&initial[0]));
    assert_eq!(actor.tree.snapshot().entries.len(), 2);
    assert_eq!(actor.candidates.len(), 2);
    assert_eq!(actor.routes.len(), 1);
    let scans = node.storage.2.load(std::sync::atomic::Ordering::Relaxed);
    actor.refresh(&node).await?;
    assert_eq!(actor.revision, revision, "refresh must not roll back the refill's later fence");
    assert_eq!(node.storage.2.load(std::sync::atomic::Ordering::Relaxed), scans, "consumed publications must not be replayed");

    let items = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let mut items = Vec::new();
            let mut complete = false;
            for message in messages.lock().unwrap().iter() {
                let proto::NodeMessage::StateFrame { frame, .. } = message else {
                    continue;
                };
                let proto::StateFrameBody::Live { revision: sent, items: sent_items, complete: last } = &frame.body else {
                    continue;
                };
                if frame.stream == stream && *sent == revision {
                    items.extend(sent_items.clone());
                    complete |= *last;
                }
            }
            if complete {
                break items;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let events = items
        .iter()
        .flat_map(|item| match &item.delta {
            Some(proto::DeltaContent::StateAndEvents { events, .. }) => events
                .iter()
                .map(|event| {
                    let event: proto::Attested<proto::Event> = (item.entity_id, event.clone()).into();
                    event.payload.id()
                })
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .collect::<Vec<_>>();
    assert_eq!(events.iter().cloned().collect::<BTreeSet<_>>(), [first.id(), other.id(), latest.id()].into());
    assert_eq!(events.len(), 3, "each committed event is delivered once");
    let first_position = events.iter().position(|id| *id == first.id()).unwrap();
    let latest_position = events.iter().position(|id| *id == latest.id()).unwrap();
    assert!(first_position < latest_position, "causal event order is preserved within an entity");
    let replacement = items.last().unwrap();
    assert_eq!(replacement.entity_id, initial[2].entity_id);
    assert!(replacement.member.is_some());
    let Some(proto::DeltaContent::StateAndEvents { events, .. }) = &replacement.delta else {
        panic!("unchanged replacement requires a snapshot")
    };
    assert!(events.is_empty());
    node.deregister_peer(peer);
    Ok(())
}

#[tokio::test]
async fn zero_limit_ignores_application_publications_without_reads_or_retention() -> anyhow::Result<()> {
    let node = node().await?;
    let scans = node.storage.2.load(std::sync::atomic::Ordering::Relaxed);
    let mut actor = limited(&node, selection(Some(0))).await?;
    apply(&node, &genesis("01")?).await?;
    let before = actor.revision;
    let current = actor.refresh(&node).await?;
    assert!(actor.revision > before);
    assert!(current.entries.is_empty());
    assert!(actor.tree.entries().is_empty());
    assert!(actor.candidates.is_empty());
    assert_eq!(node.storage.2.load(std::sync::atomic::Ordering::Relaxed), scans);
    Ok(())
}
