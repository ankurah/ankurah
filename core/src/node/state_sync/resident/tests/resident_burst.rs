use super::*;

async fn wait_for_original_tree(
    resident: &ResidentLease<&'static crate::policy::DefaultContext>,
    expected: &BTreeMap<proto::EntityId, proto::Clock>,
) -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        loop {
            // Read through the original lease, not the resident cache: the
            // cache's retry can silently replace a failed resident and conceal
            // a lost maintenance queue.
            let current = {
                let snapshot = resident.snapshot().await?.snapshot;
                snapshot.entries.len() == expected.len()
                    && expected.iter().all(|(id, head)| snapshot.entries.get(id).is_some_and(|entry| &entry.head == head))
            };
            if current {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn original_resident_survives_a_4096_row_cold_transfer_and_an_idle_update() -> anyhow::Result<()> {
    let (server, client, _messages) = super::super::capture_retry::pair().await?;
    let mut events = Vec::with_capacity(4096);
    for row in 0..4096 {
        let event = genesis(&format!("row-{row:08}"))?;
        apply(&server, &event).await?;
        events.push(event);
    }
    let mut expected: BTreeMap<_, _> = events.iter().map(|event| (event.entity_id, proto::Clock::singleton(event))).collect();
    let selection = above("", None);
    // Match a busy node whose model views already hold the canonical entities:
    // current resident payloads fill frames without the storage-read work cap.
    let _server_rows = server.fetch_entities_from_local(&selection).await?;
    let resident = acquire(&client.resident_trees, &client, selection.clone()).await?;
    let original = resident.identity();
    assert!(resident.snapshot().await?.snapshot.entries.is_empty());

    let fetched =
        tokio::time::timeout(std::time::Duration::from_secs(60), client.fetch_from_peer(selection.clone(), &vec![DEFAULT_CONTEXT]))
            .await??;
    assert_eq!(fetched.iter().map(|entity| (entity.id(), entity.head())).collect::<BTreeMap<_, _>>(), expected);
    wait_for_original_tree(&resident, &expected).await?;

    let scans = client.storage.2.load(Ordering::Relaxed);
    let exact = acquire(&client.resident_trees, &client, selection.clone()).await?;
    assert!(Weak::ptr_eq(&original, &exact.identity()), "successful transfer must not hide a replacement tree");
    let narrower = acquire(&client.resident_trees, &client, above("row-00002047", Some(10))).await?;
    assert!(Weak::ptr_eq(&original, &narrower.identity()), "a compatible limited selection shares the maintained tree");
    assert_eq!(
        narrower.snapshot().await?.snapshot.entries.keys().copied().collect::<BTreeSet<_>>(),
        events[2048..2058].iter().map(|event| event.entity_id).collect(),
    );
    assert_eq!(client.storage.2.load(Ordering::Relaxed), scans, "neither reuse path rebuilds from storage");
    drop((exact, narrower, fetched));

    // Finish every peer owner before checking maintenance without routes.
    let streams = client.peer_connections.get(&server.id).unwrap().outgoing_state_streams.clone();
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !streams.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let moved = update(&events[0], "row-99999999")?;
    let previous_key = resident.snapshot().await?.snapshot.entries[&moved.entity_id].key.clone();
    apply(&client, &moved).await?;
    expected.insert(moved.entity_id, proto::Clock::singleton(&moved));
    wait_for_original_tree(&resident, &expected).await?;
    assert_ne!(resident.snapshot().await?.snapshot.entries[&moved.entity_id].key, previous_key);
    let reused = acquire(&client.resident_trees, &client, selection).await?;
    assert!(Weak::ptr_eq(&original, &reused.identity()), "maintenance keeps the same resident reusable");
    assert_eq!(client.storage.2.load(Ordering::Relaxed), scans);
    Ok(())
}
