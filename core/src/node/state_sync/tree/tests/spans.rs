//! A snapshot read as protocol spans: offers, initial inventories, pages, and
//! whether a span digest matches it.
use super::*;

#[tokio::test]
async fn wide_clock_changes_offer_policy_without_changing_the_maintained_tree() -> anyhow::Result<()> {
    let mut tree = QueryTree::new(spec(), Entries::new());
    for count in [3000, 10000] {
        let mut input = state(1, 1);
        input.payload.state.head = proto::Clock::new(
            (0..count as u32)
                .map(|n| {
                    let mut bytes = [0; 32];
                    bytes[..4].copy_from_slice(&n.to_be_bytes());
                    (1, proto::EventId::from_bytes(bytes))
                })
                .collect::<Vec<_>>(),
        )?;
        let mut snapshot = tree.refresh_states(vec![input]).await?;
        assert!(snapshot.page(proto::TreeSpan::all()).is_err(), "follow-up pages have the smaller budget");
        let digest = snapshot.span_digest(proto::TreeSpan::all()).await?;
        let offer = snapshot.offer().await?;
        assert_eq!(matches!(offer.spans.as_slice(), [proto::SpanContent::Heads(_)]), count == 3000);
        assert_eq!(digest.digest.count(), 1);
        assert_eq!(tree.snapshot().span_digest(proto::TreeSpan::all()).await?, digest);
    }
    Ok(())
}

#[tokio::test]
async fn small_and_empty_inventories_offer_exact_heads_from_the_maintained_tree() -> anyhow::Result<()> {
    for count in [0, 32] {
        let mut query = QueryTree::new(spec(), Entries::new());
        let states = (0..count).rev().map(|id| state(id as u8, 1)).collect();
        let mut snapshot = query.refresh_states(states).await?;
        let inventory = snapshot.offer().await?;
        let [proto::SpanContent::Heads(page)] = inventory.spans.as_slice() else { panic!("expected one exact inventory") };
        assert_eq!(page.bounds, proto::TreeSpan::all());
        assert_eq!(page.heads.len(), count);
        assert_eq!(snapshot.member_map().len(), count);
        for (id, leaf) in page.heads.to_vec().iter().enumerate() {
            assert!(leaf.key.is_empty());
            assert_eq!(leaf.entity.entity_id, EntityId::from_bytes([id as u8; EntityId::BYTE_LEN]));
        }
    }
    Ok(())
}

#[tokio::test]
async fn leaf_pages_budget_the_exact_answer_and_preserve_old_wide_key_snapshots() -> anyhow::Result<()> {
    let wide_spec = TreeSpec::new(
        KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(
            proto::PropertyId::EntityId(EntityId::from_bytes([42; EntityId::BYTE_LEN])),
            ValueType::String,
        )]),
        ankql::ast::Predicate::True,
    );
    let rows = (0..32)
        .map(|id| {
            let mut row = keyed_state(id as u32, 1, Some(Value::String("key".repeat(160))))?;
            row.0.payload.state.head = proto::Clock::new(
                (0..100u32)
                    .map(|n| {
                        let mut bytes = [0; 32];
                        bytes[..4].copy_from_slice(&n.to_be_bytes());
                        (1, proto::EventId::from_bytes(bytes))
                    })
                    .collect::<Vec<_>>(),
            )?;
            Ok(row)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut query = QueryTree::new(wide_spec.clone(), Entries::new());
    let mut old = query.replace_values(rows).await?;
    let mut remaining = proto::TreeSpan::all();
    let mut pages = Vec::new();
    let mut ids = Vec::new();
    loop {
        let page = old.page(remaining.clone())?;
        assert_eq!(page.bounds.start, remaining.start);
        assert!(page.work_bytes()? <= MAX_PAGE_BYTES as u64);
        ids.extend(page.heads.to_vec().iter().map(|leaf| leaf.entity.entity_id));
        let complete = page.bounds.end == proto::TreeSpan::all().end;
        if !complete {
            let Bound::Included(last) = &page.bounds.end else { panic!("a byte-bounded page must advance") };
            remaining = proto::TreeSpan::all().after(last);
        }
        pages.push(page);
        if complete {
            break;
        }
    }
    assert!(pages.len() > 1, "clock and key bytes must split an ordinary inline-sized inventory");
    assert_eq!(ids, old.member_map().values().copied().collect::<Vec<_>>());

    let offer = old.offer().await?;
    assert!(
        matches!(offer.spans.as_slice(), [proto::SpanContent::Heads(_)]),
        "one initial inventory has its own budget, independent of follow-up pagination"
    );

    let changed = (0..32).map(|id| keyed_state(id as u32, 2, Some(Value::String("changed".into())))).collect::<anyhow::Result<Vec<_>>>()?;
    query.replace_values(changed).await?;
    assert_eq!(bincode::serialize(&old.page(proto::TreeSpan::all())?)?, bincode::serialize(&pages[0])?);
    Ok(())
}

#[tokio::test]
async fn a_page_fills_to_the_work_of_its_whole_keys() -> anyhow::Result<()> {
    // Each head counts its 436-byte key, entity ID and one-tip clock as the codec
    // writes them unshared, so a full page of them fits.
    let spec = TreeSpec::new(
        KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(
            proto::PropertyId::EntityId(EntityId::from_bytes([42; EntityId::BYTE_LEN])),
            ValueType::String,
        )]),
        ankql::ast::Predicate::True,
    );
    let rows = (0..MAX_PAGE_HEADS as u32)
        .map(|id| keyed_state(id, 1, Some(Value::String("k".repeat(433)))))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let snapshot = values_snapshot(spec, rows).await?;
    let page = snapshot.page(proto::TreeSpan::all())?;
    assert_eq!(page.heads.len(), MAX_PAGE_HEADS);
    assert!(page.work_bytes()? <= MAX_PAGE_BYTES as u64);
    Ok(())
}

#[tokio::test]
async fn canonical_pages_cover_requested_bounds() -> anyhow::Result<()> {
    let spec = TreeSpec::new(
        KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(proto::PropertyId::Id, ValueType::EntityId)]),
        ankql::ast::Predicate::True,
    );
    let mut query = QueryTree::new(spec, Entries::new());
    let snapshot = query.refresh_states((0..70).rev().map(|id| state(id, 1)).collect()).await?;
    let members = snapshot.member_map();
    let addresses = members.keys().cloned().collect::<Vec<_>>();
    let requested = proto::TreeSpan::new(Bound::Excluded(addresses[2].clone()), Bound::Included(addresses[67].clone()));
    let mut remaining = requested.clone();
    let mut received = Vec::new();
    loop {
        let page = snapshot.page(remaining.clone())?;
        assert_eq!(page.bounds.start, remaining.start);
        assert!(page.heads.len() <= MAX_PAGE_HEADS);
        for leaf in page.heads.to_vec() {
            let address = leaf_address(&leaf.key, leaf.entity.entity_id);
            assert!(requested.contains(&address));
            assert!(page.bounds.contains(&address));
            received.push(address);
        }
        if page.bounds.end == requested.end {
            break;
        }
        let Bound::Included(last) = page.bounds.end else { panic!("page must end at its last leaf") };
        remaining = requested.after(&last);
    }
    assert_eq!(received, addresses[3..68]);

    Ok(())
}

#[tokio::test]
async fn a_new_summary_frontier_is_bounded_and_covers_the_entire_snapshot_including_empty_gaps() -> anyhow::Result<()> {
    let states = (0..4096u32)
        .map(|n| {
            let mut state = state(0, 1);
            let mut bytes = [0; EntityId::BYTE_LEN];
            bytes[..4].copy_from_slice(&n.to_be_bytes());
            state.payload.entity_id = EntityId::from_bytes(bytes);
            state
        })
        .collect::<Vec<_>>();
    let mut query = QueryTree::new(spec(), Entries::new());
    let mut snapshot = query.refresh_states(states.clone()).await?;
    let inventory = snapshot.offer().await?;
    assert!(inventory.spans.len() > 1, "a new large tree must offer its bounded cached descendants");
    assert!(inventory.spans.len() <= MAX_OFFER_SPANS);
    assert!(bincode::serialized_size(&inventory)? <= FRONTIER_BYTES);
    let mut next = proto::TreeSpan::all().start;
    let mut digests = Vec::new();
    let mut empty = false;
    for region in inventory.spans {
        let proto::SpanContent::Digest(head) = region else { panic!("large offer must contain only summaries") };
        assert_eq!(head.bounds.start, next, "spans must be disjoint and exactly adjacent");
        assert_eq!(snapshot.span_digest(head.bounds.clone()).await?.digest, head.digest, "each head describes its exact range");
        empty |= head.digest.count() == 0;
        digests.push(head.digest);
        next = match head.bounds.end {
            Bound::Included(end) => Bound::Excluded(end),
            Bound::Excluded(end) => Bound::Included(end),
            Bound::Unbounded => Bound::Unbounded,
        };
    }
    assert_eq!(next, proto::TreeSpan::all().end);
    assert!(empty, "gaps before and after the populated addresses must remain explicit");
    let root = snapshot.span_digest(proto::TreeSpan::all()).await?;
    assert_eq!(ankurah_digest_tree::Digest::checked_sum(digests.iter().copied())?, root.digest);

    let mut warm = query.refresh_states(states).await?;
    let retained = warm.offer().await?;
    assert!(retained.spans.len() > 1, "retained offers must also identify bounded changed spans");
    assert!(retained.spans.len() <= MAX_OFFER_SPANS);
    assert!(bincode::serialized_size(&retained)? <= FRONTIER_BYTES);
    assert!(retained
        .spans
        .iter()
        .all(|region| matches!(region, proto::SpanContent::Digest(head) if head.digest.count() <= MAX_PAGE_HEADS as i64)));
    assert_eq!(offered_digest(&retained)?, root.digest);
    Ok(())
}

#[tokio::test]
async fn complete_initial_inventories_are_bounded_and_immutable() -> anyhow::Result<()> {
    for count in [256, MAX_OFFER_HEADS] {
        let mut query = QueryTree::new(spec(), Entries::new());
        let mut old = query.refresh_states((0..count as u32).map(|id| numbered_state(id, 1)).collect()).await?;
        let offer = old.offer().await?;
        assert!(bincode::serialized_size(&offer)? <= MAX_OFFER_BYTES as u64);
        let [proto::SpanContent::Heads(inventory)] = offer.spans.as_slice() else { panic!("one complete initial inventory") };
        assert_eq!(inventory.bounds, proto::TreeSpan::all());
        assert_eq!(inventory.heads.len(), count);
        let received = inventory.heads.to_vec().iter().map(|leaf| leaf.entity.entity_id).collect::<Vec<_>>();
        let wire = bincode::serialize(&offer)?;
        if count == MAX_OFFER_HEADS {
            assert!(wire.len() > MAX_PAGE_BYTES, "the request has a separate byte budget");
        }
        let decoded: proto::KnownState = bincode::deserialize(&wire)?;
        assert_eq!(bincode::serialize(&decoded)?, wire);
        assert_eq!(received, old.member_map().values().copied().collect::<Vec<_>>());

        query.refresh_states(vec![numbered_state(17, 2)]).await?;
        assert_eq!(bincode::serialize(&old.offer().await?)?, bincode::serialize(&offer)?);
    }
    Ok(())
}

#[tokio::test]
async fn incomplete_initial_inventory_falls_back_for_count_or_total_bytes() -> anyhow::Result<()> {
    let mut query = QueryTree::new(spec(), Entries::new());
    let mut too_many = query.refresh_states((0..2049).map(|id| numbered_state(id, 1)).collect()).await?;
    let offer = too_many.offer().await?;
    assert!(offer.spans.iter().all(|region| matches!(region, proto::SpanContent::Digest(_))));
    assert_eq!(offered_digest(&offer)?.count(), 2049);

    let head = proto::Clock::new(
        (0u32..200)
            .map(|n| {
                let mut bytes = [0; 32];
                bytes[..4].copy_from_slice(&n.to_be_bytes());
                (1, proto::EventId::from_bytes(bytes))
            })
            .collect::<Vec<_>>(),
    )?;
    let mut wide = query
        .refresh_states(
            (0..64)
                .map(|id| {
                    let mut state = numbered_state(id, 1);
                    state.payload.state.head = head.clone();
                    state
                })
                .collect(),
        )
        .await?;
    let mut pages = Vec::new();
    let mut remaining = proto::TreeSpan::all();
    loop {
        let page = wide.page(remaining)?;
        let end = page.bounds.end.clone();
        pages.push(proto::SpanContent::Heads(page));
        if end == proto::TreeSpan::all().end {
            break;
        }
        let Bound::Included(last) = end else { panic!("progress") };
        remaining = proto::TreeSpan::all().after(&last);
    }
    let oversized = proto::KnownState { spec: spec(), spans: pages };
    assert!(bincode::serialized_size(&oversized)? > MAX_OFFER_BYTES as u64);
    let offer = wide.offer().await?;
    assert!(
        offer.spans.iter().all(|region| matches!(region, proto::SpanContent::Digest(_))),
        "no partial leaf cover may escape the initial budget"
    );
    assert_eq!(offered_digest(&offer)?.count(), 64);
    Ok(())
}

#[tokio::test]
async fn limited_peers_answer_bounds_using_their_own_cut() -> anyhow::Result<()> {
    let mut requester = QueryTree::new(keyed_spec(), Entries::new());
    let mut responder = QueryTree::new(keyed_spec(), Entries::new());
    let request = range_request(-1, Some(2), false);
    let rows = |ids: &[u32]| ids.iter().map(|id| keyed_state(*id, 1, Some(Value::I64(*id as i64)))).collect::<anyhow::Result<Vec<_>>>();
    let mut left = requester.replace_values(rows(&[0, 3, 4])?).await?.select_request(&request)?;
    let mut right = responder.replace_values(rows(&[1, 2, 4])?).await?.select_request(&request)?;
    for view in [&mut left, &mut right] {
        let offer = view.offer().await?;
        let [proto::SpanContent::Heads(inventory)] = offer.spans.as_slice() else { panic!("limited inventory") };
        assert_eq!(inventory.bounds, proto::TreeSpan::all());
        assert_eq!(inventory.heads.len(), 2);
    }
    let left_last = left.members.keys().last().unwrap().to_vec();
    let point = proto::TreeSpan::new(Bound::Included(left_last.clone()), Bound::Included(left_last));
    assert_eq!(left.span_digest(point.clone()).await?.digest.count(), 1);
    assert_eq!(right.span_digest(point).await?.digest.count(), 0);
    let right_last = right.members.keys().last().unwrap().to_vec();
    let point = proto::TreeSpan::new(Bound::Included(right_last.clone()), Bound::Included(right_last));
    assert_eq!(left.span_digest(point.clone()).await?.digest.count(), 0);
    assert_eq!(right.span_digest(point).await?.digest.count(), 1);
    assert_eq!(left.span_digest(proto::TreeSpan::all()).await?.digest.count(), 2);
    assert_eq!(right.span_digest(proto::TreeSpan::all()).await?.digest.count(), 2);
    Ok(())
}

#[tokio::test]
async fn a_snapshot_matches_a_span_digest_only_with_as_many_members_within_its_bounds_and_an_equal_digest() -> anyhow::Result<()> {
    let (sender, mut requests) = tokio::sync::mpsc::channel(1);
    let mut copy = QueryTree::new(spec(), Entries::new())
        .refresh_states((0..3).map(|id| state(id, 1)).collect())
        .await?
        .asking(&DigestRequests::new(&sender));
    let more = digest_of((0..4).map(|id| state(id, 1)).collect(), proto::TreeSpan::all()).await?;
    let matched = tokio::select! {
        matched = copy.matches(&more) => matched?,
        _ = requests.recv() => anyhow::bail!("the copy read a digest to judge a span it holds fewer members in"),
    };
    assert!(!matched, "a span holding more members than the copy does not match");

    // Without its tree, the copy builds the digests it reads.
    drop(sender);
    // Below entity 2, since an address under this spec is the entity id.
    let below = proto::TreeSpan::new(Bound::Unbounded, Bound::Excluded(vec![2; EntityId::BYTE_LEN]));
    let same = digest_of(vec![state(0, 1), state(1, 1), state(9, 1)], below.clone()).await?;
    assert!(copy.matches(&same).await?, "as many members within the bounds, at the same heads");
    let other_head = digest_of(vec![state(0, 1), state(1, 2)], below).await?;
    assert!(!copy.matches(&other_head).await?, "as many members within the bounds, one at another head");
    Ok(())
}
