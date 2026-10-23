use super::*;
use crate::value::Value;
mod ordering;
use std::ops::Bound;

fn spec() -> TreeSpec { TreeSpec::new(KeySpec::new(vec![]), ankql::ast::Predicate::True) }

fn state(id: u8, head: u8) -> Attested<EntityState> {
    Attested {
        payload: EntityState {
            entity_id: EntityId::from_bytes([id; EntityId::BYTE_LEN]),
            state: proto::State { head: proto::Clock::genesis(proto::EventId::from_bytes([head; 32])), ..Default::default() },
        },
        attestations: Default::default(),
    }
}

fn numbered_state(id: u32, head: u8) -> Attested<EntityState> {
    let mut state = state(0, head);
    let mut bytes = [0; EntityId::BYTE_LEN];
    bytes[..4].copy_from_slice(&id.to_be_bytes());
    state.payload.entity_id = EntityId::from_bytes(bytes);
    state
}

fn offered_digest(offer: &proto::KnownState) -> anyhow::Result<ankurah_digest_tree::Digest> {
    ankurah_digest_tree::Digest::checked_sum(offer.spans.iter().map(|region| {
        let proto::SpanContent::Digest(head) = region else { panic!("expected only summaries") };
        head.digest
    }))
    .map_err(Into::into)
}

fn keyed_spec() -> TreeSpec {
    TreeSpec::new(
        KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(
            proto::PropertyId::EntityId(EntityId::from_bytes([42; EntityId::BYTE_LEN])),
            ValueType::I64,
        )]),
        ankql::ast::Predicate::True,
    )
}

fn keyed_state(id: u32, head: u8, value: Option<Value>) -> anyhow::Result<(Attested<EntityState>, Vec<Option<Value>>)> {
    use crate::property::backend::{LWWBackend, PropertyBackend};
    let property = proto::PropertyId::EntityId(EntityId::from_bytes([42; EntityId::BYTE_LEN]));
    let backend = LWWBackend::new();
    backend.set(property, value);
    crate::test_helpers::commit_backend(&backend, head)?;
    let values = vec![backend.get(&property)];
    let mut state = numbered_state(id, head);
    state.payload.state.state_buffers = proto::StateBuffers([("lww".into(), backend.to_state_buffer()?)].into());
    Ok((state, values))
}

fn entries(spec: &TreeSpec, rows: Vec<(Attested<EntityState>, Vec<Option<Value>>)>) -> anyhow::Result<Vec<TreeEntry>> {
    rows.into_iter()
        .map(|(state, values)| {
            Ok(TreeEntry {
                entity_id: state.payload.entity_id,
                key: CanonicalCodec::encode_nullable(spec.key_spec(), &values)?,
                head: state.payload.state.head,
            })
        })
        .collect()
}

impl QueryTree {
    async fn replace_values(&self, spec: TreeSpec, rows: Vec<(Attested<EntityState>, Vec<Option<Value>>)>) -> anyhow::Result<Snapshot> {
        self.refresh_entries(spec.clone(), entries(&spec, rows)?).await
    }
    async fn apply_values(&self, rows: Vec<(Attested<EntityState>, Vec<Option<Value>>)>, removed: Vec<EntityId>) -> anyhow::Result<()> {
        let spec = self.current_snapshot().await?.physical_spec;
        self.apply_entries(entries(&spec, rows)?, removed).await?;
        Ok(())
    }
}

#[tokio::test]
async fn digest_and_cloned_heads_advance_together_without_retaining_payloads() -> anyhow::Result<()> {
    let tree = QueryTree::default();
    let mut input = keyed_state(1, 1, Some(Value::I64(5)))?.0;
    let original_head = input.payload.state.head.clone();
    let mut old = tree.refresh(keyed_spec(), vec![input.clone()]).await?;
    let old_digest = old.span_digest(super::super::whole()).await?;
    input.payload.state.head = proto::Clock::genesis(proto::EventId::from_bytes([7; 32]));
    // Mutating the source state does not mutate the metadata that was admitted.
    assert_eq!(old.entries[&input.payload.entity_id].head, original_head);
    let changed = TreeEntry {
        entity_id: input.payload.entity_id,
        key: old.entries[&input.payload.entity_id].key.clone(),
        head: input.payload.state.head.clone(),
    };
    tree.apply_entries(vec![changed.clone()], vec![]).await?;
    let mut current = tree.current_snapshot().await?;
    assert_eq!(current.entries[&changed.entity_id], changed);
    assert_ne!(current.span_digest(super::super::whole()).await?.digest, old_digest.digest);
    assert_eq!(old.span_digest(super::super::whole()).await?, old_digest);
    assert_eq!(old.entries[&input.payload.entity_id].head, original_head);
    Ok(())
}

#[tokio::test]
async fn different_spec_requires_a_distinct_tree() -> anyhow::Result<()> {
    let tree = QueryTree::default();
    let mut old = tree.refresh(spec(), vec![state(1, 1)]).await?;
    let original = old.span_digest(super::super::whole()).await?;
    assert!(tree.replace_values(keyed_spec(), vec![keyed_state(1, 2, Some(Value::I64(8)))?]).await.is_err());
    assert_eq!(tree.current_snapshot().await?.spec, spec());
    assert_eq!(tree.current_snapshot().await?.span_digest(super::super::whole()).await?, original);
    let other = QueryTree::default();
    assert_ne!(
        other
            .replace_values(keyed_spec(), vec![keyed_state(1, 2, Some(Value::I64(8)))?])
            .await?
            .span_digest(super::super::whole())
            .await?
            .digest,
        original.digest
    );
    Ok(())
}

#[tokio::test]
async fn invalid_entry_update_preserves_heads_membership_and_digest() -> anyhow::Result<()> {
    let tree = QueryTree::default();
    let mut old = tree.replace_values(keyed_spec(), vec![keyed_state(1, 1, Some(Value::I64(8)))?]).await?;
    let digest = old.span_digest(super::super::whole()).await?;
    let id = numbered_state(1, 1).payload.entity_id;
    let invalid = TreeEntry { entity_id: id, key: vec![0xff], head: state(1, 2).payload.state.head };
    assert!(tree.apply_entries(vec![invalid], vec![id]).await.is_err());
    let mut current = tree.current_snapshot().await?;
    assert_eq!(current.entries, old.entries);
    assert_eq!(current.members(), old.members());
    assert_eq!(current.span_digest(super::super::whole()).await?, digest);
    Ok(())
}

#[tokio::test]
async fn wide_clock_changes_offer_policy_without_changing_the_maintained_tree() -> anyhow::Result<()> {
    let tree = QueryTree::default();
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
        let mut snapshot = tree.refresh(spec(), vec![input]).await?;
        assert!(snapshot.page(super::super::whole()).is_err(), "follow-up pages have the smaller budget");
        let digest = snapshot.span_digest(super::super::whole()).await?;
        let offer = snapshot.offer().await?;
        assert_eq!(matches!(offer.spans.as_slice(), [proto::SpanContent::Heads(_)]), count == 3000);
        assert_eq!(digest.digest.count(), 1);
        assert_eq!(tree.current_snapshot().await?.span_digest(super::super::whole()).await?, digest);
    }
    Ok(())
}

#[tokio::test]
async fn projected_keys_match_transient_decoding_across_sparse_movement_and_dense_refresh() -> anyhow::Result<()> {
    let initial =
        (0..256).map(|id| keyed_state(id, 1, (id != 11).then_some(Value::I64(id as i64 - 128)))).collect::<anyhow::Result<Vec<_>>>()?;
    let query = QueryTree::default();
    let mut old = query.replace_values(keyed_spec(), initial.clone()).await?;
    let original = old.span_digest(super::super::whole()).await?;
    let original_members = old.members();
    let oracle = QueryTree::default();
    let mut decoded = oracle.refresh(keyed_spec(), initial.iter().map(|(state, _)| state.clone()).collect()).await?;
    assert_eq!(original, decoded.span_digest(super::super::whole()).await?);
    assert_eq!(original_members, decoded.members());

    let mut sparse = initial;
    sparse[7] = keyed_state(7, 2, Some(Value::I64(9999)))?;
    sparse.retain(|(state, _)| state.payload.entity_id != numbered_state(21, 1).payload.entity_id);
    sparse.push(keyed_state(256, 2, Some(Value::I64(-5000)))?);
    let dense = sparse
        .iter()
        .enumerate()
        .map(|(n, (state, _))| {
            let id = u32::from_be_bytes(state.payload.entity_id.to_bytes()[..4].try_into().unwrap());
            keyed_state(id, 3, Some(Value::I64(n as i64 + 12345)))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    for rows in [sparse, dense] {
        let mut captured = query.replace_values(keyed_spec(), rows.clone()).await?;

        let oracle = QueryTree::default();
        let mut decoded = oracle.refresh(keyed_spec(), rows.into_iter().map(|(state, _)| state).collect()).await?;
        assert_eq!(captured.members(), decoded.members());
        assert_eq!(captured.span_digest(super::super::whole()).await?, decoded.span_digest(super::super::whole()).await?);
        assert_eq!(old.members(), original_members);
        assert_eq!(old.span_digest(super::super::whole()).await?, original);
    }
    Ok(())
}

#[tokio::test]
async fn small_and_empty_inventories_offer_exact_heads_from_the_maintained_tree() -> anyhow::Result<()> {
    for count in [0, 32] {
        let query = QueryTree::default();
        let states = (0..count).rev().map(|id| state(id as u8, 1)).collect();
        let mut snapshot = query.refresh(spec(), states).await?;
        let inventory = snapshot.offer().await?;
        let [proto::SpanContent::Heads(page)] = inventory.spans.as_slice() else { panic!("expected one exact inventory") };
        assert_eq!(page.bounds, super::super::whole());
        assert_eq!(page.heads.len(), count);
        assert_eq!(snapshot.members().len(), count);
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
    let query = QueryTree::default();
    let mut old = query.replace_values(wide_spec.clone(), rows).await?;
    let mut remaining = super::super::whole();
    let mut pages = Vec::new();
    let mut ids = Vec::new();
    loop {
        let page = old.page(remaining.clone())?;
        assert_eq!(page.bounds.start, remaining.start);
        assert!(bincode::serialized_size(&proto::StateAnswer::Heads(page.clone()))? <= super::super::PAGE_BYTES as u64);
        ids.extend(page.heads.to_vec().iter().map(|leaf| leaf.entity.entity_id));
        let complete = page.bounds.end == super::super::whole().end;
        if !complete {
            let Bound::Included(last) = &page.bounds.end else { panic!("a byte-bounded page must advance") };
            remaining = super::super::whole().after(last);
        }
        pages.push(page);
        if complete {
            break;
        }
    }
    assert!(pages.len() > 1, "clock and key bytes must split an ordinary inline-sized inventory");
    assert_eq!(ids, old.members().values().copied().collect::<Vec<_>>());

    let offer = old.offer().await?;
    assert!(
        matches!(offer.spans.as_slice(), [proto::SpanContent::Heads(_)]),
        "one initial inventory has its own budget, independent of follow-up pagination"
    );

    let changed = (0..32).map(|id| keyed_state(id as u32, 2, Some(Value::String("changed".into())))).collect::<anyhow::Result<Vec<_>>>()?;
    query.replace_values(wide_spec, changed).await?;
    assert_eq!(bincode::serialize(&old.page(super::super::whole())?)?, bincode::serialize(&pages[0])?);
    Ok(())
}

#[tokio::test]
async fn canonical_pages_cover_requested_bounds() -> anyhow::Result<()> {
    let spec = TreeSpec::new(
        KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(proto::PropertyId::Id, ValueType::EntityId)]),
        ankql::ast::Predicate::True,
    );
    let query = QueryTree::default();
    let snapshot = query.refresh(spec, (0..70).rev().map(|id| state(id, 1)).collect()).await?;
    let members = snapshot.members();
    let addresses = members.keys().cloned().collect::<Vec<_>>();
    let requested = proto::TreeSpan::new(Bound::Excluded(addresses[2].clone()), Bound::Included(addresses[67].clone()));
    let mut remaining = requested.clone();
    let mut received = Vec::new();
    loop {
        let page = snapshot.page(remaining.clone())?;
        assert_eq!(page.bounds.start, remaining.start);
        assert!(page.heads.len() <= super::super::HEAD_PAGE);
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
async fn retained_tree_changes_match_recomputation_and_preserve_old_snapshots() -> anyhow::Result<()> {
    let query = QueryTree::default();
    let initial = (0..70).map(|id| state(id, 1)).collect::<Vec<_>>();
    let mut old = query.refresh(spec(), initial.clone()).await?;
    let inventory = old.offer().await?;
    assert!(inventory.spans.iter().all(|region| matches!(region, proto::SpanContent::Heads(_))));

    let original = old.span_digest(super::super::whole()).await?;

    let mut unchanged = query.refresh(spec(), initial).await?;

    assert_eq!(unchanged.span_digest(super::super::whole()).await?, original);
    for states in [
        (10..80).map(|id| state(id, if id == 30 { 2 } else { 1 })).collect::<Vec<_>>(),
        (20..40).map(|id| state(id, 3)).collect(),
        Vec::new(),
    ] {
        let mut incremental = query.refresh(spec(), states.clone()).await?;

        let recomputed_query = QueryTree::default();
        let mut recomputed = recomputed_query.refresh(spec(), states).await?;
        assert_eq!(incremental.members(), recomputed.members());
        assert_eq!(incremental.span_digest(super::super::whole()).await?, recomputed.span_digest(super::super::whole()).await?);
        assert_eq!(old.span_digest(super::super::whole()).await?, original);
    }
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
    let query = QueryTree::default();
    let mut snapshot = query.refresh(spec(), states.clone()).await?;
    let inventory = snapshot.offer().await?;
    assert!(inventory.spans.len() > 1, "a new large tree must offer its bounded cached descendants");
    assert!(inventory.spans.len() <= OFFER_SPANS);
    assert!(bincode::serialized_size(&inventory)? <= OFFER_BYTES);
    let mut next = super::super::whole().start;
    let mut digests = Vec::new();
    let mut empty = false;
    for region in inventory.spans {
        let proto::SpanContent::Digest(head) = region else { panic!("large offer must contain only summaries") };
        assert_eq!(head.bounds.start, next, "spans must be disjoint and exactly adjacent");
        assert_eq!(snapshot.span_digest(head.bounds.clone()).await?.digest, head.digest, "each head describes its exact captured range");
        empty |= head.digest.count() == 0;
        digests.push(head.digest);
        next = match head.bounds.end {
            Bound::Included(end) => Bound::Excluded(end),
            Bound::Excluded(end) => Bound::Included(end),
            Bound::Unbounded => Bound::Unbounded,
        };
    }
    assert_eq!(next, super::super::whole().end);
    assert!(empty, "gaps before and after the populated addresses must remain explicit");
    let root = snapshot.span_digest(super::super::whole()).await?;
    assert_eq!(ankurah_digest_tree::Digest::checked_sum(digests.iter().copied())?, root.digest);

    let mut warm = query.refresh(spec(), states).await?;
    let retained = warm.offer().await?;
    assert!(retained.spans.len() > 1, "retained offers must also identify bounded changed spans");
    assert!(retained.spans.len() <= OFFER_SPANS);
    assert!(bincode::serialized_size(&retained)? <= OFFER_BYTES);
    assert!(retained
        .spans
        .iter()
        .all(|region| matches!(region, proto::SpanContent::Digest(head) if head.digest.count() <= super::super::HEAD_PAGE as i64)));
    assert_eq!(offered_digest(&retained)?, root.digest);
    Ok(())
}

#[tokio::test]
async fn complete_initial_inventories_are_bounded_and_immutable() -> anyhow::Result<()> {
    for count in [256, super::super::INITIAL_INVENTORY_HEADS] {
        let query = QueryTree::default();
        let mut old = query.refresh(spec(), (0..count as u32).map(|id| numbered_state(id, 1)).collect()).await?;
        let offer = old.offer().await?;
        assert!(bincode::serialized_size(&offer)? <= super::super::INITIAL_INVENTORY_BYTES as u64);
        let [proto::SpanContent::Heads(inventory)] = offer.spans.as_slice() else { panic!("one complete initial inventory") };
        assert_eq!(inventory.bounds, super::super::whole());
        assert_eq!(inventory.heads.len(), count);
        let received = inventory.heads.to_vec().iter().map(|leaf| leaf.entity.entity_id).collect::<Vec<_>>();
        let wire = bincode::serialize(&offer)?;
        if count == super::super::INITIAL_INVENTORY_HEADS {
            assert!(wire.len() > super::super::PAGE_BYTES, "the request has a separate byte budget");
        }
        let decoded: proto::KnownState = bincode::deserialize(&wire)?;
        assert_eq!(bincode::serialize(&decoded)?, wire);
        assert_eq!(received, old.members().values().copied().collect::<Vec<_>>());

        query.refresh(spec(), vec![numbered_state(17, 2)]).await?;
        assert_eq!(bincode::serialize(&old.offer().await?)?, bincode::serialize(&offer)?);
    }
    Ok(())
}

#[tokio::test]
async fn incomplete_initial_inventory_falls_back_for_count_or_total_bytes() -> anyhow::Result<()> {
    let query = QueryTree::default();
    let mut too_many = query.refresh(spec(), (0..2049).map(|id| numbered_state(id, 1)).collect()).await?;
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
        .refresh(
            spec(),
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
    let mut remaining = super::super::whole();
    loop {
        let page = wide.page(remaining)?;
        let end = page.bounds.end.clone();
        pages.push(proto::SpanContent::Heads(page));
        if end == super::super::whole().end {
            break;
        }
        let Bound::Included(last) = end else { panic!("progress") };
        remaining = super::super::whole().after(&last);
    }
    let oversized = proto::KnownState { spec: spec(), spans: pages };
    assert!(bincode::serialized_size(&oversized)? > super::super::INITIAL_INVENTORY_BYTES as u64);
    let offer = wide.offer().await?;
    assert!(
        offer.spans.iter().all(|region| matches!(region, proto::SpanContent::Digest(_))),
        "no partial leaf cover may escape the initial budget"
    );
    assert_eq!(offered_digest(&offer)?.count(), 64);
    Ok(())
}

#[tokio::test]
async fn source_row_deltas_preserve_frozen_snapshots_across_sparse_and_dense_changes() -> anyhow::Result<()> {
    let initial = (0..192).map(|id| keyed_state(id, 1, Some(Value::I64(id as i64)))).collect::<anyhow::Result<Vec<_>>>()?;
    let tree = QueryTree::default();
    let mut old = tree.replace_values(keyed_spec(), initial.clone()).await?;
    let old_head = old.span_digest(super::super::whole()).await?;
    let old_members = old.members();
    let removed = initial[11].0.payload.entity_id;
    let sparse = keyed_state(10, 2, Some(Value::I64(-100)))?;
    tree.apply_values(vec![sparse.clone()], vec![removed]).await?;
    let mut expected = initial
        .into_iter()
        .filter(|(state, _)| state.payload.entity_id != removed && state.payload.entity_id != sparse.0.payload.entity_id)
        .collect::<Vec<_>>();
    expected.push(sparse);
    for dense in [false, true] {
        if dense {
            let changes = (0..128)
                .filter(|id| *id != 11)
                .map(|id| keyed_state(id, 3, Some(Value::I64(id as i64 + 1000))))
                .collect::<anyhow::Result<Vec<_>>>()?;
            expected.retain(|(state, _)| !changes.iter().any(|(after, _)| after.payload.entity_id == state.payload.entity_id));
            expected.extend(changes.clone());
            // Include one new source row so this really crosses the dense threshold.
            let added = keyed_state(256, 3, Some(Value::I64(2000)))?;
            expected.push(added.clone());
            let mut changes = changes;
            changes.push(added);
            tree.apply_values(changes, Vec::new()).await?;
        }
        let mut current = tree.current_snapshot().await?;
        let oracle = QueryTree::default();
        let mut rebuilt = oracle.refresh(keyed_spec(), expected.iter().map(|(state, _)| state.clone()).collect()).await?;
        assert_eq!(current.members(), rebuilt.members());
        assert_eq!(current.span_digest(super::super::whole()).await?, rebuilt.span_digest(super::super::whole()).await?);
        assert_eq!(old.members(), old_members);
        assert_eq!(old.span_digest(super::super::whole()).await?, old_head);
    }
    let before = tree.current_snapshot().await?.members();
    let invalid = keyed_state(10, 4, Some(Value::String("wrong type".into())))?;
    assert!(tree.apply_values(vec![invalid], vec![expected[0].0.payload.entity_id]).await.is_err());
    assert_eq!(tree.current_snapshot().await?.members(), before, "failed encoding cannot apply even the removals");
    Ok(())
}

#[tokio::test]
async fn catalog_arrival_preserves_an_accepted_entity_id_fallback() -> anyhow::Result<()> {
    use crate::{node::Node, policy::PermissiveAgent, schema::catalog::SysPropertyRow, test_utils::TestStorage};
    use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate};

    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    let context = node.privileged_context();
    let transaction = context.begin();
    let property = transaction
        .create(&SysPropertyRow {
            name: "score".into(),
            backend: "lww".into(),
            value_type: "i64".into(),
            minted_for: None,
            target_model: None,
        })
        .await?
        .id();
    transaction.commit().await?;
    let path = PropertyPath::from(proto::PropertyId::EntityId(property));
    let selection = Selection {
        predicate: Predicate::Comparison {
            left: Box::new(Expr::Path(path.clone())),
            operator: ComparisonOperator::GreaterThan,
            right: Box::new(Expr::Literal(Value::I64(7))),
        },
        order_by: Some(vec![OrderByItem { path, direction: OrderDirection::Asc }]),
        limit: None,
    };
    let fallback = QueryTree::spec(&selection, &CatalogManager::default());
    assert!(fallback.key_spec().keyparts.is_empty(), "missing catalog types require entity-ID keys");
    assert_eq!(QueryTree::spec(&selection, &node.catalog).key_spec().keyparts.len(), 1, "catalog types now permit a keyed layout");
    let offer = proto::KnownState { spec: fallback.clone(), spans: vec![] };
    assert_eq!(
        QueryTree::offered_spec(&selection, &node.catalog, &offer)?,
        fallback,
        "the accepted fallback remains a distinct tree specification"
    );
    Ok(())
}

#[tokio::test]
async fn missing_key_counts_track_updates_removals_and_frozen_snapshots() -> anyhow::Result<()> {
    use ankql::ast::{OrderByItem, OrderDirection, Predicate};
    let spec = keyed_spec();
    let selection = Selection {
        predicate: Predicate::True,
        order_by: Some(vec![OrderByItem { path: PropertyPath::from(spec.key_spec().keyparts[0].key), direction: OrderDirection::Desc }]),
        limit: Some(2),
    };
    let cache = QueryTree::default();
    let initial = cache.replace_values(spec.clone(), vec![keyed_state(1, 1, Some(Value::I64(1)))?, keyed_state(2, 1, None)?]).await?;
    assert_eq!(initial.missing_counts, [1]);
    assert!(initial.has_descending_nulls(&selection));
    cache.apply_values(vec![keyed_state(2, 2, Some(Value::I64(2)))?], vec![]).await?;
    let present = cache.current_snapshot().await?;
    assert_eq!(present.missing_counts, [0]);
    assert!(!present.has_descending_nulls(&selection));
    cache.apply_values(vec![keyed_state(1, 2, None)?, keyed_state(3, 1, None)?], vec![]).await?;
    assert_eq!(cache.current_snapshot().await?.missing_counts, [2]);
    cache.apply_values(Vec::<(Attested<EntityState>, Vec<Option<Value>>)>::new(), vec![numbered_state(1, 2).payload.entity_id]).await?;
    assert_eq!(cache.current_snapshot().await?.missing_counts, [1]);
    cache.apply_values(Vec::<(Attested<EntityState>, Vec<Option<Value>>)>::new(), vec![numbered_state(3, 1).payload.entity_id]).await?;
    assert_eq!(cache.current_snapshot().await?.missing_counts, [0]);
    let rebuilt = cache.replace_values(spec, vec![keyed_state(4, 1, None)?]).await?;
    assert_eq!(rebuilt.missing_counts, [1]);
    assert_eq!(initial.missing_counts, [1], "later changes cannot rewrite captured null metadata");
    assert_eq!(present.missing_counts, [0]);
    assert!(initial.has_descending_nulls(&selection));
    assert!(!present.has_descending_nulls(&selection));
    Ok(())
}

#[tokio::test]
async fn scoped_snapshots_keep_one_full_tree_and_hide_unselected_rows() -> anyhow::Result<()> {
    let rows = (0..256).map(|id| keyed_state(id, 1, Some(Value::I64(id as i64)))).collect::<anyhow::Result<Vec<_>>>()?;
    let cache = QueryTree::default();
    let mut view = cache.replace_values(keyed_spec(), rows.clone()).await?.select_request(&range_request(200, None, false)).await?;
    assert_eq!(view.entries.len(), 55);
    assert_eq!(
        view.members.values().copied().collect::<Vec<_>>(),
        (201..256).map(|id| numbered_state(id, 1).payload.entity_id).collect::<Vec<_>>()
    );
    let offer = view.offer().await?;
    let [proto::SpanContent::Heads(inventory)] = offer.spans.as_slice() else { panic!("small scoped inventory") };
    assert_eq!(inventory.bounds, super::super::whole(), "scope gaps remain known-empty query addresses");
    assert_eq!(inventory.heads.len(), 55);

    let oracle = QueryTree::default();
    let mut selected = oracle.replace_values(keyed_spec(), rows[201..].to_vec()).await?;
    let expected = selected.span_digest(super::super::whole()).await?;
    let digest = view.span_digest(super::super::whole()).await?;
    assert_eq!(digest, expected);
    let children = view.refine(&digest).await?;
    assert_eq!(Digest::checked_sum(children.iter().map(|child| child.digest))?, digest.digest);
    for child in &children {
        assert_eq!(
            child.digest,
            selected.span_digest(child.bounds.clone()).await?.digest,
            "refinement must never expose the full resident digest"
        );
    }
    let mut full = cache.current_snapshot().await?;
    assert_eq!(full.span_digest(super::super::whole()).await?.digest.count(), 256, "selecting a view must not cache a partial tree");
    assert_eq!(full.entries.len(), 256);
    Ok(())
}

#[tokio::test]
async fn limits_take_independent_cuts_and_preserve_old_snapshots() -> anyhow::Result<()> {
    let rows = (0..8).map(|id| keyed_state(id, 1, Some(Value::I64(id as i64)))).collect::<anyhow::Result<Vec<_>>>()?;
    let cache = QueryTree::default();
    cache.replace_values(keyed_spec(), rows).await?;
    let mut low = cache.current_snapshot().await?.select_request(&range_request(-1000, Some(2), false)).await?;
    let mut high = cache.current_snapshot().await?.select_request(&range_request(-1000, Some(3), true)).await?;
    assert_eq!(
        low.entries.keys().copied().collect::<Vec<_>>(),
        (0..2).map(|id| numbered_state(id, 1).payload.entity_id).collect::<Vec<_>>()
    );
    assert_eq!(
        high.entries.keys().copied().collect::<Vec<_>>(),
        (5..8).map(|id| numbered_state(id, 1).payload.entity_id).collect::<Vec<_>>()
    );
    let low_before = low.span_digest(super::super::whole()).await?;
    let high_before = high.span_digest(super::super::whole()).await?;
    assert_eq!(low_before.digest.count(), 2);
    assert_eq!(high_before.digest.count(), 3);
    let high_point = high.members.keys().next().unwrap().clone();
    assert_eq!(
        low.span_digest(proto::TreeSpan::new(Bound::Included(high_point.clone()), Bound::Included(high_point))).await?.digest,
        Digest::empty()
    );

    cache.apply_values(vec![keyed_state(99, 2, Some(Value::I64(-10)))?], vec![]).await?;
    let after = cache.current_snapshot().await?.select_request(&range_request(-1000, Some(2), false)).await?;
    assert_eq!(after.entries.keys().copied().collect::<Vec<_>>(), [0, 99].map(|id| numbered_state(id, 1).payload.entity_id));
    assert_eq!(low.span_digest(super::super::whole()).await?, low_before);
    assert_eq!(high.span_digest(super::super::whole()).await?, high_before);
    assert_eq!(cache.current_snapshot().await?.entries.len(), 9);
    Ok(())
}

#[tokio::test]
async fn limited_peers_answer_bounds_using_their_own_cut() -> anyhow::Result<()> {
    let requester = QueryTree::default();
    let responder = QueryTree::default();
    let request = range_request(-1, Some(2), false);
    let rows = |ids: &[u32]| ids.iter().map(|id| keyed_state(*id, 1, Some(Value::I64(*id as i64)))).collect::<anyhow::Result<Vec<_>>>();
    let mut left = requester.replace_values(keyed_spec(), rows(&[0, 3, 4])?).await?.select_request(&request).await?;
    let mut right = responder.replace_values(keyed_spec(), rows(&[1, 2, 4])?).await?.select_request(&request).await?;
    for view in [&mut left, &mut right] {
        let offer = view.offer().await?;
        let [proto::SpanContent::Heads(inventory)] = offer.spans.as_slice() else { panic!("limited inventory") };
        assert_eq!(inventory.bounds, super::super::whole());
        assert_eq!(inventory.heads.len(), 2);
    }
    let left_last = left.members.keys().last().unwrap().clone();
    let point = proto::TreeSpan::new(Bound::Included(left_last.clone()), Bound::Included(left_last));
    assert_eq!(left.span_digest(point.clone()).await?.digest.count(), 1);
    assert_eq!(right.span_digest(point).await?.digest.count(), 0);
    let right_last = right.members.keys().last().unwrap().clone();
    let point = proto::TreeSpan::new(Bound::Included(right_last.clone()), Bound::Included(right_last));
    assert_eq!(left.span_digest(point.clone()).await?.digest.count(), 0);
    assert_eq!(right.span_digest(point).await?.digest.count(), 1);
    assert_eq!(left.span_digest(super::super::whole()).await?.digest.count(), 2);
    assert_eq!(right.span_digest(super::super::whole()).await?.digest.count(), 2);
    Ok(())
}

#[tokio::test]
async fn peer_spec_changes_only_the_view_description_not_resident_identity() -> anyhow::Result<()> {
    use ankql::ast::{ComparisonOperator, Expr, Predicate};
    let predicate = Predicate::Comparison {
        left: Box::new(Expr::Path(PropertyPath::id())),
        operator: ComparisonOperator::GreaterThan,
        right: Box::new(Expr::Literal(Value::EntityId(state(0, 1).payload.entity_id))),
    };
    let selection = Selection::from(predicate.clone());
    let wire_spec = TreeSpec::new(KeySpec::new(vec![]), predicate);
    let catalog = CatalogManager::default();
    let cache = QueryTree::default();
    let full = cache.refresh(spec(), (0..3).map(|id| state(id, 1)).collect()).await?;

    let mut view = full.with_spec(wire_spec.clone(), &selection, &catalog)?.select(&selection, &catalog).await?;
    assert_eq!(view.spec, wire_spec);
    assert_eq!(view.span_digest(super::super::whole()).await?.digest.count(), 2);
    {
        let resident = cache.0.lock().await;
        let resident = resident.as_ref().unwrap();
        assert_eq!(resident.tree.spec(), &spec());

        assert_eq!(resident.entries.len(), 3);
    }
    assert_eq!(cache.current_snapshot().await?.span_digest(super::super::whole()).await?.digest.count(), 3);
    assert_eq!(cache.0.lock().await.as_ref().unwrap().tree.spec(), &spec());
    Ok(())
}

#[tokio::test]
async fn zero_limit_view_does_not_empty_the_resident_tree() -> anyhow::Result<()> {
    let cache = QueryTree::default();
    let full = cache.refresh(spec(), (0..8).map(|id| state(id, 1)).collect()).await?;
    let mut selection = Selection::from(ankql::ast::Predicate::True);
    selection.limit = Some(0);
    let mut view = full.select(&selection, &CatalogManager::default()).await?;
    assert!(view.entries.is_empty());
    assert_eq!(view.span_digest(super::super::whole()).await?.digest, Digest::empty());

    assert_eq!(cache.current_snapshot().await?.span_digest(super::super::whole()).await?.digest.count(), 8);
    Ok(())
}

fn range_request(lower: i64, limit: Option<u64>, descending: bool) -> TreeRequest {
    use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate};
    let property = keyed_spec().key_spec().keyparts[0].key;
    let path = PropertyPath::from(property);
    let selection = Selection {
        predicate: Predicate::Comparison {
            left: Box::new(Expr::Path(path.clone())),
            operator: ComparisonOperator::GreaterThan,
            right: Box::new(Expr::Literal(Value::I64(lower))),
        },
        order_by: Some(vec![OrderByItem { path, direction: if descending { OrderDirection::Desc } else { OrderDirection::Asc } }]),
        limit,
    };
    TreeRequest::for_selection(&selection, &std::collections::HashMap::from([(property, ValueType::I64)])).unwrap()
}

#[test]
fn compatible_offered_spec_accepts_broader_admission_but_rejects_invalid_keys() -> anyhow::Result<()> {
    use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate};
    let catalog = CatalogManager::default();
    let selection = Selection::from(Predicate::Comparison {
        left: Box::new(Expr::Path(PropertyPath::id())),
        operator: ComparisonOperator::GreaterThan,
        right: Box::new(Expr::Literal(Value::EntityId(state(1, 1).payload.entity_id))),
    });
    let offer = proto::KnownState { spec: spec(), spans: vec![] };
    assert_eq!(QueryTree::offered_spec(&selection, &catalog, &offer)?, spec());
    let mut differently_ordered = selection.clone();
    differently_ordered.order_by = Some(vec![OrderByItem {
        path: PropertyPath::from(proto::PropertyId::EntityId(EntityId::from_bytes([42; EntityId::BYTE_LEN]))),
        direction: OrderDirection::Desc,
    }]);
    assert!(QueryTree::serves(&spec(), &differently_ordered, &catalog), "an unlimited order does not change the selected span");
    let invalid =
        TreeSpec::new(KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(proto::PropertyId::Id, ValueType::I64)]), Predicate::True);
    assert!(!QueryTree::serves(&invalid, &Selection::from(Predicate::True), &catalog));
    let mut invalid = ankurah_core_types::IndexKeyPart::asc(proto::PropertyId::Id, ValueType::EntityId);
    invalid.collation = Some("unsupported".into());
    assert!(!QueryTree::serves(&TreeSpec::new(KeySpec::new(vec![invalid]), Predicate::True), &selection, &catalog));
    Ok(())
}
