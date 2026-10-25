use super::snapshot::UnsupportedTreeSelection;
use super::spans::FRONTIER_BYTES;
use super::spec::accept_peer_spec;
use super::*;
use crate::schema::catalog::CatalogManager;
use crate::value::Value;
use ankql::ast::Selection;
use ankurah_core_types::{KeySpec, PropertyPath, ValueType};
use ankurah_digest_tree::{Digest, PropertyTypes, TreeRequest};
use ankurah_proto::{
    self as proto,
    state_sync::limits::{MAX_OFFER_BYTES, MAX_OFFER_HEADS, MAX_OFFER_SPANS, MAX_PAGE_BYTES, MAX_PAGE_HEADS},
    Attested, EntityState,
};
mod ordering;
mod spans;
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

fn entries(spec: &TreeSpec, rows: Vec<(Attested<EntityState>, Vec<Option<Value>>)>) -> anyhow::Result<Entries> {
    rows.into_iter()
        .map(|(state, values)| {
            let id = state.payload.entity_id;
            Ok((id, TreeEntry::encode(spec, id, &values, state.payload.state.head)?))
        })
        .collect()
}

impl QueryTree {
    async fn of_values(spec: TreeSpec, rows: Vec<(Attested<EntityState>, Vec<Option<Value>>)>) -> anyhow::Result<Self> {
        let entries = entries(&spec, rows)?;
        Ok(Self::new(spec, entries))
    }
    async fn replace_values(&mut self, rows: Vec<(Attested<EntityState>, Vec<Option<Value>>)>) -> anyhow::Result<Snapshot> {
        self.replace(entries(&self.spec, rows)?).await?;
        Ok(self.snapshot())
    }
    async fn apply_values(&mut self, rows: Vec<(Attested<EntityState>, Vec<Option<Value>>)>, removed: Vec<EntityId>) -> anyhow::Result<()> {
        let mut changes: BTreeMap<_, _> = removed.into_iter().map(|id| (id, None)).collect();
        changes.extend(entries(&self.spec, rows)?.into_iter().map(|(id, entry)| (id, Some(entry))));
        self.apply(changes).await
    }
}

async fn values_snapshot(spec: TreeSpec, rows: Vec<(Attested<EntityState>, Vec<Option<Value>>)>) -> anyhow::Result<Snapshot> {
    Ok(QueryTree::of_values(spec, rows).await?.snapshot())
}

#[tokio::test]
async fn digest_and_cloned_heads_advance_together_without_retaining_payloads() -> anyhow::Result<()> {
    let mut tree = QueryTree::new(keyed_spec(), Entries::new());
    let mut input = keyed_state(1, 1, Some(Value::I64(5)))?.0;
    let original_head = input.payload.state.head.clone();
    let mut old = tree.refresh_states(vec![input.clone()]).await?;
    let old_digest = old.span_digest(proto::TreeSpan::all()).await?;
    input.payload.state.head = proto::Clock::genesis(proto::EventId::from_bytes([7; 32]));
    // Mutating the source state does not mutate the metadata that was admitted.
    assert_eq!(old.entries[&input.payload.entity_id].head, original_head);
    let id = input.payload.entity_id;
    let changed = TreeEntry { head: input.payload.state.head.clone(), ..old.entries[&id].clone() };
    tree.apply(BTreeMap::from([(id, Some(changed.clone()))])).await?;
    let mut current = tree.snapshot();
    assert_eq!(current.entries[&id], changed);
    assert_ne!(current.span_digest(proto::TreeSpan::all()).await?.digest, old_digest.digest);
    assert_eq!(old.span_digest(proto::TreeSpan::all()).await?, old_digest);
    assert_eq!(old.entries[&input.payload.entity_id].head, original_head);
    Ok(())
}

#[tokio::test]
async fn projected_keys_match_transient_decoding_across_sparse_movement_and_dense_refresh() -> anyhow::Result<()> {
    let initial =
        (0..256).map(|id| keyed_state(id, 1, (id != 11).then_some(Value::I64(id as i64 - 128)))).collect::<anyhow::Result<Vec<_>>>()?;
    let mut query = QueryTree::new(keyed_spec(), Entries::new());
    let mut old = query.replace_values(initial.clone()).await?;
    let original = old.span_digest(proto::TreeSpan::all()).await?;
    let original_members = old.member_map();
    let mut oracle = QueryTree::new(keyed_spec(), Entries::new());
    let mut decoded = oracle.refresh_states(initial.iter().map(|(state, _)| state.clone()).collect()).await?;
    assert_eq!(original, decoded.span_digest(proto::TreeSpan::all()).await?);
    assert_eq!(original_members, decoded.member_map());

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
        let mut admitted = query.replace_values(rows.clone()).await?;

        let mut oracle = QueryTree::new(keyed_spec(), Entries::new());
        let mut decoded = oracle.refresh_states(rows.into_iter().map(|(state, _)| state).collect()).await?;
        assert_eq!(admitted.member_map(), decoded.member_map());
        assert_eq!(admitted.span_digest(proto::TreeSpan::all()).await?, decoded.span_digest(proto::TreeSpan::all()).await?);
        assert_eq!(old.member_map(), original_members);
        assert_eq!(old.span_digest(proto::TreeSpan::all()).await?, original);
    }
    Ok(())
}

#[tokio::test]
async fn retained_tree_changes_match_recomputation_and_preserve_old_snapshots() -> anyhow::Result<()> {
    let mut query = QueryTree::new(spec(), Entries::new());
    let initial = (0..70).map(|id| state(id, 1)).collect::<Vec<_>>();
    let mut old = query.refresh_states(initial.clone()).await?;
    let inventory = old.offer().await?;
    assert!(inventory.spans.iter().all(|region| matches!(region, proto::SpanContent::Heads(_))));

    let original = old.span_digest(proto::TreeSpan::all()).await?;

    let mut unchanged = query.refresh_states(initial).await?;

    assert_eq!(unchanged.span_digest(proto::TreeSpan::all()).await?, original);
    for states in [
        (10..80).map(|id| state(id, if id == 30 { 2 } else { 1 })).collect::<Vec<_>>(),
        (20..40).map(|id| state(id, 3)).collect(),
        Vec::new(),
    ] {
        let mut incremental = query.refresh_states(states.clone()).await?;

        let mut recomputed_query = QueryTree::new(spec(), Entries::new());
        let mut recomputed = recomputed_query.refresh_states(states).await?;
        assert_eq!(incremental.member_map(), recomputed.member_map());
        assert_eq!(incremental.span_digest(proto::TreeSpan::all()).await?, recomputed.span_digest(proto::TreeSpan::all()).await?);
        assert_eq!(old.span_digest(proto::TreeSpan::all()).await?, original);
    }
    Ok(())
}

#[tokio::test]
async fn source_row_deltas_preserve_frozen_snapshots_across_sparse_and_dense_changes() -> anyhow::Result<()> {
    let initial = (0..192).map(|id| keyed_state(id, 1, Some(Value::I64(id as i64)))).collect::<anyhow::Result<Vec<_>>>()?;
    let mut tree = QueryTree::new(keyed_spec(), Entries::new());
    let mut old = tree.replace_values(initial.clone()).await?;
    let old_head = old.span_digest(proto::TreeSpan::all()).await?;
    let old_members = old.member_map();
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
        let mut current = tree.snapshot();
        let mut oracle = QueryTree::new(keyed_spec(), Entries::new());
        let mut rebuilt = oracle.refresh_states(expected.iter().map(|(state, _)| state.clone()).collect()).await?;
        assert_eq!(current.member_map(), rebuilt.member_map());
        assert_eq!(current.span_digest(proto::TreeSpan::all()).await?, rebuilt.span_digest(proto::TreeSpan::all()).await?);
        assert_eq!(old.member_map(), old_members);
        assert_eq!(old.span_digest(proto::TreeSpan::all()).await?, old_head);
    }
    let before = tree.snapshot().member_map();
    let invalid = keyed_state(10, 4, Some(Value::String("wrong type".into())))?;
    assert!(tree.apply_values(vec![invalid], vec![expected[0].0.payload.entity_id]).await.is_err());
    assert_eq!(tree.snapshot().member_map(), before, "failed encoding cannot apply even the removals");
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
    let fallback = spec_for(&selection, &CatalogManager::default());
    assert!(fallback.key_spec().keyparts.is_empty(), "missing catalog types require entity-ID keys");
    assert_eq!(spec_for(&selection, &node.catalog).key_spec().keyparts.len(), 1, "catalog types now permit a keyed layout");
    let offer = proto::KnownState { spec: fallback.clone(), spans: vec![] };
    assert_eq!(
        accept_peer_spec(&selection, &node.catalog, &offer)?,
        fallback,
        "the accepted fallback remains a distinct tree specification"
    );
    Ok(())
}

#[tokio::test]
async fn scoped_snapshots_keep_one_full_tree_and_hide_unselected_rows() -> anyhow::Result<()> {
    let rows = (0..256).map(|id| keyed_state(id, 1, Some(Value::I64(id as i64)))).collect::<anyhow::Result<Vec<_>>>()?;
    let mut cache = QueryTree::new(keyed_spec(), Entries::new());
    let mut view = cache.replace_values(rows.clone()).await?.select_request(&range_request(200, None, false))?;
    assert_eq!(view.entries.len(), 55);
    assert_eq!(
        view.members.values().copied().collect::<Vec<_>>(),
        (201..256).map(|id| numbered_state(id, 1).payload.entity_id).collect::<Vec<_>>()
    );
    let offer = view.offer().await?;
    let [proto::SpanContent::Heads(inventory)] = offer.spans.as_slice() else { panic!("small scoped inventory") };
    assert_eq!(inventory.bounds, proto::TreeSpan::all(), "scope gaps remain known-empty query addresses");
    assert_eq!(inventory.heads.len(), 55);

    let mut oracle = QueryTree::new(keyed_spec(), Entries::new());
    let mut selected = oracle.replace_values(rows[201..].to_vec()).await?;
    let expected = selected.span_digest(proto::TreeSpan::all()).await?;
    let digest = view.span_digest(proto::TreeSpan::all()).await?;
    assert_eq!(digest, expected);
    let children = view.refine(&digest).await?;
    assert_eq!(Digest::checked_sum(children.iter().map(|child| child.digest))?, digest.digest);
    for child in &children {
        assert_eq!(
            child.digest,
            selected.span_digest(child.bounds.clone()).await?.digest,
            "refinement must never expose the full tree's digest"
        );
    }
    let mut full = cache.snapshot();
    assert_eq!(full.span_digest(proto::TreeSpan::all()).await?.digest.count(), 256, "selecting a view must not cache a partial tree");
    assert_eq!(full.entries.len(), 256);
    Ok(())
}

#[tokio::test]
async fn limits_take_independent_cuts_and_preserve_old_snapshots() -> anyhow::Result<()> {
    let rows = (0..8).map(|id| keyed_state(id, 1, Some(Value::I64(id as i64)))).collect::<anyhow::Result<Vec<_>>>()?;
    let mut cache = QueryTree::new(keyed_spec(), Entries::new());
    cache.replace_values(rows).await?;
    let mut low = cache.snapshot().select_request(&range_request(-1000, Some(2), false))?;
    let mut high = cache.snapshot().select_request(&range_request(-1000, Some(3), true))?;
    assert_eq!(
        low.entries.keys().copied().collect::<Vec<_>>(),
        (0..2).map(|id| numbered_state(id, 1).payload.entity_id).collect::<Vec<_>>()
    );
    assert_eq!(
        high.entries.keys().copied().collect::<Vec<_>>(),
        (5..8).map(|id| numbered_state(id, 1).payload.entity_id).collect::<Vec<_>>()
    );
    let low_before = low.span_digest(proto::TreeSpan::all()).await?;
    let high_before = high.span_digest(proto::TreeSpan::all()).await?;
    assert_eq!(low_before.digest.count(), 2);
    assert_eq!(high_before.digest.count(), 3);
    let high_point = high.members.keys().next().unwrap().to_vec();
    assert_eq!(
        low.span_digest(proto::TreeSpan::new(Bound::Included(high_point.clone()), Bound::Included(high_point))).await?.digest,
        Digest::empty()
    );

    cache.apply_values(vec![keyed_state(99, 2, Some(Value::I64(-10)))?], vec![]).await?;
    let after = cache.snapshot().select_request(&range_request(-1000, Some(2), false))?;
    assert_eq!(after.entries.keys().copied().collect::<Vec<_>>(), [0, 99].map(|id| numbered_state(id, 1).payload.entity_id));
    assert_eq!(low.span_digest(proto::TreeSpan::all()).await?, low_before);
    assert_eq!(high.span_digest(proto::TreeSpan::all()).await?, high_before);
    assert_eq!(cache.snapshot().entries.len(), 9);
    Ok(())
}

#[tokio::test]
async fn peer_spec_changes_only_the_view_description_not_the_trees_identity() -> anyhow::Result<()> {
    use ankql::ast::{ComparisonOperator, Expr, Predicate};
    let predicate = Predicate::Comparison {
        left: Box::new(Expr::Path(PropertyPath::id())),
        operator: ComparisonOperator::GreaterThan,
        right: Box::new(Expr::Literal(Value::EntityId(state(0, 1).payload.entity_id))),
    };
    let selection = Selection::from(predicate.clone());
    let wire_spec = TreeSpec::new(KeySpec::new(vec![]), predicate);
    let catalog = CatalogManager::default();
    let mut cache = QueryTree::new(spec(), Entries::new());
    let full = cache.refresh_states((0..3).map(|id| state(id, 1)).collect()).await?;

    let mut view = full.with_spec(wire_spec.clone(), &selection, &catalog)?.select(&selection, &catalog)?;
    assert_eq!(view.spec, wire_spec);
    assert_eq!(view.span_digest(proto::TreeSpan::all()).await?.digest.count(), 2);
    assert_eq!(&*cache.spec, &spec());
    assert_eq!(cache.entries().len(), 3);
    assert_eq!(cache.snapshot().span_digest(proto::TreeSpan::all()).await?.digest.count(), 3);
    Ok(())
}

#[tokio::test]
async fn zero_limit_view_does_not_empty_the_resident_tree() -> anyhow::Result<()> {
    let mut cache = QueryTree::new(spec(), Entries::new());
    let full = cache.refresh_states((0..8).map(|id| state(id, 1)).collect()).await?;
    let mut selection = Selection::from(ankql::ast::Predicate::True);
    selection.limit = Some(0);
    let mut view = full.select(&selection, &CatalogManager::default())?;
    assert!(view.entries.is_empty());
    assert_eq!(view.span_digest(proto::TreeSpan::all()).await?.digest, Digest::empty());

    assert_eq!(cache.snapshot().span_digest(proto::TreeSpan::all()).await?.digest.count(), 8);
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
    TreeRequest::for_selection(&selection, &PropertyType(property, ValueType::I64)).unwrap()
}

/// The value type of the one property a test selects on.
struct PropertyType(proto::PropertyId, ValueType);

impl PropertyTypes for PropertyType {
    fn value_type(&self, path: &PropertyPath) -> Option<ValueType> {
        (path.subpath.is_empty() && path.property_id() == self.0).then_some(self.1)
    }
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
    assert_eq!(accept_peer_spec(&selection, &catalog, &offer)?, spec());
    let mut differently_ordered = selection.clone();
    differently_ordered.order_by = Some(vec![OrderByItem {
        path: PropertyPath::from(proto::PropertyId::EntityId(EntityId::from_bytes([42; EntityId::BYTE_LEN]))),
        direction: OrderDirection::Desc,
    }]);
    assert!(serves(&spec(), &differently_ordered, &catalog), "an unlimited order does not change the selected span");
    let invalid =
        TreeSpec::new(KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(proto::PropertyId::Id, ValueType::I64)]), Predicate::True);
    assert!(!serves(&invalid, &Selection::from(Predicate::True), &catalog));
    let mut invalid = ankurah_core_types::IndexKeyPart::asc(proto::PropertyId::Id, ValueType::EntityId);
    invalid.collation = Some("unsupported".into());
    assert!(!serves(&TreeSpec::new(KeySpec::new(vec![invalid]), Predicate::True), &selection, &catalog));
    Ok(())
}

#[tokio::test]
async fn accounted_bytes_follow_each_change_as_a_fresh_build_counts_them() -> anyhow::Result<()> {
    let rows = |values: &[i64]| -> anyhow::Result<_> {
        values.iter().map(|value| keyed_state(*value as u32, 1, Some(Value::I64(*value)))).collect::<anyhow::Result<Vec<_>>>()
    };
    let mut tree = QueryTree::of_values(keyed_spec(), rows(&[1, 2, 3])?).await?;
    tree.apply_values(rows(&[4])?, vec![numbered_state(2, 1).payload.entity_id]).await?;
    tree.replace_values(rows(&[1, 3, 4, 5])?).await?;
    assert_eq!(tree.accounted_bytes(), QueryTree::of_values(keyed_spec(), rows(&[1, 3, 4, 5])?).await?.accounted_bytes());
    assert!(tree.accounted_bytes() > QueryTree::of_values(keyed_spec(), rows(&[1, 3, 4])?).await?.accounted_bytes());
    Ok(())
}
