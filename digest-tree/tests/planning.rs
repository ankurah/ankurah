mod common;
use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate, Selection};
use ankurah_core_types::{IndexKeyPart, KeySpec, PropertyId, PropertyPath, Value, ValueType};
use ankurah_digest_tree::{DigestTree, HeadHash, LeafData, TreeMatch, TreeRequest, TreeSpec};
use common::{property, types};

#[test]
fn creation_policy_does_not_dictate_existing_tree_reuse() {
    let default = TreeSpec::for_selection(&selection!("project = 'acme' ORDER BY foo"), &types()).unwrap();
    assert_eq!(default.key_spec().keyparts.iter().map(|p| p.key).collect::<Vec<_>>(), [property("project")]);
    let request = TreeRequest::for_selection(&selection!("project = 'acme' AND foo > 7"), &types()).unwrap();
    assert_eq!(default.serves(&request), None);
    let hinted = TreeSpec::new(
        KeySpec::new(vec![IndexKeyPart::asc(property("project"), ValueType::String), IndexKeyPart::asc(property("foo"), ValueType::I64)]),
        default.predicate().clone(),
    );
    assert_eq!(hinted.serves(&request), Some(TreeMatch::Span));
    for source in [selection!("project = 'acme' AND foo > 7"), selection!("project = 'acme' AND foo > 7 ORDER BY foo")] {
        let tree = TreeSpec::for_selection(&source, &types()).unwrap();
        assert_eq!(tree.key_spec(), hinted.key_spec());
        for query in [selection!("project = 'acme' AND foo > 10"), selection!("project = 'acme' AND foo > 10 ORDER BY foo")] {
            assert_eq!(tree.serves(&TreeRequest::for_selection(&query, &types()).unwrap()), Some(TreeMatch::Span));
        }
    }
    let limited = TreeSpec::for_selection(&selection!("project = 'acme' AND foo > 7 LIMIT 2"), &types()).unwrap();
    assert_eq!(limited.key_spec().keyparts.iter().map(|p| p.key).collect::<Vec<_>>(), [property("project")]);
    let range_choice = TreeSpec::for_selection(&selection!("foo > 7 AND bar > 3 ORDER BY bar"), &types()).unwrap();
    assert_eq!(range_choice.key_spec().keyparts[0].key, property("bar"));
}

#[test]
fn a_residual_must_hold_across_every_admission_branch() {
    let source = selection!("(foo > 7 AND bar = 1) OR (foo > 10 AND bar = 2)");
    let spec = TreeSpec::new(KeySpec::new(vec![IndexKeyPart::asc(property("foo"), ValueType::I64)]), source.predicate.clone());
    assert_eq!(spec.serves(&TreeRequest::for_selection(&source, &types()).unwrap()), Some(TreeMatch::Whole));
    assert_eq!(spec.serves(&TreeRequest::for_selection(&selection!("foo > 10 AND bar = 1"), &types()).unwrap()), None);
    let constant = TreeSpec::new(spec.key_spec().clone(), selection!("foo > 7 AND bar = 1").predicate);
    assert_eq!(constant.serves(&TreeRequest::for_selection(&selection!("foo > 10 AND bar = 1"), &types()).unwrap()), Some(TreeMatch::Span));
    // Empty requests require neither an admission proof nor a compatible order.
    assert!(spec
        .serves(&TreeRequest::for_selection(&selection!("project = 'elsewhere' ORDER BY project LIMIT 0"), &types()).unwrap())
        .is_some());
}

#[tokio::test]
async fn id_bounds_extremes_and_default_order_are_exact() {
    let spec = TreeSpec::new(KeySpec::new(vec![]), Predicate::True);
    let tree: DigestTree = DigestTree::new(spec);
    let model: Vec<_> = [0, 1, 128, 255]
        .into_iter()
        .map(|n| LeafData { key: vec![], entity_id: common::entity(n), head: HeadHash::of([[n; 32]]) })
        .collect();
    let mut build = tree.begin();
    build.build(model.clone()).await.unwrap();
    build.commit().await.unwrap();
    for (operator, value, expected) in [
        (ComparisonOperator::GreaterThan, 255, vec![]),
        (ComparisonOperator::GreaterThanOrEqual, 255, vec![255]),
        (ComparisonOperator::LessThan, 0, vec![]),
        (ComparisonOperator::LessThanOrEqual, 0, vec![0]),
        (ComparisonOperator::NotEqual, 255, vec![0, 1, 128]),
    ] {
        let predicate = Predicate::Comparison {
            left: Box::new(Expr::Path(PropertyPath::id())),
            operator,
            right: Box::new(Expr::Literal(Value::EntityId(common::entity(value)))),
        };
        let request = TreeRequest::for_selection(&Selection::from(predicate), &types()).unwrap();
        let mut tx = tree.begin();
        let scope = tx.scope(&request).await.unwrap();
        let page = tx.page(&scope, None, 10).await.unwrap();
        assert_eq!(
            page.leaves.iter().map(|v| v.entity_id).collect::<Vec<_>>(),
            expected.into_iter().map(common::entity).collect::<Vec<_>>()
        );
    }
    for desc in [false, true] {
        let selection = Selection {
            predicate: Predicate::True,
            order_by: desc.then(|| vec![OrderByItem { path: PropertyPath::from(PropertyId::Id), direction: OrderDirection::Desc }]),
            limit: Some(2),
        };
        let mut tx = tree.begin();
        let scope = tx.scope(&TreeRequest::for_selection(&selection, &types()).unwrap()).await.unwrap();
        let first = tx.page(&scope, None, 1).await.unwrap();
        let last = tx.page(&scope, first.next.as_ref(), 1).await.unwrap();
        let ids = [first.leaves[0].entity_id, last.leaves[0].entity_id];
        assert_eq!(ids, if desc { [common::entity(255), common::entity(128)] } else { [common::entity(0), common::entity(1)] });
        assert!(last.next.is_none());
    }
}
