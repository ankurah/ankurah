mod common;
use ankql::ast::{OrderByItem, OrderDirection, Selection};
use ankurah_core_types::{IndexKeyPart, KeySpec, PropertyPath, Value, ValueType};
use ankurah_digest_tree::{
    CanonicalCodec, Digest, DigestTree, HeadHash, KeyCodec, Leaf, LeafChange, LeafData, TreeError, TreeRequest, TreeSpec,
};
use common::{entity, property, types};

#[tokio::test]
async fn mixed_directions_limits_and_pages_match_a_sorted_model() {
    let admission = selection!("project = 'acme'");
    let key = KeySpec::new(vec![
        IndexKeyPart::asc(property("project"), ValueType::String),
        IndexKeyPart::asc(property("foo"), ValueType::I64),
        IndexKeyPart::asc(property("bar"), ValueType::I64),
    ]);
    let tree = DigestTree::new(TreeSpec::new(key, admission.predicate.clone()));
    let codec = CanonicalCodec;
    let mut model = Vec::new();
    let mut build = tree.begin();
    for n in 0..150u8 {
        let (foo, bar) = (i64::from(n % 7), i64::from((n / 7) % 5));
        let key = codec.encode_prefix(tree.spec().key_spec(), &[Value::from("acme"), Value::I64(foo), Value::I64(bar)]).unwrap();
        model.push((foo, bar, LeafData { key, entity_id: entity(n), head: HeadHash::of([[n; 32]]) }));
    }
    build.build(model.iter().map(|(_, _, leaf)| leaf.clone())).await.unwrap();
    build.commit().await.unwrap();
    for foo_desc in [false, true] {
        for bar_desc in [false, true] {
            for limit in [0, 1, 17, 61, 149, 200] {
                let direction = |desc| if desc { OrderDirection::Desc } else { OrderDirection::Asc };
                let selection = Selection {
                    predicate: admission.predicate.clone(),
                    order_by: Some(vec![
                        OrderByItem { path: PropertyPath::from(property("foo")), direction: direction(foo_desc) },
                        OrderByItem { path: PropertyPath::from(property("bar")), direction: direction(bar_desc) },
                    ]),
                    limit: Some(limit),
                };
                let mut expected = model.clone();
                expected.sort_by(|a, b| {
                    let foo = a.0.cmp(&b.0);
                    let bar = a.1.cmp(&b.1);
                    (if foo_desc { foo.reverse() } else { foo })
                        .then(if bar_desc { bar.reverse() } else { bar })
                        .then(a.2.entity_id.cmp(&b.2.entity_id))
                });
                expected.truncate(limit as usize);
                let mut tx = tree.begin();
                let scope = tx.scope(&TreeRequest::for_selection(&selection, &types()).unwrap(), &codec).await.unwrap();
                let expected_digest: Digest =
                    expected.iter().map(|(_, _, row)| Digest::from(Leaf::new(&row.key, row.entity_id, row.head).element())).sum();
                assert_eq!(tx.digest(&scope).await.unwrap(), expected_digest, "{foo_desc}/{bar_desc}/{limit}");
                assert_eq!(tx.count(&scope).await.unwrap(), expected.len() as u64);
                let mut rows = Vec::new();
                let mut cursor = None;
                loop {
                    let page = tx.page(&scope, &codec, cursor.as_ref(), 9).await.unwrap();
                    rows.extend(page.leaves.into_iter().map(|row| row.entity_id));
                    cursor = page.next;
                    if cursor.is_none() {
                        break;
                    }
                }
                assert_eq!(rows, expected.iter().map(|(_, _, leaf)| leaf.entity_id).collect::<Vec<_>>());
                for head in tx.heads(&scope).await.unwrap() {
                    let refined: Digest = tx.refine(&head).await.unwrap().iter().map(|head| head.digest).sum();
                    assert_eq!(refined, head.digest);
                }
            }
        }
    }
}

#[tokio::test]
async fn disjunctions_nulls_and_embedded_nuls_have_exact_bounds() {
    let source = selection!("foo >= -20");
    let key = KeySpec::new(vec![IndexKeyPart::asc(property("project"), ValueType::String)]);
    let tree = DigestTree::new(TreeSpec::new(key, source.predicate.clone()));
    let values = [None, Some(""), Some("\0"), Some("\0a"), Some("a"), Some("a\0"), Some("a\0b"), Some("b"), Some("é")];
    let model: Vec<_> = values
        .iter()
        .enumerate()
        .map(|(n, value)| LeafData {
            key: CanonicalCodec.encode_nullable(tree.spec().key_spec(), &[value.map(Value::from)]).unwrap(),
            entity_id: entity(n as u8),
            head: HeadHash::of([[n as u8; 32]]),
        })
        .collect();
    let mut build = tree.begin();
    build.build(model.clone()).await.unwrap();
    build.commit().await.unwrap();
    for (query, ids) in [
        (selection!("foo >= -20 AND project IS NULL"), vec![0]),
        (selection!("foo >= -20 AND project = ''"), vec![1]),
        (selection!("foo >= -20 AND project < 'a'"), vec![1, 2, 3]),
        (selection!("foo >= -20 AND project <= 'a'"), vec![1, 2, 3, 4]),
        (selection!("foo >= -20 AND project > 'a'"), vec![5, 6, 7, 8]),
        (selection!("foo >= -20 AND project != 'a'"), vec![1, 2, 3, 5, 6, 7, 8]),
        (selection!("foo >= -20 AND (project = 'a' OR project = 'b')"), vec![4, 7]),
        (selection!("foo >= -20 AND project IN ('a', 'b', 'a')"), vec![4, 7]),
    ] {
        let mut tx = tree.begin();
        let request = TreeRequest::for_selection(&query, &types()).unwrap();
        let scope = tx.scope(&request, &CanonicalCodec).await.unwrap();
        let expected: Digest =
            ids.iter().map(|&n| &model[n]).map(|row| Digest::from(Leaf::new(&row.key, row.entity_id, row.head).element())).sum();
        assert_eq!(tx.digest(&scope).await.unwrap(), expected, "{query}");
    }
}

#[tokio::test]
async fn scope_lifetime_and_multiplicity_are_checked() {
    let tree = common::populated_tree(&selection!("project = 'acme' AND foo > 7")).await;
    let request = TreeRequest::for_selection(&selection!("project = 'acme' AND foo > 10"), &types()).unwrap();
    let mut tx = tree.begin();
    let scope = tx.scope(&request, &CanonicalCodec).await.unwrap();
    assert!(matches!(tree.begin().count(&scope).await, Err(TreeError::StaleScope)));
    let row = tx.entity_leaves(entity(2)).await.unwrap().pop().unwrap();
    tx.apply_change(LeafChange { before: Some(LeafData { key: row.key, entity_id: row.entity_id, head: row.head }), after: None })
        .await
        .unwrap();
    assert!(matches!(tx.count(&scope).await, Err(TreeError::StaleScope)));
    let refreshed = tx.scope(&request, &CanonicalCodec).await.unwrap();
    assert_eq!(tx.count(&refreshed).await.unwrap(), 1);
    // The committed tree is unchanged while this transaction is open.
    let mut other = tree.begin();
    let other_scope = other.scope(&request, &CanonicalCodec).await.unwrap();
    assert_eq!(other.count(&other_scope).await.unwrap(), 2);
    let duplicate = LeafData {
        key: CanonicalCodec.encode_prefix(tree.spec().key_spec(), &[Value::from("acme"), Value::I64(20)]).unwrap(),
        entity_id: entity(3),
        head: HeadHash::of([[1; 32]]),
    };
    tx.apply_change(LeafChange { before: None, after: Some(duplicate) }).await.unwrap();
    assert!(matches!(tx.scope(&request, &CanonicalCodec).await, Err(TreeError::MultipleKeys)));
}

#[tokio::test]
async fn bounded_queries_match_a_numeric_model_across_bucket_edges() {
    use ankql::ast::{ComparisonOperator as Op, Expr, Predicate};
    let key = KeySpec::new(vec![IndexKeyPart::asc(property("foo"), ValueType::I64)]);
    let tree: DigestTree = DigestTree::new(TreeSpec::new(key, Predicate::True));
    let model: Vec<_> = (0..200u8)
        .map(|n| {
            let value = i64::from(n) - 100;
            let row = LeafData {
                key: CanonicalCodec.encode_prefix(tree.spec().key_spec(), &[Value::I64(value)]).unwrap(),
                entity_id: entity(n),
                head: HeadHash::of([[n; 32]]),
            };
            (value, row)
        })
        .collect();
    let mut build = tree.begin();
    build.build(model.iter().map(|(_, row)| row.clone())).await.unwrap();
    build.commit().await.unwrap();
    let comparison = |op, value| Predicate::Comparison {
        left: Box::new(Expr::Path(PropertyPath::from(property("foo")))),
        operator: op,
        right: Box::new(Expr::Literal(Value::I64(value))),
    };
    for lower in [-110, -99, -50, 0, 50, 99, 110] {
        for upper in [-110, -50, 0, 50, 99, 110] {
            for inclusive in [false, true] {
                let query = Selection::from(Predicate::And(
                    Box::new(comparison(if inclusive { Op::GreaterThanOrEqual } else { Op::GreaterThan }, lower)),
                    Box::new(comparison(if inclusive { Op::LessThanOrEqual } else { Op::LessThan }, upper)),
                ));
                let expected: Vec<_> =
                    model.iter().filter(|(n, _)| if inclusive { *n >= lower && *n <= upper } else { *n > lower && *n < upper }).collect();
                let mut tx = tree.begin();
                let scope = tx.scope(&TreeRequest::for_selection(&query, &types()).unwrap(), &CanonicalCodec).await.unwrap();
                let digest: Digest =
                    expected.iter().map(|(_, row)| Digest::from(Leaf::new(&row.key, row.entity_id, row.head).element())).sum();
                assert_eq!(tx.digest(&scope).await.unwrap(), digest, "{lower}/{upper}/{inclusive}");
                let page = tx.page(&scope, &CanonicalCodec, None, 300).await.unwrap();
                assert_eq!(
                    page.leaves.iter().map(|row| row.entity_id).collect::<Vec<_>>(),
                    expected.iter().map(|(_, row)| row.entity_id).collect::<Vec<_>>()
                );
            }
        }
    }
    let mut tx = tree.begin();
    let query = Selection::from(Predicate::Comparison {
        left: Box::new(Expr::Path(PropertyPath::from(property("foo")))),
        operator: Op::Between,
        right: Box::new(Expr::ExprList(vec![Expr::Literal(Value::I64(-7)), Expr::Literal(Value::I64(12))])),
    });
    let scope = tx.scope(&TreeRequest::for_selection(&query, &types()).unwrap(), &CanonicalCodec).await.unwrap();
    assert_eq!(tx.count(&scope).await.unwrap(), 20);
    let scope = tx.scope_ranges([ankurah_digest_tree::AddressRange::all()]);
    let root = tx.heads(&scope).await.unwrap().pop().unwrap();
    let children = tx.refine(&root).await.unwrap();
    assert_eq!(children.iter().map(|head| head.digest).sum::<Digest>(), root.digest);
    for head in children {
        let subset = tx.scope_ranges([head.bounds.clone()]);
        assert_eq!(tx.digest(&subset).await.unwrap(), head.digest);
        let mut invalid = head.to_bytes();
        invalid.push(0);
        assert!(ankurah_digest_tree::TreeHead::<ankurah_digest_tree::Ristretto255>::from_bytes(&invalid).is_err());
    }
}
