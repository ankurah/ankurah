use super::*;
use crate::PropertyTypes;
use crate::{leaf_address, DigestTree, HeadHash, LeafInput};
use ankql::ast::{Resolved, Selection};
use ankurah_core_types::{IndexKeyPart, ModelId, PropertyPath, Value, ValueType};
use std::collections::HashMap;

fn property(name: &str) -> PropertyId {
    let byte = match name {
        "project" => 1,
        "foo" => 2,
        "bar" => 3,
        _ => panic!("unknown fixture property"),
    };
    PropertyId::EntityId(EntityId::from_bytes([byte; 32]))
}

fn selection(text: &str) -> Selection<Resolved> {
    ankql::selection::map_references(
        &ankql::parser::parse_selection(text).unwrap(),
        &|path| PropertyPath::from(if path.steps[0] == "id" { PropertyId::Id } else { property(&path.steps[0]) }),
        &|_| ModelId::EntityId(EntityId::from_bytes([99; 32])),
    )
}

/// The value type of each whole property: project a string, foo and bar
/// numbers.
struct Types(HashMap<PropertyId, ValueType>);

impl PropertyTypes for Types {
    fn value_type(&self, path: &PropertyPath) -> Option<ValueType> {
        path.subpath.is_empty().then(|| self.0.get(&path.property_id()).copied()).flatten()
    }
}

fn types() -> Types {
    Types(HashMap::from([(property("project"), ValueType::String), (property("foo"), ValueType::I64), (property("bar"), ValueType::I64)]))
}

#[tokio::test]
async fn address_scopes_match_tree_scopes_and_sorted_members() {
    let admission = selection("project = 'acme'");
    let spec = TreeSpec::new(
        KeySpec::new(vec![
            IndexKeyPart::asc(property("project"), ValueType::String),
            IndexKeyPart::asc(property("foo"), ValueType::I64),
            IndexKeyPart::asc(property("bar"), ValueType::I64),
        ]),
        admission.predicate,
    );
    let model: Vec<_> = (0..90u8)
        .map(|n| {
            let (foo, bar) = (i64::from(n % 5), i64::from(n / 5 % 3));
            let key = CanonicalCodec::encode_prefix(spec.key_spec(), &[Value::from("acme"), Value::I64(foo), Value::I64(bar)]).unwrap();
            (foo, bar, LeafInput { key, entity_id: EntityId::from_bytes([n; 32]), head: HeadHash::of([[n; 32]]) })
        })
        .collect();
    let tree: DigestTree = DigestTree::new(spec);
    let mut tx = tree.begin();
    tx.build(model.iter().map(|(_, _, row)| row.clone())).await.unwrap();
    let addresses: BTreeMap<_, _> = model.iter().map(|(_, _, row)| (leaf_address(&row.key, row.entity_id), ())).collect();
    for disjoint in [false, true] {
        for foo_desc in [false, true] {
            for bar_desc in [false, true] {
                for id_desc in [false, true] {
                    for limit in [0, 1, 9, 37, 200] {
                        let dir = |desc| if desc { "DESC" } else { "ASC" };
                        let predicate = if disjoint { "project = 'acme' AND (foo < 1 OR foo > 3)" } else { "project = 'acme'" };
                        let query = selection(&format!(
                            "{predicate} ORDER BY foo {}, bar {}, id {} LIMIT {limit}",
                            dir(foo_desc),
                            dir(bar_desc),
                            dir(id_desc),
                        ));
                        let request = TreeRequest::for_selection(&query, &types()).unwrap();
                        let actual = tree.spec().selected_spans(&request, &addresses).unwrap();
                        let scope = tx.scope(&request).await.unwrap();
                        assert_eq!(actual, scope.spans(), "{query}");

                        let mut expected: Vec<_> = model.iter().filter(|(foo, _, _)| !disjoint || *foo < 1 || *foo > 3).collect();
                        let compare = |a: std::cmp::Ordering, desc| if desc { a.reverse() } else { a };
                        expected.sort_by(|a, b| {
                            compare(a.0.cmp(&b.0), foo_desc)
                                .then(compare(a.1.cmp(&b.1), bar_desc))
                                .then(compare(a.2.entity_id.cmp(&b.2.entity_id), id_desc))
                        });
                        expected.truncate(limit);
                        let mut expected_ids: Vec<_> = expected.iter().map(|(_, _, row)| row.entity_id).collect();
                        expected_ids.sort();
                        let mut actual_ids: Vec<_> = model
                            .iter()
                            .filter(|(_, _, row)| actual.iter().any(|span| span.contains(&leaf_address(&row.key, row.entity_id))))
                            .map(|(_, _, row)| row.entity_id)
                            .collect();
                        actual_ids.sort();
                        assert_eq!(actual_ids, expected_ids, "{query}");
                    }
                }
            }
        }
    }
    for query in ["project = 'acme'", "project = 'acme' AND foo > 2", "project = 'acme' AND foo > 10 ORDER BY foo, bar LIMIT 10"] {
        let request = TreeRequest::for_selection(&selection(query), &types()).unwrap();
        assert_eq!(tree.spec().selected_spans(&request, &addresses).unwrap(), tx.scope(&request).await.unwrap().spans());
    }
    let request = TreeRequest::for_selection(&selection("project = 'acme' ORDER BY foo, bar LIMIT 10"), &types()).unwrap();
    let mut empty: crate::TreeTransaction = DigestTree::new(tree.spec().clone()).begin();
    let empty_spans = tree.spec().selected_spans(&request, &BTreeMap::<Vec<u8>, ()>::new()).unwrap();
    assert_eq!(empty_spans, empty.scope(&request).await.unwrap().spans());
    assert_eq!(empty_spans, vec![TreeSpan::all()], "an empty local tree must allow missing peer members");
    let incompatible = TreeRequest::for_selection(&selection("project = 'acme' ORDER BY bar LIMIT 1"), &types()).unwrap();
    assert!(matches!(tree.spec().selected_spans(&incompatible, &addresses), Err(TreeError::UnsupportedSelection)));
}

#[tokio::test]
async fn entity_id_scopes_match_without_tuple_parts() {
    let spec = TreeSpec::new(KeySpec::new(vec![]), selection("true").predicate);
    let rows: Vec<_> =
        (0..20u8).map(|n| LeafInput { key: Vec::new(), entity_id: EntityId::from_bytes([n; 32]), head: HeadHash::of([[n; 32]]) }).collect();
    let addresses: BTreeMap<_, _> = rows.iter().map(|row| (leaf_address(&row.key, row.entity_id), ())).collect();
    let tree: DigestTree = DigestTree::new(spec);
    let mut tx = tree.begin();
    tx.build(rows).await.unwrap();
    for query in ["true LIMIT 0", "true LIMIT 3", "true ORDER BY id DESC LIMIT 3", "true ORDER BY id DESC LIMIT 30"] {
        let request = TreeRequest::for_selection(&selection(query), &types()).unwrap();
        assert_eq!(tree.spec().selected_spans(&request, &addresses).unwrap(), tx.scope(&request).await.unwrap().spans(), "{query}");
    }
}

#[test]
fn address_scope_validates_only_encountered_addresses() {
    let spec = TreeSpec::new(KeySpec::new(vec![IndexKeyPart::asc(property("foo"), ValueType::I64)]), selection("true").predicate);
    let request = TreeRequest::for_selection(&selection("true ORDER BY foo LIMIT 1"), &types()).unwrap();
    for malformed in [vec![0], vec![1; 33], vec![0xff; 64]] {
        assert!(matches!(spec.selected_spans(&request, &BTreeMap::from([(malformed, ())])), Err(TreeError::InvalidKey(_))));
    }
    let key = CanonicalCodec::encode_prefix(spec.key_spec(), &[Value::I64(0)]).unwrap();
    let address = leaf_address(&key, EntityId::from_bytes([0; 32]));
    let mut addresses = BTreeMap::from([(address.clone(), ()), (vec![0xff; 64], ())]);
    assert!(spec.selected_spans(&request, &addresses).unwrap().iter().any(|span| span.contains(&address)));
    addresses.remove(&address);
    let zero = TreeRequest::for_selection(&selection("true ORDER BY foo LIMIT 0"), &types()).unwrap();
    assert!(spec.selected_spans(&zero, &addresses).unwrap().is_empty());
    let scalar = TreeSpec::new(KeySpec::new(vec![]), selection("true").predicate);
    let scalar_request = TreeRequest::for_selection(&selection("true LIMIT 1"), &types()).unwrap();
    assert!(matches!(scalar.selected_spans(&scalar_request, &BTreeMap::from([(vec![0], ())])), Err(TreeError::InvalidKey(_))));
}
