use super::codec::{MAX_BYTES, MAX_ROWS, PLAIN, PLAIN_FIXED, PREFIX, PREFIX_FIXED};
use super::*;
use crate::{EventId, SpanContent, StateAnswer, StateItem, TreeSpan, TreeSpanInventory};
use std::ops::Bound;

fn rows(keys: &[Vec<u8>], tips: usize) -> Vec<KeyedEntityHead> {
    keys.iter()
        .enumerate()
        .map(|(n, key)| KeyedEntityHead {
            key: key.clone(),
            entity: EntityHead {
                entity_id: EntityId::from_bytes([n as u8; 32]),
                head: Clock::new(
                    (0..tips)
                        .map(|tip| {
                            let mut id = [0; 32];
                            id[..4].copy_from_slice(&(tip as u32).to_be_bytes());
                            ([1, 250, 251, 65535, 65536, u32::MAX][tip % 6], EventId::from_bytes(id))
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
            },
        })
        .collect()
}

fn verify(original: Vec<KeyedEntityHead>) {
    let expected = serde_json::to_value(&original).unwrap();
    let packed: PackedHeads = original.into();
    let bytes = bincode::serialize(&packed).unwrap();
    let decoded: PackedHeads = bincode::deserialize(&bytes).unwrap();
    assert_eq!(serde_json::to_value(&decoded).unwrap(), expected);
    assert_eq!(serde_json::to_value(decoded.to_vec()).unwrap(), expected);
    assert_eq!(bincode::serialize(&decoded).unwrap(), bytes);
    let json: PackedHeads = serde_json::from_value(expected.clone()).unwrap();
    assert_eq!(serde_json::to_value(&json).unwrap(), expected);
    let mut cursor = decoded.cursor();
    for row in json.to_vec() {
        let view = cursor.next().unwrap();
        assert_eq!(view.key, row.key);
        assert!(view.entity.head.equals(&row.entity.head));
    }
    assert!(cursor.next().is_none());
}

#[test]
fn full_keys_ids_and_clocks_round_trip_in_both_encodings() {
    for tips in [0, 1, 6, 256] {
        for keys in [vec![], vec![vec![]], vec![vec![], vec![1, 0, 255]], vec![vec![42; 4096]; 8]] {
            verify(rows(&keys, tips));
        }
    }
    verify(rows(&(0..2048).map(|n| format!("acme/projects/{n:08}").into_bytes()).collect::<Vec<_>>(), 1));
}

#[test]
fn plain_fallback_and_prefix_choice_use_actual_encoded_size() {
    for keys in [vec![], vec![vec![]; 128], vec![b"one".to_vec()]] {
        let packed: PackedHeads = rows(&keys, 1).into();
        assert_eq!(packed.bytes[0], PLAIN);
    }
    let packed: PackedHeads = rows(&[b"a long shared prefix/one".to_vec(), b"a long shared prefix/two".to_vec()], 1).into();
    assert_eq!(packed.bytes[0], PREFIX);
    assert!(packed.bytes.len() < packed.plain_size);
    let clone = packed.clone();
    assert_eq!(packed.bytes.as_ptr(), clone.bytes.as_ptr(), "clones share their buffer");
    let mut cursor = packed.cursor();
    drop(packed);
    assert_eq!(cursor.next().unwrap().key, b"a long shared prefix/one");
    assert_eq!(cursor.next().unwrap().key, b"a long shared prefix/two");
}

#[test]
fn initial_and_followup_inventories_use_the_same_encoding_and_absolute_bounds() {
    for start in [Bound::Unbounded, Bound::Included(vec![]), Bound::Excluded(vec![0; 251])] {
        for end in [Bound::Unbounded, Bound::Included(vec![255]), Bound::Excluded(vec![255; 251])] {
            let page = TreeSpanInventory { bounds: TreeSpan::new(start.clone(), end), heads: rows(&[vec![1], vec![2]], 6).into() };
            let initial = bincode::serialize(&SpanContent::Heads(page.clone())).unwrap();
            assert_eq!(initial, bincode::serialize(&StateItem::Heads(page.clone())).unwrap());
            assert_eq!(initial, bincode::serialize(&StateAnswer::Heads(page.clone())).unwrap());
            let SpanContent::Heads(decoded) = bincode::deserialize(&initial).unwrap() else { panic!("heads") };
            assert_eq!(decoded.bounds, page.bounds);
            assert_eq!(serde_json::to_value(decoded.heads).unwrap(), serde_json::to_value(page.heads).unwrap());
        }
    }
}

fn raw_clock(entries: &[(usize, u8)], mode: u8) -> Vec<u8> {
    let mut out = vec![mode, 1];
    if mode == PREFIX {
        out.push(0);
    }
    out.push(0); // empty key
    out.extend_from_slice(&[9; 32]);
    codec::put_uint(&mut out, entries.len());
    for (generation, id) in entries {
        codec::put_uint(&mut out, *generation);
        out.extend_from_slice(&[*id; 32]);
    }
    out
}

#[test]
fn noncanonical_clocks_are_normalized_once_and_invalid_annotations_fail() {
    for mode in [PLAIN, PREFIX] {
        let packed = PackedHeads::decode(&raw_clock(&[(7, 2), (1, 1), (1, 1)], mode)).unwrap();
        let expected = Clock::new(vec![(1, EventId::from_bytes([1; 32])), (7, EventId::from_bytes([2; 32]))]).unwrap();
        assert!(packed.cursor().next().unwrap().entity.head.equals(&expected));
        assert!(codec::validate(&packed.bytes).unwrap().3);
        for entries in [vec![(0, 1)], vec![(1, 1), (2, 1)]] {
            assert!(PackedHeads::decode(&raw_clock(&entries, mode)).is_err());
        }
    }
}

#[test]
fn rejects_malformed_payloads_before_any_cursor_can_observe_them() {
    for keys in [vec![vec![]; 2], vec![b"shared-prefix-one".to_vec(), b"shared-prefix-two".to_vec()]] {
        let page: PackedHeads = rows(&keys, 1).into();
        for n in 0..page.bytes.len() {
            assert!(PackedHeads::decode(&page.bytes[..n]).is_err());
        }
        let mut bytes = page.bytes.to_vec();
        bytes.push(0);
        assert!(PackedHeads::decode(&bytes).is_err());
        bytes = page.bytes.to_vec();
        bytes[0] = 99;
        assert!(PackedHeads::decode(&bytes).is_err());
    }
    let mut invalid_prefix = raw_clock(&[(1, 1)], PREFIX);
    invalid_prefix[2] = 1;
    assert!(PackedHeads::decode(&invalid_prefix).is_err());
    for bytes in [vec![PLAIN, 255], vec![PLAIN, 251], vec![PLAIN, 1, 254]] {
        assert!(PackedHeads::decode(&bytes).is_err());
    }
}

#[test]
fn compression_cannot_bypass_expanded_work_or_row_limits() {
    let expanded: PackedHeads = rows(&vec![vec![42; 4096]; 128], 1).into();
    assert!(expanded.bytes.len() < MAX_BYTES);
    assert!(PackedHeads::decode(&expanded.bytes).is_err());
    assert!(bincode::serialize(&expanded).is_err());
    let rows: PackedHeads = rows(&vec![vec![]; MAX_ROWS + 1], 0).into();
    assert!(PackedHeads::decode(&rows.bytes).is_err());
    assert!(bincode::serialize(&rows).is_err());
    assert!(PackedHeads::decode(&vec![PLAIN; MAX_BYTES + 1]).is_err());
}

#[test]
fn expanded_size_preserves_the_old_fixed_width_work_budget() {
    let owned = rows(&[vec![], vec![2; 251], vec![3; 4096]], 6);
    let size = bincode::serialized_size(&owned).unwrap();
    let packed: PackedHeads = owned.into();
    assert_eq!(packed.expanded_size(), size);
}

#[test]
fn retained_clocks_share_the_inventory_and_survive_cursor_drop() {
    let packed: PackedHeads = rows(&[b"shared-prefix-a".to_vec(), b"shared-prefix-b".to_vec()], 6).into();
    let expected = packed.to_vec();
    let mut cursor = packed.cursor();
    let (id, clock) = cursor.next_clock().unwrap();
    let start = packed.bytes.as_ptr() as usize;
    let clock_start = clock.bytes.as_ptr() as usize;
    assert!((start..start + packed.bytes.len()).contains(&clock_start));
    drop(packed);
    drop(cursor);
    assert_eq!(id, expected[0].entity.entity_id);
    assert_eq!(clock.to_owned(), expected[0].entity.head);
}

#[test]
fn fixed_generations_keep_previously_valid_large_clock_pages_within_budget() {
    let mut source = rows(&[vec![]], 1818);
    source[0].entity.head = Clock::new(source[0].entity.head.ids().map(|id| (u32::MAX, id.clone())).collect::<Vec<_>>()).unwrap();
    let page = TreeSpanInventory { bounds: TreeSpan::all(), heads: source.clone().into() };
    assert_eq!(page.heads.bytes[0], PLAIN_FIXED);
    assert!(4 + page.expanded_size() <= 64 * 1024);
    assert!(bincode::serialized_size(&StateItem::Heads(page)).unwrap() <= 64 * 1024);
    verify(source);
    let mut source = rows(&vec![b"shared-prefix-and-key".to_vec(); 8], 6);
    for row in &mut source {
        row.entity.head = Clock::new(row.entity.head.ids().map(|id| (u32::MAX, id.clone())).collect::<Vec<_>>()).unwrap();
    }
    let packed: PackedHeads = source.clone().into();
    assert_eq!(packed.bytes[0], PREFIX_FIXED);
    verify(source);
    for n in 0..packed.bytes.len() {
        assert!(PackedHeads::decode(&packed.bytes[..n]).is_err());
    }
}

#[test]
fn inventory_validation_checks_bounds_order_duplicates_and_canonical_keys() {
    use ankurah_core_types::{IndexKeyPart, KeySpec, PropertyId, ValueType};
    use ankurah_digest_tree::leaf_address;
    use std::ops::Bound::{Excluded, Included};

    let spec = TreeSpec::new(KeySpec::new(vec![]), ankql::ast::Predicate::True);
    let heads = rows(&[vec![], vec![], vec![]], 1);
    let address = |index: usize| leaf_address(&heads[index].key, heads[index].entity.entity_id);
    for bounds in [TreeSpan::all(), TreeSpan::new(Included(address(0)), Included(address(2)))] {
        let inventory = TreeSpanInventory { bounds, heads: heads.clone().into() };
        inventory.validate(&spec).unwrap();
        let decoded: TreeSpanInventory = bincode::deserialize(&bincode::serialize(&inventory).unwrap()).unwrap();
        decoded.validate(&spec).unwrap();
    }
    TreeSpanInventory { bounds: TreeSpan::all(), heads: Default::default() }.validate(&spec).unwrap();
    let invalid = [
        TreeSpanInventory { bounds: TreeSpan::new(Excluded(address(1)), Excluded(address(1))), heads: Default::default() },
        TreeSpanInventory { bounds: TreeSpan::new(Included(address(2)), Included(address(0))), heads: Default::default() },
        TreeSpanInventory { bounds: TreeSpan::new(Excluded(address(0)), Included(address(2))), heads: heads.clone().into() },
        TreeSpanInventory { bounds: TreeSpan::new(Included(address(0)), Excluded(address(2))), heads: heads.clone().into() },
        TreeSpanInventory { bounds: TreeSpan::all(), heads: vec![heads[1].clone(), heads[0].clone()].into() },
        TreeSpanInventory { bounds: TreeSpan::all(), heads: vec![heads[0].clone(), heads[0].clone()].into() },
        TreeSpanInventory { bounds: TreeSpan::all(), heads: rows(&[vec![1]], 1).into() },
    ];
    for inventory in invalid {
        assert!(inventory.validate(&spec).is_err(), "{inventory:?}");
        let decoded: TreeSpanInventory = bincode::deserialize(&bincode::serialize(&inventory).unwrap()).unwrap();
        assert!(decoded.validate(&spec).is_err());
    }
    let keyed = TreeSpec::new(KeySpec::new(vec![IndexKeyPart::asc(PropertyId::Id, ValueType::EntityId)]), ankql::ast::Predicate::True);
    let inventory = TreeSpanInventory { bounds: TreeSpan::all(), heads: heads.into() };
    assert!(inventory.validate(&keyed).is_err(), "an inventory valid under another key spec is not sufficient");
}
