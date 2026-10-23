use super::codec::{PLAIN, PREFIX};
use super::*;
use crate::{EventId, TreeSpan, TreeSpanInventory};

fn keyed_heads(keys: &[Vec<u8>], tips: usize) -> Vec<KeyedEntityHead> {
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
    for head in json.to_vec() {
        let view = cursor.next().unwrap();
        assert_eq!(view.key, head.key);
        assert!(view.entity.head.equals(&head.entity.head));
    }
    assert!(cursor.next().is_none());
}

#[test]
fn full_keys_ids_and_clocks_round_trip_in_both_encodings() {
    for tips in [0, 1, 6, 256] {
        for keys in [vec![], vec![vec![]], vec![vec![], vec![1, 0, 255]], vec![vec![42; 4096]; 8]] {
            verify(keyed_heads(&keys, tips));
        }
    }
    verify(keyed_heads(&(0..2048).map(|n| format!("acme/projects/{n:08}").into_bytes()).collect::<Vec<_>>(), 1));
}

#[test]
fn plain_fallback_and_prefix_choice_use_actual_encoded_size() {
    for keys in [vec![], vec![vec![]; 128], vec![b"one".to_vec()]] {
        let packed: PackedHeads = keyed_heads(&keys, 1).into();
        assert_eq!(packed.bytes[0], PLAIN);
    }
    let packed: PackedHeads = keyed_heads(&[b"a long shared prefix/one".to_vec(), b"a long shared prefix/two".to_vec()], 1).into();
    assert_eq!(packed.bytes[0], PREFIX);
    assert!(packed.bytes.len() < packed.unshared_bytes);
    let clone = packed.clone();
    assert_eq!(packed.bytes.as_ptr(), clone.bytes.as_ptr(), "clones share their buffer");
    let mut cursor = packed.cursor();
    drop(packed);
    assert_eq!(cursor.next().unwrap().key, b"a long shared prefix/one");
    assert_eq!(cursor.next().unwrap().key, b"a long shared prefix/two");
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
        assert!(codec::validate(&packed.bytes).unwrap().canonical);
        for entries in [vec![(0, 1)], vec![(1, 1), (2, 1)]] {
            assert!(PackedHeads::decode(&raw_clock(&entries, mode)).is_err());
        }
    }
}

#[test]
fn rejects_malformed_payloads_before_any_cursor_can_observe_them() {
    for keys in [vec![vec![]; 2], vec![b"shared-prefix-one".to_vec(), b"shared-prefix-two".to_vec()]] {
        let page: PackedHeads = keyed_heads(&keys, 1).into();
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
fn compression_cannot_bypass_the_unshared_size_or_head_limits() {
    let repeated: PackedHeads = keyed_heads(&vec![vec![42; 4096]; 128], 1).into();
    assert!(repeated.bytes.len() < MAX_OFFER_BYTES);
    assert!(PackedHeads::decode(&repeated.bytes).is_err());
    assert!(bincode::serialize(&repeated).is_err());
    let too_many: PackedHeads = keyed_heads(&vec![vec![]; MAX_OFFER_HEADS + 1], 0).into();
    assert!(PackedHeads::decode(&too_many.bytes).is_err());
    assert!(bincode::serialize(&too_many).is_err());
    assert!(PackedHeads::decode(&vec![PLAIN; MAX_OFFER_BYTES + 1]).is_err());
}

#[test]
fn prefix_sharing_saves_wire_bytes_but_not_work() -> bincode::Result<()> {
    let text = b"a long key text".as_slice();
    let shared: Vec<_> = (0..64u8).map(|n| [text, &[n]].concat()).collect();
    let unshared: Vec<_> = (0..64u8).map(|n| [&[n], text].concat()).collect();
    let inventory = |keys: &[Vec<u8>]| TreeSpanInventory { bounds: TreeSpan::all(), heads: keyed_heads(keys, 1).into() };
    let (shared, unshared) = (inventory(&shared), inventory(&unshared));
    assert_eq!(shared.heads.bytes[0], PREFIX);
    assert_eq!(unshared.heads.bytes[0], PLAIN);
    assert!(bincode::serialized_size(&shared)? < bincode::serialized_size(&unshared)?);
    assert_eq!(shared.work_bytes()?, unshared.work_bytes()?, "equal key lengths describe equal work");
    assert_eq!(unshared.work_bytes()?, bincode::serialized_size(&unshared)?, "plain keys share nothing");
    Ok(())
}

#[test]
fn retained_clocks_share_the_inventory_and_survive_cursor_drop() {
    let packed: PackedHeads = keyed_heads(&[b"shared-prefix-a".to_vec(), b"shared-prefix-b".to_vec()], 6).into();
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
fn only_the_plain_and_prefix_encodings_are_accepted() {
    // One head whose generation is written as four bytes, under tags other than the two defined ones.
    for tag in [0, 3, 4, 255] {
        let mut payload = vec![tag, 1, 0];
        payload.extend_from_slice(&[9; 32]);
        payload.push(1);
        payload.extend_from_slice(&u32::MAX.to_le_bytes());
        payload.extend_from_slice(&[7; 32]);
        assert!(PackedHeads::decode(&payload).is_err(), "tag {tag}");
    }
}

#[test]
fn inventory_validation_checks_bounds_order_duplicates_and_canonical_keys() {
    use ankurah_core_types::{IndexKeyPart, KeySpec, PropertyId, ValueType};
    use ankurah_digest_tree::leaf_address;
    use std::ops::Bound::{Excluded, Included};

    let spec = TreeSpec::new(KeySpec::new(vec![]), ankql::ast::Predicate::True);
    let heads = keyed_heads(&[vec![], vec![], vec![]], 1);
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
        TreeSpanInventory { bounds: TreeSpan::all(), heads: keyed_heads(&[vec![1]], 1).into() },
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
