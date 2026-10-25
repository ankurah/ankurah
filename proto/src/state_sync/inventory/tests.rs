use super::codec::{PLAIN, PREFIX};
use super::*;
use crate::{
    state_sync::limits::{MAX_OFFER_BYTES, MAX_OFFER_HEADS},
    EventId, TreeSpan, TreeSpanInventory,
};

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

/// Heads a test knows are within one inventory's limits.
fn packed(heads: Vec<KeyedEntityHead>) -> PackedHeads { PackedHeads::try_from(heads).unwrap() }

fn verify(original: Vec<KeyedEntityHead>) {
    let expected = serde_json::to_value(&original).unwrap();
    let packed = packed(original);
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
        assert_eq!(view.entity.head.to_owned(), head.entity.head);
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
        let packed = packed(keyed_heads(&keys, 1));
        assert_eq!(packed.bytes[0], PLAIN);
    }
    let packed = packed(keyed_heads(&[b"a long shared prefix/one".to_vec(), b"a long shared prefix/two".to_vec()], 1));
    assert_eq!(packed.bytes[0], PREFIX);
    assert!(packed.bytes.len() < packed.unshared_bytes);
    let clone = packed.clone();
    assert_eq!(packed.bytes.as_ptr(), clone.bytes.as_ptr(), "clones share their buffer");
    let mut cursor = packed.cursor();
    drop(packed);
    assert_eq!(cursor.next().unwrap().key, b"a long shared prefix/one");
    assert_eq!(cursor.next().unwrap().key, b"a long shared prefix/two");
}

/// A payload of two heads under one eight-byte key, so that sharing it makes the prefix
/// encoding the smaller. The first head's clock lists `entries` as given; the second's
/// is empty.
fn raw_clock(entries: &[(usize, u8)], encoding: u8) -> Vec<u8> {
    let key = [b'k'; 8];
    let mut out = vec![encoding, 2];
    for (n, entries) in [entries, &[]].into_iter().enumerate() {
        let shared = if encoding == PREFIX && n > 0 { key.len() } else { 0 };
        if encoding == PREFIX {
            codec::put_uint(&mut out, shared);
        }
        codec::put_uint(&mut out, key.len() - shared);
        out.extend_from_slice(&key[shared..]);
        out.extend_from_slice(&[n as u8; 32]);
        codec::put_uint(&mut out, entries.len());
        for (generation, id) in entries {
            codec::put_uint(&mut out, *generation);
            out.extend_from_slice(&[*id; 32]);
        }
    }
    out
}

#[test]
fn clocks_must_list_their_tips_in_ascending_event_order() {
    for mode in [PLAIN, PREFIX] {
        let packed = PackedHeads::decode(&raw_clock(&[(1, 1), (7, 2)], mode)).unwrap();
        let expected = Clock::new(vec![(1, EventId::from_bytes([1; 32])), (7, EventId::from_bytes([2; 32]))]).unwrap();
        assert_eq!(packed.cursor().next().unwrap().entity.head.to_owned(), expected);
        for entries in [vec![(7, 2), (1, 1)], vec![(1, 1), (1, 1)], vec![(1, 1), (2, 1)], vec![(0, 1)]] {
            assert!(PackedHeads::decode(&raw_clock(&entries, mode)).is_err(), "{entries:?}");
        }
    }
}

#[test]
fn rejects_malformed_payloads_before_any_cursor_can_observe_them() {
    for keys in [vec![vec![]; 2], vec![b"shared-prefix-one".to_vec(), b"shared-prefix-two".to_vec()]] {
        let page = packed(keyed_heads(&keys, 1));
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

/// A payload of `count` heads with clockless IDs whose keys all repeat one key of
/// `key_len` bytes, each after the first written as a fully shared prefix.
fn repeated_keys(count: usize, key_len: usize) -> Vec<u8> {
    let mut out = vec![PREFIX];
    codec::put_uint(&mut out, count);
    for n in 0..count {
        let (shared, suffix) = if n == 0 { (0, key_len) } else { (key_len, 0) };
        codec::put_uint(&mut out, shared);
        codec::put_uint(&mut out, suffix);
        out.extend(std::iter::repeat_n(42, suffix));
        out.extend_from_slice(&[n as u8; 32]);
        out.push(0);
    }
    out
}

#[test]
fn encoding_and_decoding_refuse_more_than_one_inventory_may_carry() {
    // Shared prefixes keep these bytes small on the wire; their unshared size is over the limit.
    let repeated = repeated_keys(128, 4096);
    assert!(repeated.len() < MAX_OFFER_BYTES);
    assert!(matches!(PackedHeads::decode(&repeated), Err(InventoryError::ExceedsLimit(_))));
    assert!(matches!(PackedHeads::try_from(keyed_heads(&vec![vec![42; 4096]; 128], 0)), Err(InventoryError::ExceedsLimit(_))));
    assert!(matches!(PackedHeads::decode(&repeated_keys(MAX_OFFER_HEADS + 1, 8)), Err(InventoryError::ExceedsLimit(_))));
    assert!(matches!(PackedHeads::try_from(keyed_heads(&vec![vec![]; MAX_OFFER_HEADS + 1], 0)), Err(InventoryError::ExceedsLimit(_))));
    assert!(PackedHeads::decode(&repeated_keys(MAX_OFFER_HEADS, 8)).is_ok());
    assert!(PackedHeads::decode(&vec![PLAIN; MAX_OFFER_BYTES + 1]).is_err());
}

#[test]
fn prefix_sharing_saves_wire_bytes_but_not_work() -> bincode::Result<()> {
    let text = b"a long key text".as_slice();
    let shared: Vec<_> = (0..64u8).map(|n| [text, &[n]].concat()).collect();
    let unshared: Vec<_> = (0..64u8).map(|n| [&[n], text].concat()).collect();
    let inventory = |keys: &[Vec<u8>]| TreeSpanInventory { bounds: TreeSpan::all(), heads: packed(keyed_heads(keys, 1)) };
    let (shared, unshared) = (inventory(&shared), inventory(&unshared));
    assert_eq!(shared.heads.bytes[0], PREFIX);
    assert_eq!(unshared.heads.bytes[0], PLAIN);
    assert!(bincode::serialized_size(&shared)? < bincode::serialized_size(&unshared)?);
    assert_eq!(shared.work_bytes()?, unshared.work_bytes()?, "equal key lengths describe equal work");
    assert_eq!(unshared.work_bytes()?, bincode::serialized_size(&unshared)?, "plain keys share nothing");
    Ok(())
}

#[test]
fn only_the_plain_and_prefix_encodings_are_accepted() {
    // A payload valid under the plain tag, retagged with tags other than the two defined ones.
    let payload = raw_clock(&[(1, 1)], PLAIN);
    assert!(PackedHeads::decode(&payload).is_ok());
    for tag in [0, 3, 4, 255] {
        let retagged = [&[tag], &payload[1..]].concat();
        assert!(PackedHeads::decode(&retagged).is_err(), "tag {tag}");
    }
}

#[test]
fn the_prefix_encoding_is_accepted_only_where_the_writer_would_choose_it() {
    // The first head shares nothing, so one head costs a byte more under the prefix
    // encoding; a second head sharing a two-byte key saves that byte back, leaving the
    // sizes equal; a third makes the prefix encoding the smaller.
    for count in 1..=3 {
        let written = packed(keyed_heads(&vec![vec![42; 2]; count], 0));
        let prefix = PackedHeads::decode(&repeated_keys(count, 2));
        assert_eq!(prefix.is_ok(), written.bytes[0] == PREFIX, "{count} heads");
    }
    let prefix = PackedHeads::decode(&repeated_keys(3, 2)).unwrap();
    assert!(prefix.bytes.len() < prefix.unshared_bytes);
}

#[test]
fn integers_are_read_only_in_their_shortest_form() {
    let read = |bytes: &[u8]| codec::Reader::new(bytes).uint().ok();
    // Each marker's smallest value, then the largest value of the shorter form before it.
    assert_eq!(read(&[251, 251, 0]), Some(251));
    assert_eq!(read(&[251, 250, 0]), None);
    assert_eq!(read(&[252, 0, 0, 1, 0]), Some(65536));
    assert_eq!(read(&[252, 255, 255, 0, 0]), None);
    assert_eq!(read(&[253, 255, 255, 255, 255, 0, 0, 0, 0]), None);
    // Within a payload: a valid payload, and the same payload with its head count in three bytes.
    let payload = raw_clock(&[(1, 1)], PLAIN);
    assert!(PackedHeads::decode(&payload).is_ok());
    assert!(PackedHeads::decode(&[&[PLAIN, 251, 2, 0], &payload[2..]].concat()).is_err());
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
        let inventory = TreeSpanInventory { bounds, heads: packed(heads.clone()) };
        inventory.validate(&spec).unwrap();
        let decoded: TreeSpanInventory = bincode::deserialize(&bincode::serialize(&inventory).unwrap()).unwrap();
        decoded.validate(&spec).unwrap();
    }
    TreeSpanInventory { bounds: TreeSpan::all(), heads: Default::default() }.validate(&spec).unwrap();
    let invalid = [
        TreeSpanInventory { bounds: TreeSpan::new(Excluded(address(1)), Excluded(address(1))), heads: Default::default() },
        TreeSpanInventory { bounds: TreeSpan::new(Included(address(2)), Included(address(0))), heads: Default::default() },
        TreeSpanInventory { bounds: TreeSpan::new(Excluded(address(0)), Included(address(2))), heads: packed(heads.clone()) },
        TreeSpanInventory { bounds: TreeSpan::new(Included(address(0)), Excluded(address(2))), heads: packed(heads.clone()) },
        TreeSpanInventory { bounds: TreeSpan::all(), heads: packed(vec![heads[1].clone(), heads[0].clone()]) },
        TreeSpanInventory { bounds: TreeSpan::all(), heads: packed(vec![heads[0].clone(), heads[0].clone()]) },
        TreeSpanInventory { bounds: TreeSpan::all(), heads: packed(keyed_heads(&[vec![1]], 1)) },
    ];
    for inventory in invalid {
        assert!(inventory.validate(&spec).is_err(), "{inventory:?}");
        let decoded: TreeSpanInventory = bincode::deserialize(&bincode::serialize(&inventory).unwrap()).unwrap();
        assert!(decoded.validate(&spec).is_err());
    }
    let keyed = TreeSpec::new(KeySpec::new(vec![IndexKeyPart::asc(PropertyId::Id, ValueType::EntityId)]), ankql::ast::Predicate::True);
    let inventory = TreeSpanInventory { bounds: TreeSpan::all(), heads: packed(heads) };
    assert!(inventory.validate(&keyed).is_err(), "an inventory valid under another key spec is not sufficient");
}

#[test]
fn an_inventory_lists_each_entity_once() {
    use ankurah_core_types::{IndexKeyPart, KeySpec, PropertyId, Value, ValueType};
    use ankurah_digest_tree::CanonicalCodec;

    // Under a keyed spec one entity can sit at two addresses in address order.
    let spec = TreeSpec::new(KeySpec::new(vec![IndexKeyPart::asc(PropertyId::Id, ValueType::I64)]), ankql::ast::Predicate::True);
    let key = |n| CanonicalCodec::encode_nullable(spec.key_spec(), &[Some(Value::I64(n))]).unwrap();
    let mut heads = keyed_heads(&[key(1), key(2)], 1);
    heads[1].entity.entity_id = heads[0].entity.entity_id;
    let inventory = TreeSpanInventory { bounds: TreeSpan::all(), heads: packed(heads) };
    assert!(matches!(inventory.validate(&spec), Err(InventoryError::DuplicateEntity(_))));
}
