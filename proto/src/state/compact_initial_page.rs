//! Compact binary encoding for initial inventories only. Response and feedback
//! pages retain their ordinary encoding and decoded resource limits.

use bincode::Options;
use serde::{de::Visitor, Deserialize, Deserializer, Serialize, Serializer};

use super::LeafPage;

const VERSION: u8 = 1;
// A valid fixed 64 KiB page can grow when large generations use varints.
const MAX_ENCODED_BYTES: usize = 68 * 1024;

fn options() -> impl Options {
    bincode::DefaultOptions::new().with_little_endian().with_varint_encoding().with_limit(MAX_ENCODED_BYTES as u64).reject_trailing_bytes()
}

pub(super) fn serialize<S: Serializer>(page: &LeafPage, serializer: S) -> Result<S::Ok, S::Error> {
    if serializer.is_human_readable() {
        return page.serialize(serializer);
    }
    let bytes = options().serialize(&(VERSION, page)).map_err(serde::ser::Error::custom)?;
    serializer.serialize_bytes(&bytes)
}

pub(super) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<LeafPage, D::Error> {
    if deserializer.is_human_readable() {
        return LeafPage::deserialize(deserializer);
    }
    deserializer.deserialize_bytes(PageVisitor)
}

struct PageVisitor;

impl<'de> Visitor<'de> for PageVisitor {
    type Value = LeafPage;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a versioned compact initial leaf page of at most 68 KiB")
    }

    fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<LeafPage, E> {
        // Bincode's slice decoder does not enforce Options::with_limit. Check
        // the borrowed blob before any typed page decoding or allocation.
        if bytes.len() > MAX_ENCODED_BYTES {
            return Err(E::custom("compact initial leaf page exceeds 68 KiB"));
        }
        let (version, payload) = bytes.split_first().ok_or_else(|| E::custom("missing compact initial leaf page version"))?;
        if *version != VERSION {
            return Err(E::custom("unsupported compact initial leaf page version"));
        }
        options().deserialize(payload).map_err(E::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AddressRange, Clock, EntityHead, EntityId, EventId, KnownLeaf, KnownRegion, StateAnswer, StateItem};
    use std::ops::Bound;

    fn event(index: u32) -> EventId {
        let mut bytes = [0; 32];
        bytes[..4].copy_from_slice(&index.to_be_bytes());
        EventId::from_bytes(bytes)
    }

    fn page(keys: Vec<Vec<u8>>, generation: u32, tips: usize) -> LeafPage {
        LeafPage {
            bounds: AddressRange::all(),
            leaves: keys
                .into_iter()
                .enumerate()
                .map(|(index, key)| KnownLeaf {
                    key,
                    entity: EntityHead {
                        entity_id: EntityId::from_bytes([index as u8; 32]),
                        head: Clock::new((0..tips).map(|tip| (generation, event(tip as u32))).collect::<Vec<_>>()).unwrap(),
                    },
                })
                .collect(),
        }
    }

    fn decoded(encoded: &[u8]) -> bincode::Result<LeafPage> {
        match bincode::deserialize::<KnownRegion>(encoded)? {
            KnownRegion::Leaves(page) => Ok(page),
            _ => panic!("expected leaves"),
        }
    }

    fn wrap_blob(blob: &[u8]) -> Vec<u8> {
        let mut bytes = 1u32.to_le_bytes().to_vec(); // KnownRegion::Leaves
        bytes.extend_from_slice(&(blob.len() as u64).to_le_bytes());
        bytes.extend_from_slice(blob);
        bytes
    }

    fn round_trip(page: LeafPage) {
        let region = KnownRegion::Leaves(page.clone());
        let encoded = bincode::serialize(&region).unwrap();
        assert_eq!(bincode::serialized_size(&region).unwrap() as usize, encoded.len());
        assert_eq!(bincode::serialize(&decoded(&encoded).unwrap()).unwrap(), bincode::serialize(&page).unwrap());
        let expected_json = serde_json::json!({ "Leaves": serde_json::to_value(&page).unwrap() });
        assert_eq!(serde_json::to_value(&region).unwrap(), expected_json);
        let from_json: KnownRegion = serde_json::from_value(expected_json).unwrap();
        assert_eq!(bincode::serialize(&from_json).unwrap(), encoded);
    }

    #[test]
    fn binary_and_json_preserve_keys_bounds_and_full_clocks() {
        let keys = vec![vec![], vec![0, 128, 255], 42i64.to_be_bytes().to_vec(), b"short\0string".to_vec(), vec![b'x'; 4096]];
        for generation in [1, 250, 251, 1000, 65535, 65536, u32::MAX] {
            for tips in [1, 2, 4] {
                for (start, end) in [
                    (Bound::Unbounded, Bound::Unbounded),
                    (Bound::Included(vec![]), Bound::Excluded(vec![255; 4096])),
                    (Bound::Excluded(vec![0]), Bound::Included(vec![255])),
                ] {
                    let mut page = page(keys.clone(), generation, tips);
                    page.bounds = AddressRange::new(start, end);
                    round_trip(page);
                }
            }
        }
        round_trip(page(vec![], 1, 0));
        round_trip(page(vec![vec![]], 1, 0));
    }

    #[test]
    fn numeric_initial_inventory_has_the_expected_versioned_size() {
        let page = page((0..128u64).map(|n| [&[1][..], &n.to_be_bytes()].concat()).collect(), 1, 1);
        let fixed = bincode::serialize(&page).unwrap();
        let compact = bincode::serialize(&KnownRegion::Leaves(page.clone())).unwrap();
        assert_eq!(compact, wrap_blob(&options().serialize(&(VERSION, &page)).unwrap()));
        assert_eq!(fixed.len(), 11920);
        assert_eq!(compact.len(), 9744);
        round_trip(page);
    }

    #[test]
    fn complete_initial_envelope_size_matches_its_encoding() {
        let mut previous = None;
        let inventory = crate::KnownState {
            spec: crate::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), ankql::ast::Predicate::True),
            regions: (0..16)
                .map(|index| {
                    let mut page = page((0..128u64).map(|n| [&[1][..], &(index * 128 + n).to_be_bytes()].concat()).collect(), 1, 1);
                    for (n, leaf) in page.leaves.iter_mut().enumerate() {
                        let mut id = [0; 32];
                        id[..4].copy_from_slice(&(index as u32 * 128 + n as u32).to_be_bytes());
                        leaf.entity.entity_id = EntityId::from_bytes(id);
                    }
                    let last = page.leaves.last().unwrap();
                    let end = ankurah_digest_tree::leaf_address(&last.key, last.entity.entity_id);
                    page.bounds = AddressRange::new(
                        previous.take().map_or(Bound::Unbounded, Bound::Excluded),
                        if index == 15 { Bound::Unbounded } else { Bound::Included(end.clone()) },
                    );
                    previous = Some(end);
                    KnownRegion::Leaves(page)
                })
                .collect(),
        };
        let encoded = bincode::serialize(&inventory).unwrap();
        assert_eq!(bincode::serialized_size(&inventory).unwrap() as usize, encoded.len());
        let decoded: crate::KnownState = bincode::deserialize(&encoded).unwrap();
        assert_eq!(bincode::serialize(&decoded).unwrap(), encoded);
    }

    #[test]
    fn compact_decoding_uses_existing_clock_normalization_and_errors() {
        // Clock serializes as these entries. Build the same leaf wire fields
        // without its constructor so deserialization owns validation.
        #[derive(Serialize)]
        struct RawPage {
            bounds: AddressRange,
            leaves: Vec<(Vec<u8>, (EntityId, Vec<(u32, EventId)>))>,
        }
        let decode = |entries: Vec<(u32, EventId)>| {
            let raw = RawPage { bounds: AddressRange::all(), leaves: vec![(vec![], (EntityId::from_bytes([0; 32]), entries))] };
            decoded(&wrap_blob(&options().serialize(&(VERSION, raw)).unwrap()))
        };
        let normalized = decode(vec![(1000, event(2)), (1, event(1)), (1, event(1))]).unwrap();
        assert_eq!(normalized.leaves[0].entity.head.entries(), &[(1, event(1)), (1000, event(2))]);
        assert!(decode(vec![(0, event(1))]).is_err());
        assert!(decode(vec![(1, event(1)), (2, event(1))]).is_err());
    }

    #[test]
    fn valid_fixed_page_with_maximum_generations_can_expand_beyond_64_kib() {
        let page = page(vec![vec![]], u32::MAX, 1818);
        let fixed_answer = bincode::serialize(&StateAnswer::Leaves(page.clone())).unwrap();
        let blob = options().serialize(&(VERSION, &page)).unwrap();
        assert!(fixed_answer.len() <= 64 * 1024);
        assert!(blob.len() > 64 * 1024);
        assert!(blob.len() <= MAX_ENCODED_BYTES);
        round_trip(page);
    }

    #[test]
    fn packed_size_does_not_replace_decoded_row_or_byte_limits() {
        for page in [page(vec![vec![]; 129], 1, 1), page(vec![vec![]], 1, 1900)] {
            let blob = options().serialize(&(VERSION, &page)).unwrap();
            assert!(blob.len() < MAX_ENCODED_BYTES);
            assert!(page.leaves.len() > 128 || bincode::serialized_size(&StateAnswer::Leaves(page.clone())).unwrap() > 64 * 1024);
            round_trip(page);
        }
    }

    #[test]
    fn malformed_and_oversized_blobs_are_rejected() {
        let original = page(vec![b"key".to_vec()], 1, 1);
        let valid = options().serialize(&(VERSION, &original)).unwrap();
        let mut wrong_version = valid.clone();
        wrong_version[0] = 2;
        let mut trailing = valid.clone();
        trailing.push(0);
        for blob in [vec![], wrong_version, valid[..valid.len() - 1].to_vec(), trailing, vec![VERSION; MAX_ENCODED_BYTES + 1]] {
            assert!(decoded(&wrap_blob(&blob)).is_err());
        }
        assert!(decoded(&wrap_blob(&vec![VERSION; MAX_ENCODED_BYTES + 1])).unwrap_err().to_string().contains("exceeds 68 KiB"));
        let oversized = page(vec![vec![0; MAX_ENCODED_BYTES]], 1, 1);
        assert!(bincode::serialize(&KnownRegion::Leaves(oversized)).is_err());
        let mut impossible_length = 1u32.to_le_bytes().to_vec();
        impossible_length.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(decoded(&impossible_length).is_err());
    }

    #[test]
    fn ordinary_response_and_feedback_pages_keep_fixed_encoding() {
        let page = page(vec![b"k".to_vec()], 1000, 2);
        // Independent fixed encoding of all-range bounds, vector lengths,
        // entity bytes and clock generations, rather than the initial codec.
        let mut expected = Vec::new();
        expected.extend_from_slice(&0u32.to_le_bytes()); // Bound::Unbounded, twice
        expected.extend_from_slice(&0u32.to_le_bytes());
        expected.extend_from_slice(&1u64.to_le_bytes());
        expected.extend_from_slice(&1u64.to_le_bytes());
        expected.push(b'k');
        expected.extend_from_slice(&[0; 32]);
        expected.extend_from_slice(&2u64.to_le_bytes());
        for index in 0..2 {
            expected.extend_from_slice(&1000u32.to_le_bytes());
            expected.extend_from_slice(&event(index).to_bytes());
        }
        assert_eq!(bincode::serialize(&page).unwrap(), expected);
        for (tag, encoded) in [
            (1u32, bincode::serialize(&StateItem::Leaves(page.clone())).unwrap()),
            (1, bincode::serialize(&StateAnswer::Leaves(page)).unwrap()),
        ] {
            assert_eq!(encoded, [tag.to_le_bytes().as_slice(), &expected].concat());
        }
    }
}
