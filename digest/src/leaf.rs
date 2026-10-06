use ankurah_core_types::EntityId;
use sha2::{Digest as _, Sha256};

use crate::hash_to_curve::hash_to_ristretto255;
use crate::point::LeafPoint;

/// The domain separation tag leaf points are hashed under (RFC 9380 section
/// 3.1), keeping them apart from every other use of the same hash.
///
/// It names the application, the purpose and a version. The version changes
/// with any change to [`Leaf::encode`] or to the hash-to-curve suite
/// (`ristretto255_XMD:SHA-512_R255MAP_RO_`), so one tag never names two
/// constructions: points made under different versions are independent even
/// where their encoded leaves are the same bytes.
pub const LEAF_DST: &[u8] = b"org.ankurah.digest.leaf.v0";

/// The domain tag that precedes a head's event ids in [`HeadHash`].
pub const HEAD_TAG: &[u8] = b"org.ankurah.digest.head.v0";

/// The canonical hash of an entity's head, which is how a leaf names the
/// entity's state.
///
/// A head is a set of event ids, so its hash must not depend on the order or
/// the repetition of the ids it is given: it is SHA-256 over [`HEAD_TAG`]
/// followed by each distinct event id once, in ascending bytewise order. Event
/// ids are fixed-width, so the concatenation needs no delimiters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeadHash([u8; 32]);

impl HeadHash {
    /// Hash a head given its event ids in any order.
    pub fn of(event_ids: impl IntoIterator<Item = [u8; 32]>) -> Self {
        let mut ids: Vec<[u8; 32]> = event_ids.into_iter().collect();
        ids.sort_unstable();
        ids.dedup();
        let mut hasher = Sha256::new();
        hasher.update(HEAD_TAG);
        for id in &ids {
            hasher.update(id);
        }
        Self(hasher.finalize().into())
    }

    /// The head hash with these 32 bytes, for rebuilding a leaf from the head
    /// hash a row stores or the wire carries. It reconstructs a value and
    /// checks nothing: it cannot verify an advertised head against its event
    /// ids, which only hashing the events with [`HeadHash::of`] does.
    pub fn from_bytes(bytes: [u8; 32]) -> Self { Self(bytes) }

    /// The 32 bytes of the hash.
    pub fn as_bytes(&self) -> &[u8; 32] { &self.0 }
}

/// One entity filed under one key of one index: the unit a digest tree sums.
///
/// The leaf binds its key as well as its entity and head, so an entity filed
/// under two keys of one index is two distinct leaves with unrelated points,
/// and moving an entity to another key replaces its leaf. A tree must hold at
/// most one leaf per distinct key and entity; the crate documentation explains
/// why the digest's guarantee rests on that.
#[derive(Clone, Copy, Debug)]
pub struct Leaf<'a> {
    key: &'a [u8],
    entity_id: EntityId,
    head: HeadHash,
}

impl<'a> Leaf<'a> {
    /// The leaf of `entity_id` with head `head`, filed under `key`.
    ///
    /// `key` is the canonical index key the entity is filed under, as the
    /// core's index key encoder produces it for the index the tree serves: the
    /// same bytes on every storage engine. It is empty for the entity-id index,
    /// where the entity id the leaf already carries is the whole key.
    pub fn new(key: &'a [u8], entity_id: EntityId, head: HeadHash) -> Self { Self { key, entity_id, head } }

    /// The canonical encoding the leaf's point is hashed from: the key, the
    /// entity id and the head hash, in that order, each preceded by its length
    /// in bytes as an unsigned 64-bit big-endian integer.
    ///
    /// The length prefixes make the encoding injective, so no two distinct
    /// leaves share an encoding even when one key is a prefix of another.
    pub fn encode(&self) -> Vec<u8> {
        let entity_id = self.entity_id.to_bytes();
        let parts: [&[u8]; 3] = [self.key, &entity_id, self.head.as_bytes()];
        let mut encoded = Vec::with_capacity(parts.iter().map(|part| 8 + part.len()).sum());
        for part in parts {
            encoded.extend_from_slice(&(part.len() as u64).to_be_bytes());
            encoded.extend_from_slice(part);
        }
        encoded
    }

    /// The leaf's point: RFC 9380 `hash_to_ristretto255` of [`Leaf::encode`]
    /// under [`LEAF_DST`].
    pub fn point(&self) -> LeafPoint { LeafPoint(hash_to_ristretto255(&self.encode(), LEAF_DST)) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curve25519_dalek::ristretto::RistrettoPoint;

    fn hex(bytes: &[u8]) -> String { bytes.iter().map(|byte| format!("{byte:02x}")).collect() }

    fn unhex(s: &str) -> Vec<u8> { (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect() }

    #[test]
    fn head_hash_treats_the_head_as_a_set() {
        let (a, b, c) = ([1u8; 32], [2u8; 32], [3u8; 32]);
        assert_eq!(HeadHash::of([c, a, b]), HeadHash::of([a, b, c]), "order");
        assert_eq!(HeadHash::of([a, b, a]), HeadHash::of([a, b]), "repetition");
        assert_ne!(HeadHash::of([a, b]), HeadHash::of([a, c]));
        assert_ne!(HeadHash::of([a]), HeadHash::of([a, b]));
    }

    #[test]
    fn a_head_hash_rebuilt_from_its_bytes_is_the_same_value() {
        let head = HeadHash::of([[1u8; 32], [2u8; 32]]);
        assert_eq!(HeadHash::from_bytes(*head.as_bytes()), head);
    }

    /// A leaf's point checked against its known 64 uniform bytes and the
    /// known canonical encoding of the point.
    fn assert_point_matches(leaf: &Leaf, uniform_bytes: &str, point: &str) {
        let uniform_bytes: [u8; 64] = unhex(uniform_bytes).try_into().unwrap();
        assert_eq!(leaf.point(), LeafPoint(RistrettoPoint::from_uniform_bytes(&uniform_bytes)));
        assert_eq!(hex(leaf.point().0.compress().as_bytes()), point);
    }

    /// Known answers for one entity's leaves, each pinned at its encoding, its
    /// 64 uniform bytes and its point: filed under the canonical ascending key
    /// of the string "blue" (its UTF-8 bytes and a zero terminator); in the
    /// entity-id index, whose key is empty; under the composite key ("blue",
    /// "sky"), whose bytes begin with the first key's; under the key of the
    /// string "a\0b", whose zero byte the key encoder escapes as 00 ff; and
    /// under "blue" with an empty head. Every value was computed independently
    /// in Python: the head hashes, the encodings and the uniform bytes with
    /// hashlib, following the definitions above and RFC 9380 section 5.3.1,
    /// and the points with an implementation of RFC 9496's element derivation
    /// and encoding. They are pinned so that any other implementation of the
    /// leaf hash can be checked against them.
    #[test]
    fn leaf_points_match_known_answers() {
        let entity_id = EntityId::from_bytes(std::array::from_fn(|i| i as u8));
        let head = HeadHash::from_bytes(unhex("08d80e932baa3f23e52effe20e309b03dae93d74adbba74962427c038afe94dd").try_into().unwrap());
        assert_eq!(head, HeadHash::of([[0xaa; 32], [0x11; 32]]));
        let empty_head =
            HeadHash::from_bytes(unhex("00f6de2079099bd503a07158f10da38202683a6374822bb72c738c4f5d0fe208").try_into().unwrap());
        assert_eq!(empty_head, HeadHash::of([]));

        let vectors: [(&[u8], HeadHash, &str, &str, &str); 5] = [
            (
                b"blue\0",
                head,
                "0000000000000005626c756500\
                 0000000000000020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\
                 000000000000002008d80e932baa3f23e52effe20e309b03dae93d74adbba74962427c038afe94dd",
                "575ed7df8d5c36bdc30dd2662946155f1deef00ff3d152aae23a4d983a0cab22ca999c4c672328ad435e310d574d2db3387de5d3cefc1aa8697c71c83790e1fe",
                "0a922ebabb4be30a5a754ce3740a9966f6d529877b78e9165b1a0e46c6b78212",
            ),
            (
                b"",
                head,
                "0000000000000000\
                 0000000000000020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\
                 000000000000002008d80e932baa3f23e52effe20e309b03dae93d74adbba74962427c038afe94dd",
                "3b1aba62df4d742ee83643bec08ee5ea1dc5a50e3c9ecffbf2ad8eabed7b43a6951b860860790f2a0ed0191d02872f1bec39e31f86a657e875798cf804b2dd34",
                "887d4ede28887ba10ffa31e2fb5b4eb614bdd54940549635858cbb69d6acea28",
            ),
            (
                b"blue\0sky\0",
                head,
                "0000000000000009626c756500736b7900\
                 0000000000000020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\
                 000000000000002008d80e932baa3f23e52effe20e309b03dae93d74adbba74962427c038afe94dd",
                "4f73c826ed1011e241daa4a0bc62ad27765b6d1739fc27c95c023d428b491595e643f4b8f2f80183d8992aa43926e8a2d227af383c3391e0f579695a839f1daa",
                "54f12e8da336940e2dd4e8814d39958395b05a068b95a89ecf3187766ec46f43",
            ),
            (
                b"a\0\xffb\0",
                head,
                "00000000000000056100ff6200\
                 0000000000000020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\
                 000000000000002008d80e932baa3f23e52effe20e309b03dae93d74adbba74962427c038afe94dd",
                "97320ec4c6c7f28d9d18a1760325d3c9741dda4d3e19f72d001915d53b37f0ee426be93d2c8ab2771f21772a276fc77efc57aa1e0c9bcad2f755a944508f0b13",
                "4e196198d12e2b65ae03d041f8fc103d835384ba2d08c4fd386b70c75c75f723",
            ),
            (
                b"blue\0",
                empty_head,
                "0000000000000005626c756500\
                 0000000000000020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\
                 000000000000002000f6de2079099bd503a07158f10da38202683a6374822bb72c738c4f5d0fe208",
                "292bb6d9113f79db77696c0640dd2966410a32ac58cf2bb5c661352c54a3805e5f2361f506f0d7af6cebff1d6d5bec62d8431f05825e5e4d2c6cd212c51eb2d3",
                "6c91efcdd7b75493ff9f020dd409a9aca3c28d7e682f9bb808a2a65d8993d060",
            ),
        ];
        for (key, head, encoding, uniform_bytes, point) in vectors {
            let leaf = Leaf::new(key, entity_id, head);
            assert_eq!(hex(&leaf.encode()), encoding);
            assert_point_matches(&leaf, uniform_bytes, point);
        }
    }

    /// Keys of 255 and 256 bytes, the bytes 0 to 254 and 0 to 255: the eight
    /// length bytes carry the whole length, which a single byte could not.
    /// The uniform bytes and points come from the same Python implementation.
    #[test]
    fn keys_of_255_and_256_bytes_keep_their_whole_length() {
        let entity_id = EntityId::from_bytes(std::array::from_fn(|i| i as u8));
        let head = HeadHash::of([[0xaa; 32], [0x11; 32]]);
        let vectors = [
            (
                255,
                "00000000000000ff",
                "a814d321d48a7d473d4c49f485542ab73084343e2f84d9be47b0ec78a67a0579bdacdc40e93178813f25e2dee1bf398b73e064f45436e19ac384447179e66c28",
                "ee2c2ddfb7e7a2280eac7f1bfe5a12582c973f5ca9087b42d7ff3f2861d7cc58",
            ),
            (
                256,
                "0000000000000100",
                "aa8f69aa010bd789fc713a69df6344f15f15b63d70f710a91dee30bb6e218614672475f71ad892887446d8466241cb24d91799ce79034498609850a1827ecd2c",
                "98e43d964bd8b516061a914f81a7c35fa88468f5b92cc289c979c08114416214",
            ),
        ];
        for (len, length_bytes, uniform_bytes, point) in vectors {
            let key: Vec<u8> = (0..len).map(|i| i as u8).collect();
            let leaf = Leaf::new(&key, entity_id, head);
            let encoded = leaf.encode();
            assert_eq!(hex(&encoded[..8]), length_bytes);
            assert_eq!(encoded[8..8 + len], key[..]);
            assert_eq!(encoded.len(), 8 + len + 2 * (8 + 32));
            assert_point_matches(&leaf, uniform_bytes, point);
        }
    }
}
