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

    /// Known answers for one entity's leaf in two indexes: one keyed by a
    /// string property, filed under the canonical ascending key of "blue"
    /// (the UTF-8 bytes and a zero terminator), and the entity-id index, whose
    /// key is empty. The head hash, the encodings and the 64 uniform bytes were
    /// computed independently (Python's hashlib, following the definitions
    /// above and RFC 9380 section 5.3.1); the points follow from those bytes
    /// through the RFC 9496 element derivation. The canonical encodings of the
    /// points are pinned so that any other implementation of the leaf hash can
    /// be checked against them.
    #[test]
    fn leaf_points_match_known_answers() {
        let entity_id = EntityId::from_bytes(std::array::from_fn(|i| i as u8));
        let head = HeadHash::of([[0xaa; 32], [0x11; 32]]);
        assert_eq!(hex(head.as_bytes()), "08d80e932baa3f23e52effe20e309b03dae93d74adbba74962427c038afe94dd");

        let vectors: [(&[u8], &str, &str, &str); 2] = [
            (
                b"blue\0",
                "0000000000000005626c756500\
                 0000000000000020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\
                 000000000000002008d80e932baa3f23e52effe20e309b03dae93d74adbba74962427c038afe94dd",
                "575ed7df8d5c36bdc30dd2662946155f1deef00ff3d152aae23a4d983a0cab22ca999c4c672328ad435e310d574d2db3387de5d3cefc1aa8697c71c83790e1fe",
                "0a922ebabb4be30a5a754ce3740a9966f6d529877b78e9165b1a0e46c6b78212",
            ),
            (
                b"",
                "0000000000000000\
                 0000000000000020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\
                 000000000000002008d80e932baa3f23e52effe20e309b03dae93d74adbba74962427c038afe94dd",
                "3b1aba62df4d742ee83643bec08ee5ea1dc5a50e3c9ecffbf2ad8eabed7b43a6951b860860790f2a0ed0191d02872f1bec39e31f86a657e875798cf804b2dd34",
                "887d4ede28887ba10ffa31e2fb5b4eb614bdd54940549635858cbb69d6acea28",
            ),
        ];
        for (key, encoding, uniform_bytes, point) in vectors {
            let leaf = Leaf::new(key, entity_id, head);
            assert_eq!(hex(&leaf.encode()), encoding);
            let uniform_bytes: [u8; 64] = unhex(uniform_bytes).try_into().unwrap();
            assert_eq!(leaf.point(), LeafPoint(RistrettoPoint::from_uniform_bytes(&uniform_bytes)));
            assert_eq!(hex(leaf.point().0.compress().as_bytes()), point);
        }
    }
}
