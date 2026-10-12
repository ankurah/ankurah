//! Prefix addressing: the name of a tree node.

use std::cmp::Ordering;
use std::fmt;

/// The name of a tree node: the leading bits shared by the leaf addresses
/// beneath it.
///
/// Prefixes order as their tree nodes are met walking the tree depth first: a
/// prefix comes before every prefix it contains, and everything it contains
/// comes before the next prefix outside it. [`NodePrefix::to_bytes`] encodes
/// a prefix so that byte order is this order, which is how the tree keeps its
/// tree node rows ordered by prefix in a map keyed by bytes.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct NodePrefix {
    /// The bits, packed most significant first; bits past `len` are zero.
    bits: Vec<u8>,
    len: u32,
}

/// Why a byte string is not the encoding of a tree node's prefix.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NodePrefixError {
    /// A byte past the 254 places of the seven-level walk.
    #[error("byte {0} names no tree node")]
    InvalidByte(u8),
    /// A chunk of fewer than seven bits that is not the last.
    #[error("a chunk shorter than seven bits can only end a prefix")]
    ShortChunkBeforeEnd,
}

/// Bits per encoded byte; see [`NodePrefix::to_bytes`].
const CHUNK: u32 = 7;

impl NodePrefix {
    /// The empty prefix, which names the root and contains every address.
    pub fn root() -> Self { Self { bits: Vec::new(), len: 0 } }

    /// The first `len` bits of `address`.
    ///
    /// # Panics
    /// When `address` holds fewer than `len` bits.
    pub fn of(address: &[u8], len: u32) -> Self {
        assert!(len as usize <= address.len() * 8, "a {}-byte address has no {len}-bit prefix", address.len());
        let mut bits = address[..(len as usize).div_ceil(8)].to_vec();
        if !len.is_multiple_of(8) {
            *bits.last_mut().expect("a partial byte exists") &= 0xFF << (8 - len % 8);
        }
        Self { bits, len }
    }

    /// How many bits the prefix holds: the depth of the tree node it names.
    pub fn bit_len(&self) -> u32 { self.len }

    /// Whether this is the empty prefix, which names the root.
    pub fn is_root(&self) -> bool { self.len == 0 }

    /// The bit at `index`, counting from the most significant.
    ///
    /// # Panics
    /// When `index` is not below [`NodePrefix::bit_len`].
    pub fn bit(&self, index: u32) -> bool {
        assert!(index < self.len, "bit {index} of a {}-bit prefix", self.len);
        self.bits[(index / 8) as usize] & (0x80 >> (index % 8)) != 0
    }

    /// This prefix followed by one more bit.
    ///
    /// # Panics
    /// When this prefix already holds `u32::MAX` bits, the most its length
    /// counts.
    pub fn child(&self, bit: bool) -> Self {
        let len = self.len.checked_add(1).expect("a prefix holds at most u32::MAX bits");
        let mut child = self.clone();
        if child.len.is_multiple_of(8) {
            child.bits.push(0);
        }
        if bit {
            child.bits[(child.len / 8) as usize] |= 0x80 >> (child.len % 8);
        }
        child.len = len;
        child
    }

    /// Whether `other` is this prefix or lies beneath it.
    pub fn contains(&self, other: &NodePrefix) -> bool { other.len >= self.len && Self::of(&other.bits, self.len) == *self }

    /// Whether `address` begins with this prefix.
    pub fn contains_address(&self, address: &[u8]) -> bool {
        address.len() * 8 >= self.len as usize && Self::of(address, self.len) == *self
    }

    /// The first prefix, in prefix order, after every prefix this one
    /// contains; `None` when nothing follows, as for the root.
    pub fn after_subtree(&self) -> Option<NodePrefix> {
        let mut len = self.len;
        while len > 0 && self.bit(len - 1) {
            len -= 1;
        }
        (len > 0).then(|| Self::of(&self.bits, len - 1).child(true))
    }

    /// The first byte string past every byte string that begins with the
    /// encoding of this prefix or of a prefix it contains (`None` when nothing
    /// follows, as for the root): where the keys of a subtree's rows end, when
    /// each row's key begins with its tree node's encoding. It lies no later
    /// than the encoding of [`NodePrefix::after_subtree`], and can lie before
    /// it: a string that begins with an ancestor's encoding and continues with
    /// a byte no encoding holds, as a tree node's delta rows do, falls between.
    pub fn subtree_key_end(&self) -> Option<Vec<u8>> {
        if self.is_root() {
            return None;
        }
        // Every prefix beneath this one encodes as bytes that begin before the
        // end of the subtree's last prefix within this one's last chunk: this
        // prefix extended with ones to the chunk's end.
        let mut last = self.clone();
        while !last.len.is_multiple_of(CHUNK) {
            last = last.child(true);
        }
        let mut end = last.to_bytes();
        *end.last_mut().expect("a prefix other than the root encodes as some bytes") += 1;
        Some(end)
    }

    /// The least address this prefix contains, and the first address past
    /// them all (`None` when no address follows): the bounds of the leaf
    /// addresses beneath the tree node.
    pub(crate) fn address_bounds(&self) -> (Vec<u8>, Option<Vec<u8>>) {
        let start = self.bits.clone();
        let mut end = self.bits.clone();
        if self.len == 0 {
            return (start, None);
        }
        // Add one at the prefix's last bit, carrying toward the front.
        let last = self.len - 1;
        let mut index = (last / 8) as usize;
        let (sum, mut carry) = end[index].overflowing_add(0x80 >> (last % 8));
        end[index] = sum;
        while carry {
            if index == 0 {
                return (start, None);
            }
            index -= 1;
            (end[index], carry) = end[index].overflowing_add(1);
        }
        // A carry leaves zero bytes at the end, and they must go: for the
        // prefix 0000 0000 1 the bound 01 00 admits the one-byte address 01,
        // too short to hold the prefix yet below the bound. The bound 01 still
        // lies past every address beneath the prefix.
        while end.last() == Some(&0) {
            end.pop();
        }
        (start, Some(end))
    }

    /// Encode the prefix so that byte order is prefix order.
    ///
    /// Each byte stands for up to seven bits: it names one vertex of a binary
    /// tree seven levels deep by the vertex's place in a depth-first walk
    /// (0 to 253). Every chunk but the last is seven bits long, so a prefix
    /// of `n` bits takes `n / 7` bytes, rounded up. Because a depth-first walk
    /// meets a vertex before its descendants and the left subtree before the
    /// right, comparing these bytes compares the prefixes in prefix order.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len.div_ceil(CHUNK) as usize);
        let mut start = 0;
        while start < self.len {
            let depth = (self.len - start).min(CHUNK);
            let mut place = 0u32;
            for level in 1..=depth {
                // Entering a child passes its parent; a right child also passes
                // its left sibling's whole subtree.
                place += 1;
                if self.bit(start + level - 1) {
                    place += subtree_size(level);
                }
            }
            out.push((place - 1) as u8);
            start += depth;
        }
        out
    }

    /// Decode [`NodePrefix::to_bytes`].
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, NodePrefixError> {
        let mut prefix = Self::root();
        for (position, &byte) in bytes.iter().enumerate() {
            if u32::from(byte) >= 2 * subtree_size(1) {
                return Err(NodePrefixError::InvalidByte(byte));
            }
            let mut place = u32::from(byte);
            let mut level = 0;
            loop {
                level += 1;
                let right = place >= subtree_size(level);
                if right {
                    place -= subtree_size(level);
                }
                prefix = prefix.child(right);
                if place == 0 {
                    break;
                }
                place -= 1;
            }
            if level < CHUNK && position + 1 < bytes.len() {
                return Err(NodePrefixError::ShortChunkBeforeEnd);
            }
        }
        Ok(prefix)
    }
}

/// The number of vertices in the subtree of a vertex at `level` (1 to 7) of
/// the seven-level tree that one encoded byte walks.
fn subtree_size(level: u32) -> u32 { (1 << (CHUNK + 1 - level)) - 1 }

impl Ord for NodePrefix {
    fn cmp(&self, other: &Self) -> Ordering {
        let common = self.len.min(other.len);
        let whole = (common / 8) as usize;
        self.bits[..whole]
            .cmp(&other.bits[..whole])
            .then_with(|| match common % 8 {
                0 => Ordering::Equal,
                rest => {
                    let mask = 0xFFu8 << (8 - rest);
                    (self.bits[whole] & mask).cmp(&(other.bits[whole] & mask))
                }
            })
            .then(self.len.cmp(&other.len))
    }
}

impl PartialOrd for NodePrefix {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}

impl fmt::Debug for NodePrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodePrefix(")?;
        for index in 0..self.len {
            write!(f, "{}", u8::from(self.bit(index)))?;
        }
        write!(f, ")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::SmallRng, Rng, SeedableRng};

    fn random_prefix(rng: &mut SmallRng) -> NodePrefix {
        // Short lengths make shared prefixes, and so the interesting
        // comparisons, common.
        let len = rng.gen_range(0..=40);
        let address: Vec<u8> = (0..5).map(|_| rng.gen::<u8>() & 0xC3).collect();
        NodePrefix::of(&address, len)
    }

    #[test]
    fn byte_order_is_prefix_order() {
        let mut rng = SmallRng::seed_from_u64(7);
        let prefixes: Vec<NodePrefix> = (0..400).map(|_| random_prefix(&mut rng)).collect();
        for a in &prefixes {
            assert_eq!(NodePrefix::from_bytes(&a.to_bytes()).as_ref(), Ok(a), "round trip");
            for b in &prefixes {
                assert_eq!(a.cmp(b), a.to_bytes().cmp(&b.to_bytes()), "{a:?} against {b:?}");
            }
        }
    }

    #[test]
    fn a_prefix_precedes_its_subtree_and_the_subtree_is_contiguous() {
        let mut rng = SmallRng::seed_from_u64(11);
        let prefixes: Vec<NodePrefix> = (0..400).map(|_| random_prefix(&mut rng)).collect();
        for a in &prefixes {
            let after = a.after_subtree();
            for b in &prefixes {
                let inside = *a <= *b && after.as_ref().is_none_or(|after| b < after);
                assert_eq!(inside, a.contains(b), "{a:?} against {b:?}");
            }
        }
    }

    /// A subtree's key end lies past every string that begins with the
    /// encoding of the prefix or of one beneath it, before any string that
    /// begins with an ancestor's encoding and the byte 255 and follows the
    /// subtree, and no later than the next prefix's encoding.
    #[test]
    fn the_subtree_key_end_follows_the_strings_the_subtree_begins() {
        let mut rng = SmallRng::seed_from_u64(29);
        for _ in 0..1000 {
            // Runs of ones make prefixes that end an ancestor's subtree common.
            let address: Vec<u8> = (0..12).map(|_| [0x00, 0xff, rng.gen()][rng.gen_range(0..3)]).collect();
            let ancestor_len = rng.gen_range(0..=60);
            let prefix_len = rng.gen_range(ancestor_len + 1..=80);
            let descendant_len = rng.gen_range(prefix_len + 1..=96);
            let (ancestor, prefix) = (NodePrefix::of(&address, ancestor_len), NodePrefix::of(&address, prefix_len));
            let descendant = NodePrefix::of(&address, descendant_len);
            let tail: Vec<u8> = (0..rng.gen_range(0..4)).map(|_| rng.gen()).collect();
            let end = prefix.subtree_key_end();
            let before_end = |bytes: &[u8]| end.as_ref().is_none_or(|end| bytes < end.as_slice());
            for begins in [prefix.to_bytes(), descendant.to_bytes()] {
                for string in [begins.clone(), [&begins[..], &[255], &tail].concat(), [&begins[..], &tail].concat()] {
                    assert!(string >= prefix.to_bytes() && before_end(&string), "{prefix:?} over {descendant:?}");
                }
            }
            let past_ancestor = [&ancestor.to_bytes()[..], &[255], &tail].concat();
            assert!(past_ancestor < prefix.to_bytes() || !before_end(&past_ancestor), "{ancestor:?} above {prefix:?}");
            if let Some(after) = prefix.after_subtree() {
                assert!(end.is_some_and(|end| end <= after.to_bytes()), "{prefix:?}");
            }
        }
        assert_eq!(NodePrefix::root().subtree_key_end(), None);
    }

    /// Whether `address` lies within the bounds of `prefix`.
    fn admits(prefix: &NodePrefix, address: &[u8]) -> bool {
        let (start, end) = prefix.address_bounds();
        address >= start.as_slice() && end.is_none_or(|end| address < end.as_slice())
    }

    #[test]
    fn address_bounds_hold_exactly_the_contained_addresses() {
        let mut rng = SmallRng::seed_from_u64(13);
        for _ in 0..200 {
            let prefix = random_prefix(&mut rng);
            for _ in 0..50 {
                let len = rng.gen_range(0..=6);
                let address: Vec<u8> = (0..len).map(|_| rng.gen::<u8>() & 0xC3).collect();
                assert_eq!(admits(&prefix, &address), prefix.contains_address(&address), "{prefix:?} against {address:?}");
            }
        }
    }

    /// Every prefix of up to sixteen bits against every address of up to two
    /// bytes: a prefix's bounds hold exactly the addresses that begin with its
    /// bits, judged by a bit test of its own rather than by `NodePrefix`.
    #[test]
    fn address_bounds_are_exact_through_sixteen_bits() {
        let mut addresses: Vec<Vec<u8>> = vec![Vec::new()];
        addresses.extend((0..=u8::MAX).map(|byte| vec![byte]));
        addresses.extend((0..=u16::MAX).map(|bytes| bytes.to_be_bytes().to_vec()));
        addresses.sort();
        let bits_of = |address: &[u8]| (8 * address.len() as u32, address.iter().fold(0u32, |value, &byte| value << 8 | u32::from(byte)));
        // The d-bit prefix p sits at 2^d - 1 + p, so every prefix through
        // sixteen bits has a slot.
        let slot = |depth: u32, value: u32| (1usize << depth) - 1 + value as usize;
        let mut beginning_with = vec![0usize; slot(17, 0)];
        for address in &addresses {
            let (len, value) = bits_of(address);
            for depth in 0..=len {
                beginning_with[slot(depth, value >> (len - depth))] += 1;
            }
        }
        for depth in 0..=16 {
            for value in 0..1u32 << depth {
                let prefix = NodePrefix::of(&((value << (16 - depth)) as u16).to_be_bytes(), depth);
                let (start, end) = prefix.address_bounds();
                let from = addresses.partition_point(|address| *address < start);
                let to = end.map_or(addresses.len(), |end| addresses.partition_point(|address| *address < end));
                // The bounds hold a run of the sorted addresses. Each one in it
                // begins with the prefix, and the run is as long as the count
                // of such addresses, so no address outside it does.
                for address in &addresses[from..to] {
                    let (len, bits) = bits_of(address);
                    assert!(len >= depth && bits >> (len - depth) == value, "{prefix:?} admits {address:02x?}");
                }
                assert_eq!(to - from, beginning_with[slot(depth, value)], "{prefix:?} misses an address beneath it");
            }
        }
    }

    /// Bounds where the exhaustive check does not reach: a carry across whole
    /// bytes, prefixes of all ones, chunk edges, and addresses as long as real
    /// leaves (a key, then a 32-byte entity id).
    #[test]
    fn address_bounds_at_the_edges() {
        let bits = |text: &str| text.chars().filter(|c| *c != ' ').fold(NodePrefix::root(), |prefix, bit| prefix.child(bit == '1'));
        let hex = |text: &str| text.split(' ').map(|byte| u8::from_str_radix(byte, 16).unwrap()).collect::<Vec<u8>>();
        let check = check_bounds;
        let zeros = |n: usize| vec![0u8; n];

        check(NodePrefix::root(), (vec![], None), &[vec![], hex("ff"), vec![0xFF; 33]], &[]);
        // The review's counterexamples: a carry used to leave a zero byte in
        // the end bound, admitting shorter addresses.
        check(
            bits("0000 0000 1"),
            (hex("00 80"), Some(hex("01"))),
            &[hex("00 80"), hex("00 ff ff"), hex("00 80 00 00 00 00")],
            &[hex("01"), hex("00"), hex("00 7f ff"), hex("01 00")],
        );
        let deep = NodePrefix::of(&[zeros(33), hex("80")].concat(), 265);
        check(
            deep,
            ([zeros(33), hex("80")].concat(), Some([zeros(32), hex("01")].concat())),
            &[[zeros(33), hex("80")].concat(), [zeros(33), vec![0xFF; 32]].concat()],
            // The empty key's encoding 00, then the entity id 00 x 31 01.
            &[[zeros(32), hex("01")].concat(), [zeros(33), hex("7f")].concat()],
        );
        // All ones: nothing follows the subtree.
        check(bits("1"), (hex("80"), None), &[hex("80"), hex("ff ff")], &[hex("7f ff"), vec![]]);
        check(bits("1111 111"), (hex("fe"), None), &[hex("fe"), hex("ff")], &[hex("fd ff")]);
        check(bits("1111 1111"), (hex("ff"), None), &[hex("ff"), vec![0xFF; 6]], &[hex("fe ff")]);
        check(bits("1111 1111 1111 1111"), (hex("ff ff"), None), &[hex("ff ff"), vec![0xFF; 33]], &[hex("ff"), hex("ff fe ff")]);
        // Around one encoded chunk (seven bits) and one byte.
        check(bits("0000 000"), (hex("00"), Some(hex("02"))), &[hex("00"), hex("01 ff")], &[hex("02"), vec![]]);
        check(bits("0000 001"), (hex("02"), Some(hex("04"))), &[hex("03 ff")], &[hex("01 ff"), hex("04")]);
        check(bits("0000 0001"), (hex("01"), Some(hex("02"))), &[hex("01"), hex("01 00")], &[hex("00 ff"), hex("02")]);
        check(
            bits("0000 0001 1"),
            (hex("01 80"), Some(hex("02"))),
            &[hex("01 80"), hex("01 ff ff")],
            &[hex("01"), hex("01 7f"), hex("02")],
        );
        // Around two chunks (fourteen bits) and two bytes.
        check(
            bits("0000 0000 1111 11"),
            (hex("00 fc"), Some(hex("01"))),
            &[hex("00 fc"), hex("00 ff ff")],
            &[hex("00"), hex("00 fb ff"), hex("01")],
        );
        check(bits("0000 0000 1111 111"), (hex("00 fe"), Some(hex("01"))), &[hex("00 fe 00")], &[hex("00 fd"), hex("01")]);
        check(
            bits("0000 0000 1111 1111"),
            (hex("00 ff"), Some(hex("01"))),
            &[hex("00 ff"), hex("00 ff 00")],
            &[hex("00"), hex("00 fe ff"), hex("01")],
        );
        check(
            bits("0000 0001 1111 1111 1"),
            (hex("01 ff 80"), Some(hex("02"))),
            &[hex("01 ff 80"), hex("01 ff ff ff ff ff")],
            &[hex("01 ff"), hex("01 ff 7f ff ff ff"), hex("02")],
        );
    }

    /// Check a prefix's bounds, and that exactly the addresses `inside` lie
    /// within them, judged by the bounds and by `contains_address` alike.
    fn check_bounds(prefix: NodePrefix, bounds: (Vec<u8>, Option<Vec<u8>>), inside: &[Vec<u8>], outside: &[Vec<u8>]) {
        assert_eq!(prefix.address_bounds(), bounds, "{prefix:?}");
        for address in inside {
            assert!(admits(&prefix, address) && prefix.contains_address(address), "{prefix:?} holds {address:02x?}");
        }
        for address in outside {
            assert!(!admits(&prefix, address) && !prefix.contains_address(address), "{prefix:?} excludes {address:02x?}");
        }
    }

    #[test]
    fn encodings_name_the_seven_level_walk() {
        let bits = |text: &str| text.chars().fold(NodePrefix::root(), |prefix, bit| prefix.child(bit == '1'));
        assert_eq!(bits("0").to_bytes(), [0]);
        assert_eq!(bits("00").to_bytes(), [1]);
        assert_eq!(bits("0000001").to_bytes(), [7]);
        assert_eq!(bits("1").to_bytes(), [127]);
        assert_eq!(bits("1111111").to_bytes(), [253]);
        assert_eq!(bits("11111110").to_bytes(), [253, 0]);
        assert_eq!(NodePrefix::from_bytes(&[254]), Err(NodePrefixError::InvalidByte(254)));
        assert_eq!(NodePrefix::from_bytes(&[0, 0]), Err(NodePrefixError::ShortChunkBeforeEnd));
    }
}
