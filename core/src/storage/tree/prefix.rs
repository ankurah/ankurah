use std::cmp::Ordering;
use std::fmt;

/// The name of a digest-tree node: the leading bits shared by the leaf
/// addresses beneath it.
///
/// Prefixes order as their nodes are met walking the tree depth first: a
/// prefix comes before every prefix it contains, and everything it contains
/// comes before the next prefix outside it. [`NodePrefix::to_bytes`] encodes
/// a prefix so that byte order is this order, which is how an engine keeps
/// node rows "ordered by prefix" in a map keyed by bytes.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct NodePrefix {
    /// The bits, packed most significant first; bits past `len` are zero.
    bits: Vec<u8>,
    len: u32,
}

/// Why a byte string is not the encoding of a node prefix.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NodePrefixError {
    #[error("byte {0} names no node")]
    InvalidByte(u8),
    #[error("a chunk shorter than seven bits can only end a prefix")]
    ShortChunkBeforeEnd,
    #[error("a prefix holds at most {max} bits", max = NodePrefix::MAX_BITS)]
    TooLong,
}

/// Bits per encoded byte; see [`NodePrefix::to_bytes`].
const CHUNK: u32 = 7;

/// The per-index key length limit, in bytes, that bounds node prefixes:
/// 4096 for now.
const KEY_LIMIT: u32 = 4096;

impl NodePrefix {
    /// The most bits a prefix holds. A node never needs more bits than the
    /// leaf addresses it divides, and an address is a key of at most 4096
    /// bytes, the per-index key length limit for now, followed by a 32-byte
    /// entity id. The deepest split a store allows, a refresher setting, is
    /// no deeper than this. A longer key still files its leaf, which no node
    /// separates from its neighbours beyond this depth.
    pub const MAX_BITS: u32 = 8 * (KEY_LIMIT + 32);

    /// The empty prefix, which names the root and contains every address.
    pub fn root() -> Self { Self { bits: Vec::new(), len: 0 } }

    /// The first `len` bits of `address`.
    ///
    /// # Panics
    /// When `address` holds fewer than `len` bits, or `len` exceeds
    /// [`NodePrefix::MAX_BITS`].
    pub fn of(address: &[u8], len: u32) -> Self {
        assert!(len <= Self::MAX_BITS, "a prefix holds at most {} bits, not {len}", Self::MAX_BITS);
        assert!(len as usize <= address.len() * 8, "a {}-byte address has no {len}-bit prefix", address.len());
        let mut bits = address[..(len as usize).div_ceil(8)].to_vec();
        if !len.is_multiple_of(8) {
            *bits.last_mut().expect("a partial byte exists") &= 0xFF << (8 - len % 8);
        }
        Self { bits, len }
    }

    pub fn bit_len(&self) -> u32 { self.len }

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
    /// When this prefix already holds [`NodePrefix::MAX_BITS`] bits.
    pub fn child(&self, bit: bool) -> Self {
        assert!(self.len < Self::MAX_BITS, "a prefix holds at most {} bits", Self::MAX_BITS);
        let mut child = self.clone();
        if child.len.is_multiple_of(8) {
            child.bits.push(0);
        }
        if bit {
            child.bits[(child.len / 8) as usize] |= 0x80 >> (child.len % 8);
        }
        child.len += 1;
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

    /// The least address this prefix contains, and the first address past
    /// them all (`None` when no address follows): the bounds of the leaf
    /// addresses beneath the node.
    pub(super) fn address_bounds(&self) -> (Vec<u8>, Option<Vec<u8>>) {
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
    /// Each byte stands for up to seven bits: it names one node of a binary
    /// tree seven levels deep by the node's place in a depth-first walk
    /// (0 to 253). Every chunk but the last is seven bits long, so a prefix
    /// of `n` bits takes `n / 7` bytes, rounded up. Because a depth-first walk
    /// meets a node before its descendants and the left subtree before the
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

    /// Decode [`NodePrefix::to_bytes`], refusing a prefix longer than
    /// [`NodePrefix::MAX_BITS`].
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
                if prefix.len == Self::MAX_BITS {
                    return Err(NodePrefixError::TooLong);
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

/// The number of nodes in the subtree of a node at `level` (1 to 7) of the
/// seven-level tree that one encoded byte walks.
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
        let check = |prefix: NodePrefix, bounds: (Vec<u8>, Option<Vec<u8>>), inside: &[Vec<u8>], outside: &[Vec<u8>]| {
            assert_eq!(prefix.address_bounds(), bounds, "{prefix:?}");
            for address in inside {
                assert!(admits(&prefix, address) && prefix.contains_address(address), "{prefix:?} holds {address:02x?}");
            }
            for address in outside {
                assert!(!admits(&prefix, address) && !prefix.contains_address(address), "{prefix:?} excludes {address:02x?}");
            }
        };
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

    /// The longest prefix, as long as the longest leaf address, encodes and
    /// decodes; one bit more is refused, whether decoded or built.
    #[test]
    fn prefixes_stop_at_the_longest_address() {
        let max = NodePrefix::MAX_BITS;
        assert_eq!(max, 33_024, "4096-byte keys and 32-byte entity ids");
        let longest = NodePrefix::of(&vec![0xFF; (max / 8) as usize], max);
        assert_eq!(NodePrefix::from_bytes(&longest.to_bytes()), Ok(longest));
        // A chunk of n zero bits encodes as n - 1, the place of its last node
        // in the walk, so these bytes spell runs of zero bits.
        let zeros = |n: u32| {
            let mut bytes = vec![6u8; (n / CHUNK) as usize];
            if !n.is_multiple_of(CHUNK) {
                bytes.push((n % CHUNK - 1) as u8);
            }
            bytes
        };
        assert_eq!(NodePrefix::from_bytes(&zeros(max)), Ok(NodePrefix::of(&vec![0; (max / 8) as usize], max)));
        assert_eq!(NodePrefix::from_bytes(&zeros(max + 1)), Err(NodePrefixError::TooLong));
        assert_eq!(NodePrefix::from_bytes(&zeros(max + CHUNK)), Err(NodePrefixError::TooLong));
    }

    #[test]
    #[should_panic(expected = "at most")]
    fn no_child_extends_the_longest_prefix() {
        NodePrefix::of(&vec![0; (NodePrefix::MAX_BITS / 8) as usize], NodePrefix::MAX_BITS).child(false);
    }

    #[test]
    #[should_panic(expected = "at most")]
    fn no_prefix_is_taken_past_the_longest() {
        NodePrefix::of(&vec![0; (NodePrefix::MAX_BITS / 8) as usize + 1], NodePrefix::MAX_BITS + 1);
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
