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
    pub fn child(&self, bit: bool) -> Self {
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

    #[test]
    fn address_bounds_hold_exactly_the_contained_addresses() {
        let mut rng = SmallRng::seed_from_u64(13);
        for _ in 0..200 {
            let prefix = random_prefix(&mut rng);
            let (start, end) = prefix.address_bounds();
            for _ in 0..50 {
                let address: Vec<u8> = (0..6).map(|_| rng.gen::<u8>() & 0xC3).collect();
                let inside = address >= start && end.as_ref().is_none_or(|end| address < *end);
                assert_eq!(inside, prefix.contains_address(&address), "{prefix:?} against {address:?}");
            }
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
