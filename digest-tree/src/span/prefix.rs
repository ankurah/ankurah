//! The names of tree nodes: the leading bits their addresses share.

use std::fmt;
use std::ops::Bound;

use super::Span;

/// A tree node's name: the leading bits its addresses share. Outside the crate
/// it is an opaque value inside span digests.
///
/// Prefixes order as their tree nodes are met walking the tree depth first: a
/// prefix comes before every prefix it contains, and everything it contains
/// comes before the next prefix outside it. So a subtree's nodes are one run
/// of keys in a store ordered by prefix, and its encoding keeps that order
/// in bytes.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodePrefix {
    /// The bits, packed most significant first; bits past `bits` are zero, so
    /// comparing the bytes, then the lengths, gives prefix order.
    bytes: Vec<u8>,

    /// How many bits the prefix holds.
    bits: u32,
}

/// Bits per encoded byte; see [`NodePrefix::to_bytes`].
const BITS_PER_ENCODED_BYTE: u32 = 7;

impl NodePrefix {
    /// The empty prefix, which names the root and contains every address.
    pub(crate) fn root() -> Self { Self { bytes: Vec::new(), bits: 0 } }

    /// The first `bits` bits of `address`.
    ///
    /// # Panics
    /// When `address` holds fewer than `bits` bits.
    pub(crate) fn of(address: &[u8], bits: u32) -> Self {
        assert!(bits as usize <= address.len() * 8, "a {}-byte address has no {bits}-bit prefix", address.len());
        let mut bytes = address[..(bits as usize).div_ceil(8)].to_vec();

        if !bits.is_multiple_of(8) {
            *bytes.last_mut().expect("a partial byte exists") &= 0xFF << (8 - bits % 8);
        }

        Self { bytes, bits }
    }

    /// How many bits the prefix holds: the depth of the tree node it names.
    pub(crate) fn bit_len(&self) -> u32 { self.bits }

    /// The bit at `index`, counting from the most significant.
    ///
    /// # Panics
    /// When `index` is not below [`NodePrefix::bit_len`].
    pub(crate) fn bit(&self, index: u32) -> bool {
        assert!(index < self.bits, "bit {index} of a {}-bit prefix", self.bits);
        self.bytes[(index / 8) as usize] & (0x80 >> (index % 8)) != 0
    }

    /// This prefix followed by one more bit.
    pub(crate) fn child(&self, bit: bool) -> Self {
        let mut child = self.clone();
        child.push(bit);
        child
    }

    /// The prefix that differs from this one in its last bit alone: the other
    /// child of the same branch. The root has none.
    pub(crate) fn sibling(&self) -> Option<Self> {
        let last = self.bits.checked_sub(1)?;
        let mut sibling = self.clone();
        sibling.bytes[(last / 8) as usize] ^= 0x80 >> (last % 8);

        Some(sibling)
    }

    /// Extend this prefix by one more bit, in place.
    fn push(&mut self, bit: bool) {
        let index = self.bits;
        self.bits = index.checked_add(1).expect("a prefix holds at most u32::MAX bits");

        if index.is_multiple_of(8) {
            self.bytes.push(0);
        }

        if bit {
            self.bytes[(index / 8) as usize] |= 0x80 >> (index % 8);
        }
    }

    /// Whether `other` is this prefix or lies beneath it.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn contains(&self, other: &NodePrefix) -> bool { other.bits >= self.bits && self.begins(&other.bytes) }

    /// Whether `address` begins with this prefix.
    pub(crate) fn contains_address(&self, address: &[u8]) -> bool { address.len() * 8 >= self.bits as usize && self.begins(address) }

    /// Whether `bytes`, which hold at least as many bits as this prefix, begin
    /// with its bits.
    fn begins(&self, bytes: &[u8]) -> bool {
        let whole = (self.bits / 8) as usize;
        self.bytes[..whole] == bytes[..whole]
            && match self.bits % 8 {
                0 => true,
                rest => self.bytes[whole] == bytes[whole] & (0xFFu8 << (8 - rest)),
            }
    }

    /// The first prefix, in prefix order, after every prefix this one
    /// contains; `None` when nothing follows, as for the root.
    pub(crate) fn after_subtree(&self) -> Option<NodePrefix> {
        let mut bits = self.bits;
        while bits > 0 && self.bit(bits - 1) {
            bits -= 1;
        }

        (bits > 0).then(|| Self::of(&self.bytes, bits - 1).child(true))
    }

    /// The addresses beneath this prefix: from the least address that begins
    /// with its bits up to the first that lies past them all.
    pub(crate) fn span(&self) -> Span {
        if self.bits == 0 {
            return Span::all();
        }

        let start = Bound::Included(self.bytes.clone());

        // Add one at the prefix's last bit, carrying toward the front; a carry
        // out of the first byte means no address follows.
        let mut end = self.bytes.clone();
        let last = self.bits - 1;
        let mut index = (last / 8) as usize;
        let (sum, mut carry) = end[index].overflowing_add(0x80 >> (last % 8));
        end[index] = sum;

        while carry {
            if index == 0 {
                return Span::new(start, Bound::Unbounded);
            }

            index -= 1;
            (end[index], carry) = end[index].overflowing_add(1);
        }

        // A carry leaves zero bytes at the end, and they must go: for the
        // prefix 0000 0000 1 the bound 01 00 would admit the one-byte address
        // 01, too short to hold the prefix yet below the bound. The bound 01
        // still lies past every address beneath the prefix.
        while end.last() == Some(&0) {
            end.pop();
        }

        Span::new(start, Bound::Excluded(end))
    }

    /// Encode the prefix so that byte order is prefix order.
    ///
    /// Each byte stands for up to seven bits: it names one vertex of a binary
    /// tree seven levels deep by the vertex's place in a depth-first walk
    /// (0 to 253). Every chunk but the last is seven bits long, so a prefix
    /// of `n` bits takes `n / 7` bytes, rounded up. Because a depth-first walk
    /// meets a vertex before its descendants and the left subtree before the
    /// right, comparing these bytes compares the prefixes in prefix order.
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.bits.div_ceil(BITS_PER_ENCODED_BYTE) as usize);
        let mut start = 0;

        while start < self.bits {
            let depth = (self.bits - start).min(BITS_PER_ENCODED_BYTE);
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

    /// Decode [`NodePrefix::to_bytes`], or say why the bytes name no prefix.
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        // Each byte holds up to seven bits, and a prefix counts its bits in a
        // `u32`; a longer encoding comes from a peer, never from a prefix.
        if bytes.len() > (u32::MAX / BITS_PER_ENCODED_BYTE) as usize {
            return Err("more bytes than a prefix's bits can number");
        }

        let mut prefix = Self::root();
        for (position, &byte) in bytes.iter().enumerate() {
            if u32::from(byte) >= 2 * subtree_size(1) {
                return Err("a byte past the places of a seven-level walk");
            }

            let mut place = u32::from(byte);
            let mut level = 0;

            loop {
                level += 1;
                let right = place >= subtree_size(level);

                if right {
                    place -= subtree_size(level);
                }

                prefix.push(right);

                if place == 0 {
                    break;
                }

                place -= 1;
            }

            if level < BITS_PER_ENCODED_BYTE && position + 1 < bytes.len() {
                return Err("a chunk shorter than seven bits before the last");
            }
        }

        Ok(prefix)
    }
}

/// The number of vertices in the subtree of a vertex at `level` (1 to 7) of
/// the seven-level tree that one encoded byte walks.
fn subtree_size(level: u32) -> u32 { (1 << (BITS_PER_ENCODED_BYTE + 1 - level)) - 1 }

impl fmt::Debug for NodePrefix {
    /// The bits, as zeros and ones.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodePrefix(")?;
        (0..self.bits).try_for_each(|index| write!(f, "{}", u8::from(self.bit(index))))?;
        write!(f, ")")
    }
}

#[cfg(test)]
mod tests;
