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
        let mut child = self.clone();
        child.push(bit);
        child
    }

    /// Extend this prefix by one more bit, as [`NodePrefix::child`] does, in
    /// place.
    fn push(&mut self, bit: bool) {
        let index = self.len;
        self.len = index.checked_add(1).expect("a prefix holds at most u32::MAX bits");
        if index.is_multiple_of(8) {
            self.bits.push(0);
        }
        if bit {
            self.bits[(index / 8) as usize] |= 0x80 >> (index % 8);
        }
    }

    /// Whether `other` is this prefix or lies beneath it.
    pub fn contains(&self, other: &NodePrefix) -> bool { other.len >= self.len && self.begins(&other.bits) }

    /// Whether `address` begins with this prefix.
    pub fn contains_address(&self, address: &[u8]) -> bool { address.len() * 8 >= self.len as usize && self.begins(address) }

    /// Whether `bits`, which hold at least as many bits as this prefix, begin
    /// with its bits.
    fn begins(&self, bits: &[u8]) -> bool {
        let whole = (self.len / 8) as usize;
        self.bits[..whole] == bits[..whole]
            && match self.len % 8 {
                0 => true,
                rest => {
                    let mask = 0xFFu8 << (8 - rest);
                    self.bits[whole] == bits[whole] & mask
                }
            }
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
    pub fn subtree_key_end(&self) -> Option<Vec<u8>> { self.subtree_keys().1 }

    /// The encoding of this prefix, where the keys of the subtree's rows
    /// begin, and [`NodePrefix::subtree_key_end`], where they end, made from
    /// one encoding.
    pub(crate) fn subtree_keys(&self) -> (Vec<u8>, Option<Vec<u8>>) {
        let encoding = self.to_bytes();
        if self.is_root() {
            return (encoding, None);
        }
        // The last byte is the place, in its chunk's walk, of the vertex this
        // prefix ends at, and the vertices of that vertex's subtree, every
        // prefix beneath this one within the chunk, follow it: the place past
        // them all ends every key beneath this prefix.
        let last_chunk = self.len - (self.len - 1) / CHUNK * CHUNK;
        let mut end = encoding.clone();
        *end.last_mut().expect("a prefix other than the root encodes as some bytes") += subtree_size(last_chunk) as u8;
        (encoding, Some(end))
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
                prefix.push(right);
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
mod tests;
