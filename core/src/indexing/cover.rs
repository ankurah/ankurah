//! The cover of a range of one index's keys.
//!
//! Two members compare an interest through the digests of the blocks of its cover: the
//! canonical list of aligned blocks of the key bits that tiles the range exactly. The list
//! follows from the index, the canonical key encoding and the range bounds alone, which is what
//! a session names. [`Cover::new`] takes no tree,
//! so two members derive the same blocks whatever depth, fan-out or stored nodes their trees
//! have, and each then answers every block from its own rows or leaves. Which index and which
//! range serve a Selection is decided beside the planner, in ankurah-storage-common.
//!
//! # Addresses and blocks
//!
//! A tree files each entity under its address: the canonical key, which is the entity's key
//! part values encoded by [`encode_component_typed`] in the types and directions of the
//! index's KeySpec and concatenated, followed by the 32-byte entity id. An entity-id index, of
//! every entity or of one component's members, has no key parts, so there the address is the
//! entity id itself. A [`Block`] is every address
//! whose bits start with one bit string, stated as bytes and a bit length. Reading an address
//! as the binary fraction 0.b1b2b3..., the block of an n-bit string s is the interval
//! [0.s, 0.s + 2^-n), and a range of keys is an interval of addresses.
//!
//! # From bounds to an interval of addresses
//!
//! A [`KeyRange`] fixes the leading key parts to the values of its prefix, whose encodings
//! concatenate to the bytes P, and bounds the part after them: the next key part, or, once the
//! prefix fixes every key part, the entity id that ends every address, which is how a range of
//! an entity-id index bounds the entity id. The entity id's 32 bytes are what an ascending
//! entity-id key part would encode, so it bounds like one while the address holds it once.
//! For encoded bytes c, the first address under c is the fraction 0.c; past every address
//! under c is c with its last byte that is not 0xFF incremented and the bytes after it
//! dropped, or the end of the address space when c is empty or all 0xFF. With v the encoding
//! of a bound's value x and Pv the bytes P followed by v, on a key part whose direction is
//! ascending:
//!
//! | bound               | the interval of addresses                 |
//! |---------------------|-------------------------------------------|
//! | lower `Included(x)` | starts at the first address under Pv      |
//! | lower `Excluded(x)` | starts past every address under Pv        |
//! | lower `Unbounded`   | starts at the first address under P       |
//! | upper `Included(x)` | ends past every address under Pv          |
//! | upper `Excluded(x)` | ends at the first address under Pv        |
//! | upper `Unbounded`   | ends past every address under P           |
//!
//! A descending key part's encoding reverses the order of its values, so there the upper
//! bound of the values starts the interval and the lower bound ends it, each by the row of
//! the table for the opposite side. With both bounds open the interval is the block of P, so
//! a prefix match is one block, and the open range of an entity-id index is the root block.
//!
//! # NaN
//!
//! NaN equals nothing and satisfies no comparison, yet the encoder files it above every
//! number, +∞ included. A range whose prefix or bound holds NaN therefore names no key, and its
//! cover is empty. A range that bounds a float part below and leaves it open above stops at +∞,
//! inclusive, instead of running on past NaN: on an ascending part the interval ends past the
//! addresses of +∞, and on a descending part it starts at them. Every other open side already
//! ends short of NaN, and a float part open on both sides is not constrained at all, so its
//! range holds NaN with every other value.
//!
//! # Terminators
//!
//! The table treats Pv as whole encoded values, terminators included. Fixed-width values
//! (integers, floats, booleans, entity ids) have no terminator, and a descending string or
//! byte string ends with 0xFF 0xFF, an inner 0xFF being escaped as 0xFF 0x00. Those encodings
//! are prefix-free: no value's encoding begins another's. Every address holds all of its key
//! parts and the entity id after them, so under prefix-free encodings no address is a proper
//! prefix of Pv; each address then lies wholly inside or wholly outside every block, and
//! inside the interval exactly when its key satisfies the bounds. An ascending string, byte
//! string or object ends with one 0x00, an inner 0x00 being escaped as 0x00 0xFF, so the
//! encoding of "a" begins the encoding of every value that starts with "a" and a NUL, and
//! those continue with the same bytes as the address of an entity filed under "a" whose next
//! key part or entity id begins with 0xFF: no block separates them. A JSON string has the same
//! flaw in either direction. [`Cover::new`] refuses a range that rests on such a key part
//! ([`RangeError::AmbiguousEncoding`]) until the encoding of those values is made prefix-free.
//!
//! # The decomposition
//!
//! The cover of an interval is the minimal list of blocks whose union is exactly the
//! interval, in address order. Where the interval's start and end first differ, at bit k, the
//! start has a 0 and the end a 1. The start contributes the block of its bits up to its last 1
//! bit after bit k (or of its first k + 1 bits when it has none), then, deepest first, at each
//! of its 0 bits after bit k and before that last 1 bit, the block of its bits up to that bit
//! with that bit set. The end contributes, at each of its 1 bits after bit k, the block of its
//! bits up to that bit with that bit cleared. An interval that runs to the end of the address
//! space takes the start's blocks from bit 0. That is at most one block per bit on each side.

use std::ops::Bound;

use ankurah_proto::PropertyId;
use thiserror::Error;

use super::encoding::{encode_component_typed, encodes_alike, KeyEncoding};
use super::key_spec::{IndexDirection, IndexKeyPart};
use crate::storage::tree::HashedIndex;
use crate::value::{Value, ValueType};

/// A range of one index's keys: the leading key parts fixed to the values of `prefix`, and the
/// part after them bounded on each side in the order of its values, whatever its direction.
/// That part is the next key part or, once `prefix` fixes every key part, the entity id that
/// ends every address; a range of an entity-id index bounds the entity id.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyRange {
    pub prefix: Vec<Value>,
    pub lower: Bound<Value>,
    pub upper: Bound<Value>,
}

impl KeyRange {
    /// Every key whose leading parts are `prefix`.
    pub fn prefix(prefix: Vec<Value>) -> Self { Self { prefix, lower: Bound::Unbounded, upper: Bound::Unbounded } }
}

/// Every address whose bits start with one bit string.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Block {
    /// The bit string, most significant bit first, in as few bytes as hold it; the bits of the
    /// last byte past `bit_len` are zero, so equal blocks have equal bytes.
    bytes: Vec<u8>,
    bit_len: usize,
}

impl Block {
    /// Every address.
    pub fn root() -> Self { Self { bytes: Vec::new(), bit_len: 0 } }

    /// The block of the first `bit_len` bits of `bits`, reading bits past its end as zero.
    pub fn new(bits: &[u8], bit_len: usize) -> Self {
        let mut bytes: Vec<u8> = bits.iter().copied().chain(std::iter::repeat(0)).take(bit_len.div_ceil(8)).collect();
        let partial = bit_len % 8;
        if partial != 0 {
            if let Some(last) = bytes.last_mut() {
                *last &= 0xFF << (8 - partial);
            }
        }
        Self { bytes, bit_len }
    }

    pub fn bytes(&self) -> &[u8] { &self.bytes }

    pub fn bit_len(&self) -> usize { self.bit_len }

    /// Whether `address` starts with this block's bits.
    pub fn contains(&self, address: &[u8]) -> bool { address.len() * 8 >= self.bit_len && Block::new(address, self.bit_len) == *self }

    /// The block that shares this block's parent: the same bits with the last one inverted.
    fn sibling(mut self) -> Self {
        let last = self.bit_len - 1;
        self.bytes[last / 8] ^= 0x80 >> (last % 8);
        self
    }
}

/// The cover of a range of one index's keys: the index, the key encoding and the bounds a
/// session names, and the blocks both members derive from them.
#[derive(Debug, Clone, PartialEq)]
pub struct Cover {
    index: HashedIndex,
    encoding: KeyEncoding,
    range: KeyRange,
    blocks: Vec<Block>,
}

impl Cover {
    /// The full replica's cover: every key of the entity-id index, one block at the root.
    pub fn root() -> Self {
        Self { index: HashedIndex::EntityId, encoding: KeyEncoding::V1, range: KeyRange::prefix(Vec::new()), blocks: vec![Block::root()] }
    }

    /// Decompose `range` of `index` into its blocks, refusing a range whose blocks would not
    /// hold exactly the entities the range names.
    pub fn new(index: HashedIndex, range: KeyRange) -> Result<Self, RangeError> {
        let (start, end) = range.interval(key_parts(&index))?;
        let blocks = if range.names_nan() { Vec::new() } else { tile(&start, &end) };
        Ok(Self { blocks, index, encoding: KeyEncoding::V1, range })
    }

    pub fn index(&self) -> &HashedIndex { &self.index }

    pub fn encoding(&self) -> KeyEncoding { self.encoding }

    pub fn range(&self) -> &KeyRange { &self.range }

    /// The blocks, in address order.
    pub fn blocks(&self) -> &[Block] { &self.blocks }
}

/// The key parts under which `index` files an entity, before its entity id.
fn key_parts(index: &HashedIndex) -> &[IndexKeyPart<PropertyId>] {
    match index {
        HashedIndex::EntityId => &[],
        HashedIndex::Component { key_spec, .. } => &key_spec.keyparts,
    }
}

/// Why a range of an index has no exact cover. Parts are numbered in address order: the key
/// parts from 0, then the entity id.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum RangeError {
    #[error("the range constrains key part {part}, but the index has {parts} key parts")]
    NoSuchPart { part: usize, parts: usize },
    #[error("part {part} holds {expected:?} values, which cannot represent this {found:?} value")]
    TypeMismatch { part: usize, expected: ValueType, found: ValueType },
    #[error("key part {part} encodes {value_type:?} values {direction:?} with a terminator that can begin another value's encoding")]
    AmbiguousEncoding { part: usize, value_type: ValueType, direction: IndexDirection },
}

impl KeyRange {
    /// The interval of addresses this range holds in an index keyed by `parts`, by the rules in
    /// the module documentation.
    fn interval(&self, parts: &[IndexKeyPart<PropertyId>]) -> Result<(Point, Point), RangeError> {
        let bounded = self.prefix.len();
        if bounded > parts.len() {
            return Err(RangeError::NoSuchPart { part: parts.len(), parts: parts.len() });
        }
        let mut fixed = Vec::new();
        for (index, (value, part)) in self.prefix.iter().zip(parts).enumerate() {
            fixed.extend(encode(index, part, value)?);
        }
        // The part after the prefix: the next key part, or the entity id once every key part is fixed.
        let part = parts.get(bounded).unwrap_or(&ENTITY_ID);
        let upper = self.upper_excluding_nan(part);
        // In address order: a descending part's upper value bound starts the interval.
        let (from, to) = if part.direction.is_desc() { (&upper, &self.lower) } else { (&self.lower, &upper) };
        let under = |value: &Value| -> Result<Vec<u8>, RangeError> { Ok([fixed.as_slice(), &encode(bounded, part, value)?].concat()) };
        let start = match from {
            Bound::Unbounded => Point::first_under(&fixed),
            Bound::Included(value) => Point::first_under(&under(value)?),
            Bound::Excluded(value) => Point::past(&under(value)?),
        };
        let end = match to {
            Bound::Unbounded => Point::past(&fixed),
            Bound::Included(value) => Point::past(&under(value)?),
            Bound::Excluded(value) => Point::first_under(&under(value)?),
        };
        Ok((start, end))
    }

    /// The upper bound of `part`, except that a float part bounded below and open above stops
    /// at +∞, short of the NaN the encoder files above every number.
    fn upper_excluding_nan(&self, part: &IndexKeyPart<PropertyId>) -> Bound<Value> {
        match (&self.lower, &self.upper) {
            (Bound::Included(_) | Bound::Excluded(_), Bound::Unbounded) if part.value_type == ValueType::F64 => {
                Bound::Included(Value::F64(f64::INFINITY))
            }
            _ => self.upper.clone(),
        }
    }

    /// Whether a prefix or bound value is NaN, which equals nothing and satisfies no comparison.
    fn names_nan(&self) -> bool {
        let bounds = [&self.lower, &self.upper].into_iter().filter_map(|bound| match bound {
            Bound::Included(value) | Bound::Excluded(value) => Some(value),
            Bound::Unbounded => None,
        });
        self.prefix.iter().chain(bounds).any(|value| matches!(value, Value::F64(number) if number.is_nan()))
    }
}

/// The entity id that ends every address, as the key part whose encoding its bytes are.
static ENTITY_ID: IndexKeyPart<PropertyId> = IndexKeyPart {
    key: PropertyId::Id,
    sub_path: None,
    direction: IndexDirection::Asc,
    value_type: ValueType::EntityId,
    nulls: None,
    collation: None,
};

/// The canonical encoding of `value` as key part number `index`. Refused when the part's
/// encoding is not prefix-free or the value is not of the part's type; integer widths share one
/// encoding, so an integer of any width may bound an integer part, and the encoder refuses one
/// that does not fit.
fn encode(index: usize, part: &IndexKeyPart<PropertyId>, value: &Value) -> Result<Vec<u8>, RangeError> {
    if !is_prefix_free(part.value_type, part.direction) {
        return Err(RangeError::AmbiguousEncoding { part: index, value_type: part.value_type, direction: part.direction });
    }
    let found = ValueType::of(value);
    let mismatch = RangeError::TypeMismatch { part: index, expected: part.value_type, found };
    if !encodes_alike(found, part.value_type) {
        return Err(mismatch);
    }
    encode_component_typed(value, part.value_type, part.direction.is_desc()).map_err(|_| mismatch)
}

/// Whether no value's encoding begins another value's encoding. When the canonical encoding
/// makes every value prefix-free, this check and [`RangeError::AmbiguousEncoding`] go away.
fn is_prefix_free(value_type: ValueType, direction: IndexDirection) -> bool {
    match value_type {
        ValueType::I16 | ValueType::I32 | ValueType::I64 | ValueType::F64 | ValueType::Bool | ValueType::EntityId => true,
        ValueType::String | ValueType::Binary | ValueType::Object => direction.is_desc(),
        ValueType::Json => false,
    }
}

/// A point of the address space, read as a binary fraction.
#[derive(Debug, Clone, PartialEq)]
enum Point {
    /// The fraction 0.b1b2b3... of these bytes; trailing zero bytes do not move it.
    At(Vec<u8>),
    /// 1: past every address.
    End,
}

impl Point {
    /// The first address under the encoded bytes `code`.
    fn first_under(code: &[u8]) -> Self { Point::At(code.to_vec()) }

    /// The point just past every address under the encoded bytes `code`.
    fn past(code: &[u8]) -> Self {
        match code.iter().rposition(|&byte| byte != 0xFF) {
            Some(last) => {
                let mut next = code[..=last].to_vec();
                next[last] += 1;
                Point::At(next)
            }
            None => Point::End,
        }
    }
}

/// The minimal list of blocks, in address order, whose union is exactly [start, end).
fn tile(start: &Point, end: &Point) -> Vec<Block> {
    let Point::At(start) = start else { return Vec::new() };
    match end {
        Point::End => blocks_from(start, 0),
        Point::At(end) => match first_difference(start, end) {
            Some(split) if !bit(start, split) => {
                let mut blocks = blocks_from(start, split + 1);
                blocks.extend(blocks_until(end, split + 1));
                blocks
            }
            // Equal points, or a start past the end: an empty interval.
            _ => Vec::new(),
        },
    }
}

/// The blocks tiling [start, the end of the `depth`-bit block that holds start).
fn blocks_from(start: &[u8], depth: usize) -> Vec<Block> {
    let Some(last) = last_one(start).filter(|&last| last >= depth) else { return vec![Block::new(start, depth)] };
    let mut blocks = vec![Block::new(start, last + 1)];
    blocks.extend((depth..last).rev().filter(|&index| !bit(start, index)).map(|index| Block::new(start, index + 1).sibling()));
    blocks
}

/// The blocks tiling [the start of the `depth`-bit block that holds end, end).
fn blocks_until(end: &[u8], depth: usize) -> Vec<Block> {
    let Some(last) = last_one(end).filter(|&last| last >= depth) else { return Vec::new() };
    (depth..=last).filter(|&index| bit(end, index)).map(|index| Block::new(end, index + 1).sibling()).collect()
}

/// Bit `index` of `bytes`, most significant first, reading bits past the end as zero.
fn bit(bytes: &[u8], index: usize) -> bool { bytes.get(index / 8).is_some_and(|byte| byte & (0x80 >> (index % 8)) != 0) }

/// The index of the last 1 bit of `bytes`.
fn last_one(bytes: &[u8]) -> Option<usize> {
    let at = bytes.iter().rposition(|&byte| byte != 0)?;
    Some(at * 8 + 7 - bytes[at].trailing_zeros() as usize)
}

/// The index of the first bit at which two fractions differ.
fn first_difference(a: &[u8], b: &[u8]) -> Option<usize> {
    (0..a.len().max(b.len())).find_map(|at| {
        let differing = a.get(at).copied().unwrap_or(0) ^ b.get(at).copied().unwrap_or(0);
        (differing != 0).then(|| at * 8 + differing.leading_zeros() as usize)
    })
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use ankql::ast::{ComparisonOperator, Expr, Predicate, PropertyPath, Resolved};
    use ankurah_proto::{EntityId, ModelId};
    use rand::{rngs::SmallRng, Rng, SeedableRng};

    use super::*;
    use crate::indexing::KeySpec;
    use crate::selection::filter::{evaluate_predicate, Filterable};

    fn property(name: &str) -> PropertyId {
        let mut bytes = [0u8; 32];
        bytes[..name.len()].copy_from_slice(name.as_bytes());
        PropertyId::EntityId(EntityId::from_bytes(bytes))
    }

    fn asc(name: &str, value_type: ValueType) -> IndexKeyPart<PropertyId> { IndexKeyPart::asc(property(name), value_type) }

    fn desc(name: &str, value_type: ValueType) -> IndexKeyPart<PropertyId> { IndexKeyPart::desc(property(name), value_type) }

    fn component() -> ModelId { ModelId::EntityId(EntityId::from_bytes([7; 32])) }

    /// An index of the members of one component.
    fn index(parts: Vec<IndexKeyPart<PropertyId>>) -> HashedIndex {
        HashedIndex::Component { component: component(), key_spec: KeySpec::new(parts) }
    }

    fn range(prefix: Vec<Value>, lower: Bound<Value>, upper: Bound<Value>) -> KeyRange { KeyRange { prefix, lower, upper } }

    fn entity(first: u8, rest: u8) -> EntityId {
        let mut bytes = [rest; 32];
        bytes[0] = first;
        EntityId::from_bytes(bytes)
    }

    /// The extreme entity ids, their neighbours, and the two ids either side of the top bit:
    /// the ids after every row's key, and the bounds of ranges on the entity id.
    fn entities() -> Vec<EntityId> {
        let ending = |rest: u8, last: u8| {
            let mut bytes = [rest; 32];
            bytes[31] = last;
            EntityId::from_bytes(bytes)
        };
        vec![entity(0x00, 0x00), ending(0x00, 0x01), entity(0x7F, 0xFF), entity(0x80, 0x00), ending(0xFF, 0xFE), entity(0xFF, 0xFF)]
    }

    // ---- checking a list of blocks against the interval it should tile ----

    /// Points compared as fractions: bytes padded with zeros to one length compare in order.
    fn compare(a: &Point, b: &Point) -> Ordering {
        match (a, b) {
            (Point::End, Point::End) => Ordering::Equal,
            (Point::End, Point::At(_)) => Ordering::Greater,
            (Point::At(_), Point::End) => Ordering::Less,
            (Point::At(a), Point::At(b)) => {
                let len = a.len().max(b.len());
                let padded = |bytes: &[u8]| bytes.iter().copied().chain(std::iter::repeat(0)).take(len).collect::<Vec<u8>>();
                padded(a).cmp(&padded(b))
            }
        }
    }

    fn start_of(block: &Block) -> Point { Point::At(block.bytes.clone()) }

    /// The block's start plus 2^-bit_len, carrying.
    fn end_of(block: &Block) -> Point {
        let mut bytes = block.bytes.clone();
        for index in (0..block.bit_len).rev() {
            let mask = 0x80 >> (index % 8);
            bytes[index / 8] ^= mask;
            if bytes[index / 8] & mask != 0 {
                return Point::At(bytes);
            }
        }
        Point::End
    }

    /// The blocks are adjacent, in order, start at `start`, end at `end`, and none could be
    /// replaced by its parent: the canonical minimal tiling, at most two blocks per bit.
    fn assert_tiles(start: &Point, end: &Point, blocks: &[Block]) {
        if compare(start, end) != Ordering::Less {
            assert!(blocks.is_empty(), "{start:?}..{end:?} is empty but tiled by {blocks:?}");
            return;
        }
        let (first, last) = (blocks.first().expect("a nonempty interval has blocks"), blocks.last().unwrap());
        assert_eq!(compare(&start_of(first), start), Ordering::Equal, "{start:?}..{end:?}: {blocks:?}");
        assert_eq!(compare(&end_of(last), end), Ordering::Equal, "{start:?}..{end:?}: {blocks:?}");
        for pair in blocks.windows(2) {
            assert_eq!(compare(&end_of(&pair[0]), &start_of(&pair[1])), Ordering::Equal, "{start:?}..{end:?}: {blocks:?}");
        }
        for block in blocks.iter().filter(|block| block.bit_len > 0) {
            let parent = Block::new(&block.bytes, block.bit_len - 1);
            let parent_fits = compare(&start_of(&parent), start) != Ordering::Less && compare(&end_of(&parent), end) != Ordering::Greater;
            assert!(!parent_fits, "{start:?}..{end:?}: {block:?} should be merged into {parent:?}");
        }
        let bits = |point: &Point| match point {
            Point::At(bytes) => bytes.len() * 8,
            Point::End => 0,
        };
        assert!(blocks.len() <= 2 * bits(start).max(bits(end)).max(1), "{start:?}..{end:?}: {} blocks", blocks.len());
    }

    #[test]
    fn every_interval_between_one_byte_points_is_tiled_exactly_and_minimally() {
        let ends = (0..=255u8).map(|byte| Point::At(vec![byte])).chain([Point::End]).collect::<Vec<_>>();
        for start in 0..=255u8 {
            let start = Point::At(vec![start]);
            for end in &ends {
                assert_tiles(&start, end, &tile(&start, end));
            }
        }
    }

    #[test]
    fn random_intervals_between_long_points_are_tiled_exactly_and_minimally() {
        let mut rng = SmallRng::seed_from_u64(0x5eed);
        let point = |rng: &mut SmallRng| {
            let len = rng.gen_range(0..=6);
            // Runs of 0x00 and 0xFF are where carries and terminators meet.
            let bytes = (0..len).map(|_| match rng.gen_range(0..4) {
                0 => 0x00,
                1 => 0xFF,
                _ => rng.gen(),
            });
            Point::At(bytes.collect())
        };
        for case in 0..20_000 {
            let start = point(&mut rng);
            let end = if case % 10 == 0 { Point::End } else { point(&mut rng) };
            assert_tiles(&start, &end, &tile(&start, &end));
        }
    }

    // ---- covers of key ranges, checked row by row ----

    fn address(index: &HashedIndex, values: &[Value], entity: &EntityId) -> Vec<u8> {
        let mut address = Vec::new();
        for (value, part) in values.iter().zip(key_parts(index)) {
            address.extend(encode_component_typed(value, part.value_type, part.direction.is_desc()).unwrap());
        }
        address.extend(entity.to_bytes());
        address
    }

    /// One entity as an index files it: its value for each key part, and its entity id.
    struct Row<'a> {
        parts: &'a [IndexKeyPart<PropertyId>],
        values: &'a [Value],
        entity: EntityId,
    }

    impl Filterable for Row<'_> {
        fn value(&self, property: &PropertyId) -> Option<Value> {
            if *property == PropertyId::Id {
                return Some(Value::EntityId(self.entity));
            }
            let at = self.parts.iter().position(|part| part.key == *property)?;
            Some(self.values[at].clone())
        }
    }

    /// The predicate a Selection would state for the range: each prefix value equal to its key
    /// part's value, and the bounded part, a key part or the entity id, within the bounds.
    fn predicate(parts: &[IndexKeyPart<PropertyId>], range: &KeyRange) -> Predicate<Resolved> {
        let compare = |key: PropertyId, operator, value: &Value| Predicate::Comparison {
            left: Box::new(Expr::Path(PropertyPath::from(key))),
            operator,
            right: Box::new(Expr::Literal(value.clone())),
        };
        let mut conjuncts: Vec<_> =
            parts.iter().zip(&range.prefix).map(|(part, value)| compare(part.key, ComparisonOperator::Equal, value)).collect();
        let bounded = parts.get(range.prefix.len()).map_or(PropertyId::Id, |part| part.key);
        match &range.lower {
            Bound::Included(value) => conjuncts.push(compare(bounded, ComparisonOperator::GreaterThanOrEqual, value)),
            Bound::Excluded(value) => conjuncts.push(compare(bounded, ComparisonOperator::GreaterThan, value)),
            Bound::Unbounded => {}
        }
        match &range.upper {
            Bound::Included(value) => conjuncts.push(compare(bounded, ComparisonOperator::LessThanOrEqual, value)),
            Bound::Excluded(value) => conjuncts.push(compare(bounded, ComparisonOperator::LessThan, value)),
            Bound::Unbounded => {}
        }
        conjuncts.into_iter().fold(Predicate::True, |all, conjunct| Predicate::And(Box::new(all), Box::new(conjunct)))
    }

    /// Every row lies in at most one block of the cover, and in one exactly when Selection
    /// evaluation of the range's predicate admits it. Each row is tried with every id of
    /// [`entities`]; those starting 0x00 and 0xFF sit right after the key's last byte.
    fn assert_exact(index: &HashedIndex, range: &KeyRange, rows: &[Vec<Value>]) {
        let cover = Cover::new(index.clone(), range.clone()).unwrap_or_else(|error| panic!("{range:?}: {error}"));
        let parts = key_parts(index);
        let predicate = predicate(parts, range);
        for values in rows {
            for entity in entities() {
                let address = address(index, values, &entity);
                let holding = cover.blocks().iter().filter(|block| block.contains(&address)).count();
                assert!(holding <= 1, "{range:?}: row {values:?} lies in {holding} blocks");
                let admitted = evaluate_predicate(&Row { parts, values, entity }, &predicate).unwrap();
                assert_eq!(holding == 1, admitted, "{range:?}: row {values:?}, cover {:?}", cover.blocks());
            }
        }
    }

    /// Every combination of an open, inclusive or exclusive bound on each side, for every pair
    /// of bound values from `values`, inverted pairs (empty ranges) included.
    fn every_range(prefix: &[Value], values: &[Value]) -> Vec<KeyRange> {
        let mut ranges = Vec::new();
        for low in values {
            for high in values {
                for lower in [Bound::Unbounded, Bound::Included(low.clone()), Bound::Excluded(low.clone())] {
                    for upper in [Bound::Unbounded, Bound::Included(high.clone()), Bound::Excluded(high.clone())] {
                        ranges.push(range(prefix.to_vec(), lower.clone(), upper));
                    }
                }
            }
        }
        ranges
    }

    fn integers() -> Vec<Value> { [i64::MIN, -2, -1, 0, 1, 2, 5, 6, 255, 256, i64::MAX].map(Value::I64).to_vec() }

    /// NUL, whose descending byte is escaped, and the bytes beside it, shared prefixes, and
    /// two-, three- and four-byte UTF-8 up to the last code point.
    fn descending_strings() -> Vec<Value> {
        [
            "",
            "\0",
            "\0\0",
            "\0\u{1}",
            "\u{1}",
            "\u{1}\0",
            "a",
            "a\0",
            "a\0b",
            "a\u{1}",
            "ab",
            "b",
            "\u{7f}",
            "\u{e9}",
            "\u{20ac}",
            "\u{ffff}",
            "\u{10000}",
            "\u{1f600}",
            "\u{10ffff}",
        ]
        .map(|text| Value::String(text.to_owned()))
        .to_vec()
    }

    /// The empty value, NUL and 0xFF, whose descending encodings are the terminator and an
    /// escaped byte, embedded NULs, and shared prefixes.
    fn descending_bytes() -> Vec<Vec<u8>> {
        vec![
            vec![],
            vec![0x00],
            vec![0x00, 0x00],
            vec![0x00, 0xFF],
            vec![0x01],
            vec![0x61],
            vec![0x61, 0x00],
            vec![0x61, 0x00, 0x62],
            vec![0x61, 0x62],
            vec![0xFE],
            vec![0xFF],
            vec![0xFF, 0x00],
            vec![0xFF, 0xFF],
        ]
    }

    /// Group ids adjacent in byte order, and the two extremes.
    fn groups() -> Vec<Value> {
        [entity(0x10, 0x00), entity(0x10, 0x01), entity(0x0F, 0xFF), entity(0x00, 0x00), entity(0xFF, 0xFF)].map(Value::EntityId).to_vec()
    }

    #[test]
    fn a_range_on_an_ascending_integer_holds_exactly_the_rows_inside_it() {
        let index = index(vec![asc("score", ValueType::I64)]);
        let rows = integers().into_iter().map(|value| vec![value]).collect::<Vec<_>>();
        for range in every_range(&[], &integers()) {
            assert_exact(&index, &range, &rows);
        }
    }

    #[test]
    fn a_range_on_a_descending_integer_holds_exactly_the_rows_inside_it() {
        let index = index(vec![desc("score", ValueType::I64)]);
        let rows = integers().into_iter().map(|value| vec![value]).collect::<Vec<_>>();
        for range in every_range(&[], &integers()) {
            assert_exact(&index, &range, &rows);
        }
    }

    #[test]
    fn a_range_on_a_descending_string_holds_exactly_the_rows_inside_it() {
        // NUL is the byte whose descending encoding is escaped, beside the 0xFF 0xFF terminator.
        let index = index(vec![desc("name", ValueType::String)]);
        let rows = descending_strings().into_iter().map(|value| vec![value]).collect::<Vec<_>>();
        for range in every_range(&[], &descending_strings()) {
            assert_exact(&index, &range, &rows);
        }
    }

    #[test]
    fn a_range_on_descending_binary_or_object_values_holds_exactly_the_rows_inside_it() {
        for (value_type, wrap) in [(ValueType::Binary, Value::Binary as fn(Vec<u8>) -> Value), (ValueType::Object, Value::Object)] {
            let index = index(vec![desc("blob", value_type)]);
            let values = descending_bytes().into_iter().map(wrap).collect::<Vec<_>>();
            let rows = values.iter().map(|value| vec![value.clone()]).collect::<Vec<_>>();
            for range in every_range(&[], &values) {
                assert_exact(&index, &range, &rows);
            }
        }
    }

    /// Integers at the limits of each width, as rows and as bounds: a bound narrower than its
    /// part widens to it, a wider one narrows when it fits, and one that does not fit is
    /// refused rather than clamped.
    #[test]
    fn ranges_on_integers_hold_exactly_the_rows_inside_them_at_the_limits_of_every_width() {
        let i16s = [i16::MIN, i16::MIN + 1, -1, 0, 1, i16::MAX - 1, i16::MAX];
        let i32s = [i32::MIN, i32::MIN + 1, -1, 0, 1, i32::MAX - 1, i32::MAX];
        let beside_i16 = [i16::MIN as i64 - 1, i16::MIN.into(), i16::MAX.into(), i16::MAX as i64 + 1];
        let beside_i32 = [i32::MIN as i64 - 1, i32::MIN.into(), i32::MAX.into(), i32::MAX as i64 + 1];
        let narrow = i16s.map(Value::I16);
        let cases = [
            (
                ValueType::I16,
                narrow.to_vec(),
                i16s.iter().flat_map(|&n| [Value::I16(n), Value::I32(n.into()), Value::I64(n.into())]).collect::<Vec<_>>(),
                vec![Value::I32(i16::MAX as i32 + 1), Value::I64(i16::MIN as i64 - 1)],
            ),
            (
                ValueType::I32,
                i32s.into_iter().chain(beside_i16.map(|n| n as i32)).map(Value::I32).collect(),
                narrow.iter().cloned().chain(i32s.iter().flat_map(|&n| [Value::I32(n), Value::I64(n.into())])).collect(),
                vec![Value::I64(i32::MAX as i64 + 1), Value::I64(i32::MIN as i64 - 1)],
            ),
            (
                ValueType::I64,
                [i64::MIN, -1, 0, 1, i64::MAX].into_iter().chain(beside_i16).chain(beside_i32).map(Value::I64).collect(),
                narrow.iter().cloned().chain(i32s.map(Value::I32)).collect(),
                Vec::new(),
            ),
        ];
        for (value_type, rows, bounds, too_wide) in cases {
            let rows = rows.into_iter().map(|value| vec![value]).collect::<Vec<_>>();
            for index in [index(vec![asc("rank", value_type)]), index(vec![desc("rank", value_type)])] {
                for range in every_range(&[], &bounds) {
                    assert_exact(&index, &range, &rows);
                }
                for value in &too_wide {
                    assert_eq!(
                        Cover::new(index.clone(), range(Vec::new(), Bound::Included(value.clone()), Bound::Unbounded)),
                        Err(RangeError::TypeMismatch { part: 0, expected: value_type, found: ValueType::of(value) })
                    );
                }
            }
        }
    }

    #[test]
    fn ranges_on_floats_booleans_and_entity_ids_hold_exactly_the_rows_inside_them() {
        // NaN, as a row and as a bound, beside the infinities and both zeros.
        let floats = [f64::NAN, f64::NEG_INFINITY, -1.5, -0.0, 0.0, 0.5, 1.0, f64::INFINITY].map(Value::F64).to_vec();
        let booleans = [false, true].map(Value::Bool).to_vec();
        for (index, values) in [
            (index(vec![asc("weight", ValueType::F64)]), floats.clone()),
            (index(vec![desc("weight", ValueType::F64)]), floats),
            (index(vec![asc("done", ValueType::Bool)]), booleans.clone()),
            (index(vec![desc("done", ValueType::Bool)]), booleans),
            (index(vec![asc("group", ValueType::EntityId)]), groups()),
            (index(vec![desc("group", ValueType::EntityId)]), groups()),
        ] {
            let rows = values.iter().map(|value| vec![value.clone()]).collect::<Vec<_>>();
            for range in every_range(&[], &values) {
                assert_exact(&index, &range, &rows);
            }
        }
    }

    /// The review's counterexample: weight >= 0.0 was tiled [80.., End) ascending, the one block
    /// 80/1, and [0, 0x80) descending, the block 00/1, and each held the row of a NaN weight
    /// and the entity id 00×32, which the Selection rejects.
    #[test]
    fn a_float_range_bounded_below_stops_at_infinity_short_of_nan() {
        for (weight, nan) in [(asc("weight", ValueType::F64), [0xFF; 8]), (desc("weight", ValueType::F64), [0x00; 8])] {
            let weights = index(vec![weight]);
            let from_zero = |upper| Cover::new(weights.clone(), range(Vec::new(), Bound::Included(Value::F64(0.0)), upper)).unwrap();
            let cover = from_zero(Bound::Unbounded);
            let nan_row = [nan.as_slice(), &[0; 32]].concat();
            assert!(!cover.blocks().iter().any(|block| block.contains(&nan_row)), "{:?}", cover.blocks());
            assert_eq!(cover.blocks(), from_zero(Bound::Included(Value::F64(f64::INFINITY))).blocks());
        }
    }

    #[test]
    fn a_nan_bound_or_prefix_names_no_key() {
        let nan = || Value::F64(f64::NAN);
        let floats = [f64::NAN, f64::NEG_INFINITY, 0.0, f64::INFINITY].map(Value::F64);
        let scores = [i64::MIN, 0, i64::MAX].map(Value::I64);
        for weight in [asc("weight", ValueType::F64), desc("weight", ValueType::F64)] {
            let weights = index(vec![weight.clone()]);
            let rows = floats.iter().map(|value| vec![value.clone()]).collect::<Vec<_>>();
            for range in [
                KeyRange::prefix(vec![nan()]),
                range(Vec::new(), Bound::Included(nan()), Bound::Unbounded),
                range(Vec::new(), Bound::Unbounded, Bound::Excluded(nan())),
                range(Vec::new(), Bound::Excluded(Value::F64(0.0)), Bound::Included(nan())),
            ] {
                assert_eq!(Cover::new(weights.clone(), range.clone()).unwrap().blocks(), [], "{range:?}");
                assert_exact(&weights, &range, &rows);
            }
            // A NaN prefix names no key, however the next part is bounded.
            let weights_then_scores = index(vec![weight, asc("score", ValueType::I64)]);
            let rows = floats.iter().flat_map(|weight| scores.iter().map(|score| vec![weight.clone(), score.clone()])).collect::<Vec<_>>();
            for range in every_range(&[nan()], &scores) {
                assert_eq!(Cover::new(weights_then_scores.clone(), range.clone()).unwrap().blocks(), [], "{range:?}");
                assert_exact(&weights_then_scores, &range, &rows);
            }
        }
        // NaN given for an integer part is a value of another type.
        assert_eq!(
            Cover::new(index(vec![asc("score", ValueType::I64)]), range(Vec::new(), Bound::Included(nan()), Bound::Unbounded)),
            Err(RangeError::TypeMismatch { part: 0, expected: ValueType::I64, found: ValueType::F64 })
        );
    }

    #[test]
    fn a_range_after_a_float_prefix_holds_exactly_the_rows_inside_it() {
        // Rows of a NaN weight file next to those of +∞, in either direction.
        let floats = || [f64::NAN, f64::NEG_INFINITY, -0.0, 0.0, f64::INFINITY].map(Value::F64).to_vec();
        let scores = || [i64::MIN, -1, 0, 1, i64::MAX].map(Value::I64).to_vec();
        for weight in [asc("weight", ValueType::F64), desc("weight", ValueType::F64)] {
            let index = index(vec![weight, desc("score", ValueType::I64)]);
            let rows = floats()
                .into_iter()
                .flat_map(|weight| scores().into_iter().map(move |score| vec![weight.clone(), score]))
                .collect::<Vec<_>>();
            for prefix in [f64::NEG_INFINITY, -0.0, 0.0, f64::INFINITY] {
                for range in every_range(&[Value::F64(prefix)], &scores()) {
                    assert_exact(&index, &range, &rows);
                }
            }
        }
    }

    #[test]
    fn a_range_after_a_prefix_of_a_multi_part_key_holds_exactly_the_rows_inside_it() {
        // Rows of the neighbouring groups bracket the prefix's block on both sides.
        for index in [
            index(vec![asc("group", ValueType::EntityId), asc("score", ValueType::I64)]),
            index(vec![asc("group", ValueType::EntityId), desc("score", ValueType::I64)]),
        ] {
            let rows = groups()
                .into_iter()
                .flat_map(|group| integers().into_iter().map(move |score| vec![group.clone(), score]))
                .collect::<Vec<_>>();
            for range in every_range(&groups()[..1], &integers()) {
                assert_exact(&index, &range, &rows);
            }
        }
    }

    /// The empty string's descending encoding is the bare terminator 0xFF 0xFF, and "a\0" ends
    /// in an escaped NUL beside it; the parts after them are descending too.
    #[test]
    fn a_range_after_descending_prefixes_holds_exactly_the_rows_inside_it() {
        let text = |text: &str| Value::String(text.to_owned());
        let names = ["", "\0", "a", "a\0", "a\0b", "b"].map(text);
        let blobs = [vec![], vec![0x00], vec![0xFF]].map(Value::Binary);
        let scores = [i64::MIN, -1, 0, 1, i64::MAX].map(Value::I64);
        let index = index(vec![desc("name", ValueType::String), desc("blob", ValueType::Binary), desc("score", ValueType::I64)]);
        let mut rows = Vec::new();
        for name in &names {
            for blob in &blobs {
                rows.extend(scores.iter().map(|score| vec![name.clone(), blob.clone(), score.clone()]));
            }
        }
        for name in [text(""), text("a\0")] {
            for range in every_range(std::slice::from_ref(&name), &blobs) {
                assert_exact(&index, &range, &rows);
            }
            for blob in &blobs {
                for range in every_range(&[name.clone(), blob.clone()], &scores) {
                    assert_exact(&index, &range, &rows);
                }
            }
        }
    }

    #[test]
    fn a_prefix_of_a_multi_part_key_is_one_block() {
        let index = index(vec![asc("group", ValueType::EntityId), asc("done", ValueType::Bool), desc("score", ValueType::I64)]);
        let group = groups()[0].clone();
        let rows = groups()
            .into_iter()
            .flat_map(|group| [false, true].map(move |done| (group.clone(), done)))
            .flat_map(|(group, done)| integers().into_iter().map(move |score| vec![group.clone(), Value::Bool(done), score]))
            .collect::<Vec<_>>();
        for prefix in [vec![group.clone()], vec![group.clone(), Value::Bool(true)]] {
            let range = KeyRange::prefix(prefix.clone());
            let cover = Cover::new(index.clone(), range.clone()).unwrap();
            let encoded =
                prefix.iter().zip(key_parts(&index)).flat_map(|(value, part)| encode(0, part, value).unwrap()).collect::<Vec<_>>();
            assert_eq!(cover.blocks(), [Block::new(&encoded, encoded.len() * 8)]);
            assert_exact(&index, &range, &rows);
        }
    }

    #[test]
    fn the_full_replica_is_the_root_block_of_the_entity_id_index() {
        let cover = Cover::new(HashedIndex::EntityId, KeyRange::prefix(Vec::new())).unwrap();
        assert_eq!(cover, Cover::root());
        assert_eq!(Cover::root().blocks(), [Block::root()]);
        for entity in [entity(0x00, 0x00), entity(0x80, 0x01), entity(0xFF, 0xFF)] {
            assert!(Block::root().contains(&entity.to_bytes()));
        }
        // A component's entity-id index holds its members at the root as well.
        let members = Cover::new(index(Vec::new()), KeyRange::prefix(Vec::new())).unwrap();
        assert_eq!(members.blocks(), [Block::root()]);
        assert_ne!(members.index(), Cover::root().index());
    }

    #[test]
    fn a_range_on_the_entity_id_of_an_entity_id_index_holds_exactly_the_entities_inside_it() {
        let ids = entities().into_iter().map(Value::EntityId).collect::<Vec<_>>();
        for index in [HashedIndex::EntityId, index(Vec::new())] {
            // Each row is an entity id alone; assert_exact tries every id of `entities`.
            for range in every_range(&[], &ids) {
                assert_exact(&index, &range, &[Vec::new()]);
            }
            // The address holds the id once: one id is the block of its 256 bits.
            let id = entities()[2];
            let one =
                Cover::new(index.clone(), range(Vec::new(), Bound::Included(Value::EntityId(id)), Bound::Included(Value::EntityId(id))));
            assert_eq!(one.unwrap().blocks(), [Block::new(&id.to_bytes(), 256)]);
            // A bound is the one way to name the id: no prefix fixes it.
            assert_eq!(
                Cover::new(index.clone(), KeyRange::prefix(vec![Value::EntityId(id)])),
                Err(RangeError::NoSuchPart { part: 0, parts: 0 })
            );
            assert_eq!(
                Cover::new(index, range(Vec::new(), Bound::Excluded(Value::I64(5)), Bound::Unbounded)),
                Err(RangeError::TypeMismatch { part: 0, expected: ValueType::EntityId, found: ValueType::I64 })
            );
        }
    }

    #[test]
    fn a_range_on_the_entity_id_after_a_whole_key_holds_exactly_the_rows_inside_it() {
        let ids = entities().into_iter().map(Value::EntityId).collect::<Vec<_>>();
        let index = index(vec![desc("score", ValueType::I64)]);
        let rows = [-1, 0, 1].map(|score| vec![Value::I64(score)]);
        for range in every_range(&[Value::I64(0)], &ids) {
            assert_exact(&index, &range, &rows);
        }
    }

    /// Blocks follow the bounds, not any tree: a range of 16 integers falls into blocks of 61 to
    /// 64 bits, which no tree with four or eight bits per level stores as nodes. Each member sums
    /// whatever nodes lie inside a block and scans leaves where it keeps none.
    #[test]
    fn blocks_follow_the_bounds_whatever_a_tree_stores() {
        let index = index(vec![asc("score", ValueType::I64)]);
        let cover = Cover::new(index, range(Vec::new(), Bound::Included(Value::I64(5)), Bound::Excluded(Value::I64(21)))).unwrap();
        let key = |score: i64| encode_component_typed(&Value::I64(score), ValueType::I64, false).unwrap();
        // 5, then 6 and 7, then 8 to 15, then 16 to 19, then 20.
        let expected = [(key(5), 64), (key(6), 63), (key(8), 61), (key(16), 62), (key(20), 64)].map(|(bits, len)| Block::new(&bits, len));
        assert_eq!(cover.blocks(), expected);
    }

    #[test]
    fn a_range_resting_on_an_encoding_that_is_not_prefix_free_is_refused() {
        let ambiguous = |part, value_type, direction| RangeError::AmbiguousEncoding { part, value_type, direction };
        let names = index(vec![asc("name", ValueType::String)]);
        let text = Value::String("a".to_owned());
        assert_eq!(
            Cover::new(names.clone(), KeyRange::prefix(vec![text.clone()])),
            Err(ambiguous(0, ValueType::String, IndexDirection::Asc))
        );
        assert_eq!(
            Cover::new(names.clone(), range(Vec::new(), Bound::Included(text.clone()), Bound::Unbounded)),
            Err(ambiguous(0, ValueType::String, IndexDirection::Asc))
        );
        let json = index(vec![desc("data", ValueType::Json)]);
        assert_eq!(
            Cover::new(json, range(Vec::new(), Bound::Unbounded, Bound::Excluded(Value::Json(serde_json::json!("a"))))),
            Err(ambiguous(0, ValueType::Json, IndexDirection::Desc))
        );
        // A part no bound rests on can be ambiguous: the prefix's block holds every value of it.
        let group = groups()[0].clone();
        let trailing = index(vec![asc("group", ValueType::EntityId), asc("name", ValueType::Binary)]);
        assert!(Cover::new(trailing, KeyRange::prefix(vec![group])).is_ok());
        // The open range of an ambiguous part is its whole index.
        assert_eq!(Cover::new(names, KeyRange::prefix(Vec::new())).unwrap().blocks(), [Block::root()]);
    }

    #[test]
    fn a_value_of_another_type_is_refused_and_integer_widths_are_one_type() {
        let scores = index(vec![asc("score", ValueType::I64)]);
        let lower = |value| range(Vec::new(), Bound::Included(value), Bound::Unbounded);
        assert_eq!(
            Cover::new(scores.clone(), lower(Value::String("5".to_owned()))),
            Err(RangeError::TypeMismatch { part: 0, expected: ValueType::I64, found: ValueType::String })
        );
        assert_eq!(
            Cover::new(scores.clone(), lower(Value::F64(5.0))),
            Err(RangeError::TypeMismatch { part: 0, expected: ValueType::I64, found: ValueType::F64 })
        );
        assert_eq!(
            Cover::new(scores.clone(), lower(Value::I32(5))).unwrap().blocks(),
            Cover::new(scores, lower(Value::I64(5))).unwrap().blocks()
        );
        let small = index(vec![asc("rank", ValueType::I16)]);
        assert_eq!(
            Cover::new(small, lower(Value::I64(70_000))),
            Err(RangeError::TypeMismatch { part: 0, expected: ValueType::I16, found: ValueType::I64 })
        );
    }

    #[test]
    fn a_prefix_past_the_last_key_part_is_refused() {
        let scores = index(vec![asc("score", ValueType::I64)]);
        assert_eq!(
            Cover::new(scores.clone(), KeyRange::prefix(vec![Value::I64(1), Value::I64(2)])),
            Err(RangeError::NoSuchPart { part: 1, parts: 1 })
        );
        // After the whole key the bounds are on the entity id, part 1 of this index's addresses.
        assert_eq!(
            Cover::new(scores.clone(), range(vec![Value::I64(1)], Bound::Included(Value::I64(2)), Bound::Unbounded)),
            Err(RangeError::TypeMismatch { part: 1, expected: ValueType::EntityId, found: ValueType::I64 })
        );
        let whole_key = Cover::new(scores, KeyRange::prefix(vec![Value::I64(1)])).unwrap();
        let one = encode_component_typed(&Value::I64(1), ValueType::I64, false).unwrap();
        assert_eq!(whole_key.blocks(), [Block::new(&one, 64)]);
    }

    #[test]
    fn a_block_states_its_bits_canonically() {
        assert_eq!(Block::new(&[0b1011_0111], 3), Block::new(&[0b1010_0000], 3));
        assert_eq!(Block::new(&[0xAB], 3).bytes(), [0b1010_0000]);
        assert_eq!(Block::new(&[], 12).bytes(), [0, 0]);
        let block = Block::new(&[0xAB, 0xC0], 10);
        assert!(block.contains(&[0xAB, 0xC0, 0x01]));
        assert!(block.contains(&[0xAB, 0xFF]));
        assert!(!block.contains(&[0xAB, 0x80]));
        assert!(!block.contains(&[0xAB]), "an address shorter than the block's bits is not in it");
    }
}
