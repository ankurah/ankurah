//! Experimental v2 inventory payload, only linked by the benchmark.
//! The v1 path reads the actual production encoding as a borrowed control.

use super::legacy::{SpanContent, TreeSpanInventory};
use ankurah_proto::{Clock, EntityHead, EntityId, KeyedEntityHead, TreeSpan};
use bincode::Options;
use serde::de::DeserializeOwned;
use std::{io::Cursor, ops::Bound};

pub type Result<T> = std::result::Result<T, &'static str>;
pub const MAX_BYTES: usize = 256 * 1024;
const MAX_ROWS: usize = 2048;

fn options() -> impl Options { bincode::DefaultOptions::new().with_little_endian().with_varint_encoding().reject_trailing_bytes() }

fn put_uint(out: &mut Vec<u8>, value: usize) {
    match value {
        0..=250 => out.push(value as u8),
        251..=65535 => {
            out.push(251);
            out.extend_from_slice(&(value as u16).to_le_bytes());
        }
        65536..=4294967295 => {
            out.push(252);
            out.extend_from_slice(&(value as u32).to_le_bytes());
        }
        _ => {
            out.push(253);
            out.extend_from_slice(&(value as u64).to_le_bytes());
        }
    }
}

/// Keep bounds, IDs, clocks and integer coding identical to production.
/// Version 2 adds one shared-prefix length before each key suffix.
pub fn encode(page: &TreeSpanInventory, prefix: bool) -> Vec<u8> {
    let maximum = options().serialized_size(page).unwrap() as usize + page.leaves.len() * 9 + 13;
    let mut out = Vec::with_capacity(maximum);
    out.extend_from_slice(&1u32.to_le_bytes()); // SpanContent::Heads
    out.extend_from_slice(&0u64.to_le_bytes()); // compact blob length, filled below
    out.push(if prefix { 2 } else { 1 });
    options().serialize_into(&mut out, &page.bounds).unwrap();
    put_uint(&mut out, page.leaves.len());
    let mut previous: &[u8] = &[];
    for leaf in &page.leaves {
        let shared = if prefix { previous.iter().zip(&leaf.key).take_while(|(a, b)| a == b).count() } else { 0 };
        if prefix {
            put_uint(&mut out, shared);
        }
        put_uint(&mut out, leaf.key.len() - shared);
        out.extend_from_slice(&leaf.key[shared..]);
        options().serialize_into(&mut out, &leaf.entity).unwrap();
        previous = &leaf.key;
    }
    let length = (out.len() - 12) as u64;
    out[4..12].copy_from_slice(&length.to_le_bytes());
    out
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(length).ok_or("length overflow")?;
        let bytes = self.bytes.get(self.offset..end).ok_or("truncated data")?;
        self.offset = end;
        Ok(bytes)
    }
    fn uint(&mut self) -> Result<usize> {
        let value = match self.take(1)?[0] {
            n @ 0..=250 => n as u64,
            251 => u16::from_le_bytes(self.take(2)?.try_into().unwrap()) as u64,
            252 => u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as u64,
            253 => u64::from_le_bytes(self.take(8)?.try_into().unwrap()),
            _ => return Err("unsupported integer"),
        };
        usize::try_from(value).map_err(|_| "integer overflow")
    }
    fn slice(&mut self) -> Result<&'a [u8]> {
        let length = self.uint()?;
        self.take(length)
    }
    fn bound(&mut self) -> Result<Bound<&'a [u8]>> {
        match self.uint()? {
            0 => Ok(Bound::Unbounded),
            1 => Ok(Bound::Included(self.slice()?)),
            2 => Ok(Bound::Excluded(self.slice()?)),
            _ => Err("invalid bound"),
        }
    }
    fn owned<T: DeserializeOwned>(&mut self) -> Result<T> {
        let mut input = Cursor::new(&self.bytes[self.offset..]);
        let result = options().with_limit(self.bytes.len() as u64).deserialize_from(&mut input).map_err(|_| "invalid owned value")?;
        self.offset += input.position() as usize;
        Ok(result)
    }
    fn clock(&mut self) -> Result<ClockView<'a>> {
        let start = self.offset;
        let count = self.uint()?;
        if count > (self.bytes.len() - self.offset) / 33 {
            return Err("truncated clock");
        }
        let mut previous: Option<&[u8]> = None;
        let mut canonical = true;
        for _ in 0..count {
            let generation = self.uint()?;
            if generation == 0 || generation > u32::MAX as usize {
                return Err("invalid generation");
            }
            let id = self.take(32)?;
            canonical &= previous.is_none_or(|p| p < id);
            previous = Some(id);
        }
        let bytes = &self.bytes[start..self.offset];
        if canonical {
            Ok(ClockView::Borrowed(bytes))
        } else {
            // Preserve production's sorting/deduplication and conflicting-generation errors.
            options().deserialize(bytes).map(ClockView::Normalized).map_err(|_| "invalid clock")
        }
    }
    fn finish(self) -> Result<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err("trailing bytes")
        }
    }
}

fn body(encoded: &[u8]) -> Result<(Reader<'_>, bool)> {
    if encoded.len() < 13 || encoded.len() - 12 > MAX_BYTES {
        return Err("invalid envelope size");
    }
    if encoded[..4] != 1u32.to_le_bytes() {
        return Err("not an inventory");
    }
    if u64::from_le_bytes(encoded[4..12].try_into().unwrap()) != (encoded.len() - 12) as u64 {
        return Err("invalid blob length");
    }
    let prefix = match encoded[12] {
        1 => false,
        2 => true,
        _ => return Err("unknown version"),
    };
    Ok((Reader { bytes: &encoded[13..], offset: 0 }, prefix))
}

pub enum ClockView<'a> {
    Borrowed(&'a [u8]),
    Normalized(Clock),
}
impl ClockView<'_> {
    pub fn visit(&self, mut f: impl FnMut(u32, &[u8])) {
        match self {
            Self::Borrowed(bytes) => {
                let mut reader = Reader { bytes, offset: 0 };
                let count = reader.uint().unwrap();
                for _ in 0..count {
                    f(reader.uint().unwrap() as u32, reader.take(32).unwrap());
                }
            }
            Self::Normalized(clock) => {
                for (generation, id) in clock.entries() {
                    f(*generation, id.as_bytes());
                }
            }
        }
    }
    pub fn owned(&self) -> Clock {
        match self {
            Self::Borrowed(bytes) => options().deserialize(bytes).unwrap(),
            Self::Normalized(clock) => clock.clone(),
        }
    }
}

pub type BoundsView<'a> = (Bound<&'a [u8]>, Bound<&'a [u8]>);

/// Visit every key/ID/normalized clock. One reusable key buffer only in prefix mode.
/// No references escape the callback: the key buffer is reused for the next row.
pub fn visit(encoded: &[u8], mut f: impl FnMut(&[u8], &[u8], &ClockView<'_>)) -> Result<BoundsView<'_>> {
    let (mut reader, prefix) = body(encoded)?;
    let bounds = (reader.bound()?, reader.bound()?);
    let count = reader.uint()?;
    if count > MAX_ROWS {
        return Err("too many rows");
    }
    let mut key = Vec::new();
    let mut expanded_bytes = 0usize;
    for _ in 0..count {
        let shared = if prefix { reader.uint()? } else { 0 };
        let suffix = reader.slice()?;
        if shared > key.len() {
            return Err("prefix exceeds previous key");
        }
        expanded_bytes = expanded_bytes.checked_add(shared + suffix.len()).ok_or("expanded size overflow")?;
        if expanded_bytes > MAX_BYTES {
            return Err("expanded key work exceeds limit");
        }
        let full_key = if prefix {
            key.truncate(shared);
            key.extend_from_slice(suffix);
            key.as_slice()
        } else {
            suffix
        };
        let id = reader.take(32)?;
        let clock = reader.clock()?;
        f(full_key, id, &clock);
    }
    reader.finish()?;
    Ok(bounds)
}

/// Owned output control: isolate encoding from borrowed-vs-owned representation.
/// Reuse production EntityHead/Clock decoding, including normalization.
pub fn owned(encoded: &[u8]) -> Result<TreeSpanInventory> {
    let (mut reader, prefix) = body(encoded)?;
    let bounds: TreeSpan = reader.owned()?;
    let count = reader.uint()?;
    if count > MAX_ROWS {
        return Err("too many rows");
    }
    let mut leaves: Vec<KeyedEntityHead> = Vec::with_capacity(count);
    let mut expanded_bytes = 0usize;
    for _ in 0..count {
        let shared = if prefix { reader.uint()? } else { 0 };
        let suffix = reader.slice()?;
        let previous = leaves.last().map_or(&[][..], |leaf| leaf.key.as_slice());
        if shared > previous.len() {
            return Err("prefix exceeds previous key");
        }
        expanded_bytes = expanded_bytes.checked_add(shared + suffix.len()).ok_or("expanded size overflow")?;
        if expanded_bytes > MAX_BYTES {
            return Err("expanded key work exceeds limit");
        }
        let mut key = Vec::with_capacity(shared + suffix.len());
        key.extend_from_slice(&previous[..shared]);
        key.extend_from_slice(suffix);
        let entity: EntityHead = reader.owned()?;
        leaves.push(KeyedEntityHead { key, entity });
    }
    reader.finish()?;
    Ok(TreeSpanInventory { bounds, leaves })
}

/// Pre-timing verification compares all bytes/normalized fields, not a checksum alone.
pub fn verify(page: &TreeSpanInventory, bytes: &[u8]) {
    let decoded = owned(bytes).unwrap();
    assert_eq!(bincode::serialize(&decoded).unwrap(), bincode::serialize(page).unwrap());
    let mut rows = Vec::new();
    let bounds = visit(bytes, |key, id, clock| {
        rows.push(KeyedEntityHead {
            key: key.to_vec(),
            entity: EntityHead { entity_id: EntityId::from_bytes(id.try_into().unwrap()), head: clock.owned() },
        })
    })
    .unwrap();
    assert_eq!(bounds, page.bounds.bounds());
    let decoded = TreeSpanInventory { bounds: page.bounds.clone(), leaves: rows };
    assert_eq!(bincode::serialize(&decoded).unwrap(), bincode::serialize(page).unwrap());
}

pub fn malformed_checks() {
    use ankurah_proto::EventId;
    let page = TreeSpanInventory {
        bounds: TreeSpan::all(),
        leaves: vec![KeyedEntityHead {
            key: b"shared/value".to_vec(),
            entity: EntityHead {
                entity_id: EntityId::from_bytes([1; 32]),
                head: Clock::new(vec![(1, EventId::from_bytes([2; 32]))]).unwrap(),
            },
        }],
    };
    for prefix in [false, true] {
        let valid = encode(&page, prefix);
        for end in 0..valid.len() {
            assert!(owned(&valid[..end]).is_err());
            assert!(visit(&valid[..end], |_, _, _| {}).is_err());
            if end >= 13 {
                // Also reach the inner parser, rather than failing only its envelope length.
                let mut truncated = valid[..end].to_vec();
                truncated[4..12].copy_from_slice(&((end - 12) as u64).to_le_bytes());
                assert!(owned(&truncated).is_err());
                assert!(visit(&truncated, |_, _, _| {}).is_err());
            }
        }
        let mut trailing = valid.clone();
        trailing.push(0);
        let length = (trailing.len() - 12) as u64;
        trailing[4..12].copy_from_slice(&length.to_le_bytes());
        assert!(owned(&trailing).is_err());
        assert!(visit(&trailing, |_, _, _| {}).is_err());
    }
    let mut invalid_prefix = encode(&page, true);
    invalid_prefix[16] = 1; // all bounds=0,0; count=1; first shared-prefix length
    assert!(owned(&invalid_prefix).is_err());
    assert!(visit(&invalid_prefix, |_, _, _| {}).is_err());
    for tag in [3, 255] {
        let mut invalid_version = encode(&page, true);
        invalid_version[12] = tag;
        assert!(owned(&invalid_version).is_err());
        assert!(visit(&invalid_version, |_, _, _| {}).is_err());
    }
    for start in [Bound::Unbounded, Bound::Included(vec![]), Bound::Excluded(vec![0; 251])] {
        for end in [Bound::Unbounded, Bound::Included(vec![255]), Bound::Excluded(vec![255; 251])] {
            let mut bounded = page.clone();
            bounded.bounds = TreeSpan::new(start.clone(), end);
            for prefix in [false, true] {
                verify(&bounded, &encode(&bounded, prefix));
            }
        }
    }

    #[derive(serde::Serialize)]
    struct Raw {
        bounds: TreeSpan,
        leaves: Vec<RawLeaf>,
    }
    #[derive(serde::Serialize)]
    struct RawLeaf {
        key: Vec<u8>,
        id: EntityId,
        clock: Vec<(u32, EventId)>,
    }
    for clock in [
        vec![(9, EventId::from_bytes([4; 32])), (1, EventId::from_bytes([2; 32])), (1, EventId::from_bytes([2; 32]))],
        vec![(0, EventId::from_bytes([2; 32]))],
        vec![(1, EventId::from_bytes([2; 32])), (2, EventId::from_bytes([2; 32]))],
    ] {
        let raw = Raw { bounds: TreeSpan::all(), leaves: vec![RawLeaf { key: vec![], id: EntityId::from_bytes([3; 32]), clock }] };
        let blob = options().serialize(&(1u8, raw)).unwrap();
        let mut bytes = 1u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(&(blob.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&blob);
        let production = bincode::deserialize::<SpanContent>(&bytes);
        assert_eq!(production.is_ok(), owned(&bytes).is_ok());
        assert_eq!(production.is_ok(), visit(&bytes, |_, _, _| {}).is_ok());
        if let Ok(SpanContent::Heads(ref page)) = production {
            verify(page, &bytes);
        }
        // The empty first key gains a zero shared-prefix field in v2.
        bytes[12] = 2;
        bytes.insert(16, 0);
        let length = (bytes.len() - 12) as u64;
        bytes[4..12].copy_from_slice(&length.to_le_bytes());
        assert_eq!(production.is_ok(), owned(&bytes).is_ok());
        assert_eq!(production.is_ok(), visit(&bytes, |_, _, _| {}).is_ok());
        if let Ok(SpanContent::Heads(page)) = production {
            verify(&page, &bytes);
        }
    }
    let too_many = TreeSpanInventory { bounds: TreeSpan::all(), leaves: vec![page.leaves[0].clone(); MAX_ROWS + 1] };
    for prefix in [false, true] {
        let bytes = encode(&too_many, prefix);
        assert!(bytes.len() < MAX_BYTES);
        assert!(owned(&bytes).is_err());
        assert!(visit(&bytes, |_, _, _| {}).is_err());
    }
    let too_expanded = TreeSpanInventory {
        bounds: TreeSpan::all(),
        leaves: (0..3)
            .map(|_| {
                let mut leaf = page.leaves[0].clone();
                leaf.key = vec![9; MAX_BYTES / 2];
                leaf
            })
            .collect(),
    };
    let bytes = encode(&too_expanded, true);
    assert!(bytes.len() < MAX_BYTES);
    assert!(owned(&bytes).is_err());
    assert!(visit(&bytes, |_, _, _| {}).is_err());
}
