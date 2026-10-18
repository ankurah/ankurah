// Historical baseline, frozen from ae2fb892; benchmark only.
//! Compact binary encoding for initial inventories only. Response and feedback
//! pages retain their ordinary encoding and decoded resource limits.

use bincode::Options;
use serde::{de::Visitor, Deserialize, Deserializer, Serialize, Serializer};

use super::TreeSpanInventory;

const VERSION: u8 = 1;
// The initial inventory has its own request budget, independent of follow-up pages.
const MAX_ENCODED_BYTES: usize = 256 * 1024;

fn options() -> impl Options {
    bincode::DefaultOptions::new().with_little_endian().with_varint_encoding().with_limit(MAX_ENCODED_BYTES as u64).reject_trailing_bytes()
}

pub(super) fn serialize<S: Serializer>(page: &TreeSpanInventory, serializer: S) -> Result<S::Ok, S::Error> {
    if serializer.is_human_readable() {
        return page.serialize(serializer);
    }
    let bytes = options().serialize(&(VERSION, page)).map_err(serde::ser::Error::custom)?;
    serializer.serialize_bytes(&bytes)
}

pub(super) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<TreeSpanInventory, D::Error> {
    if deserializer.is_human_readable() {
        return TreeSpanInventory::deserialize(deserializer);
    }
    deserializer.deserialize_bytes(PageVisitor)
}

struct PageVisitor;

impl<'de> Visitor<'de> for PageVisitor {
    type Value = TreeSpanInventory;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a versioned compact initial head inventory of at most 256 KiB")
    }

    fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<TreeSpanInventory, E> {
        // Bincode's slice decoder does not enforce Options::with_limit. Check
        // the borrowed blob before any typed page decoding or allocation.
        if bytes.len() > MAX_ENCODED_BYTES {
            return Err(E::custom("compact initial head inventory exceeds 256 KiB"));
        }
        let (version, payload) = bytes.split_first().ok_or_else(|| E::custom("missing compact initial head inventory version"))?;
        if *version != VERSION {
            return Err(E::custom("unsupported compact initial head inventory version"));
        }
        options().deserialize(payload).map_err(E::custom)
    }
}
