//! Owned baseline at ae2fb892, retained so production adoption can be compared.
use ankurah_proto::{KeyedEntityHead, TreeSpan, TreeSpanDigest};
use serde::{Deserialize, Serialize};
#[path = "legacy_compact.rs"]
mod compact;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeSpanInventory {
    pub bounds: TreeSpan,
    pub leaves: Vec<KeyedEntityHead>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpanContent {
    Digest(TreeSpanDigest),
    Heads(#[serde(with = "compact")] TreeSpanInventory),
}
