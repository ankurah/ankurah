//! Wall-clock source for advisory timestamps (genesis events, presence
//! freshness). Follows the same wasm arrangement the `ulid` crate uses:
//! `web-time` stands in for `std::time` on wasm targets, where
//! `std::time::SystemTime::now` is unavailable.

#[cfg(target_family = "wasm")]
use web_time::{SystemTime, UNIX_EPOCH};

#[cfg(not(target_family = "wasm"))]
use std::time::{SystemTime, UNIX_EPOCH};

/// Current unix time in milliseconds. Advisory only: creator-supplied
/// timestamps carry the same trust level ULID timestamps did.
pub fn unix_ms_now() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or_default() }
