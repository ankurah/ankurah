//! Limits both peers of a comparison enforce. A sender sizes what it sends to fit
//! them, and a receiver refuses whatever exceeds them, so each is part of the
//! protocol: a peer that changed one alone would have its messages refused.

/// The work of one inventory page.
pub const MAX_PAGE_BYTES: usize = 64 * 1024;
/// Heads in one inventory page.
pub const MAX_PAGE_HEADS: usize = 128;
/// Spans in one KnownState.
pub const MAX_OFFER_SPANS: usize = 128;
/// The work of one KnownState. The codec caps every packed inventory at this size
/// too, since an offer carries the largest inventory the protocol sends.
pub const MAX_OFFER_BYTES: usize = 256 * 1024;
/// Heads across one KnownState's inventories. The codec caps every packed inventory
/// at this count too.
pub const MAX_OFFER_HEADS: usize = 2048;
/// Heads questions in one CompareSpans, so that its answers stay within
/// `MAX_OFFER_BYTES` of work: four pages of at most `MAX_PAGE_BYTES`.
pub const MAX_HEAD_QUESTIONS: usize = 4;
