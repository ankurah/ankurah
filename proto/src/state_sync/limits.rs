//! Limits both peers of a state stream enforce. A responder sizes what it sends to
//! fit them, and a requester refuses whatever exceeds them, so each is part of the
//! protocol: a peer that changed one alone would have its messages refused.

use std::time::Duration;

/// The encoded size of one frame or one StateFeedback, and the work either describes.
pub const MAX_FRAME_BYTES: usize = MAX_PAGE_BYTES + 4096;
/// The work of one frame's items, of one inventory page, or of one StateFeedback's answers.
pub const MAX_PAGE_BYTES: usize = 64 * 1024;
/// Items in one Round, Live or LiveRepair frame.
pub const MAX_FRAME_ITEMS: usize = 128;
/// Answers or repair requests in one StateFeedback.
pub const MAX_FEEDBACK_ANSWERS: usize = 128;
/// Heads in one inventory page, sent as a Heads item or answer.
pub const MAX_PAGE_HEADS: usize = 128;
/// Spans in one KnownState.
pub const MAX_OFFER_SPANS: usize = 128;
/// The work of one KnownState. The codec caps every packed inventory at this size
/// too, since an offer carries the largest inventory the protocol sends.
pub const MAX_OFFER_BYTES: usize = 256 * 1024;
/// Heads across one KnownState's inventories. The codec caps every packed inventory
/// at this count too.
pub const MAX_OFFER_HEADS: usize = 2048;
/// The encoded size of one entity delta, whether it travels whole or in chunks.
pub const MAX_DELTA_BYTES: usize = 64 * 1024 * 1024;
/// Bytes of frames a responder may send beyond those the requester has processed.
pub const WINDOW_BYTES: usize = 4 * 1024 * 1024;
/// Bytes beyond the window a responder may spend on repairs and a stream's final frame.
pub const CONTROL_BYTES: usize = 2 * MAX_FRAME_BYTES;
/// Frames a responder may send beyond those the requester has processed.
pub const MAX_FRAMES_IN_FLIGHT: usize = 128;
/// State streams one connection may hold open at once.
pub const MAX_OPEN_STREAMS: usize = 128;
/// How long a stream may sit idle before its responder expires it. A requester waits
/// no longer than this for a response or for a missing frame.
pub const STREAM_TTL: Duration = Duration::from_secs(120);
