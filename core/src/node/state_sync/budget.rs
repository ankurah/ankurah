//! Compression reduces traffic, not the work a message is allowed to describe.
use ankurah_proto::{KnownState, SpanContent, StateAnswer, StateFrame, StateFrameBody, StateItem};

pub(super) fn offer_bytes(known: &KnownState) -> bincode::Result<u64> {
    let saved: usize = known
        .spans
        .iter()
        .map(|span| match span {
            SpanContent::Heads(page) => page.heads.prefix_savings(),
            SpanContent::Digest(_) => 0,
        })
        .sum();
    Ok(bincode::serialized_size(known)? + saved as u64)
}

pub(super) fn item_bytes(item: &StateItem) -> bincode::Result<u64> {
    let encoded = bincode::serialized_size(item)?;
    Ok(match item {
        StateItem::Heads(page) => encoded.max(4 + page.expanded_size()),
        _ => encoded,
    })
}

pub(super) fn answer_bytes(answer: &StateAnswer) -> bincode::Result<u64> {
    let encoded = bincode::serialized_size(answer)?;
    Ok(match answer {
        StateAnswer::Heads(page) => encoded.max(4 + page.expanded_size()),
        _ => encoded,
    })
}

pub(super) fn answers_bytes(answers: &[StateAnswer]) -> bincode::Result<u64> {
    answers.iter().try_fold(8, |size, answer| Ok(size + answer_bytes(answer)?))
}

pub(super) fn frame_expansion(frame: &StateFrame) -> bincode::Result<u64> {
    let items = match &frame.body {
        StateFrameBody::Round { items, .. } => items,
        _ => return Ok(0),
    };
    // Snapshot/repair payloads do not compress here; avoid serializing those
    // potentially large states again merely to discover that their delta is zero.
    items.iter().try_fold(0, |size, item| {
        Ok(size
            + match item {
                StateItem::Heads(page) => (4 + page.expanded_size()).saturating_sub(bincode::serialized_size(item)?),
                _ => 0,
            })
    })
}
