//! A view's window: the members a plan's limit admits, read in the plan's
//! direction, and the spans that hold exactly them.
//!
//! Keys sort as the owner orders members, any mix of directions across their
//! parts included, so the window is the first members in address order, or
//! the last: one read per span until the limit is met.

use std::ops::Bound;

use crate::tree::rows::StagedRows;
use crate::{DigestGroup, Error, Payload, Row, RowKey, ScanDirection, Span, Store};

/// The spans of `spans`, which are disjoint and in address order, that hold
/// the first `limit` members read in `direction`. If fewer members lie within
/// `spans`, all of `spans`: a peer may hold the members missing here.
pub(super) async fn window<K, L, G, S>(
    rows: &StagedRows<'_, K, L, G, S>,
    spans: Vec<Span>,
    direction: ScanDirection,
    limit: u64,
) -> Result<Vec<Span>, Error>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    if limit == 0 {
        return Ok(Vec::new());
    }

    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let mut read = 0;
    let mut last = None;

    let mut in_order: Vec<&Span> = spans.iter().collect();
    if direction == ScanDirection::Reverse {
        in_order.reverse();
    }

    for span in in_order {
        let leaves = rows.leaves(span, direction, limit - read).await?;
        read += leaves.len();
        last = leaves.into_iter().last().map(|(address, _)| address).or(last);

        if read == limit {
            break;
        }
    }

    match last {
        Some(last) if read == limit => {
            let admitted = match direction {
                ScanDirection::Forward => Span::new(Bound::Unbounded, Bound::Included(last)),
                ScanDirection::Reverse => Span::new(Bound::Included(last), Bound::Unbounded),
            };

            Ok(Span::intersect_all(&spans, &[admitted]))
        }

        _ => Ok(spans),
    }
}
