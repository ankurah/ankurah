//! Where a scan reads on from after a batch.

use super::*;

/// A batch's last member outside every span still to read can only mean the
/// tree's own reads broke, since a read refuses a store's rows outside the
/// range it asked for. It is reported as the tree's fault rather than a
/// panic, so the stream yields it and ends, as it does any failed read.
#[test]
fn a_last_member_outside_the_spans_still_to_read_is_inconsistent() {
    let unread = vec![Span::new(Bound::Included(vec![1]), Bound::Included(vec![3]))];
    assert!(matches!(past(unread, &[4], ScanDirection::Forward), Err(Error::Inconsistent(_))));
}
