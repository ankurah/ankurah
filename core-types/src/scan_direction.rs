/// Which way a read runs over the keys it reads, in their order: smallest
/// first, or largest first. A key spec's parts say which way each part's
/// values sort within a key; a scan direction says which way a read runs over
/// the keys that result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScanDirection {
    /// Smallest key first.
    Forward,

    /// Largest key first.
    Reverse,
}
