/// Traversal order over encoded keys. A key spec controls how each part's
/// values sort within a key; scan direction controls traversal of the resulting keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScanDirection {
    /// Smallest key first.
    Forward,

    /// Largest key first.
    Reverse,
}
