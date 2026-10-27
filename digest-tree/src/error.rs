//! What can go wrong, grouped by whose fault it is.

use std::fmt;

/// A digest tree's errors, grouped by whose fault each is.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    // The caller's.
    /// A member couldn't make its key under the tree's spec. A transaction
    /// records nothing of the change that named it, and a build that held it
    /// writes nothing.
    #[error("member {member:02x?} has no key under the tree's spec: {source}")]
    Key {
        /// The member's id, as its bytes.
        member: Vec<u8>,

        /// Why the member couldn't make its key.
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// A change named a version of a member that isn't the one held. At the
    /// change's call, the transaction had left the member in another version,
    /// and the change isn't recorded. When the commit is staged, the tree's
    /// leaf is missing or holds another payload: the tree and its owner
    /// disagree, so nothing is written, and the owner rebuilds the tree.
    #[error("member {member:02x?} is not held in the version a change names")]
    NotHeld {
        /// The member's id, as its bytes.
        member: Vec<u8>,
    },

    /// An insert, or an update moving a member to another key, named a
    /// version already held: by the transaction, at an insert's call, or by
    /// the tree under that key, when the commit is staged, and then nothing is
    /// written.
    #[error("member {member:02x?} is already held")]
    AlreadyHeld {
        /// The member's id, as its bytes.
        member: Vec<u8>,
    },

    /// An update's new version is of another member than its old one.
    #[error("an update gives member {old:02x?} the id {new:02x?}")]
    IdChanged {
        /// The old version's id, as its bytes.
        old: Vec<u8>,

        /// The new version's id, as its bytes.
        new: Vec<u8>,
    },

    /// `DigestTree::build` was given two members with the same id. Keeping
    /// either would hide the mistake.
    #[error("member {member:02x?} is given twice")]
    DuplicateMember {
        /// The member's id, as its bytes.
        member: Vec<u8>,
    },

    /// `DigestTree::create` or `build` was given a store that already holds
    /// rows: a tree's, or others'.
    #[error("the store already holds rows")]
    StoreNotEmpty,

    /// `DigestTree::open` was given a store that holds no tree.
    #[error("the store holds no tree")]
    NoTree,

    /// `DigestTree::open` found a tree summed in another digest group.
    #[error("the store's tree is summed in another digest group")]
    WrongGroup,

    // A peer's.
    /// Bytes from a peer don't decode as a span digest.
    #[error("malformed span digest: {0}")]
    MalformedSpanDigest(&'static str),

    // The store's.
    /// The store couldn't read or write.
    #[error(transparent)]
    Store(#[from] StoreError),

    // The tree's.
    /// The tree's own invariant broke: a bug, reported and never repaired
    /// silently. The owner discards the tree.
    #[error("the tree is inconsistent: {0}")]
    Inconsistent(&'static str),
}

/// A store's failure to read or write, and whether trying again can help.
#[derive(Debug)]
pub struct StoreError {
    source: Box<dyn std::error::Error + Send + Sync>,
    retryable: bool,
}

impl StoreError {
    /// A failure that trying again may fix, such as a lost connection.
    pub fn retryable(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self { source: source.into(), retryable: true }
    }

    /// A failure that trying again won't fix, such as corrupt bytes.
    pub fn permanent(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self { source: source.into(), retryable: false }
    }

    /// Whether trying again may help.
    pub fn is_retryable(&self) -> bool { self.retryable }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = if self.retryable { "retryable" } else { "permanent" };
        write!(f, "store failure ({kind}): {}", self.source)
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { Some(self.source.as_ref()) }
}
