//! The commit log: what each commit did to each entity, in commit order.
//!
//! The log exists so that state derived from commits can follow the store
//! without reading entity state, which may already be newer than the commit
//! being followed. Its first consumer is the refresher, which folds the log
//! into the digest trees of [`super::tree`].
//!
//! Each commit that sets entity states writes, beside those states and in the
//! same transaction, one [`LogRow`] per entity, all carrying the commit's
//! [`LogPosition`]. Every change of an entity's head or keys reaches the log in
//! the transaction that makes it, including a change that moves keys without a
//! commit on the entity, such as a derived group membership or a catalog
//! change.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use ankurah_proto::{Clock, EntityId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::{tree::TreeId, StorageEngine};

/// One life of a store's commit log. A store mints a new incarnation when it
/// is created and again when it is reset, so that positions recorded before a
/// reset never compare with positions after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogIncarnation(Ulid);

impl LogIncarnation {
    pub fn mint() -> Self { Self(Ulid::new()) }
}

/// A commit's place in one incarnation of a store's commit log.
///
/// The engine assigns a commit its position when it fixes the commit's order
/// against every other commit (its serialization decision), never before, and
/// positions grow in that order; durability follows under the engine's
/// durability setting, and a log row is as durable as its commit. An aborted
/// transaction leaves its position unused, so positions may have gaps; the
/// [stable position](CommitLog::stable_position) says where no gap can still
/// fill, and the [durable position](CommitLog::durable_position) how far
/// commits survive a crash. Positions compare only within one incarnation:
/// across incarnations `partial_cmp` is `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogPosition {
    incarnation: LogIncarnation,
    offset: u64,
}

impl LogPosition {
    pub fn new(incarnation: LogIncarnation, offset: u64) -> Self { Self { incarnation, offset } }

    /// The first position of an incarnation: no commit lies below it.
    pub fn start(incarnation: LogIncarnation) -> Self { Self::new(incarnation, 0) }

    pub fn incarnation(&self) -> LogIncarnation { self.incarnation }

    pub fn offset(&self) -> u64 { self.offset }

    /// The position just after this one.
    pub fn next(&self) -> Self { Self::new(self.incarnation, self.offset.checked_add(1).expect("log position overflow")) }
}

impl PartialOrd for LogPosition {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.incarnation == other.incarnation).then(|| self.offset.cmp(&other.offset))
    }
}

/// What one commit did to one entity, carrying everything a digest tree needs
/// to fold the change without reading entity state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRow {
    /// The commit's position, which every row of the commit shares.
    pub position: LogPosition,
    pub entity_id: EntityId,
    /// The entity's head after the commit.
    pub head: Clock,
    /// The entity's canonical keys after the commit under each tree registered
    /// when the commit serialized. An empty set means the tree's index files
    /// the entity under no key; a tree registered later is absent.
    pub keys: BTreeMap<TreeId, BTreeSet<Vec<u8>>>,
}

/// Log rows read forward from a position, and where the next read starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogPage {
    /// The rows of consecutive whole positions, in position order and then
    /// entity id order. A page never holds part of a position's rows.
    pub rows: Vec<LogRow>,
    /// Where the next read starts. Every position below it that this read
    /// covered is settled, and its rows, if any, are in `rows`.
    pub next: LogPosition,
}

/// Why the commit log could not be read or trimmed.
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("the position belongs to log incarnation {requested:?}, not the current {current:?}")]
    IncarnationMismatch { requested: LogIncarnation, current: LogIncarnation },
    #[error("log rows below {floor:?} are no longer retained")]
    BelowRetentionFloor { floor: LogPosition },
    #[error("storage error: {0}")]
    Storage(Box<dyn std::error::Error + Send + Sync + 'static>),
}

/// The commit log that an engine's transactions write, read forward and
/// trimmed from below.
///
/// A reset ([`StorageEngine::delete_all`]) discards the log and mints a new
/// [`LogIncarnation`].
#[async_trait]
pub trait CommitLog: StorageEngine {
    /// The position below which every commit is settled: committed with its
    /// rows readable, or aborted leaving its position unused. No transaction
    /// can still commit below it, so a reader that stops here never skips a
    /// slow commit at a lower position.
    async fn stable_position(&self) -> Result<LogPosition, LogError>;

    /// The position below which every commit is durable: settled, as below the
    /// stable position, and kept through a crash under the engine's durability
    /// setting. It never lies above the stable position, never moves back
    /// within an incarnation, and advances on its own as the engine makes
    /// commits durable.
    ///
    /// A refresher folds only below the lesser of the stable and the durable
    /// positions, that is below this one, so a crash never leaves a tree
    /// describing a commit that recovery discards; and a store acknowledges a
    /// position to a partner only below this one.
    async fn durable_position(&self) -> Result<LogPosition, LogError>;

    /// The lowest position whose rows the log still holds; reading from below
    /// it fails. It only rises, through [`CommitLog::discard_log_below`] or the
    /// engine's own documented retention policy.
    async fn retention_floor(&self) -> Result<LogPosition, LogError>;

    /// Discard the rows below `position`, raising the retention floor to it,
    /// or to the stable position if that is lower.
    async fn discard_log_below(&self, position: LogPosition) -> Result<(), LogError>;

    /// Read the rows from `from` up to the stable position, a whole position
    /// at a time: the page stops after the first position at which it holds
    /// at least `limit` rows, or at the stable position. Its `next` never lies
    /// below `from`.
    async fn read_log(&self, from: LogPosition, limit: usize) -> Result<LogPage, LogError>;
}
