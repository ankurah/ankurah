//! What the cases make and do: keys of text and numbers as values, the
//! writes and reads of them, an entity's records, and the calls the cases
//! make in a transaction or a snapshot of their own, or poll once. The
//! entity's records and the poll serve any storage test, an engine's own or
//! the tree manager's suite's, and so does a deadline that turns a hang into a
//! failure.

use std::future::Future;
use std::pin::{pin, Pin};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures::future::{self, Either};
use futures_timer::Delay;

use crate::error::{MutationError, RetrievalError};
use crate::storage::{LockName, MapRead, MapRows, MapSnapshot, MapWrite, StorageEngine, StorageTransaction};

/// The bytes of a key, from its text.
macro_rules! key {
    ($key:expr) => {
        ::std::vec::Vec::<u8>::from($key)
    };
}

/// A put of the number `$n` under `$key` in the map `$map`.
macro_rules! put {
    ($map:expr, $key:expr, $n:expr) => {
        $crate::storage::MapWrite::Put {
            map: $crate::storage::MapId::new($map),
            key: $crate::test_helpers::map_conformance::fixtures::key!($key),
            value: vec![$n],
        }
    };
}

/// A delete of `$key` in the map `$map`.
macro_rules! delete {
    ($map:expr, $key:expr) => {
        $crate::storage::MapWrite::Delete {
            map: $crate::storage::MapId::new($map),
            key: $crate::test_helpers::map_conformance::fixtures::key!($key),
        }
    };
}

/// A clear of the map `$map`.
macro_rules! clear {
    ($map:expr) => {
        $crate::storage::MapWrite::Clear { map: $crate::storage::MapId::new($map) }
    };
}

/// A read of each of `$keys` in the map `$map`.
macro_rules! get {
    ($map:expr, [$($key:expr),* $(,)?]) => {
        $crate::storage::MapRead::Get { map: $crate::storage::MapId::new($map), keys: vec![$($crate::test_helpers::map_conformance::fixtures::key!($key)),*] }
    };
}

/// A read of the first `$limit` entries of the map `$map` between bounds of
/// text, running in `$direction`.
macro_rules! range {
    ($map:expr, $start:expr, $end:expr, $direction:expr, $limit:expr) => {
        $crate::storage::MapRead::Range {
            map: $crate::storage::MapId::new($map),
            start: ::std::ops::Bound::map($start, |key: &str| $crate::test_helpers::map_conformance::fixtures::key!(key)),
            end: ::std::ops::Bound::map($end, |key: &str| $crate::test_helpers::map_conformance::fixtures::key!(key)),
            direction: $direction,
            limit: $limit,
        }
    };
}

/// A read of every entry the map `$map` holds, in key order, or in
/// `$direction`'s.
macro_rules! everything {
    ($map:expr) => {
        $crate::test_helpers::map_conformance::fixtures::everything!($map, $crate::storage::ScanDirection::Forward)
    };

    ($map:expr, $direction:expr) => {
        $crate::test_helpers::map_conformance::fixtures::range!(
            $map,
            ::std::ops::Bound::Unbounded,
            ::std::ops::Bound::Unbounded,
            $direction,
            usize::MAX
        )
    };
}

/// Entries as a range read gives them: each key's text with its number.
macro_rules! entries {
    ($($key:expr => $n:expr),* $(,)?) => {{
        let entries: Vec<(String, u8)> = vec![$((String::from($key), $n)),*];
        entries
    }};
}

/// The lock named `$name`.
macro_rules! lock {
    ($name:expr) => {
        $crate::storage::LockName::new($name)
    };
}

pub(crate) use {clear, delete, entries, everything, get, key, lock, put, range};

/// An entity's first state, from the seed `$seed`, and the event it follows,
/// for a storage transaction's own records: no other seed's entity shares
/// its id.
#[doc(hidden)]
#[macro_export]
macro_rules! __map_conformance_entity {
    ($seed:expr) => {{
        use $crate::property::backend::PropertyBackend;

        let backend = $crate::property::backend::LWWBackend::new();
        let event = $crate::test_helpers::commit_backend(&backend, $seed).expect("a genesis event");
        let state = $crate::proto::State {
            state_buffers: $crate::proto::StateBuffers(::std::collections::BTreeMap::from([(
                $crate::property::backend::LWWBackend::property_backend_name().to_owned(),
                backend.to_state_buffer().unwrap(),
            )])),
            memberships: ::std::collections::BTreeSet::new(),
            head: $crate::proto::Clock::singleton(&event),
        };

        let entity = $crate::proto::EntityState { entity_id: event.entity_id, state };
        ($crate::proto::Attested::opt(entity, None), $crate::proto::Attested::opt(event, None))
    }};
}

#[doc(inline)]
pub use crate::__map_conformance_entity as entity;

/// The one answer to a call of one read.
pub(super) fn only(mut rows: Vec<MapRows>) -> MapRows {
    assert_eq!(rows.len(), 1, "an engine answered one read with {} answers", rows.len());
    rows.remove(0)
}

/// The number each key a `Get` asked for holds.
pub(crate) fn found(rows: MapRows) -> Vec<Option<u8>> {
    match rows {
        MapRows::Got(values) => values.into_iter().map(|value| value.map(number)).collect(),
        MapRows::Ranged(_) => panic!("an engine answered a get with a range's entries"),
    }
}

/// The entries a `Range` gave, each key as text with its number.
pub(crate) fn ranged(rows: MapRows) -> Vec<(String, u8)> {
    match rows {
        MapRows::Ranged(entries) => {
            entries.into_iter().map(|(key, value)| (String::from_utf8(key).expect("a key the cases wrote"), number(value))).collect()
        }
        MapRows::Got(_) => panic!("an engine answered a range with a get's values"),
    }
}

/// The number a value the cases wrote holds.
fn number(value: Vec<u8>) -> u8 {
    match value[..] {
        [n] => n,
        _ => panic!("an engine gave back a value the cases didn't write: {value:?}"),
    }
}

/// Write `writes` in a transaction of their own, and commit it.
pub(super) async fn commit<E: StorageEngine>(engine: &E, writes: Vec<MapWrite>) -> anyhow::Result<()> {
    let mut trx = engine.transaction();
    trx.map_write(writes).await?;
    trx.commit().await?.committed()?;
    Ok(())
}

/// Answer `read` in a snapshot of its own.
pub(super) async fn committed<E: StorageEngine>(engine: &E, read: MapRead) -> Result<MapRows, RetrievalError> {
    Ok(only(engine.map_snapshot(&[]).await?.map_read(vec![read]).await?))
}

/// Take `lock`, write `writes` and commit, in a transaction of its own.
pub(super) async fn committed_under<E: StorageEngine>(engine: &E, lock: LockName, writes: Vec<MapWrite>) -> anyhow::Result<()> {
    let mut trx = engine.transaction();
    trx.map_lock_and_read(&[lock], vec![]).await?;
    trx.map_write(writes).await?;
    trx.commit().await?.committed()?;
    Ok(())
}

/// Answer `read` through a transaction, lent as core lends one.
pub(super) async fn read(trx: &mut dyn StorageTransaction, read: MapRead) -> Result<MapRows, MutationError> {
    Ok(only(trx.map_read(vec![read]).await?))
}

/// The name a call asked for out of order, and the one it had to come after.
pub(crate) fn out_of_order<T: std::fmt::Debug>(result: Result<T, MutationError>) -> (LockName, LockName) {
    match result {
        Err(MutationError::LockOutOfOrder { name, after }) => (name, after),
        other => panic!("expected a lock out of order, got {other:?}"),
    }
}

/// Poll `future` once, with a waker that does nothing: whether it is ready
/// yet, without waiting for it.
pub fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> { future.poll(&mut Context::from_waker(Waker::noop())) }

/// Far past any honest wait in a test: a deadline that turns a hang into a
/// failure, never one that orders events.
pub const HANG: Duration = Duration::from_secs(30);

/// A step that must not wait, as a broken store could leave it waiting
/// forever: what `step` gives, or, past [`HANG`], a failure that says what
/// was `waiting`. Its timer runs wherever an engine's tests run, a browser
/// included, as tokio's doesn't.
pub(crate) async fn before_hang<T>(waiting: &str, step: impl Future<Output = T>) -> T {
    match future::select(pin!(step), pin!(Delay::new(HANG))).await {
        Either::Left((done, _)) => done,
        Either::Right(_) => panic!("{waiting}: waited past {HANG:?}"),
    }
}
