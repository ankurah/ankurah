use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    hash::Hash,
    sync::{Arc, RwLock},
};

use tracing::warn;

use crate::{
    Peek,
    broadcast::{Broadcast, BroadcastId},
    context::CurrentObserver,
    signal::{Get, Listener, ListenerGuard, Signal},
};

/// A keyed lookup table derived from an upstream collection signal, kept in
/// step with it without anyone maintaining it.
///
/// It exists so a consumer can address the items of a collection signal by
/// something other than their position -- a name, a label, a durable id --
/// and read that lookup at the moment it needs an answer, instead of running
/// a callback that folds items into a structure it then has to own, clear,
/// and keep from drifting.
///
/// The index OWNS its upstream: you hold the index, and the index holds the
/// thing it indexes.
///
/// It rebuilds WHOLE on the first read after the upstream notifies, so a
/// burst of upstream changes costs one rebuild and a quiet index costs
/// nothing. Rebuilding whole is also what keeps the table from drifting: it
/// is a pure function of the upstream's current items, so an item that leaves
/// the collection leaves the table, and an item whose key changed moves,
/// without anyone having written that logic or a reverse index to support it.
///
/// Reads of the upstream go through [`Peek`], deliberately. This index
/// manages its own subscription, so reading through a tracking accessor would
/// file a second, redundant dependency against whatever observer happens to
/// be running -- and would make this index's own untracked lookups track,
/// which is the one thing they promise not to do.
///
/// There is deliberately no `Subscribe` impl. Everything here is driven by
/// [`Peek`] and [`Signal::listen`].
///
/// Clones share one table and one subscription.
pub struct Indexed<Upstream, K, V>(Arc<Inner<Upstream, K, V>>);

/// The table and the notification that announces it, in one allocation.
///
/// They live together, and apart from [`Inner`], because the listener that
/// invalidates the table must also fire the notification, in that order, and
/// must not hold [`Inner`] -- which owns the guard that owns the listener.
struct Table<K, V> {
    /// The built table, or `None` when the upstream has notified since it was
    /// last built.
    cached: RwLock<Option<HashMap<K, V>>>,
    /// Fired after invalidation, never before.
    ///
    /// The index announces its own changes rather than re-exporting the
    /// upstream's, because a reader woken by the upstream directly could read
    /// this table before the invalidating listener had cleared it: listeners
    /// on one broadcast fire in whatever order the listener map iterates (see
    /// [`Broadcast::send`]), so "ours was installed first" is not an ordering
    /// anyone may rely on. Waking through this broadcast is strictly after
    /// the table went stale.
    notify: Broadcast<()>,
}

struct Inner<Upstream, K, V> {
    /// The signal being indexed, owned.
    source: Upstream,
    /// Reads the upstream's current items and folds them into a table. The
    /// item type is erased in here, which is what keeps [`Indexed`] to three
    /// parameters and therefore nameable in a struct field.
    rebuild: Box<dyn Fn(&Upstream) -> HashMap<K, V> + Send + Sync>,
    table: Arc<Table<K, V>>,
    /// Unsubscribes the invalidating listener when the last clone goes.
    _invalidator: ListenerGuard,
}

/// Fold the upstream's items into a table.
///
/// An item whose `entry` fails is LEFT OUT, and the failure is warned about,
/// naming the item type and whatever the caller's error says. One
/// half-readable item must not cost the table every other item alongside it,
/// and it must not turn the table into a `Result` that every caller then
/// unwraps for a condition that is one item's problem. Signals cannot name an
/// item's identity, so the caller's error is where that identity belongs.
///
/// Two items producing the same key are a contradiction this table cannot
/// resolve: the FIRST in upstream order wins, and the collision is warned
/// about. Where a key legitimately addresses more than one item, fold into a
/// collection value instead of reaching for this and hoping.
fn build<Rows, Row, K, V, E, Entry>(label: &str, rows: &Rows, entry: &Entry) -> HashMap<K, V>
where
    for<'a> &'a Rows: IntoIterator<Item = &'a Row>,
    Entry: Fn(&Row) -> Result<(K, V), E>,
    K: Eq + Hash + Debug,
    E: Display,
{
    let mut table = HashMap::new();
    for row in rows {
        match entry(row) {
            Ok((key, value)) => {
                if table.contains_key(&key) {
                    warn!("indexed {label}: key {key:?} addresses more than one item; keeping the first");
                    continue;
                }
                table.insert(key, value);
            }
            Err(error) => warn!("indexed {label}: skipping an item whose key could not be read: {error}"),
        }
    }
    table
}

impl<Upstream, K, V> Indexed<Upstream, K, V>
where
    Upstream: Signal + Clone + 'static,
    K: Eq + Hash + Debug + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Index `source`'s items by whatever `entry` reads off each one.
    ///
    /// `entry` returns the key AND the value to file under it, together, so
    /// an item is read once per rebuild rather than once per lookup, and so
    /// an item that cannot be read is dealt with in one place (see [`build`])
    /// instead of at every lookup that reaches it. That is also why the value
    /// is the caller's to choose: whatever decoding a lookup would otherwise
    /// repeat belongs inside this one fallible step.
    ///
    /// `entry` must not read this index. It runs while the table is being
    /// built, and the build holds the table's lock.
    pub fn new<Rows, Row, E, Entry>(source: Upstream, entry: Entry) -> Self
    where
        Upstream: Peek<Rows>,
        Rows: 'static,
        Row: 'static,
        for<'a> &'a Rows: IntoIterator<Item = &'a Row>,
        Entry: Fn(&Row) -> Result<(K, V), E> + Send + Sync + 'static,
        E: Display,
    {
        let label = std::any::type_name::<Row>();
        let rebuild: Box<dyn Fn(&Upstream) -> HashMap<K, V> + Send + Sync> =
            Box::new(move |source: &Upstream| build(label, &source.peek(), &entry));

        let table = Arc::new(Table { cached: RwLock::new(None), notify: Broadcast::new() });
        let invalidator = {
            let table = table.clone();
            source.listen(Arc::new(move |_| {
                // The write guard is a statement temporary and is released
                // here, before the notification: a woken reader rebuilds the
                // table, and would deadlock against a guard still held.
                *table.cached.write().unwrap() = None;
                table.notify.send(());
            }))
        };

        Self(Arc::new(Inner { source, rebuild, table, _invalidator: invalidator }))
    }

    /// The signal this index derives from.
    ///
    /// An index owns its upstream, so a caller with something to ask the
    /// upstream itself -- a live query's own liveness, say -- asks it here
    /// rather than keeping a second handle beside the index and having to
    /// keep the two in step.
    pub fn source(&self) -> &Upstream { &self.0.source }

    /// Ensure the table is built, then LEND it, registering nothing.
    ///
    /// This is what a reader whose question is not a single key asks -- a
    /// scan, a filter, a count of matching items. [`Peek::peek`] answers the
    /// same question by cloning the whole table, which is the wrong price for
    /// a reader that only wants to look.
    pub fn peek_table<R>(&self, f: impl FnOnce(&HashMap<K, V>) -> R) -> R { self.with_table(f) }

    /// Ensure the table is built, then lend it. Untracked: every caller that
    /// tracks does so explicitly, before calling this.
    fn with_table<R>(&self, f: impl FnOnce(&HashMap<K, V>) -> R) -> R {
        {
            let guard = self.0.table.cached.read().unwrap();
            if let Some(table) = guard.as_ref() {
                return f(table);
            }
        }

        let mut guard = self.0.table.cached.write().unwrap();
        // Re-check: another reader may have built it between the two locks.
        if guard.is_none() {
            *guard = Some((self.0.rebuild)(&self.0.source));
        }
        f(guard.as_ref().expect("the table was just built"))
    }

    /// The value filed under `key`, registering a dependency on this index
    /// with the current observer.
    ///
    /// The dependency is on the whole index, not on one key: a reader re-runs
    /// when any item changes. Coarse on purpose -- waking too often is
    /// correct, waking too rarely is not.
    pub fn lookup(&self, key: &K) -> Option<V>
    where V: Clone {
        CurrentObserver::track(self);
        self.with_table(|table| table.get(key).cloned())
    }

    /// The value filed under `key`, registering nothing.
    ///
    /// This is what a resolution path uses: turning a name into an identity
    /// in the middle of some unrelated work must not make that work depend on
    /// every item in the collection.
    pub fn peek_lookup(&self, key: &K) -> Option<V>
    where V: Clone {
        self.with_table(|table| table.get(key).cloned())
    }

    /// Whether any item is filed under `key`. Tracked, like [`Self::lookup`].
    pub fn contains_key(&self, key: &K) -> bool {
        CurrentObserver::track(self);
        self.with_table(|table| table.contains_key(key))
    }

    /// Whether any item is filed under `key`, registering nothing.
    pub fn peek_contains_key(&self, key: &K) -> bool { self.with_table(|table| table.contains_key(key)) }

    /// How many keys the table holds. Tracked.
    pub fn len(&self) -> usize {
        CurrentObserver::track(self);
        self.with_table(|table| table.len())
    }

    /// Whether the table holds no keys at all. Tracked.
    pub fn is_empty(&self) -> bool {
        CurrentObserver::track(self);
        self.with_table(|table| table.is_empty())
    }

    /// Every key the table holds, in arbitrary order. Tracked.
    pub fn keys(&self) -> Vec<K>
    where K: Clone {
        CurrentObserver::track(self);
        self.with_table(|table| table.keys().cloned().collect())
    }
}

impl<Upstream, K, V> Clone for Indexed<Upstream, K, V> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<Upstream, K, V> Signal for Indexed<Upstream, K, V> {
    fn listen(&self, listener: Listener) -> ListenerGuard { ListenerGuard::new(self.0.table.notify.reference().listen(listener)) }

    fn broadcast_id(&self) -> BroadcastId { self.0.table.notify.id() }
}

impl<Upstream, K, V> Peek<HashMap<K, V>> for Indexed<Upstream, K, V>
where
    Upstream: Signal + Clone + 'static,
    K: Eq + Hash + Debug + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// The whole table, cloned. Reach for [`Indexed::peek_lookup`] when one
    /// key is what you wanted.
    fn peek(&self) -> HashMap<K, V> { self.with_table(|table| table.clone()) }
}

impl<Upstream, K, V> Get<HashMap<K, V>> for Indexed<Upstream, K, V>
where
    Upstream: Signal + Clone + 'static,
    K: Eq + Hash + Debug + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// The whole table, cloned, and tracked. Reach for [`Indexed::lookup`]
    /// when one key is what you wanted.
    fn get(&self) -> HashMap<K, V> {
        CurrentObserver::track(self);
        self.with_table(|table| table.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CallbackObserver, signal::mutable::Mut};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Debug, PartialEq)]
    struct Row {
        id: u32,
        name: &'static str,
        /// A row this test declares half-written: its key cannot be read.
        readable: bool,
    }

    fn row(id: u32, name: &'static str) -> Row { Row { id, name, readable: true } }
    fn unreadable(id: u32, name: &'static str) -> Row { Row { id, name, readable: false } }

    /// The ordinary entry: name to id, refusing a half-written row.
    fn by_name(row: &Row) -> Result<(String, u32), &'static str> {
        if row.readable { Ok((row.name.to_owned(), row.id)) } else { Err("this row is half written") }
    }

    /// An entry that counts how many items it was asked to read, so a test
    /// can tell one rebuild from several without reaching inside the index.
    fn counting(counter: Arc<AtomicUsize>) -> impl Fn(&Row) -> Result<(String, u32), &'static str> + Send + Sync + 'static {
        move |row: &Row| {
            counter.fetch_add(1, Ordering::SeqCst);
            by_name(row)
        }
    }

    #[test]
    fn rebuilds_from_the_upstream_when_it_notifies() {
        let rows = Mut::new(vec![row(1, "a"), row(2, "b")]);
        let index = Indexed::new(rows.read(), by_name);

        assert_eq!(index.peek_lookup(&"a".to_owned()), Some(1));
        assert_eq!(index.peek_lookup(&"b".to_owned()), Some(2));

        // A whole rebuild is what makes both of these true at once: the
        // surviving row carries its new value, and the row that left the
        // collection leaves the table. An incrementally maintained index
        // would need a reverse index to get the second one right.
        rows.set(vec![row(9, "a")]);
        assert_eq!(index.peek_lookup(&"a".to_owned()), Some(9));
        assert_eq!(index.peek_lookup(&"b".to_owned()), None);
    }

    #[test]
    fn a_re_keyed_item_moves_rather_than_lingering_under_its_old_key() {
        let rows = Mut::new(vec![row(1, "before")]);
        let index = Indexed::new(rows.read(), by_name);
        assert_eq!(index.peek_lookup(&"before".to_owned()), Some(1));

        rows.set(vec![row(1, "after")]);
        assert_eq!(index.peek_lookup(&"after".to_owned()), Some(1));
        assert_eq!(index.peek_lookup(&"before".to_owned()), None);
    }

    #[test]
    fn a_burst_of_upstream_changes_costs_one_rebuild() {
        let reads = Arc::new(AtomicUsize::new(0));
        let rows = Mut::new(vec![row(1, "a"), row(2, "b")]);
        let index = Indexed::new(rows.read(), counting(reads.clone()));

        // Nothing is built until something asks.
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert_eq!(index.peek_lookup(&"a".to_owned()), Some(1));
        assert_eq!(reads.load(Ordering::SeqCst), 2);

        // Three upstream changes with no read between them.
        for id in 10..13 {
            rows.set(vec![row(id, "a"), row(id + 100, "b")]);
        }
        assert_eq!(reads.load(Ordering::SeqCst), 2, "invalidation alone must not rebuild");

        // One read, one rebuild, and it reflects the last change.
        assert_eq!(index.peek_lookup(&"a".to_owned()), Some(12));
        assert_eq!(reads.load(Ordering::SeqCst), 4);
        assert_eq!(index.peek_lookup(&"b".to_owned()), Some(112));
        assert_eq!(reads.load(Ordering::SeqCst), 4, "a second read of a warm table must not rebuild");
    }

    #[test]
    fn an_item_whose_key_cannot_be_read_is_skipped_and_the_rest_survive() {
        let rows = Mut::new(vec![row(1, "a"), unreadable(2, "b"), row(3, "c")]);
        let index = Indexed::new(rows.read(), by_name);

        assert_eq!(index.peek_lookup(&"a".to_owned()), Some(1));
        assert_eq!(index.peek_lookup(&"c".to_owned()), Some(3));
        assert_eq!(index.peek_lookup(&"b".to_owned()), None, "the half-written row contributes nothing");
        assert_eq!(index.len(), 2, "and costs no other row its place");
    }

    #[test]
    fn every_item_failing_yields_an_empty_table_rather_than_an_error() {
        let rows = Mut::new(vec![unreadable(1, "a"), unreadable(2, "b")]);
        let index = Indexed::new(rows.read(), by_name);

        assert!(index.is_empty());
        assert_eq!(index.peek_lookup(&"a".to_owned()), None);
    }

    #[test]
    fn two_items_under_one_key_keep_the_first() {
        let rows = Mut::new(vec![row(1, "same"), row(2, "same")]);
        let index = Indexed::new(rows.read(), by_name);

        assert_eq!(index.peek_lookup(&"same".to_owned()), Some(1));
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn peek_lookup_does_not_track_even_when_the_table_is_cold() {
        let rows = Mut::new(vec![row(1, "a")]);
        let index = Indexed::new(rows.read(), by_name);

        let runs = Arc::new(AtomicUsize::new(0));
        let observer = CallbackObserver::new(Arc::new({
            let (runs, index) = (runs.clone(), index.clone());
            move || {
                runs.fetch_add(1, Ordering::SeqCst);
                // The table has never been built at this point on the first
                // run, so this is the COLD path -- the one that would reach
                // the upstream through a tracking accessor under a design
                // built on `With`.
                let _ = index.peek_lookup(&"a".to_owned());
            }
        }));

        observer.trigger();
        assert_eq!(runs.load(Ordering::SeqCst), 1);

        rows.set(vec![row(2, "a")]);
        assert_eq!(runs.load(Ordering::SeqCst), 1, "an untracked lookup must not wake its reader");
    }

    #[test]
    fn lookup_tracks() {
        let rows = Mut::new(vec![row(1, "a")]);
        let index = Indexed::new(rows.read(), by_name);

        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observer = CallbackObserver::new(Arc::new({
            let (seen, index) = (seen.clone(), index.clone());
            move || seen.lock().unwrap().push(index.lookup(&"a".to_owned()))
        }));

        observer.trigger();
        rows.set(vec![row(2, "a")]);

        // The second run is what pins the ordering the index's own broadcast
        // buys: the reader wakes strictly after the table went stale, so it
        // reads the new value rather than the one it already had.
        assert_eq!(*seen.lock().unwrap(), vec![Some(1), Some(2)]);
    }

    #[test]
    fn the_lookup_surface_answers_from_one_table() {
        let rows = Mut::new(vec![row(1, "a"), row(2, "b")]);
        let index = Indexed::new(rows.read(), by_name);

        assert_eq!(index.lookup(&"a".to_owned()), Some(1));
        assert_eq!(index.peek_lookup(&"missing".to_owned()), None);
        assert!(index.contains_key(&"b".to_owned()));
        assert!(index.peek_contains_key(&"b".to_owned()));
        assert!(!index.peek_contains_key(&"missing".to_owned()));
        assert_eq!(index.len(), 2);
        assert!(!index.is_empty());

        let mut keys = index.keys();
        keys.sort();
        assert_eq!(keys, vec!["a".to_owned(), "b".to_owned()]);

        let whole = Peek::peek(&index);
        assert_eq!(whole.len(), 2);
        assert_eq!(whole.get("a"), Some(&1));
        assert_eq!(Get::get(&index), whole);
    }

    #[test]
    fn the_upstream_and_the_whole_table_are_reachable_without_cloning_either() {
        let rows = Mut::new(vec![row(1, "a"), row(2, "b")]);
        let index = Indexed::new(rows.read(), by_name);

        // The upstream the index moved in, still readable through it.
        assert_eq!(Peek::peek(index.source()).len(), 2);

        // A scan the lookup surface has no name for, answered without
        // cloning the table it scans.
        let odd = index.peek_table(|table| table.values().filter(|id| *id % 2 == 1).count());
        assert_eq!(odd, 1);
    }

    #[test]
    fn clones_share_one_table_and_one_subscription() {
        let reads = Arc::new(AtomicUsize::new(0));
        let rows = Mut::new(vec![row(1, "a"), row(2, "b")]);
        let index = Indexed::new(rows.read(), counting(reads.clone()));
        let clone = index.clone();

        assert_eq!(index.peek_lookup(&"a".to_owned()), Some(1));
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        assert_eq!(clone.peek_lookup(&"a".to_owned()), Some(1));
        assert_eq!(reads.load(Ordering::SeqCst), 2, "a clone reads the table the original built");

        rows.set(vec![row(7, "a"), row(8, "b")]);
        assert_eq!(clone.peek_lookup(&"a".to_owned()), Some(7));
        assert_eq!(index.peek_lookup(&"a".to_owned()), Some(7), "one invalidation, one rebuild, both handles");
        assert_eq!(reads.load(Ordering::SeqCst), 4);

        assert_eq!(index.broadcast_id(), clone.broadcast_id());
    }

    #[test]
    fn the_index_keeps_its_upstream_alive() {
        // The upstream moves in; the caller is free to drop its own handle
        // and keep only the index.
        let index = {
            let rows = Mut::new(vec![row(1, "a")]);
            Indexed::new(rows.read(), by_name)
        };
        assert_eq!(index.peek_lookup(&"a".to_owned()), Some(1));
    }
}
