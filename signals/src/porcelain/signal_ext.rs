use std::{
    fmt::{Debug, Display},
    hash::Hash,
};

use crate::{
    Peek,
    signal::{Signal, indexed::Indexed},
};

/// Derivation vocabulary available on every signal.
///
/// It is a blanket extension trait for the same reason [`crate::DynSubscribe`]
/// is one: what you can derive from a signal belongs to the signal
/// vocabulary, not to any one signal type. A combinator reachable only
/// through its own constructor is fine to write and awkward to discover;
/// this is where it becomes ordinary.
///
/// Each method carries its own bounds rather than the trait taking a value
/// parameter, so the trait stays implemented for everything while a method
/// applies only where its shape does -- and so a signal that lends more than
/// one kind of value never makes a call here ambiguous.
pub trait SignalExt: Signal + Clone + Sized + 'static {
    /// Index this signal's items by whatever `entry` reads off each one.
    ///
    /// The index takes this signal BY VALUE, because the index owns what it
    /// indexes: you hold the index, and it holds the collection. Keep your
    /// own handle by cloning first.
    ///
    /// See [`Indexed`] for what the resulting table promises -- notably that
    /// an item whose `entry` fails is skipped with a warning rather than
    /// poisoning the table or erroring the signal.
    fn index_by<Rows, Row, K, V, E, Entry>(self, entry: Entry) -> Indexed<Self, K, V>
    where
        Self: Peek<Rows>,
        Rows: 'static,
        Row: 'static,
        for<'a> &'a Rows: IntoIterator<Item = &'a Row>,
        Entry: Fn(&Row) -> Result<(K, V), E> + Send + Sync + 'static,
        E: Display,
        K: Eq + Hash + Debug + Send + Sync + 'static,
        V: Send + Sync + 'static,
    {
        Indexed::new(self, entry)
    }
}

impl<S> SignalExt for S where S: Signal + Clone + 'static {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        broadcast::{Broadcast, BroadcastId},
        signal::{Listener, ListenerGuard},
    };
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, PartialEq)]
    struct Row {
        id: u32,
        name: &'static str,
    }

    fn by_name(row: &Row) -> Result<(String, u32), &'static str> { Ok((row.name.to_owned(), row.id)) }

    /// A stand-in for the shape a live query presents: it materializes its
    /// items on demand from storage it shares with its writer, announces
    /// changes on its own broadcast, and lends nothing by reference -- so
    /// [`Peek`] is the only way in. Deliberately implements neither `With`
    /// nor `Get`, so this test cannot pass by some other route.
    #[derive(Clone)]
    struct FixtureQuery {
        rows: Arc<Mutex<Vec<Row>>>,
        broadcast: Broadcast<()>,
    }

    impl FixtureQuery {
        fn new(rows: Vec<Row>) -> Self { Self { rows: Arc::new(Mutex::new(rows)), broadcast: Broadcast::new() } }

        fn set(&self, rows: Vec<Row>) {
            *self.rows.lock().unwrap() = rows;
            self.broadcast.send(());
        }
    }

    impl Signal for FixtureQuery {
        fn listen(&self, listener: Listener) -> ListenerGuard { ListenerGuard::new(self.broadcast.reference().listen(listener)) }

        fn broadcast_id(&self) -> BroadcastId { self.broadcast.id() }
    }

    impl Peek<Vec<Row>> for FixtureQuery {
        fn peek(&self) -> Vec<Row> { self.rows.lock().unwrap().clone() }
    }

    #[test]
    fn a_query_shaped_upstream_gets_index_by_from_the_blanket() {
        let query = FixtureQuery::new(vec![Row { id: 1, name: "a" }, Row { id: 2, name: "b" }]);

        // No constructor named, no turbofish: the method arrives on a signal
        // that ankurah-signals has never heard of, through the blanket impl.
        let index = query.clone().index_by(by_name);

        assert_eq!(index.peek_lookup(&"a".to_owned()), Some(1));
        assert_eq!(index.peek_lookup(&"b".to_owned()), Some(2));

        query.set(vec![Row { id: 9, name: "a" }]);
        assert_eq!(index.peek_lookup(&"a".to_owned()), Some(9));
        assert_eq!(index.peek_lookup(&"b".to_owned()), None);
    }

    #[test]
    fn index_by_moves_its_upstream_in() {
        // The caller keeps no handle at all; the index is the thing held.
        let index = FixtureQuery::new(vec![Row { id: 1, name: "a" }]).index_by(by_name);
        assert_eq!(index.peek_lookup(&"a".to_owned()), Some(1));
    }

    #[test]
    fn the_trait_reaches_the_ordinary_signals_too() {
        use crate::signal::mutable::Mut;

        let rows = Mut::new(vec![Row { id: 1, name: "a" }]);
        let index = rows.read().index_by(by_name);
        assert_eq!(index.peek_lookup(&"a".to_owned()), Some(1));

        rows.set(vec![Row { id: 4, name: "a" }]);
        assert_eq!(index.peek_lookup(&"a".to_owned()), Some(4));
    }
}
