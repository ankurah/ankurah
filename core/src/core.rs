use tokio::sync::mpsc;

use futures_signals::signal::Signal;

use crate::types::ID;

/// A model is a struct that represents the present values for a given record
/// Schema is defined primarily by the Model object, and the Record is derived from that via macro.
pub trait Model {
    //Lets assume that id is always type ID for now
    fn id(&self) -> ID;
}

/// A specific instance of a record in the collection
pub trait Record {
    type Model: Model;
    fn current(&self) -> &Self::Model;
}

/// A collection of records
pub trait Collection<M: Model> {
    type Record: Record<Model = M>;
    fn signal(&self) -> impl Signal<Item = CollectionChangeset<M>>;
}

/// A set of operations that have occurred on a collection
struct CollectionChangeset<M: Model> {
    pub operations: Vec<CollectionOperation<M>>,
}

enum CollectionOperation<R: Record> {
    // TODO
}

struct RecordChangeset<M: Model> {
    // TODO
}

enum RecordOperation<R: Record> {
    Create,
    Update(Vec<ValueChangeOperation<R::Model>>),
    Delete,
}

enum ValueChangeOperation<V: Value> {
    // TODO
}

// For simplicty, we're using futures_signals for now, but we will have to reimplement this later, because futures_signals doesn't support wasm-bindgen for javascript usage
// struct CollectionSignal<M: Model> {
//     channel: mpsc::Sender<CollectionChangeset<M>>,
// }

// struct RecordSignal<M: Model> {
//     channel: mpsc::Sender<RecordChangeset<M>>,
// }
// struct ValueSignal<V: Value> {
//     channel: mpsc::Sender<ValueChangeOperation<V>>,
// }

pub trait Value {}
