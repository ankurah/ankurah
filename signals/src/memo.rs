use std::marker::PhantomData;
use std::sync::{Arc, RwLock};

use crate::{ObserverSet, ValueObserverSet};

pub struct MemoSignal<I, O: 'static, F: Fn(&I) -> O> {
    _upstream: PhantomData<I>,
    value: Arc<RwLock<O>>,
    observers: ObserverSet,
    _map_function: PhantomData<F>,
    value_observers: ValueObserverSet<O>,
}
