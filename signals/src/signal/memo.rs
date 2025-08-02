use crate::{
    core::{CurrentContext, Value},
    traits::{Get, Signal},
};
use std::{
    marker::PhantomData,
    sync::{Arc, RwLock},
};

/// Similar to a Map Signal, but it is stateful. Unfinished
pub struct Memo<I, O: 'static, F: Fn(&I) -> O> {
    function: F,
    upstream: Value<I>,
    value: Value<O>,
    pub(crate) broadcast: tokio::sync::broadcast::Sender<()>,
}

// impl<I, O: 'static, F: Fn(&I) -> O> Signal<O> for Memo<I, O, F> {}

// impl<I, O: Clone + 'static, F: Fn(&I) -> O> Get<O> for Memo<I, O, F> {
//     fn get(&self) -> O {
//         CurrentContext::track(self);
//         self.value.clone()
//     }
// }

// impl WithValue<I, O> for Memo<I, O, F> {
//     fn with_value(&self, f: impl Fn(&I) -> O) -> O { f(&self.value.read().unwrap()) }
// }
