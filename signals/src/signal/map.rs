use crate::core::Value;
use crate::subscription::{Subscriber, SubscriberSet};
use crate::traits::Get;
use std::sync::Arc;

/// A signal that is a map of another signal
/// Stateless.
pub struct Map<I, O: 'static, F: Fn(&I) -> O> {
    upstream: Value<I>,
    map_function: Arc<F>,
    subscribers: SubscriberSet<O>,
}

impl<I, O: 'static, F> Map<I, O, F>
where F: Fn(&I) -> O + Send + Sync + 'static
{
    // fn with_value<R>(&self, f: impl FnOnce(&O) -> R) -> R {
    //     // todo - figure out how to subscribe the current global observer to this signal
    //     self.input.with_value(|input| {
    //         let mapped = (self.map_function)(input);
    //         f(&mapped)
    //     })
    // }

    // pub fn subscribe<S>(&self, subscriber: S)
    // where S: Fn(&O) + Send + Sync + 'static {
    //     self.with_value(|value| subscriber(value));
    //     self.subscribers.write().unwrap().subscribe(Subscriber::Callback(Box::new(subscriber)));
    // }
}

// impl<I, O: 'static, F> Get<O> for Map<I, O, F>
// where
//     U: WithValue<I>,
//     F: Fn(&I) -> O + Send + Sync + 'static,
// {
//     fn get(&self) -> O { self.with_value(|v| v.clone()) }
// }

// impl<I, O: Clone + 'static, F> Stateful<O> for Map<I, O, F>
// where F: Fn(&I) -> O + Send + Sync + 'static
// {
//     fn value(&self) -> O { self.with_value(|v| v.clone()) }
// }

// impl<I, O: std::fmt::Display + 'static, F> std::fmt::Display for Map<I, O, F>
// where F: Fn(&I) -> O + Send + Sync + 'static
// {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.with_value(|v| write!(f, "{}", v)) }
// }
