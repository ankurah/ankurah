use std::sync::{Arc, RwLock};

use crate::subscription::{Subscriber, SubscriptionHandle};

pub trait Signal<T> {
    fn subscribe<S: Into<Subscriber<T>>>(&self, subscriber: S) -> SubscriptionHandle;
}

// pub(crate) trait Stateful<T> {
//     fn state(&self) -> Arc<RwLock<T>>;

//     // fn map<O, F: Fn(&T) -> O + Send + Sync + 'static>(&self, f: F) -> MapSignal<T, O, F>
//     // where Self: Sized {
//     //     MapSignal::new(self, f)
//     // }
// }

pub trait Get<T> {
    fn get(&self) -> T;
}
// pub trait WithValue<T> {
//     fn with_value<R>(&self, f: impl Fn(&T) -> R) -> R;
// }

pub trait Subscribe<T> {
    fn subscribe(&self, f: impl Fn(&T) + Send + Sync + 'static) -> SubscriptionHandle;
}

pub trait Notify: Send + Sync {
    fn notify(&self);
}

pub trait NotifyValue<T>: Send + Sync {
    fn notify(&self, value: &T);
}

pub trait CloneableNotify: Notify {
    fn clone_box(&self) -> Box<dyn CloneableNotify>;
}

impl<T: Notify + Clone + 'static> CloneableNotify for T {
    fn clone_box(&self) -> Box<dyn CloneableNotify> { Box::new(self.clone()) }
}
