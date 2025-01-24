use std::sync::{Arc, RwLock};

pub(crate) trait Stateful<T> {
    fn state(&self) -> Arc<RwLock<T>>;

    // fn map<O, F: Fn(&T) -> O + Send + Sync + 'static>(&self, f: F) -> MapSignal<T, O, F>
    // where Self: Sized {
    //     MapSignal::new(self, f)
    // }
}

pub trait Value<T> {
    fn value(&self) -> T;
}
pub trait WithValue<T> {
    fn with_value<R>(&self, f: impl Fn(&T) -> R) -> R;
}

pub trait Notify: Send + Sync {
    fn notify(&self);
}

pub trait Subscribe<T> {
    fn subscribe(&self, f: impl Fn(&T) + Send + Sync + 'static);
}
