use std::sync::{Arc, RwLock, Weak};

use crate::{subscription::Subscriber, traits::Notify};

pub struct Observer(Arc<Inner>);
struct Inner {
    callback: Box<dyn Fn() + Send + Sync>,
}

impl Observer {
    pub fn new<F: Fn() + Send + Sync + 'static>(callback: Arc<F>) -> Self {
        Self(Arc::new(Inner { callback: Box::new(move || callback()) }))
    }

    // /// Execute a function with the current global observer, if set
    // pub fn with_context<F: FnOnce(Option<Self>)>(f: F) {
    //     if let Some(observer) = OBSERVER_CONTEXT.read().unwrap().as_ref() {
    //         f(Some(Observer(Arc::clone(&observer.0))));
    //     }
    // }
}

impl Notify for Observer {
    fn notify(&self) {
        println!("DEBUG: Observer notified");
        (self.0.callback)();
    }
}
impl Notify for Inner {
    fn notify(&self) {
        println!("DEBUG: Inner observer notified");
        (self.callback)();
    }
}

impl<T> Into<Subscriber<T>> for Observer {
    fn into(self) -> Subscriber<T> { Subscriber::Notify(Arc::downgrade(&self.0) as Weak<dyn Notify>) }
}
impl<T> Into<Subscriber<T>> for &Observer {
    fn into(self) -> Subscriber<T> { Subscriber::Notify(Arc::downgrade(&self.0) as Weak<dyn Notify>) }
}

impl Clone for Observer {
    fn clone(&self) -> Self { Self(Arc::clone(&self.0)) }
}
