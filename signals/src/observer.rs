use std::sync::{Arc, RwLock};

use crate::traits::Notify;

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
    fn notify(&self) { (self.0.callback)(); }
}
