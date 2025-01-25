use crate::Notify;
use std::sync::{Arc, RwLock, Weak};

pub struct ObserverSet(RwLock<Vec<Weak<ObserverInner>>>);

pub struct Observer(Arc<ObserverInner>);
struct ObserverInner {
    callback: Box<dyn Fn() + Send + Sync>,
}

// impl Notify for ObserverInner {
//     fn notify(&self) { (self.callback)(); }
// }

static GLOBAL_OBSERVER: RwLock<Option<Observer>> = RwLock::new(None);

impl Observer {
    pub fn new<F: Fn() + Send + Sync + 'static>(callback: Arc<F>) -> Self {
        Self(Arc::new(ObserverInner { callback: Box::new(move || callback()) }))
    }

    pub fn set_global(&self) {
        //
        *GLOBAL_OBSERVER.write().unwrap() = Some(Observer(Arc::clone(&self.0)));
    }

    pub fn unset_global(&self) { *GLOBAL_OBSERVER.write().unwrap() = None; }

    /// Execute a function with the current global observer, if set
    pub fn with_current<F: FnOnce(Option<Self>)>(f: F) {
        if let Some(observer) = GLOBAL_OBSERVER.read().unwrap().as_ref() {
            f(Some(Observer(Arc::clone(&observer.0))));
        }
    }
}
impl ObserverInner {
    fn notify(&self) { (self.callback)(); }
}

impl ObserverSet {
    pub fn new() -> Self { Self(RwLock::new(Vec::new())) }
    pub fn notify(&self) {
        let observers = self.0.read().unwrap();
        for observer in observers.iter() {
            if let Some(observer) = observer.upgrade() {
                observer.notify();
            }
        }
    }
    pub(crate) fn track_current(&self) {
        Observer::with_current(|observer| {
            if let Some(observer) = observer {
                let mut observers = self.0.write().unwrap();
                let ptr = Arc::as_ptr(&observer.0);
                // binary search by ptr address directly on Weak pointers
                match observers.binary_search_by(|o| o.as_ptr().cmp(&ptr)) {
                    Ok(_) => (), // Already exists, do nothing
                    Err(pos) => observers.insert(pos, Arc::downgrade(&observer.0)),
                }
            }
        });
    }
}
