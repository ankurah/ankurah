use std::sync::Arc;

// TODO create an inner that Both can share. then use deref to try to golf this a bit

pub struct ValueCell<T>(Arc<std::sync::RwLock<T>>);

/// A read-only value container that shares storage with Value<T>
pub struct ReadValueCell<T>(Arc<std::sync::RwLock<T>>);

impl<T> Clone for ValueCell<T> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<T> Clone for ReadValueCell<T> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<T> ValueCell<T> {
    pub fn new(value: T) -> Self { Self(Arc::new(std::sync::RwLock::new(value))) }

    pub fn set(&self, value: T) {
        let mut current = self.0.write().unwrap();
        *current = value;
    }
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let guard = self.0.read().unwrap();
        f(&*guard)
    }
    /// Calls a closure with a mutable borrow of the current value.
    /// The write lock is held only for the duration of the closure.
    pub fn with_mut<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        let mut guard = self.0.write().unwrap();
        f(&mut *guard)
    }
    #[allow(unused)]
    pub fn set_with<R>(&self, value: T, f: impl Fn(&T) -> R) -> R {
        let mut current = self.0.write().unwrap();
        *current = value;
        f(&*current)
    }

    /// Create a read-only view of this value
    pub fn readvalue(&self) -> ReadValueCell<T> { ReadValueCell(self.0.clone()) }
}

impl<T: Clone> ValueCell<T> {
    pub fn value(&self) -> T { self.0.read().unwrap().clone() }
}

impl<T> ReadValueCell<T> {
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let guard = self.0.read().unwrap();
        f(&*guard)
    }
}

impl<T: Clone> ReadValueCell<T> {
    pub fn value(&self) -> T { self.0.read().unwrap().clone() }
}
