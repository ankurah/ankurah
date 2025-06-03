use std::sync::{Arc, OnceLock};
use tokio::sync::Notify;

#[derive(Clone)]
pub struct OneTimeValue<T> {
    inner: Arc<Inner<T>>,
}

struct Inner<T> {
    value: OnceLock<T>,
    notify: Notify,
}

impl<T: Clone> OneTimeValue<T> {
    pub fn new() -> Self { Self { inner: Arc::new(Inner { value: OnceLock::new(), notify: Notify::new() }) } }

    pub fn set(&self, value: T) -> Result<(), T> {
        match self.inner.value.set(value) {
            Ok(()) => {
                self.inner.notify.notify_waiters();
                Ok(())
            }
            Err(value) => Err(value),
        }
    }

    pub async fn wait(&self) -> T {
        if let Some(value) = self.inner.value.get() {
            return value.clone();
        }

        loop {
            self.inner.notify.notified().await;
            if let Some(value) = self.inner.value.get() {
                return value.clone();
            }
        }
    }

    pub fn get(&self) -> Option<T> { self.inner.value.get().cloned() }

    pub fn is_set(&self) -> bool { self.inner.value.get().is_some() }
}
