use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::Notify;

#[derive(Clone)]
pub struct OneTimeFlag {
    inner: Arc<Inner>,
}

struct Inner {
    is_set: AtomicBool,
    notify: Notify,
}

impl OneTimeFlag {
    pub fn new() -> Self { Self { inner: Arc::new(Inner { is_set: AtomicBool::new(false), notify: Notify::new() }) } }

    pub fn set(&self) {
        if !self.inner.is_set.swap(true, Ordering::SeqCst) {
            self.inner.notify.notify_waiters();
        }
    }

    pub async fn wait(&self) {
        if self.inner.is_set.load(Ordering::SeqCst) {
            return;
        }
        self.inner.notify.notified().await;
    }

    pub fn is_set(&self) -> bool { self.inner.is_set.load(Ordering::SeqCst) }
}
