use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Clones share cancellation; each default value starts uncanceled.
#[derive(Clone, Default)]
pub(crate) struct CancelFlag(Arc<Cancellation>);

#[derive(Default)]
struct Cancellation {
    canceled: AtomicBool,
    changed: tokio::sync::Notify,
}

impl CancelFlag {
    pub(crate) fn cancel(&self) {
        self.0.canceled.store(true, Ordering::Release);
        self.0.changed.notify_waiters();
    }
    pub(crate) fn canceled(&self) -> bool { self.0.canceled.load(Ordering::Acquire) }

    pub(crate) async fn wait_for_cancel(&self) {
        let changed = self.0.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if !self.canceled() {
            changed.await;
        }
    }

    /// Cancel the old flag and replace this handle with a fresh uncanceled flag.
    pub(crate) fn cancel_and_swap(&mut self) { std::mem::take(self).cancel(); }
}
