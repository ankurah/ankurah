use crate::util::task::TaskHandle;

pub struct SubscriptionGuard {
    handle: TaskHandle,
}

impl SubscriptionGuard {
    pub fn new(handle: TaskHandle) -> Self { Self { handle } }
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) { self.handle.abort(); }
}

// Old Subscriber enum and SubscriberSet removed - now using tokio::broadcast
