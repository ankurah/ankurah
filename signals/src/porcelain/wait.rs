use crate::signal::{GetReadCell, Signal};

/// Trait for waiting on signal values asynchronously
pub trait Wait<T: 'static> {
    /// Wait for the signal to match a specific value
    fn wait_value(&self, target_value: T) -> impl std::future::Future<Output = ()> + Send
    where T: PartialEq + Clone + Send + Sync;

    /// Wait for the signal to reach a value matching the given predicate
    fn wait_for<F, R>(&self, predicate: F) -> impl std::future::Future<Output = R::Output> + Send
    where
        F: Fn(&T) -> R + Send + Sync + 'static,
        R: WaitResult,
        T: Send + Sync;
}
/// Helper trait for `wait_for` to allow flexible predicate return types.
///
/// ## Semantics
/// - `result()` returns `Some(output)` to stop waiting and return `output`
/// - `result()` returns `None` to continue waiting for the next signal update
pub trait WaitResult {
    type Output;
    /// Returns Some(output) if we should stop waiting, None if we should continue
    fn result(self) -> Option<Self::Output>;
}

// Blanket impl for bool: true = stop with (), false = continue waiting
impl WaitResult for bool {
    type Output = ();
    fn result(self) -> Option<Self::Output> { if self { Some(()) } else { None } }
}

// Blanket impl for Option<T>: Some(value) = stop with value, None = continue waiting
impl<T> WaitResult for Option<T> {
    type Output = T;
    fn result(self) -> Option<Self::Output> { self }
}

// Blanket implementation of Wait for anything that implements Signal and GetRoValue
#[cfg(feature = "tokio")]
impl<T, S> Wait<T> for S
where
    S: Signal + GetReadCell<T> + Sync,
    T: Clone + Send + Sync + 'static,
{
    fn wait_value(&self, target_value: T) -> impl std::future::Future<Output = ()> + Send
    where T: PartialEq + Clone + Send + Sync {
        async move {
            // Check if current value already matches

            use std::sync::Arc;
            if self.get_readcell().with(|v| *v == target_value) {
                return;
            }

            // Create a channel to bridge sync broadcast to async
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

            // Subscribe to change notifications
            let _subscription = self.listen(Arc::new(move |_| {
                let _ = tx.send(());
            }));

            // Loop over notifications until we find a match
            loop {
                match rx.recv().await {
                    Some(_) => {
                        if self.get_readcell().with(|v| *v == target_value) {
                            break;
                        }
                    }
                    None => {
                        // Channel was closed, stop waiting
                        break;
                    }
                }
            }
        }
    }

    fn wait_for<F, R>(&self, predicate: F) -> impl std::future::Future<Output = R::Output> + Send
    where
        F: Fn(&T) -> R + Send + Sync + 'static,
        R: WaitResult,
        T: Send + Sync,
    {
        async move {
            // Check current value first

            use std::sync::Arc;
            if let Some(result) = self.get_readcell().with(|value| predicate(value).result()) {
                return result;
            }

            // Create a channel to bridge sync broadcast to async
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

            // Subscribe to change notifications
            let _subscription = self.listen(Arc::new(move |_| {
                let _ = tx.send(());
            }));

            // Wait for notifications
            loop {
                match rx.recv().await {
                    Some(_) => {
                        if let Some(result) = self.get_readcell().with(|value| predicate(value).result()) {
                            return result;
                        }
                    }
                    None => {
                        // Channel was closed, this should not happen since we hold &self
                        break;
                    }
                }
            }

            // This should never happen since the signal cannot be dropped while we hold &self
            unreachable!("Subscription channel closed unexpectedly - this should not be possible");
        }
    }
}
