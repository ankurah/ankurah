use std::sync::{Arc, RwLock};

use crate::{
    core::{CurrentContext, Value},
    subscription::{Subscriber, SubscriberSet, SubscriptionHandle},
    traits::{Get, Signal, WaitResult},
};

/// Read-only signal
pub struct Read<T> {
    pub(crate) value: Value<T>,
    pub(crate) subscribers: SubscriberSet<T>,
}

impl<T> Read<T> {
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        CurrentContext::track(self);
        self.value.with(f)
    }

    /// Wait for the signal to match a specific value
    ///
    /// This method waits until the signal's value equals the given target value using `PartialEq`.
    ///
    /// # Example
    /// ```rust
    /// # use ankurah_signals::*;
    /// # tokio_test::block_on(async {
    /// let signal = Mut::new(42); // Already at target value
    ///
    /// // Wait for the signal to reach the value 42 (already there)
    /// signal.read().wait_value(42).await;
    /// # });
    /// ```
    pub async fn wait_value(&self, target_value: T) -> ()
    where T: PartialEq + Clone + Send + Sync + 'static {
        // Check if current value already matches
        if self.value.with(|v| *v == target_value) {
            return;
        }

        // Create a notification channel
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        // Subscribe to notifications
        let _handle = self.subscribe(Subscriber::Notify(Box::new(tx)));

        // Loop over notifications until we find a match
        while rx.recv().await.is_some() {
            if self.value.with(|v| *v == target_value) {
                break;
            }
        }

        // Subscription handle drops here, automatically unsubscribing
    }

    /// Wait for the signal to reach a value matching the given predicate
    ///
    /// This method supports flexible return types through the [`WaitResult`] trait.
    ///
    /// # Example
    /// ```rust
    /// # use ankurah_signals::*;
    /// # tokio_test::block_on(async {
    /// let signal = Mut::new(11);
    ///
    /// // Wait for a boolean predicate - returns ()
    /// signal.read().wait_for(|&v| v > 5).await;
    ///
    /// // Wait for a the predicate and include a value in the response
    /// assert_eq!(1, signal.read().wait_for(|&v| {
    ///     let rem = v % 5;
    ///     if rem == 0 { None } else { Some(rem) }
    /// }).await);
    /// # });
    /// ```
    ///
    /// ## Behavior
    /// - The predicate is called with the current value immediately
    /// - If it returns a "ready" result (true, Some(value)), that result is returned
    /// - Otherwise, the method subscribes to signal updates and calls the predicate on each change
    /// - Returns when the predicate indicates readiness by returning true or Some(value)
    ///
    /// [`WaitResult`]: crate::traits::WaitResult
    pub async fn wait_for<F, R>(&self, predicate: F) -> R::Output
    where
        F: Fn(&T) -> R,
        R: WaitResult,
        T: Send + Sync,
    {
        // Check current value first
        if let Some(result) = self.with(|value| predicate(value).result()) {
            return result;
        }

        // Need to subscribe and wait
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

        // Keep the subscription handle alive for the duration of waiting
        let _handle = self.subscribe(Subscriber::Notify(Box::new(sender)));

        // Wait for notifications
        while let Some(()) = receiver.recv().await {
            if let Some(result) = self.with(|value| predicate(value).result()) {
                return result;
            }
        }

        // This should never happen since Read<T> cannot be dropped while we hold &self
        unreachable!("Subscription channel closed unexpectedly - this should not be possible");
    }
}

impl<T> Read<T>
where T: Clone
{
    pub fn value(&self) -> T { self.value.value() }
}

impl<T> Clone for Read<T> {
    fn clone(&self) -> Self { Self { value: self.value.clone(), subscribers: self.subscribers.clone() } }
}

impl<T: Clone> Get<T> for Read<T> {
    fn get(&self) -> T {
        CurrentContext::track(self);
        self.value.value()
    }
}

impl<T> Signal<T> for Read<T> {
    fn subscribe<S: Into<Subscriber<T>>>(&self, subscriber: S) -> SubscriptionHandle { self.subscribers.subscribe(subscriber) }
}

impl<T: std::fmt::Display> std::fmt::Display for Read<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.with(|v| write!(f, "{}", v)) }
}
