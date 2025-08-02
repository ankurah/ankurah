/// Core trait for signals - provides observation capability
pub trait Signal<T: 'static> {
    /// Get a receiver for observing changes to this signal
    fn observe(&self) -> tokio::sync::broadcast::Receiver<()>;

    /// Subscribe to changes with any type that implements Subscriber
    fn subscribe<S>(&self, subscriber: S) -> crate::SubscriptionGuard
    where
        S: Subscriber<T>,
        Self: Sized + Clone + Get<T> + Send + 'static,
    {
        subscriber.subscribe(self)
    }
}

/// Core trait for signal observers - dyn safe
pub trait Observer {
    /// Observe a signal - takes a type-erased receiver
    fn observe_signal_receiver(&self, receiver: tokio::sync::broadcast::Receiver<()>);

    /// Get a unique identifier for this observer (for equality comparison)
    fn observer_id(&self) -> usize;
}

/// Trait for types that can subscribe to signal changes
pub trait Subscriber<T: 'static> {
    /// Subscribe to signal changes, spawning any necessary tasks
    /// Returns a handle that can be used to unsubscribe
    fn subscribe<S: Signal<T> + Clone + crate::traits::Get<T> + Send + 'static>(self, signal: &S) -> crate::SubscriptionGuard;
}

// Implementation for closures that take T by value (owned)
impl<T, F> Subscriber<T> for F
where
    T: Clone + Send + Sync + 'static,
    F: Fn(T) + Send + Sync + 'static,
{
    fn subscribe<S: Signal<T> + Clone + crate::traits::Get<T> + Send + 'static>(self, signal: &S) -> crate::SubscriptionGuard {
        let mut receiver = signal.observe();
        let signal = signal.clone();

        let handle = crate::util::task::spawn(async move {
            use tokio::sync::broadcast::error::RecvError;
            loop {
                match receiver.recv().await {
                    Ok(_) | Err(RecvError::Lagged(_)) => {
                        self(signal.get());
                    }
                    Err(RecvError::Closed) => {
                        break;
                    }
                }
            }
        });

        crate::SubscriptionGuard::new(handle)
    }
}

// Implementation for tokio unbounded sender
impl<T> Subscriber<T> for tokio::sync::mpsc::UnboundedSender<T>
where T: Clone + Send + Sync + 'static
{
    fn subscribe<S: Signal<T> + Clone + crate::traits::Get<T> + Send + 'static>(self, signal: &S) -> crate::SubscriptionGuard {
        let mut receiver = signal.observe();
        let signal = signal.clone();

        let handle = crate::util::task::spawn(async move {
            use tokio::sync::broadcast::error::RecvError;
            loop {
                match receiver.recv().await {
                    Ok(_) | Err(RecvError::Lagged(_)) => {
                        let value = signal.get();
                        if self.send(value).is_err() {
                            // Receiver was dropped, end task
                            break;
                        }
                    }
                    Err(RecvError::Closed) => {
                        break;
                    }
                }
            }
        });

        crate::SubscriptionGuard::new(handle)
    }
}

pub trait Get<T: 'static> {
    fn get(&self) -> T;
}

pub trait Notify {
    fn notify(&self);
}

pub trait NotifyValue<T: 'static> {
    fn notify(&self, value: &T);
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

/// Simple Notify implementation for tokio unbounded sender
impl Notify for tokio::sync::mpsc::UnboundedSender<()> {
    fn notify(&self) { let _ = self.send(()); }
}
