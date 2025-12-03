use std::sync::{Arc, RwLock};

use crate::{
    Peek,
    context::CurrentObserver,
    porcelain::{Subscribe, SubscriptionGuard, subscribe::IntoSubscribeListener},
    signal::{Get, Listener, ListenerGuard, Signal, With},
};

/// Like Map, but caches the transformed value until upstream notifies.
/// Useful when transform is expensive, or when output identity matters (e.g. JsValue for React).
pub struct Memo<Upstream, Input, Output, Transform> {
    source: Upstream,
    transform: Transform,
    /// Cached output - None means invalidated, needs recompute
    cached: Arc<RwLock<Option<Output>>>,
    /// Keeps subscription to upstream alive - invalidates cache on notify
    _subscription: ListenerGuard,
    _phantom: std::marker::PhantomData<Input>,
}

impl<Upstream, Input, Output, Transform> Memo<Upstream, Input, Output, Transform>
where
    Upstream: Signal + With<Input> + Clone,
    Transform: Fn(&Input) -> Output,
    Input: 'static,
    Output: Send + Sync + 'static,
{
    pub fn new(source: Upstream, transform: Transform) -> Self {
        let cached: Arc<RwLock<Option<Output>>> = Arc::new(RwLock::new(None));

        // Subscribe to upstream - invalidate cache on notify
        let cached_ref = cached.clone();
        let subscription = source.listen(Arc::new(move |_| {
            *cached_ref.write().unwrap() = None;
        }));

        Self { source, transform, cached, _subscription: subscription, _phantom: std::marker::PhantomData }
    }

    /// Ensure cache is populated, then call f with a reference to the cached value
    fn with_cached<R>(&self, f: impl FnOnce(&Output) -> R) -> R {
        // Fast path: check if cached
        {
            let guard = self.cached.read().unwrap();
            if let Some(ref value) = *guard {
                return f(value);
            }
        }

        // Slow path: compute and cache
        let mut guard = self.cached.write().unwrap();
        // Double-check after acquiring write lock
        if guard.is_none() {
            let output = self.source.with(|input| (self.transform)(input));
            *guard = Some(output);
        }

        f(guard.as_ref().unwrap())
    }
}

impl<Upstream, Input, Output, Transform> Clone for Memo<Upstream, Input, Output, Transform>
where
    Upstream: Signal + With<Input> + Clone,
    Transform: Fn(&Input) -> Output + Clone,
    Input: 'static,
    Output: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        // Create a new Memo with fresh subscription - shares source but has own cache
        Self::new(self.source.clone(), self.transform.clone())
    }
}

impl<Upstream, Input, Output, Transform> Signal for Memo<Upstream, Input, Output, Transform>
where Upstream: Signal
{
    fn listen(&self, listener: Listener) -> ListenerGuard { self.source.listen(listener) }

    fn broadcast_id(&self) -> crate::broadcast::BroadcastId { self.source.broadcast_id() }
}

impl<Upstream, Input, Output, Transform> With<Output> for Memo<Upstream, Input, Output, Transform>
where
    Upstream: Signal + With<Input> + Clone,
    Transform: Fn(&Input) -> Output,
    Input: 'static,
    Output: Send + Sync + 'static,
{
    fn with<R>(&self, f: impl FnOnce(&Output) -> R) -> R {
        CurrentObserver::track(&self.source);
        self.with_cached(f)
    }
}

impl<Upstream, Input, Output, Transform> Get<Output> for Memo<Upstream, Input, Output, Transform>
where
    Upstream: Signal + With<Input> + Clone,
    Transform: Fn(&Input) -> Output,
    Input: 'static,
    Output: Clone + Send + Sync + 'static,
{
    fn get(&self) -> Output {
        CurrentObserver::track(&self.source);
        self.with_cached(|v| v.clone())
    }
}

impl<Upstream, Input, Output, Transform> Peek<Output> for Memo<Upstream, Input, Output, Transform>
where
    Upstream: Signal + With<Input> + Clone,
    Transform: Fn(&Input) -> Output,
    Input: 'static,
    Output: Clone + Send + Sync + 'static,
{
    fn peek(&self) -> Output { self.with_cached(|v| v.clone()) }
}

impl<Upstream, Input, Output, Transform> Subscribe<Output> for Memo<Upstream, Input, Output, Transform>
where
    Upstream: Signal + With<Input> + Clone + Send + Sync + 'static,
    Transform: Fn(&Input) -> Output + Send + Sync + Clone + 'static,
    Input: Send + Sync + 'static,
    Output: Clone + Send + Sync + 'static,
{
    fn subscribe<L>(&self, listener: L) -> SubscriptionGuard
    where L: IntoSubscribeListener<Output> {
        let listener = listener.into_subscribe_listener();
        let source = self.source.clone();
        let transform = self.transform.clone();
        let cached = self.cached.clone();

        let subscription = self.source.listen(Arc::new(move |_| {
            // Invalidate and recompute
            let output = source.with(|input| transform(input));
            *cached.write().unwrap() = Some(output.clone());
            listener(output);
        }));

        SubscriptionGuard::new(subscription)
    }
}
