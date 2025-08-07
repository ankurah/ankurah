use std::sync::Arc;

use crate::{
    context::CurrentObserver,
    porcelain::{Subscribe, SubscriptionGuard, subscribe::IntoSubscribeListener},
    signal::{Get, Signal, With},
};

/// A signal that transforms values from another signal on-demand without storing the transformed values
pub struct Map<Upstream, Input, Output, Transform> {
    source: Upstream,
    transform: Transform,
    _phantom: std::marker::PhantomData<(Input, Output)>,
}

impl<Upstream, Input, Output, Transform> Map<Upstream, Input, Output, Transform>
where
    Upstream: Signal + With<Input>,
    Transform: Fn(&Input) -> Output,
    Input: 'static,
    Output: 'static,
{
    pub fn new(source: Upstream, transform: Transform) -> Self { Self { source, transform, _phantom: std::marker::PhantomData } }
}

impl<Upstream, Input, Output, Transform> Clone for Map<Upstream, Input, Output, Transform>
where
    Upstream: Clone,
    Transform: Clone,
{
    fn clone(&self) -> Self { Self { source: self.source.clone(), transform: self.transform.clone(), _phantom: std::marker::PhantomData } }
}

impl<Upstream, Input, Output, Transform> Signal for Map<Upstream, Input, Output, Transform>
where Upstream: Signal
{
    fn listen(&self, listener: crate::broadcast::Listener) -> crate::broadcast::ListenerGuard { self.source.listen(listener) }

    fn broadcast_id(&self) -> crate::broadcast::BroadcastId { self.source.broadcast_id() }
}

// Not clear whether we should actually impl With<Output> for Map because each output value is owned
impl<Upstream, Input, Output, Transform> With<Output> for Map<Upstream, Input, Output, Transform>
where
    Upstream: Signal + With<Input>,
    Transform: Fn(&Input) -> Output,
    Input: 'static,
    Output: 'static,
{
    fn with<R>(&self, f: impl FnOnce(&Output) -> R) -> R {
        // Track the source signal with the current observer
        CurrentObserver::track(&self.source);
        // Get the source value and transform it on-demand
        self.source.with(|input| {
            let output = (self.transform)(input);
            f(&output)
        })
    }
}

impl<Upstream, Input, Output, Transform> Get<Output> for Map<Upstream, Input, Output, Transform>
where
    Upstream: Signal + With<Input>,
    Transform: Fn(&Input) -> Output,
    Input: 'static,
    Output: 'static,
{
    fn get(&self) -> Output {
        // Track the source signal with the current observer
        CurrentObserver::track(&self.source);
        // Get the source value and transform it on-demand, returning owned value
        self.source.with(|input| (self.transform)(input))
    }
}

impl<Upstream, Input, Output, Transform> Subscribe<Output> for Map<Upstream, Input, Output, Transform>
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

        let subscription = self.source.listen(Arc::new(move || {
            source.with(|input| {
                listener(transform(input));
            });
        }));

        SubscriptionGuard::new(subscription)
    }
}
