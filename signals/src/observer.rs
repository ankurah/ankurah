use std::sync::{Arc, RwLock, Weak};

use crate::{CurrentContext, ObserverContext, SubscriptionHandle, subscription::Subscriber, traits::Notify};

/// A RendererContext is an observer that wraps a renderer callback
/// which automatically contextualizes itself for render, including those which
/// are triggered by the observed values from previous renders
pub struct Renderer(Arc<RendererInner>);
pub struct RendererInner {
    renderer: Box<dyn Fn() + Send + Sync>,
    subscription_handles: RwLock<Vec<SubscriptionHandle<'static>>>,
}

/// An Observer automatically subscribes to all signals whose values are gotten while it is the current context
/// It calls the callback without contextualizing itself

#[cfg_attr(feature = "wasm", wasm_bindgen)]
pub struct Observer(Arc<ObserverInner>);

pub struct ObserverInner {
    callback: Box<dyn Fn() + Send + Sync>,
    subscription_handles: RwLock<Vec<SubscriptionHandle<'static>>>,
}

impl Renderer {
    /// Create a new renderer context from a callback
    pub fn new<F: Fn() + Send + Sync + 'static>(callback: F) -> Self {
        Self(Arc::new(RendererInner { renderer: Box::new(callback), subscription_handles: RwLock::new(vec![]) }))
    }
}
impl std::ops::Deref for Renderer {
    type Target = Arc<RendererInner>;
    fn deref(&self) -> &Self::Target { &self.0 }
}
impl RendererInner {
    /// Render the callback using this context
    pub fn render(self: &Arc<Self>) { self.with_context(&self.renderer); }
    pub fn with_context<F: Fn()>(self: &Arc<Self>, f: &F) {
        CurrentContext::set(self.clone());
        f();
        CurrentContext::unset();
    }
    pub fn subscriber<T>(self: &Arc<Self>) -> Subscriber<T> { Subscriber::Notify(Box::new(Arc::downgrade(self))) }
    pub fn clear(self: &Arc<Self>) { self.subscription_handles.write().unwrap().clear() }
    pub fn clone(self: &Arc<Self>) -> Renderer { Renderer(Arc::clone(&self)) }
}

impl Observer {
    /// Create a new observer from a callback - contextualizing will be managed externally
    pub fn new<F: Fn() + Send + Sync + 'static>(callback: Arc<F>) -> Self {
        Self(Arc::new(ObserverInner { callback: Box::new(move || callback()), subscription_handles: RwLock::new(vec![]) }))
    }
}

#[cfg_attr(feature = "wasm", wasm_bindgen)]
impl Observer {
    #[wasm_bindgen(constructor)]
    pub fn new_js(callback: &js_sys::Function) -> Self {
        Self(Arc::new(ObserverInner { callback: Box::new(move || callback()), subscription_handles: RwLock::new(vec![]) }))
    }
    pub fn start(&self) { CurrentContext::set(self.clone()); }
    pub fn finish(&self) { CurrentContext::unset(); }
}

impl std::ops::Deref for Observer {
    type Target = Arc<ObserverInner>;
    fn deref(&self) -> &Self::Target { &self.0 }
}
impl ObserverInner {
    pub fn subscriber<T>(self: &Arc<Self>) -> Subscriber<T> { Subscriber::Notify(Box::new(Arc::downgrade(self))) }
    pub fn clear(self: &Arc<Self>) { self.subscription_handles.write().unwrap().clear(); }

    /// Execute a function with this observer as the current context
    pub fn with_context<F: Fn()>(self: &Arc<Self>, f: &F) {
        CurrentContext::set(self.clone());
        f();
        CurrentContext::unset();
    }

    /// Notify all subscribers of this observer - contextualized to this observer
    pub fn trigger(self: &Arc<Self>) { self.with_context(&self.callback); }
    pub fn clone(self: &Arc<Self>) -> Observer { Observer(Arc::clone(&self)) }
}

impl Notify for Weak<ObserverInner> {
    fn notify(&self) {
        if let Some(inner) = self.upgrade() {
            inner.trigger();
        }
    }
}
impl Notify for Weak<RendererInner> {
    fn notify(&self) {
        if let Some(inner) = self.upgrade() {
            inner.render();
        }
    }
}

impl<T> Into<Subscriber<T>> for ObserverContext {
    fn into(self) -> Subscriber<T> {
        match self {
            ObserverContext::Renderer(renderer) => Subscriber::Notify(Box::new(Arc::downgrade(&renderer.0))),
            ObserverContext::Observer(observer) => Subscriber::Notify(Box::new(Arc::downgrade(&observer.0))),
        }
    }
}
impl<T> Into<Subscriber<T>> for &ObserverContext {
    fn into(self) -> Subscriber<T> {
        match self {
            ObserverContext::Renderer(renderer) => Subscriber::Notify(Box::new(Arc::downgrade(&renderer.0))),
            ObserverContext::Observer(observer) => Subscriber::Notify(Box::new(Arc::downgrade(&observer.0))),
        }
    }
}
