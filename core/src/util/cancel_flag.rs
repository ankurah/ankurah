use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;

/// Clones share cancellation; each default value starts uncanceled.
#[derive(Clone, Default)]
pub(crate) struct CancelFlag(Arc<AtomicBool>);

impl CancelFlag {
    pub(crate) fn cancel(&self) { self.0.store(true, Relaxed); }
    pub(crate) fn canceled(&self) -> bool { self.0.load(Relaxed) }

    /// Cancel the old flag and replace this handle with a fresh uncanceled flag.
    pub(crate) fn cancel_and_swap(&mut self) { std::mem::take(self).cancel(); }
}
