use std::future::Future;

#[cfg(not(target_arch = "wasm32"))]
/// Spawn a task and return a handle that can be used to abort it
pub fn spawn<F>(future: F) -> TaskHandle
where F: Future<Output = ()> + Send + 'static {
    let handle = tokio::spawn(future);
    TaskHandle::Tokio(handle)
}

#[cfg(target_arch = "wasm32")]
/// Spawn a task on WASM - returns a handle that does nothing when aborted
pub fn spawn<F>(future: F) -> TaskHandle
where F: Future<Output = ()> + 'static {
    wasm_bindgen_futures::spawn_local(future);
    TaskHandle::Wasm
}

/// A handle to a spawned task that can be aborted
pub enum TaskHandle {
    #[cfg(not(target_arch = "wasm32"))]
    Tokio(tokio::task::JoinHandle<()>),
    #[cfg(target_arch = "wasm32")]
    Wasm,
}

impl TaskHandle {
    /// Abort the task
    pub fn abort(&self) {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            TaskHandle::Tokio(handle) => handle.abort(),
            #[cfg(target_arch = "wasm32")]
            TaskHandle::Wasm => {
                // WASM tasks can't be aborted, but that's okay
                // They'll naturally terminate when receivers are dropped
            }
        }
    }
}
