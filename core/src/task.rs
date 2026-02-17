use std::future::Future;

#[cfg(not(target_arch = "wasm32"))]
static RUNTIME_HANDLE: std::sync::OnceLock<tokio::runtime::Handle> = std::sync::OnceLock::new();

/// Inject a Tokio runtime handle for use by ankurah.
///
/// The handle is used as a fallback when `spawn` is called from threads
/// without a Tokio runtime context (e.g. GC/finalizer threads in FFI).
#[cfg(not(target_arch = "wasm32"))]
pub fn set_runtime_handle(handle: tokio::runtime::Handle) {
    RUNTIME_HANDLE.set(handle).ok();
}

/// Spawn a task.
///
/// On native, tries the thread-local runtime context first, then falls back
/// to the handle provided via [`set_runtime_handle`].
#[cfg(not(target_arch = "wasm32"))]
pub fn spawn<F>(future: F)
where
    F: Future + Send + 'static,
    <F as futures::Future>::Output: std::marker::Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            handle.spawn(future);
        }
        Err(_) => {
            RUNTIME_HANDLE
                .get()
                .expect("task::spawn: no Tokio runtime context on this thread. Call ankurah::set_runtime_handle() at init time.")
                .spawn(future);
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub fn spawn<F>(future: F)
where F: Future<Output = ()> + Send + 'static {
    wasm_bindgen_futures::spawn_local(future);
}
