use std::future::Future;

#[cfg(not(target_arch = "wasm32"))]
/// Spawn a task
pub fn spawn<F>(future: F)
where
    F: Future + Send + 'static,
    <F as futures::Future>::Output: std::marker::Send,
{
    tokio::spawn(future);
}

#[cfg(target_arch = "wasm32")]
pub fn spawn<F>(future: F)
where F: Future<Output = ()> + Send + 'static {
    wasm_bindgen_futures::spawn_local(future);
}
