use anyhow::{anyhow, Result};
use js_sys::{Function, Object, Promise, Reflect};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{window, Navigator};

use super::require::WBGRequire; // .require trait

#[derive(Debug)]
pub struct NavigatorLock;

// The API schedules work on the JS event loop; it does not cross threads in wasm32, but
// trait bounds in our async stack may require Send. This is safe in wasm since there's a single thread.
unsafe impl Send for NavigatorLock {}
unsafe impl Sync for NavigatorLock {}

impl NavigatorLock {
    pub async fn with<F, Fut>(lock_name: &str, work: F) -> Result<()>
    where
        F: 'static + FnOnce() -> Fut + Send,
        Fut: 'static + core::future::Future<Output = Result<()>>,
    {
        // Wrap the non-Send internals so the outer future remains Send in async stacks that require it
        send_wrapper::SendWrapper::new(async move {
            let window = window().require("get window")?;
            let navigator: Navigator = window.navigator();

            // Check if locks API is available (requires secure context)
            let locks_result = Reflect::get(navigator.as_ref(), &JsValue::from_str("locks"));

            // If locks API is not available, just run the work without locking
            // This can happen in non-secure contexts (http:// on non-localhost)
            if locks_result.is_err() || locks_result.as_ref().unwrap().is_undefined() {
                tracing::warn!("Web Locks API not available (requires secure context), running without lock");
                return work().await;
            }

            let locks = locks_result.unwrap();

            let options = Object::new();
            Reflect::set(&options, &JsValue::from_str("mode"), &JsValue::from_str("exclusive")).ok();

            let work_js = wasm_bindgen::closure::Closure::once_into_js(move || {
                let fut = async move {
                    match work().await {
                        Ok(()) => Ok(JsValue::UNDEFINED),
                        Err(e) => Err(JsValue::from_str(&format!("{}", e))),
                    }
                };
                wasm_bindgen_futures::future_to_promise(fut)
            });

            let request_fn: Function = Reflect::get(locks.as_ref(), &JsValue::from_str("request"))
                .map_err(|e| anyhow!("locks.request not available: {:?}", e))?
                .unchecked_into();

            let promise_val = request_fn
                .call3(locks.as_ref(), &JsValue::from_str(lock_name), &options.into(), work_js.as_ref())
                .map_err(|e| anyhow!("locks.request call failed: {:?}", e))?;

            let promise: Promise = promise_val.unchecked_into();
            JsFuture::from(promise).await.map_err(|e| anyhow!("locks.request rejected: {:?}", e))?;

            Ok(())
        })
        .await
    }
}
