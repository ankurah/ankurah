use anyhow::anyhow;
use wasm_bindgen::{JsCast, JsValue};

/// Helper trait a bit like expect, except it's tailored for wasm-bindgen use cases
/// This helps eliminate a lot of boilerplate code associated with error type conversions
pub trait WBGRequire<T> {
    fn require(self, err: &'static str) -> anyhow::Result<T>;
}
impl<T> WBGRequire<T> for Result<T, JsValue> {
    fn require(self, err: &'static str) -> anyhow::Result<T> {
        Ok(self.map_err(|e| anyhow!("{} - {}", err, crate::error::extract_message(e)))?)
    }
}
impl<T> WBGRequire<T> for Option<T> {
    fn require(self, err: &'static str) -> anyhow::Result<T> { Ok(self.ok_or(anyhow!("{} is None", err))?) }
}
impl<T> WBGRequire<T> for Result<Option<T>, JsValue> {
    fn require(self, err: &'static str) -> anyhow::Result<T> {
        match self {
            Ok(Some(res)) => Ok(res),
            Ok(None) => Err(anyhow!("{} is None", err)),
            Err(e) => Err(anyhow!("{} Err: {:?}", err, e)),
        }
    }
}
impl<T> WBGRequire<T> for Result<T, web_sys::Event> {
    fn require(self, err: &'static str) -> anyhow::Result<T> {
        self.map_err(|e| {
            // Try to extract more detailed error information
            if let Some(target) = e.target() {
                if let Ok(request) = target.dyn_into::<web_sys::IdbRequest>() {
                    if let Ok(error) = request.error() {
                        if let Some(dom_exception) = error {
                            anyhow!("{}: {} (code: {})", err, dom_exception.message(), dom_exception.code())
                        } else {
                            anyhow!("{}: Unknown error", err)
                        }
                    } else {
                        anyhow!("{}: No error object", err)
                    }
                } else {
                    anyhow!("{}: Event type {}", err, e.type_())
                }
            } else {
                anyhow!("{}: No event target", err)
            }
        })
    }
}
