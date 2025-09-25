use anyhow::anyhow;
use wasm_bindgen::{JsCast, JsValue};

pub trait Require<T> {
    fn require(self, err: &'static str) -> anyhow::Result<T>;
}
impl<T> Require<T> for Result<T, JsValue> {
    fn require(self, err: &'static str) -> anyhow::Result<T> { Ok(self.map_err(|e| anyhow!("{} - {}", err, extract_message(e)))?) }
}
impl<T> Require<T> for Option<T> {
    fn require(self, err: &'static str) -> anyhow::Result<T> { Ok(self.ok_or(anyhow!("{} is None", err))?) }
}
impl<T> Require<T> for Result<Option<T>, JsValue> {
    fn require(self, err: &'static str) -> anyhow::Result<T> {
        match self {
            Ok(Some(res)) => Ok(res),
            Ok(None) => Err(anyhow!("{} is None", err)),
            Err(e) => Err(anyhow!("{} Err: {:?}", err, e)),
        }
    }
}
impl<T> Require<T> for Result<T, web_sys::Event> {
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
fn extract_message(err: JsValue) -> String {
    // If it's a JS Error object, grab its `message`
    if let Some(e) = err.dyn_ref::<js_sys::Error>() {
        return format!("{}: {}", e.name(), e.message());
    }

    // If it's already a string, convert directly
    if let Some(s) = err.as_string() {
        return s;
    }

    // Fallback: stringify the value
    js_sys::JSON::stringify(&err).ok().and_then(|s| s.as_string()).unwrap_or_else(|| format!("{:?}", err))
}
