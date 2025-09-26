use wasm_bindgen::{JsCast, JsValue};

pub(crate) fn extract_message(err: JsValue) -> String {
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
