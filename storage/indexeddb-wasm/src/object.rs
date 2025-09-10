use ankurah_core::error::{MutationError, RetrievalError};
use send_wrapper::SendWrapper;
use wasm_bindgen::JsValue;

pub struct Object {
    obj: SendWrapper<JsValue>,
}

impl std::ops::Deref for Object {
    type Target = JsValue;
    fn deref(&self) -> &Self::Target { &self.obj }
}

impl Object {
    pub fn new(obj: JsValue) -> Self { Self { obj: SendWrapper::new(obj) } }
    pub fn get(&self, key: &Property) -> Result<JsValue, RetrievalError> {
        js_sys::Reflect::get(&self.obj, key).map_err(|_e| RetrievalError::StorageError(anyhow::anyhow!("Failed to get {}", key).into()))
    }
    pub fn set<K, V>(&self, key: K, value: V) -> Result<bool, MutationError>
    where
        K: Into<JsValue>,
        V: TryInto<JsValue>,
        V::Error: std::error::Error + Send + Sync + 'static,
    {
        let js_key = key.into();
        let js_value = value.try_into().map_err(|e| MutationError::General(Box::new(e)))?;
        js_sys::Reflect::set(&self.obj, &js_key, &js_value)
            .map_err(|_e| MutationError::FailedToSetProperty("field", js_value.as_string().unwrap_or_default()))
    }
}

pub struct Property {
    key: SendWrapper<JsValue>,
    name: &'static str,
}

impl Property {
    pub fn new(key: &'static str) -> Self { Self { key: SendWrapper::new(key.into()), name: key } }
}
impl std::ops::Deref for Property {
    type Target = JsValue;
    fn deref(&self) -> &Self::Target { &self.key }
}
impl std::fmt::Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.name) }
}

impl From<&Property> for JsValue {
    fn from(prop: &Property) -> Self { (*prop.key).clone() }
}
