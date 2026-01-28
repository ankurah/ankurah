use ankurah_core::error::StorageError;
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
    pub fn get<T: TryFrom<JsValue>>(&self, key: &JsValue) -> Result<T, StorageError> {
        let v = js_sys::Reflect::get(&self.obj, key)
            .map_err(|_e| StorageError::BackendError(anyhow::anyhow!("Failed to get {}", key.as_string().unwrap_or_default()).into()))?;
        // if v.is_null() || v.is_undefined() {
        //     return Err(StorageError::BackendError(anyhow::anyhow!("Failed to get {}", key).into()));
        // }
        v.try_into()
            .map_err(|_e| StorageError::BackendError(anyhow::anyhow!("Failed to convert {}", key.as_string().unwrap_or_default()).into()))
    }
    pub fn get_opt<T: TryFrom<JsValue>>(&self, key: &JsValue) -> Result<Option<T>, StorageError> {
        let v = js_sys::Reflect::get(&self.obj, key)
            .map_err(|_e| StorageError::BackendError(anyhow::anyhow!("Failed to get {}", key.as_string().unwrap_or_default()).into()))?;
        if v.is_null() || v.is_undefined() {
            return Ok(None);
        }
        Ok(Some(v.try_into().map_err(|_e| {
            StorageError::BackendError(anyhow::anyhow!("Failed to convert {}", key.as_string().unwrap_or_default()).into())
        })?))
    }

    pub fn set<K, V>(&self, key: K, value: V) -> Result<bool, StorageError>
    where
        K: Into<JsValue>,
        V: TryInto<JsValue>,
        V::Error: std::error::Error + Send + Sync + 'static,
    {
        let js_key = key.into();
        let js_value = value.try_into().map_err(|e| StorageError::BackendError(Box::new(e)))?;
        js_sys::Reflect::set(&self.obj, &js_key, &js_value)
            .map_err(|_e| StorageError::BackendError(format!("Failed to set property: {}", js_value.as_string().unwrap_or_default()).into()))
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
