use crate::value::Value;
use wasm_bindgen::{JsCast, JsValue};

impl From<Value> for JsValue {
    fn from(value: Value) -> Self {
        match value {
            Value::String(s) => JsValue::from_str(&s),
            Value::I16(i) => JsValue::from_f64(i as f64),
            Value::I32(i) => JsValue::from_f64(i as f64),
            Value::I64(i) => JsValue::from_f64(i as f64),
            Value::F64(f) => JsValue::from_f64(f),
            Value::Bool(b) => JsValue::from_bool(b),
            Value::EntityId(entity_id) => JsValue::from_str(&entity_id.to_base64()),
            Value::Object(bytes) => js_sys::Uint8Array::from(&bytes[..]).into(),
            Value::Binary(bytes) => js_sys::Uint8Array::from(&bytes[..]).into(),
        }
    }
}

impl From<&Value> for JsValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::String(s) => JsValue::from_str(s),
            Value::I16(i) => JsValue::from_f64(*i as f64),
            Value::I32(i) => JsValue::from_f64(*i as f64),
            Value::I64(i) => JsValue::from_f64(*i as f64),
            Value::F64(f) => JsValue::from_f64(*f),
            Value::Bool(b) => JsValue::from_bool(*b),
            Value::EntityId(entity_id) => JsValue::from_str(&entity_id.to_base64()),
            Value::Object(bytes) => js_sys::Uint8Array::from(&bytes[..]).into(),
            Value::Binary(bytes) => js_sys::Uint8Array::from(&bytes[..]).into(),
        }
    }
}

impl TryFrom<JsValue> for Value {
    type Error = JsValue;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        if value.is_null() || value.is_undefined() {
            return Err(value);
        }

        // Try string first
        if let Some(s) = value.as_string() {
            return Ok(Value::String(s));
        }

        // Try boolean
        if let Some(b) = value.as_bool() {
            return Ok(Value::Bool(b));
        }

        // Try number
        if let Some(n) = value.as_f64() {
            // Use I32 as default for integers, only use I64 if value doesn't fit in I32
            if n.fract() == 0.0 {
                let n_int = n as i64;
                if n_int >= i32::MIN as i64 && n_int <= i32::MAX as i64 {
                    return Ok(Value::I32(n_int as i32));
                } else {
                    return Ok(Value::I64(n_int));
                }
            } else {
                return Ok(Value::F64(n));
            }
        }

        // Try Uint8Array (for binary data)
        if value.is_instance_of::<js_sys::Uint8Array>() {
            let array: js_sys::Uint8Array = value.unchecked_into();
            let mut bytes = vec![0u8; array.length() as usize];
            array.copy_to(&mut bytes);
            return Ok(Value::Binary(bytes));
        }

        // If nothing matches, return error
        Err(value)
    }
}
