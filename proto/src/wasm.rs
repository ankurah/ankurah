use js_sys;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;

use crate::AttestationSet;
use crate::StateBuffers;
use crate::{Clock, DecodeError, EventBody, EventId};

impl TryFrom<JsValue> for EventId {
    type Error = DecodeError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        let id_str = value.as_string().ok_or(DecodeError::NotStringValue)?;
        EventId::from_base64(&id_str)
    }
}

impl From<&EventId> for JsValue {
    fn from(val: &EventId) -> Self { val.to_base64().into() }
}

impl TryFrom<JsValue> for Clock {
    type Error = DecodeError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        let array: js_sys::Uint8Array = value.dyn_into().map_err(|_| DecodeError::InvalidFormat)?;
        let mut buffer = vec![0; array.length() as usize];
        array.copy_to(&mut buffer);
        let set: Clock = bincode::deserialize(&buffer).map_err(|_| DecodeError::InvalidFormat)?;

        Ok(set)
    }
}

impl TryFrom<&Clock> for JsValue {
    type Error = DecodeError;

    fn try_from(val: &Clock) -> Result<Self, Self::Error> {
        let buffer = bincode::serialize(&val).map_err(|_| DecodeError::InvalidFormat)?;
        let array = js_sys::Uint8Array::new_with_length(buffer.len() as u32);
        array.copy_from(&buffer);
        Ok(array.into())
    }
}

impl TryFrom<JsValue> for AttestationSet {
    type Error = DecodeError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        let array: js_sys::Uint8Array = value.dyn_into().map_err(|_| DecodeError::InvalidFormat)?;
        let mut buffer = vec![0; array.length() as usize];
        array.copy_to(&mut buffer);
        let set: AttestationSet = bincode::deserialize(&buffer).map_err(|_| DecodeError::InvalidFormat)?;

        Ok(set)
    }
}

impl TryFrom<&AttestationSet> for JsValue {
    type Error = DecodeError;

    fn try_from(val: &AttestationSet) -> Result<Self, Self::Error> {
        let buffer = bincode::serialize(&val).map_err(|_| DecodeError::InvalidFormat)?;
        let array = js_sys::Uint8Array::new_with_length(buffer.len() as u32);
        array.copy_from(&buffer);
        Ok(array.into())
    }
}

impl TryFrom<JsValue> for EventBody {
    type Error = DecodeError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        let array: js_sys::Uint8Array = value.dyn_into().map_err(|_| DecodeError::InvalidFormat)?;
        let mut buffer = vec![0; array.length() as usize];
        array.copy_to(&mut buffer);
        let body: EventBody = bincode::deserialize(&buffer).map_err(|_| DecodeError::InvalidFormat)?;

        Ok(body)
    }
}

impl TryFrom<&EventBody> for JsValue {
    type Error = DecodeError;

    fn try_from(val: &EventBody) -> Result<Self, Self::Error> {
        let buffer = bincode::serialize(&val).map_err(|_| DecodeError::InvalidFormat)?;
        let array = js_sys::Uint8Array::new_with_length(buffer.len() as u32);
        array.copy_from(&buffer);
        Ok(array.into())
    }
}

impl TryFrom<JsValue> for StateBuffers {
    type Error = DecodeError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        let array: js_sys::Uint8Array = value.dyn_into().map_err(|_| DecodeError::InvalidFormat)?;
        let mut buffer = vec![0; array.length() as usize];
        array.copy_to(&mut buffer);
        let set: StateBuffers = bincode::deserialize(&buffer).map_err(|_| DecodeError::InvalidFormat)?;

        Ok(set)
    }
}

impl TryFrom<&StateBuffers> for JsValue {
    type Error = DecodeError;

    fn try_from(val: &StateBuffers) -> Result<Self, Self::Error> {
        let buffer = bincode::serialize(&val).map_err(|_| DecodeError::InvalidFormat)?;
        let array = js_sys::Uint8Array::new_with_length(buffer.len() as u32);
        array.copy_from(&buffer);
        Ok(array.into())
    }
}
