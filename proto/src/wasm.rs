use js_sys;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;

use crate::AttestationSet;
use crate::CollectionId;
use crate::StateBuffers;
use crate::{Clock, DecodeError, EntityId, EventId, OperationSet};

impl TryFrom<JsValue> for EntityId {
    type Error = DecodeError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        let id_str = value.as_string().ok_or(DecodeError::NotStringValue)?;
        EntityId::from_base64(&id_str)
    }
}

impl From<&EntityId> for JsValue {
    fn from(val: &EntityId) -> Self { val.to_base64().into() }
}

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
        // Convert JsValue to a JavaScript array
        let array: js_sys::Array = value.dyn_into().map_err(|_| DecodeError::InvalidFormat)?;

        // Convert each array element to an EventID
        let mut event_ids = Vec::new();
        for i in 0..array.length() {
            let id_str = array.get(i).as_string().ok_or(DecodeError::NotStringValue)?;
            let event_id = EventId::from_base64(&id_str)?;
            // binary search for the insertion point, and don't insert if it's already present
            let index = event_ids.binary_search(&event_id).unwrap_or_else(|i| i);
            if index == event_ids.len() || event_ids[index] != event_id {
                event_ids.insert(index, event_id);
            }
        }

        Ok(Clock(event_ids))
    }
}

impl From<&Clock> for JsValue {
    fn from(val: &Clock) -> Self {
        // Create a new JavaScript array
        let array = js_sys::Array::new();

        // Convert each EventID to base64 string and add to array
        for event_id in val.iter() {
            let base64_str = event_id.to_base64();
            array.push(&JsValue::from_str(&base64_str));
        }

        array.into()
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

impl TryFrom<JsValue> for OperationSet {
    type Error = DecodeError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        // let array: js_sys::Uint8Array = value.dyn_into().map_err(|_| DecodeError::InvalidFormat)?;
        // let mut buffer = vec![0; array.length() as usize];
        // array.copy_to(&mut buffer);
        let string = value.as_string().ok_or_else(|| DecodeError::InvalidFormat)?;
        let set: OperationSet = serde_json::from_str(&string).map_err(|_| DecodeError::InvalidFormat)?;

        Ok(set)
    }
}

impl TryFrom<&OperationSet> for JsValue {
    type Error = DecodeError;

    fn try_from(val: &OperationSet) -> Result<Self, Self::Error> {
        let string = serde_json::to_string(&val).map_err(|_| DecodeError::InvalidFormat)?;
        // let array = js_sys::Uint8Array::new_with_length(buffer.len() as u32);
        // array.copy_from(&buffer);
        // Ok(array.into())
        Ok(string.into())
    }
}

impl TryFrom<JsValue> for StateBuffers {
    type Error = DecodeError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        let string = value.as_string().ok_or_else(|| DecodeError::InvalidFormat)?;

        // let array: js_sys::Uint8Array = value.dyn_into().map_err(|_| DecodeError::InvalidFormat)?;
        // let mut buffer = vec![0; array.length() as usize];
        // array.copy_to(&mut buffer);
        let set: StateBuffers = serde_json::from_str(&string).map_err(|_| DecodeError::InvalidFormat)?;

        Ok(set)
    }
}

impl TryFrom<&StateBuffers> for JsValue {
    type Error = DecodeError;

    fn try_from(val: &StateBuffers) -> Result<Self, Self::Error> {
        // let buffer = bincode::serialize(&val).map_err(|_| DecodeError::InvalidFormat)?;
        let string = serde_json::to_string(&val).map_err(|_| DecodeError::InvalidFormat)?;
        // let array = js_sys::Uint8Array::new_with_length(buffer.len() as u32);
        // array.copy_from(&buffer);
        // Ok(array.into())
        Ok(string.into())
    }
}

impl TryFrom<JsValue> for CollectionId {
    type Error = DecodeError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> { Ok(value.as_string().ok_or(DecodeError::NotStringValue)?.into()) }
}

impl From<&CollectionId> for JsValue {
    fn from(val: &CollectionId) -> Self { val.as_str().into() }
}

impl From<CollectionId> for JsValue {
    fn from(val: CollectionId) -> Self {
        let s: String = val.into();
        s.into()
    }
}
