use js_sys;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;

use crate::AttestationSet;
use crate::CollectionId;
use crate::StateBuffers;
use crate::{Clock, DecodeError, EntityId, EventId, GClock, OperationSet};

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

/// Decode one stored generation number from a JsValue, checked and
/// fail-loud: the ONE conversion rule for every IndexedDB generation read
/// (the entity record's GClock pairs below and the event column's
/// read_generation in the indexeddb engine). Numbers round-trip through JS
/// as f64 and a u32 is exactly representable, so the happy path is
/// lossless; anything else refuses typed rather than coercing (M4
/// remediation item 3 set the standard for the GClock decode; the M6
/// close-out extends it to the event column). NaN and the infinities fail
/// the fract() test (their fract is NaN), fractions fail it directly, and
/// the range test rejects negatives and values beyond u32::MAX; only an
/// exact u32 passes, so the `as` cast is lossless by construction. A
/// non-number decodes as InvalidFormat.
pub fn decode_generation(value: &JsValue) -> Result<u32, DecodeError> {
    let raw = value.as_f64().ok_or(DecodeError::InvalidFormat)?;
    if raw.fract() != 0.0 || !(0.0..=u32::MAX as f64).contains(&raw) {
        return Err(DecodeError::InvalidGeneration(raw));
    }
    Ok(raw as u32)
}

/// GClock is stored self-contained as an array of `[generation, eventIdBase64]`
/// pairs, one per head tip. The generation read routes through
/// [`decode_generation`], the shared checked conversion (M4 remediation
/// item 3: the saturating cast it replaced silently mapped NaN and
/// negatives to 0, truncated fractions, and clamped overflow,
/// contradicting the loud-failure discipline of the other engine homes).
/// Construction normalizes to the canonical order like every other GClock
/// path.
impl TryFrom<JsValue> for GClock {
    type Error = DecodeError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        let array: js_sys::Array = value.dyn_into().map_err(|_| DecodeError::InvalidFormat)?;
        let mut entries = Vec::new();
        for i in 0..array.length() {
            let pair: js_sys::Array = array.get(i).dyn_into().map_err(|_| DecodeError::InvalidFormat)?;
            let generation = decode_generation(&pair.get(0))?;
            let id_str = pair.get(1).as_string().ok_or(DecodeError::NotStringValue)?;
            entries.push((generation, EventId::from_base64(&id_str)?));
        }
        Ok(GClock::new(entries))
    }
}

impl From<&GClock> for JsValue {
    fn from(val: &GClock) -> Self {
        let array = js_sys::Array::new();
        for (generation, id) in val.iter() {
            let pair = js_sys::Array::new();
            pair.push(&JsValue::from_f64(*generation as f64));
            pair.push(&JsValue::from_str(&id.to_base64()));
            array.push(&pair);
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
        let array: js_sys::Uint8Array = value.dyn_into().map_err(|_| DecodeError::InvalidFormat)?;
        let mut buffer = vec![0; array.length() as usize];
        array.copy_to(&mut buffer);
        let set: OperationSet = bincode::deserialize(&buffer).map_err(|_| DecodeError::InvalidFormat)?;

        Ok(set)
    }
}

impl TryFrom<&OperationSet> for JsValue {
    type Error = DecodeError;

    fn try_from(val: &OperationSet) -> Result<Self, Self::Error> {
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
