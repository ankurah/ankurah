//! Canonical, ascending key encoding. Every part is self-delimiting, including
//! strings containing NUL, so a tuple prefix names one contiguous address range.

use ankurah_core_types::{EntityId, IndexKeyPart, KeySpec, PropertyId, Value, ValueType};

use crate::error::TreeError;

/// This crate's canonical key codec. Missing values sort before present
/// ones and do not change admission. NaNs and JSON ordering are unsupported;
/// floating zero has one encoding, and strings use UTF-8 byte order.
///
/// The type has no values: it names the codec its functions belong to.
pub enum CanonicalCodec {}

impl CanonicalCodec {
    /// Encode a tuple prefix, explicitly representing missing/null values.
    pub fn encode_nullable(spec: &KeySpec<PropertyId>, values: &[Option<Value>]) -> Result<Vec<u8>, TreeError> {
        if values.len() > spec.keyparts.len() {
            return Err(TreeError::InvalidKey("too many tuple parts"));
        }
        let mut bytes = Vec::new();
        for (part, value) in spec.keyparts.iter().zip(values) {
            bytes.extend(encode_part(part, value.as_ref())?);
        }
        Ok(bytes)
    }
    /// Encode the first `values.len()` parts of a key spec.
    pub fn encode_prefix(spec: &KeySpec<PropertyId>, values: &[Value]) -> Result<Vec<u8>, TreeError> {
        if values.len() > spec.keyparts.len() {
            return Err(TreeError::InvalidKey("too many tuple parts"));
        }
        let mut bytes = Vec::new();
        for (part, value) in spec.keyparts.iter().zip(values) {
            bytes.extend(encode_part(part, Some(value))?);
        }
        Ok(bytes)
    }
    /// Split a complete key into its self-delimiting encoded parts, rejecting
    /// malformed, noncanonical or trailing bytes.
    pub fn split<'a>(spec: &KeySpec<PropertyId>, key: &'a [u8]) -> Result<Vec<&'a [u8]>, TreeError> {
        let mut parts = Vec::with_capacity(spec.keyparts.len());
        Self::walk(spec, key, |part| parts.push(part))?;
        Ok(parts)
    }

    /// Check that `key` is a complete key of `spec`, refusing what
    /// [`CanonicalCodec::split`] refuses, without allocating: for a caller
    /// that validates many keys and needs none of their parts.
    pub fn check(spec: &KeySpec<PropertyId>, key: &[u8]) -> Result<(), TreeError> { Self::walk(spec, key, |_| {}) }

    /// Hand each of `key`'s encoded parts to `each`, in order, checking that
    /// each is canonical and that no bytes follow the last.
    fn walk<'a>(spec: &KeySpec<PropertyId>, key: &'a [u8], mut each: impl FnMut(&'a [u8])) -> Result<(), TreeError> {
        let mut rest = key;
        for part in &spec.keyparts {
            validate_part(part)?;
            let len = match rest.first() {
                Some(0) => 1,
                Some(1) => match part.value_type {
                    ValueType::I16 => 3,
                    ValueType::I32 => 5,
                    ValueType::I64 | ValueType::F64 => 9,
                    ValueType::Bool => 2,
                    ValueType::EntityId => 33,
                    ValueType::String | ValueType::Binary | ValueType::Object => variable_len(&rest[1..])? + 1,
                    ValueType::Json => return Err(TreeError::InvalidKey("JSON has no canonical key order")),
                },
                _ => return Err(TreeError::InvalidKey("missing or invalid part marker")),
            };
            let (encoded, tail) = rest.split_at_checked(len).ok_or(TreeError::InvalidKey("truncated part"))?;
            if encoded[0] == 1 {
                if part.value_type == ValueType::Bool && encoded[1] > 1 {
                    return Err(TreeError::InvalidKey("invalid boolean"));
                }
                if part.value_type == ValueType::F64 {
                    let bits = u64::from_be_bytes(encoded[1..].try_into().unwrap());
                    let bits = if bits >> 63 == 1 { bits ^ (1 << 63) } else { !bits };
                    let value = f64::from_bits(bits);
                    if value.is_nan() || (value == 0.0 && bits != 0) {
                        return Err(TreeError::InvalidKey("noncanonical float"));
                    }
                }
                if part.value_type == ValueType::String {
                    check_utf8(&encoded[1..encoded.len() - 2])?;
                }
            }
            each(encoded);
            rest = tail;
        }
        if !rest.is_empty() {
            return Err(TreeError::InvalidKey("trailing key bytes"));
        }
        Ok(())
    }
}

/// Check that an escaped string's body, its bytes with each zero byte
/// followed by 255, unescapes to UTF-8. A zero byte is a whole character of
/// its own, never inside another's encoding, so the string is UTF-8 when
/// each run between its zero bytes is.
fn check_utf8(body: &[u8]) -> Result<(), TreeError> {
    let mut runs = body.split(|byte| *byte == 0);
    let first = runs.next().unwrap_or_default();
    // Every later run begins with the 255 that completes its zero byte's escape.
    for run in std::iter::once(first).chain(runs.map(|run| &run[1..])) {
        std::str::from_utf8(run).map_err(|_| TreeError::InvalidKey("invalid UTF-8"))?;
    }
    Ok(())
}

pub(crate) fn validate_part(part: &IndexKeyPart<PropertyId>) -> Result<(), TreeError> {
    if part.collation.is_some() {
        return Err(TreeError::InvalidKey("custom collations are unsupported"));
    }
    if part.value_type == ValueType::Json {
        return Err(TreeError::InvalidKey("JSON has no canonical key order"));
    }
    Ok(())
}

pub(crate) fn encode_part(part: &IndexKeyPart<PropertyId>, value: Option<&Value>) -> Result<Vec<u8>, TreeError> {
    validate_part(part)?;
    let Some(value) = value else { return Ok(vec![0]) };
    let mut bytes = vec![1];
    let integer = match value {
        Value::I16(v) => Some(i64::from(*v)),
        Value::I32(v) => Some(i64::from(*v)),
        Value::I64(v) => Some(*v),
        _ => None,
    };
    match (part.value_type, value) {
        (ValueType::I16, _) if integer.is_some() => {
            let n = i16::try_from(integer.unwrap()).map_err(|_| TreeError::InvalidKey("integer overflow"))?;
            bytes.extend(((n as u16) ^ (1 << 15)).to_be_bytes());
        }
        (ValueType::I32, _) if integer.is_some() => {
            let n = i32::try_from(integer.unwrap()).map_err(|_| TreeError::InvalidKey("integer overflow"))?;
            bytes.extend(((n as u32) ^ (1 << 31)).to_be_bytes());
        }
        (ValueType::I64, _) if integer.is_some() => bytes.extend(((integer.unwrap() as u64) ^ (1 << 63)).to_be_bytes()),
        (ValueType::F64, Value::F64(n)) => {
            if n.is_nan() {
                return Err(TreeError::InvalidKey("NaN has no query ordering"));
            }
            let bits = if *n == 0.0 { 0 } else { n.to_bits() };
            bytes.extend((if bits >> 63 == 1 { !bits } else { bits ^ (1 << 63) }).to_be_bytes());
        }
        (ValueType::F64, _) if integer.is_some() => {
            let n = integer.unwrap();
            let float = n as f64;
            if float as i128 != i128::from(n) {
                return Err(TreeError::InvalidKey("integer is not exactly representable as a float"));
            }
            return encode_part(part, Some(&Value::F64(float)));
        }
        (ValueType::Bool, Value::Bool(v)) => bytes.push(u8::from(*v)),
        (ValueType::EntityId, Value::EntityId(id)) => bytes.extend(id.to_bytes()),
        (ValueType::EntityId, Value::String(text)) => {
            let id = text.parse::<EntityId>().map_err(|_| TreeError::InvalidKey("invalid entity id"))?;
            bytes.extend(id.to_bytes());
        }
        (ValueType::String, Value::String(text)) => escaped(&mut bytes, text.as_bytes()),
        (ValueType::Binary, Value::Binary(raw)) | (ValueType::Object, Value::Object(raw)) => escaped(&mut bytes, raw),
        _ => return Err(TreeError::InvalidKey("value does not have the declared type")),
    }
    Ok(bytes)
}

fn escaped(out: &mut Vec<u8>, raw: &[u8]) {
    for &byte in raw {
        out.push(byte);
        if byte == 0 {
            out.push(255);
        }
    }
    out.extend([0, 0]);
}
fn variable_len(bytes: &[u8]) -> Result<usize, TreeError> {
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == 0 {
            match bytes.get(i + 1) {
                Some(0) => return Ok(i + 2),
                Some(255) => i += 1,
                _ => return Err(TreeError::InvalidKey("invalid variable-length escape")),
            }
        }
        i += 1;
    }
    Err(TreeError::InvalidKey("unterminated variable-length part"))
}
