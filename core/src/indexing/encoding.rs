//! Canonical byte encoding of index key parts.
//!
//! An index key is the concatenation of its parts, each encoded by
//! [`encode_component_typed`] for the part's declared type and direction.
//! Consumers compare keys as raw bytes: sled's index trees store them, and
//! `resultset` sorts ORDER BY results by them. Nothing decodes a key; sled
//! finds the entity through the fixed-width id it appends after the parts.
//! For raw byte order to be tuple order whatever a key's later parts hold,
//! each part's encoding is, as a function of the value:
//!
//! - injective: distinct values encode to distinct bytes;
//! - order-preserving: byte order is value order (reversed for a descending
//!   part);
//! - prefix-free: no encoding is a proper prefix of another, so where a part
//!   ends is settled by its own bytes, never by what follows.
//!
//! Prefix-freeness is also what makes the keys that begin with the encoding
//! of a tuple prefix exactly the keys of tuples with that prefix, which
//! [`prefix_range_end`] turns into scan bounds.
//!
//! # Fixed-width parts
//!
//! Integers, floats, booleans and entity ids encode through
//! [`Collatable::to_bytes`] at a width fixed by the part's type, so they are
//! prefix-free among themselves, and `Collatable` orders them: big-endian
//! with the sign bit flipped for integers, IEEE bits rearranged for floats
//! with NaN last, one byte for booleans, the raw bytes of an entity id. The
//! two floating-point zeros compare equal and share one key.
//!
//! # Variable-length parts
//!
//! A string, binary or object part is its payload with every 0x00 byte
//! escaped as 0x00 0xFF, followed by the terminator 0x00 0x00.
//!
//! - Prefix-free: inside an escaped payload every 0x00 is followed by 0xFF,
//!   so 0x00 0x00 occurs only as the terminator, at the very end. An
//!   encoding that were a proper prefix of another would place its
//!   terminator inside the other's payload.
//! - Order-preserving: payloads that first differ at some byte differ there
//!   in the encoding too, in the same direction: a raw byte compares as
//!   itself, and an escaped 0x00 still begins with 0x00, below every other
//!   byte. When one payload is a proper prefix of the other, the shorter
//!   encoding has its terminator where the longer has its next payload byte,
//!   either a raw byte above 0x00 or the escape 0x00 0xFF, both greater than
//!   0x00 0x00.
//! - Injective: strict order preservation leaves no two values one key.
//!
//! # Descending parts
//!
//! A descending part is the bitwise complement of the ascending encoding.
//! Complementing keeps lengths and is a bijection on bytes, so injectivity
//! and prefix-freeness carry over; and since ascending encodings are
//! prefix-free, two of them first differ at a byte both have, where the
//! complement reverses the comparison.
//!
//! # JSON parts
//!
//! A JSON part is a kind tag followed by that kind's encoding: nothing for
//! null, the fixed-width boolean, integer or float, or the escaped and
//! terminated string. The tags order the kinds null < bool < int < float <
//! string, and within a kind the value encodes as above, so the whole is
//! prefix-free and ordered. A number encodes as an i64 when it fits and as an
//! f64 otherwise. Arrays, objects and numbers beyond both are not sortable
//! and all encode as null: the one deliberate exception to injectivity.

use super::key_spec::KeySpec;
use crate::collation::Collatable;
use crate::value::{Value, ValueType};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("Type mismatch: expected {0:?}, got {1:?}")]
    TypeMismatch(ValueType, ValueType),
}

// Kind tags of JSON parts, in sort order: null < bool < int < float < string.
const JSON_TAG_NULL: u8 = 0x00;
const JSON_TAG_BOOL: u8 = 0x10;
const JSON_TAG_INT: u8 = 0x20;
const JSON_TAG_FLOAT: u8 = 0x30;
const JSON_TAG_STRING: u8 = 0x40;

/// Encode one key part: `value` cast to the part's type, in the part's
/// direction. (No NULL handling for now - TODO: add NULL support later)
pub fn encode_component_typed(value: &Value, expected_type: ValueType, descending: bool) -> Result<Vec<u8>, IndexError> {
    // Cast value to expected type (short-circuits if types already match)
    let value = value.cast_to(expected_type).map_err(|_| IndexError::TypeMismatch(expected_type, ValueType::of(value)))?;

    let ascending = match &value {
        Value::String(s) => escape_and_terminate(s.as_bytes()),
        Value::Object(bytes) | Value::Binary(bytes) => escape_and_terminate(bytes),
        Value::I16(_) | Value::I32(_) | Value::I64(_) | Value::F64(_) | Value::Bool(_) | Value::EntityId(_) => value.to_bytes(),
        Value::Json(json) => encode_json_value(json),
    };
    Ok(if descending { complement(ascending) } else { ascending })
}

/// The variable-length framing: every 0x00 payload byte becomes 0x00 0xFF,
/// and the part ends with 0x00 0x00.
fn escape_and_terminate(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 2);
    for &b in payload {
        out.push(b);
        if b == 0x00 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0x00, 0x00]);
    out
}

/// The descending form of an ascending encoding.
fn complement(mut bytes: Vec<u8>) -> Vec<u8> {
    for b in &mut bytes {
        *b = !*b;
    }
    bytes
}

/// Kind tag, then the kind's own encoding, so "9" (string) != 9 (int).
fn encode_json_value(json: &serde_json::Value) -> Vec<u8> {
    let (tag, payload) = match json {
        serde_json::Value::Null => (JSON_TAG_NULL, vec![]),
        serde_json::Value::Bool(b) => (JSON_TAG_BOOL, vec![if *b { 1 } else { 0 }]),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                (JSON_TAG_INT, Value::I64(i).to_bytes())
            } else if let Some(f) = n.as_f64() {
                (JSON_TAG_FLOAT, Value::F64(f).to_bytes())
            } else {
                // Fallback for very large numbers
                (JSON_TAG_NULL, vec![])
            }
        }
        serde_json::Value::String(s) => (JSON_TAG_STRING, escape_and_terminate(s.as_bytes())),
        // Objects and arrays are unsortable - encode as null
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => (JSON_TAG_NULL, vec![]),
    };

    let mut out = Vec::with_capacity(1 + payload.len());
    out.push(tag);
    out.extend(payload);
    out
}

/// Type-aware encoding using KeySpec for validation and optimization
/// TODO: Add NULL handling later
pub fn encode_tuple_values_with_key_spec<K>(values: &[Value], key_spec: &KeySpec<K>) -> Result<Vec<u8>, IndexError> {
    let mut out = Vec::new();
    for (i, v) in values.iter().enumerate() {
        if i >= key_spec.keyparts.len() {
            break; // Don't encode more values than key spec defines
        }
        let keypart = &key_spec.keyparts[i];

        // Use type-aware encoding without type tags
        let bytes = encode_component_typed(v, keypart.value_type, keypart.direction.is_desc())?;
        out.extend_from_slice(&bytes);
    }
    Ok(out)
}

/// The exclusive end of the key range that begins with `prefix`.
///
/// Because parts are prefix-free, the keys whose leading parts encode to
/// `prefix` are exactly the keys that begin with those bytes, and they fill
/// the contiguous range `[prefix, prefix_range_end(prefix))`: the end is the
/// least byte string greater than every key with that prefix, which is the
/// prefix without its trailing 0xFF bytes and with its last remaining byte
/// incremented. An empty or all-0xFF prefix has no such end (`None`): every
/// key from `prefix` on begins with it. Engines derive scan bounds from these
/// two ends instead of incrementing encoded bytes.
pub fn prefix_range_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let last_below_max = prefix.iter().rposition(|&b| b != 0xFF)?;
    let mut end = prefix[..=last_below_max].to_vec();
    end[last_below_max] += 1;
    Some(end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexing::IndexKeyPart;
    use crate::value::Value;
    use ankurah_core_types::EntityId;
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use std::cmp::Ordering;

    #[test]
    fn test_desc_ordering() {
        let a = encode_component_typed(&Value::String("a".to_string()), ValueType::String, true).unwrap();
        let b = encode_component_typed(&Value::String("b".to_string()), ValueType::String, true).unwrap();

        // DESC: "a" should sort after "b" (reversed)
        assert!(a > b);
    }

    #[test]
    fn test_asc_ordering() {
        let a = encode_component_typed(&Value::String("a".to_string()), ValueType::String, false).unwrap();
        let b = encode_component_typed(&Value::String("b".to_string()), ValueType::String, false).unwrap();

        // ASC: "a" should sort before "b"
        assert!(a < b);
    }

    fn encode(value: &Value, value_type: ValueType, descending: bool) -> Vec<u8> {
        encode_component_typed(value, value_type, descending).unwrap()
    }

    fn encode_tuple(values: &[Value], spec: &KeySpec<String>) -> Vec<u8> { encode_tuple_values_with_key_spec(values, spec).unwrap() }

    // --- Cases the one-byte terminator got wrong ---

    /// With a one-byte terminator, ([00], [FF]) and ([], [FF, 00]) both
    /// encoded to 00 FF 00 FF 00.
    #[test]
    fn binary_tuples_that_move_bytes_across_the_part_boundary_do_not_collide() {
        let spec = KeySpec::new(vec![IndexKeyPart::asc("a", ValueType::Binary), IndexKeyPart::asc("b", ValueType::Binary)]);
        let first = encode_tuple(&[Value::Binary(vec![0x00]), Value::Binary(vec![0xFF])], &spec);
        let second = encode_tuple(&[Value::Binary(vec![]), Value::Binary(vec![0xFF, 0x00])], &spec);
        assert_ne!(first, second);
    }

    /// "a" sorts before "a\0b", so every ("a", _) tuple sorts before
    /// every ("a\0b", _) tuple, even when the next part begins with 0xFF.
    #[test]
    fn string_part_order_survives_a_descending_integer_after_it() {
        let spec = KeySpec::new(vec![IndexKeyPart::asc("name", ValueType::String), IndexKeyPart::desc("n", ValueType::I64)]);
        let short = encode_tuple(&[Value::String("a".into()), Value::I64(i64::MIN)], &spec);
        let extended = encode_tuple(&[Value::String("a\0b".into()), Value::I64(0)], &spec);
        assert!(short < extended, "{short:02x?} must sort before {extended:02x?}");
    }

    /// A scan for "a" must be able to end where "a" ends, so "a\0b" must
    /// not extend its encoding.
    #[test]
    fn ascending_string_is_not_a_prefix_of_its_nul_extension() {
        let short = encode(&Value::String("a".into()), ValueType::String, false);
        let extended = encode(&Value::String("a\0b".into()), ValueType::String, false);
        assert!(!extended.starts_with(&short), "{short:02x?} is a prefix of {extended:02x?}");
    }

    /// Descending JSON strings reverse value order, prefixes included.
    #[test]
    fn descending_json_strings_reverse_prefix_order() {
        let json = |s: &str| Value::Json(serde_json::Value::String(s.into()));
        let short = encode(&json("a"), ValueType::Json, true);
        let extended = encode(&json("a\0"), ValueType::Json, true);
        assert!(short > extended, "descending {short:02x?} must sort after {extended:02x?}");
    }

    #[test]
    fn prefix_range_end_is_the_least_key_above_every_extension() {
        assert_eq!(prefix_range_end(&[0x61, 0x00, 0x00]), Some(vec![0x61, 0x00, 0x01]));
        assert_eq!(prefix_range_end(&[0x61, 0xFF]), Some(vec![0x62]));
        assert_eq!(prefix_range_end(&[0x00]), Some(vec![0x01]));
        assert_eq!(prefix_range_end(&[]), None);
        assert_eq!(prefix_range_end(&[0xFF, 0xFF]), None);
    }

    // --- Property tests: injective, order-preserving, prefix-free ---

    /// The value order each type's encoding must preserve.
    fn value_cmp(a: &Value, b: &Value) -> Ordering {
        match (a, b) {
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Binary(a), Value::Binary(b)) | (Value::Object(a), Value::Object(b)) => a.cmp(b),
            (Value::I16(a), Value::I16(b)) => a.cmp(b),
            (Value::I32(a), Value::I32(b)) => a.cmp(b),
            (Value::I64(a), Value::I64(b)) => a.cmp(b),
            (Value::F64(a), Value::F64(b)) => float_cmp(*a, *b),
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::EntityId(a), Value::EntityId(b)) => a.to_bytes().cmp(&b.to_bytes()),
            (Value::Json(a), Value::Json(b)) => json_cmp(a, b),
            _ => panic!("samples of one type only: {a:?} vs {b:?}"),
        }
    }

    /// Numeric order with both zeros equal and NaN last.
    fn float_cmp(a: f64, b: f64) -> Ordering {
        let canonical = |f: f64| {
            if f.is_nan() {
                f64::NAN
            } else if f == 0.0 {
                0.0
            } else {
                f
            }
        };
        canonical(a).total_cmp(&canonical(b))
    }

    /// Kind order null < bool < int < float < string, then value order within a kind.
    fn json_cmp(a: &serde_json::Value, b: &serde_json::Value) -> Ordering {
        use serde_json::Value as J;
        fn kind(v: &J) -> u8 {
            match v {
                J::Null | J::Array(_) | J::Object(_) => 0,
                J::Bool(_) => 1,
                J::Number(n) if n.as_i64().is_some() => 2,
                J::Number(_) => 3,
                J::String(_) => 4,
            }
        }
        kind(a).cmp(&kind(b)).then_with(|| match (a, b) {
            (J::Bool(a), J::Bool(b)) => a.cmp(b),
            (J::Number(a), J::Number(b)) => match (a.as_i64(), b.as_i64()) {
                (Some(a), Some(b)) => a.cmp(&b),
                _ => float_cmp(a.as_f64().unwrap(), b.as_f64().unwrap()),
            },
            (J::String(a), J::String(b)) => a.cmp(b),
            _ => Ordering::Equal,
        })
    }

    /// Sort `values` by their value order and drop those comparing equal.
    fn distinct_sorted(mut values: Vec<Value>) -> Vec<Value> {
        values.sort_by(value_cmp);
        values.dedup_by(|a, b| value_cmp(a, b) == Ordering::Equal);
        values
    }

    /// Every sequence over `alphabet` up to `max_len` symbols, plus `random`
    /// longer ones from a fixed seed.
    fn sequences<T: Clone>(alphabet: &[T], max_len: usize, random: usize, max_random_len: usize) -> Vec<Vec<T>> {
        let mut out = vec![vec![]];
        let mut last_length = vec![vec![]];
        for _ in 0..max_len {
            last_length = last_length
                .iter()
                .flat_map(|prefix| alphabet.iter().map(move |symbol| [prefix.as_slice(), std::slice::from_ref(symbol)].concat()))
                .collect();
            out.extend(last_length.iter().cloned());
        }
        let mut rng = StdRng::seed_from_u64(0x5eed);
        for _ in 0..random {
            let len = rng.gen_range(0..=max_random_len);
            out.push((0..len).map(|_| alphabet[rng.gen_range(0..alphabet.len())].clone()).collect());
        }
        out
    }

    /// Strings mixing the escaped byte, bytes next to it and multi-byte scalars.
    fn string_samples() -> Vec<Value> {
        let alphabet = ['\0', '\u{1}', 'a', '\u{7f}', '\u{80}', '\u{10ffff}'];
        distinct_sorted(sequences(&alphabet, 3, 300, 12).into_iter().map(|chars| Value::String(chars.into_iter().collect())).collect())
    }

    /// Bytes around the escape, the terminator and the complement's 0xFF.
    fn byte_sequences() -> Vec<Vec<u8>> { sequences(&[0x00, 0x01, 0x7f, 0xfe, 0xff], 3, 300, 12) }

    /// Edges of the width's range, values around zero and the byte carries, and seeded random values.
    fn integer_samples(min: i64, max: i64) -> Vec<i64> {
        let mut rng = StdRng::seed_from_u64(1);
        let mut out = vec![min, min + 1, -300, -256, -255, -2, -1, 0, 1, 2, 255, 256, 300, max - 1, max];
        out.extend((0..200).map(|_| rng.gen_range(min..=max)));
        out
    }

    fn float_samples() -> Vec<f64> {
        let mut rng = StdRng::seed_from_u64(2);
        let nearest = f64::from_bits(1);
        let mut out =
            vec![f64::NEG_INFINITY, f64::MIN, -1.5, -1.0, -nearest, -0.0, 0.0, nearest, 1.0, 1.5, f64::MAX, f64::INFINITY, f64::NAN];
        out.extend((0..200).map(|_| rng.gen::<f64>() * 2e6 - 1e6));
        out
    }

    fn entity_id_samples() -> Vec<Value> {
        let mut rng = StdRng::seed_from_u64(3);
        let mut ids = vec![[0u8; 32], [0xFF; 32], [0x80; 32]];
        ids.push({
            let mut id = [0u8; 32];
            id[31] = 1;
            id
        });
        ids.push({
            let mut id = [0xFFu8; 32];
            id[31] = 0xFE;
            id
        });
        ids.extend((0..50).map(|_| rng.gen::<[u8; 32]>()));
        distinct_sorted(ids.into_iter().map(|id| Value::EntityId(EntityId::from_bytes(id))).collect())
    }

    fn json_samples() -> Vec<Value> {
        use serde_json::Value as J;
        let mut out = vec![J::Null, J::Bool(false), J::Bool(true)];
        out.extend(integer_samples(i64::MIN, i64::MAX).into_iter().map(J::from));
        out.extend(float_samples().into_iter().filter_map(serde_json::Number::from_f64).map(J::Number));
        out.push(J::from(u64::MAX));
        out.extend(string_samples().into_iter().map(|s| match s {
            Value::String(s) => J::String(s),
            _ => unreachable!(),
        }));
        distinct_sorted(out.into_iter().map(Value::Json).collect())
    }

    /// Every type's samples, distinct and in value order, with the type they are declared as.
    fn samples_by_type() -> Vec<(ValueType, Vec<Value>)> {
        let binaries = || byte_sequences().into_iter().map(Value::Binary).collect();
        let objects = || byte_sequences().into_iter().map(Value::Object).collect();
        vec![
            (ValueType::String, string_samples()),
            (ValueType::Binary, distinct_sorted(binaries())),
            (ValueType::Object, distinct_sorted(objects())),
            (
                ValueType::I16,
                distinct_sorted(integer_samples(i16::MIN.into(), i16::MAX.into()).into_iter().map(|n| Value::I16(n as i16)).collect()),
            ),
            (
                ValueType::I32,
                distinct_sorted(integer_samples(i32::MIN.into(), i32::MAX.into()).into_iter().map(|n| Value::I32(n as i32)).collect()),
            ),
            (ValueType::I64, distinct_sorted(integer_samples(i64::MIN, i64::MAX).into_iter().map(Value::I64).collect())),
            (ValueType::F64, distinct_sorted(float_samples().into_iter().map(Value::F64).collect())),
            (ValueType::Bool, vec![Value::Bool(false), Value::Bool(true)]),
            (ValueType::EntityId, entity_id_samples()),
            (ValueType::Json, json_samples()),
        ]
    }

    /// `keys` are the encodings of distinct values in value order: they must
    /// be strictly monotone in the direction's byte order (which gives
    /// injectivity and order preservation) and no key may be a proper prefix
    /// of another. In byte order a key's extensions follow it directly, so
    /// checking each key against its byte-order neighbour covers every pair.
    fn assert_injective_ordered_prefix_free(keys: &[Vec<u8>], descending: bool, what: &str) {
        let expected = if descending { Ordering::Greater } else { Ordering::Less };
        for (i, pair) in keys.windows(2).enumerate() {
            assert_eq!(
                pair[0].cmp(&pair[1]),
                expected,
                "{what}: keys {i} and {} out of order: {:02x?} vs {:02x?}",
                i + 1,
                pair[0],
                pair[1]
            );
        }
        let mut by_bytes = keys.to_vec();
        by_bytes.sort();
        for pair in by_bytes.windows(2) {
            assert!(!pair[1].starts_with(&pair[0]), "{what}: {:02x?} is a prefix of {:02x?}", pair[0], pair[1]);
        }
    }

    #[test]
    fn every_type_encodes_injectively_in_order_and_prefix_free_in_both_directions() {
        for (value_type, values) in samples_by_type() {
            assert!(values.len() > 1, "{value_type:?} needs samples");
            for descending in [false, true] {
                let keys: Vec<_> = values.iter().map(|v| encode(v, value_type, descending)).collect();
                assert_injective_ordered_prefix_free(&keys, descending, &format!("{value_type:?} descending={descending}"));
            }
        }
    }

    /// Tuple order: part by part, each in its own direction.
    fn tuple_cmp(a: &[Value], b: &[Value], spec: &KeySpec<String>) -> Ordering {
        a.iter()
            .zip(b)
            .zip(&spec.keyparts)
            .map(|((a, b), part)| if part.direction.is_desc() { value_cmp(b, a) } else { value_cmp(a, b) })
            .find(|o| *o != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
    }

    #[test]
    fn tuples_of_variable_length_parts_concatenate_injectively_in_order_and_prefix_free() {
        let samples: std::collections::HashMap<_, _> = samples_by_type().into_iter().collect();
        // A thin sample per part keeps the cartesian product small.
        let thin = |value_type: ValueType| -> Vec<Value> {
            samples[&value_type].iter().step_by(samples[&value_type].len() / 40 + 1).cloned().collect()
        };
        let specs = [
            vec![IndexKeyPart::asc("a", ValueType::Binary), IndexKeyPart::asc("b", ValueType::Binary)],
            vec![IndexKeyPart::asc("a", ValueType::Binary), IndexKeyPart::desc("b", ValueType::Binary)],
            vec![IndexKeyPart::desc("a", ValueType::String), IndexKeyPart::asc("b", ValueType::String)],
            vec![IndexKeyPart::asc("a", ValueType::String), IndexKeyPart::desc("b", ValueType::I64)],
            vec![IndexKeyPart::desc("a", ValueType::Json), IndexKeyPart::asc("b", ValueType::Object)],
            vec![
                IndexKeyPart::asc("a", ValueType::Object),
                IndexKeyPart::asc("b", ValueType::F64),
                IndexKeyPart::desc("c", ValueType::Json),
            ],
        ];
        for keyparts in specs {
            let spec = KeySpec::new(keyparts);
            let mut tuples = vec![vec![]];
            for part in &spec.keyparts {
                tuples =
                    tuples.iter().flat_map(|t| thin(part.value_type).into_iter().map(move |v| [t.as_slice(), &[v]].concat())).collect();
            }
            tuples.sort_by(|a, b| tuple_cmp(a, b, &spec));
            let keys: Vec<_> = tuples.iter().map(|t| encode_tuple(t, &spec)).collect();
            assert_injective_ordered_prefix_free(&keys, false, &spec.name_with("", ", "));
        }
    }

    #[test]
    fn unsortable_json_encodes_as_null() {
        let null = encode(&Value::Json(serde_json::Value::Null), ValueType::Json, false);
        for json in [serde_json::json!([1, 2]), serde_json::json!({"a": 1})] {
            assert_eq!(encode(&Value::Json(json), ValueType::Json, false), null);
        }
    }
}
