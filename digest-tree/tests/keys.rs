use ankurah_core_types::{EntityId, IndexKeyPart, KeySpec, PropertyId, Value, ValueType};
use ankurah_digest_tree::{CanonicalCodec, KeyCodec};
use proptest::prelude::*;
fn spec(types: &[ValueType]) -> KeySpec<PropertyId> {
    KeySpec::new(types.iter().enumerate().map(|(i, t)| IndexKeyPart::asc(PropertyId::EntityId(EntityId::from_bytes([i as u8; 32])), *t)).collect())
}
proptest! {
    #[test]
    fn string_tuples_are_prefix_free_and_order_preserving(a in ".{0,40}", b in ".{0,40}", x in any::<i64>(), y in any::<i64>()) {
        let spec = spec(&[ValueType::String, ValueType::I64]);
        let encode = |s: &str, n| CanonicalCodec.encode_prefix(&spec, &[Value::from(s), Value::I64(n)]).unwrap();
        let first = encode(&a, x); let second = encode(&b, y);
        prop_assert_eq!(first.cmp(&second), a.as_bytes().cmp(b.as_bytes()).then(x.cmp(&y)));
        prop_assert_eq!(CanonicalCodec.split(&spec, &first).unwrap().concat(), first.clone());
        if first != second { prop_assert!(!first.starts_with(&second) && !second.starts_with(&first)); }
    }
    #[test]
    fn integer_and_float_encodings_follow_numeric_order(a in any::<i64>(), b in any::<i64>(), x in any::<f64>(), y in any::<f64>()) {
        let ints = spec(&[ValueType::I64]);
        let encode = |n| CanonicalCodec.encode_prefix(&ints, &[Value::I64(n)]).unwrap();
        prop_assert_eq!(encode(a).cmp(&encode(b)), a.cmp(&b));
        if !x.is_nan() && !y.is_nan() {
            let floats = spec(&[ValueType::F64]);
            let encode = |n| CanonicalCodec.encode_prefix(&floats, &[Value::F64(n)]).unwrap();
            prop_assert_eq!(encode(x).cmp(&encode(y)), x.partial_cmp(&y).unwrap());
        }
    }
}
#[test]
fn null_zero_escapes_and_bad_encodings_are_explicit() {
    let strings = spec(&[ValueType::String]);
    let values = ["", "\0", "\0\0", "\0a", "a", "a\0", "a\0b", "aa", "é", "雪"];
    let encoded: Vec<_> = values.iter().map(|s| CanonicalCodec.encode_prefix(&strings, &[Value::from(*s)]).unwrap()).collect();
    for pair in encoded.windows(2) { assert!(pair[0] < pair[1]); assert!(!pair[1].starts_with(&pair[0])); }
    assert!(CanonicalCodec.encode_nullable(&strings, &[None]).unwrap() < encoded[0]);
    for bytes in [vec![], vec![1], vec![1, 0], vec![1, 0, 1], vec![1, 255, 0, 0], vec![0, 0]] {
        assert!(CanonicalCodec.split(&strings, &bytes).is_err(), "{bytes:?}");
    }
    let floats = spec(&[ValueType::F64]);
    assert_eq!(CanonicalCodec.encode_prefix(&floats, &[Value::F64(-0.0)]).unwrap(), CanonicalCodec.encode_prefix(&floats, &[Value::F64(0.0)]).unwrap());
    assert!(CanonicalCodec.encode_prefix(&floats, &[Value::F64(f64::NAN)]).is_err());
    assert!(CanonicalCodec.encode_prefix(&spec(&[ValueType::I16]), &[Value::I64(99999)]).is_err());
    assert!(CanonicalCodec.encode_prefix(&floats, &[Value::I64(i64::MAX)]).is_err());
    assert!(CanonicalCodec.encode_prefix(&floats, &[Value::I64(7)]).is_ok());
}
