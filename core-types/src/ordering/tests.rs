use super::*;

fn json(text: &str) -> Value { Value::Json(serde_json::from_str(text).unwrap()) }

#[test]
fn json_numbers_preserve_decimal_equality_and_precision() {
    for equivalents in
        [vec!["0", "-0.0", "0e123", "-0e-123"], vec!["2", "2.0", "2e0", "0.2e1", "20e-1"], vec!["-200", "-2e2", "-200.00", "-0.2e3"]]
    {
        let first = value_bytes(&json(equivalents[0]));
        for text in equivalents {
            assert_eq!(value_bytes(&json(text)), first, "{text}");
        }
    }
    let ordered = [
        "-1e300",
        "-18446744073709551615",
        "-100.01",
        "-100",
        "-2",
        "-0.0001",
        "0",
        "0.0001",
        "0.09",
        "0.1",
        "0.11",
        "1",
        "1.01",
        "1.1",
        "2",
        "10",
        "9007199254740992",
        "9007199254740993",
        "18446744073709551615",
        "1e300",
    ];
    for pair in ordered.windows(2) {
        assert!(value_bytes(&json(pair[0])) < value_bytes(&json(pair[1])), "{} < {}", pair[0], pair[1]);
    }
}

#[test]
fn jsonb_type_and_container_order_matches_postgresql_c() {
    // Independently checked with PostgreSQL 16 in a C/UTF8 database. Container
    // cardinality precedes contents; object iteration uses key length then bytes.
    let ordered = [
        "[]",
        "null",
        "\"a\"",
        "2",
        "10",
        "false",
        "true",
        "[null]",
        "[1]",
        "[[]]",
        "[[1]]",
        "[{},0]",
        "{}",
        "{\"a\":1}",
        "{\"b\":1,\"aa\":1}",
        "{\"b\":2,\"aa\":0}",
    ];
    for pair in ordered.windows(2) {
        assert!(value_bytes(&json(pair[0])) < value_bytes(&json(pair[1])), "{} < {}", pair[0], pair[1]);
    }
    assert_eq!(value_bytes(&json("{\"aa\":1,\"b\":2}")), value_bytes(&json("{\"b\":2.0,\"aa\":1.0}")));
}

#[test]
fn floats_have_a_total_order_with_canonical_nan_and_zero() {
    let ordered = [f64::NEG_INFINITY, -1.0, -f64::from_bits(1), 0.0, f64::from_bits(1), 1.0, f64::INFINITY, f64::NAN];
    for pair in ordered.windows(2) {
        assert_eq!(compare_values(&Value::F64(pair[0]), &Value::F64(pair[1])), Ordering::Less);
    }
    assert_eq!(value_bytes(&Value::F64(-0.0)), value_bytes(&Value::F64(0.0)));
    assert_eq!(value_bytes(&Value::F64(f64::from_bits(0xfff8_0000_0000_0001))), value_bytes(&Value::F64(f64::NAN)));
    assert_ne!(compare_values(&Value::I64(2), &json("2")), Ordering::Equal);
}

#[test]
fn tuple_components_reverse_without_prefix_ambiguity_and_honor_null_placement() {
    for value_type in [ValueType::String, ValueType::Json, ValueType::NodeId] {
        let values = match value_type {
            ValueType::String => {
                vec![Value::String("".into()), Value::String("\0".into()), Value::String("a".into()), Value::String("aa".into())]
            }
            ValueType::NodeId => [0, 16, 64, 208, 240, 255].map(|byte| Value::NodeId(crate::NodeId::from_bytes([byte; 32]))).to_vec(),
            _ => ["[]", "null", "\"a\"", "1", "1.01", "true", "[0]", "{}"].map(json).to_vec(),
        };
        for direction in [IndexDirection::Asc, IndexDirection::Desc] {
            for nulls in [None, Some(NullsOrder::First), Some(NullsOrder::Last)] {
                let part: IndexKeyPart<()> = IndexKeyPart {
                    nulls,
                    ..match direction {
                        IndexDirection::Asc => IndexKeyPart::asc((), value_type),
                        IndexDirection::Desc => IndexKeyPart::desc((), value_type),
                    }
                };
                let key = |value: Option<&Value>| {
                    let mut key = Vec::new();
                    append_key_part(&part, value, &mut key);
                    key
                };
                let missing = key(None);
                for pair in values.windows(2) {
                    let (left, right) = (key(Some(&pair[0])), key(Some(&pair[1])));
                    assert!(!left.starts_with(&right) && !right.starts_with(&left));
                    assert_eq!(left.cmp(&right), if direction == IndexDirection::Asc { Ordering::Less } else { Ordering::Greater });
                }
                for value in &values {
                    assert_eq!(missing < key(Some(value)), nulls.unwrap_or_else(|| default_nulls(direction)) == NullsOrder::First);
                }
            }
        }
    }
}

#[test]
fn json_order_uses_the_accepted_numbers_without_rounding_large_integers() {
    let integer = json("9007199254740993");
    let adjacent = json("9007199254740992");
    assert!(value_bytes(&integer) > value_bytes(&adjacent));
    assert!(value_bytes(&json("18446744073709551615")) > value_bytes(&integer));
    for number in [f64::MIN, -f64::from_bits(1), f64::from_bits(1), f64::MAX] {
        let number = serde_json::Value::from(number);
        let canonical = number.to_string();
        assert_eq!(value_bytes(&Value::Json(number)), value_bytes(&json(&canonical)));
    }
}
