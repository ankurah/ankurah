use ankurah_core::property::PropertyValue;
use ankurah_storage_common::{Endpoint, IndexBounds, IndexColumnBound};
use ankurah_storage_sled::planner_integration::{encode_tuple_for_sled, normalize};

#[test]
fn normalize_equality_only_single_prefix_opens_upper() {
    // name = "Alice"
    let bounds = IndexBounds::new(vec![IndexColumnBound {
        column: "name".to_string(),
        low: Endpoint::incl(PropertyValue::String("Alice".to_string())),
        high: Endpoint::incl(PropertyValue::String("Alice".to_string())),
    }]);

    let (canon, eq_len, eq_vals) = normalize(&bounds);
    assert_eq!(eq_len, 1);
    assert_eq!(eq_vals, vec![PropertyValue::String("Alice".to_string())]);
    assert_eq!(canon.upper, None); // prefix-open scan
}

#[test]
fn normalize_equality_then_inequality() {
    // name = 'Alice' AND age > 25
    let bounds = IndexBounds::new(vec![
        IndexColumnBound {
            column: "name".to_string(),
            low: Endpoint::incl(PropertyValue::String("Alice".to_string())),
            high: Endpoint::incl(PropertyValue::String("Alice".to_string())),
        },
        IndexColumnBound {
            column: "age".to_string(),
            low: Endpoint::excl(PropertyValue::I32(25)),
            high: Endpoint::UnboundedHigh(ankurah_storage_common::ValueType::I32),
        },
    ]);

    let (canon, eq_len, _eq_vals) = normalize(&bounds);
    assert_eq!(eq_len, 1);
    assert!(canon.lower.is_some());
    assert!(canon.upper.is_none());
    assert_eq!(canon.lower.as_ref().unwrap().1, true); // open lower due to >
}

#[test]
fn tuple_encoding_orders_correctly() {
    let k1 = encode_tuple_for_sled(&[PropertyValue::String("a".into()), PropertyValue::I32(1)]);
    let k2 = encode_tuple_for_sled(&[PropertyValue::String("a".into()), PropertyValue::I32(2)]);
    let k3 = encode_tuple_for_sled(&[PropertyValue::String("b".into()), PropertyValue::I32(0)]);

    assert!(k1 < k2, "suffix integer order should be preserved");
    assert!(k2 < k3, "prefix string order should dominate");
}
