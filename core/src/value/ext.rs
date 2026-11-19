use ankurah_core_types::Value;

/// Core-side operations on shared values.
pub trait ValueExt {
    /// Read a JSON subvalue without converting its scalar representation.
    /// Binary, object, and string values are interpreted as JSON.
    fn json_at_path(&self, path: impl IntoIterator<Item = impl AsRef<str>>) -> Option<serde_json::Value>;

    /// Read an ordered JSON subvalue with PostgreSQL JSONB path semantics.
    /// Objects use literal keys; arrays accept integer indices, with negative
    /// indices counting from their end. Missing paths remain distinct from null.
    fn json_order_at_path(&self, path: impl IntoIterator<Item = impl AsRef<str>>) -> Option<serde_json::Value>;
}

impl ValueExt for Value {
    fn json_at_path(&self, path: impl IntoIterator<Item = impl AsRef<str>>) -> Option<serde_json::Value> {
        let parsed;
        let json = match self {
            Self::Json(json) => json,
            _ => {
                parsed = self.parse_as_json().ok()?;
                &parsed
            }
        };
        path.into_iter().try_fold(json, |current, key| current.get(key.as_ref())).cloned()
    }

    fn json_order_at_path(&self, path: impl IntoIterator<Item = impl AsRef<str>>) -> Option<serde_json::Value> {
        let parsed;
        let json = match self {
            Self::Json(json) => json,
            _ => {
                parsed = self.parse_as_json().ok()?;
                &parsed
            }
        };
        path.into_iter()
            .try_fold(json, |current, key| match current {
                serde_json::Value::Object(object) => object.get(key.as_ref()),
                serde_json::Value::Array(array) => {
                    // jsonb_get_element uses strtoint: a sign and leading C
                    // whitespace are accepted, but trailing whitespace is not.
                    let key = key.as_ref().trim_start_matches([' ', '\t', '\n', '\r', '\u{b}', '\u{c}']);
                    let index = key.parse::<i32>().ok()?;
                    let index = if index < 0 { array.len().checked_sub(index.checked_neg()? as usize)? } else { index as usize };
                    array.get(index)
                }
                _ => None,
            })
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_at_path_preserves_values_across_representations() {
        let json = serde_json::json!({ "nested": { "integer": u64::MAX, "string": "42", "null": null, "array": [1, true] } });
        for value in [
            Value::Json(json.clone()),
            Value::Binary(serde_json::to_vec(&json).unwrap()),
            Value::Object(serde_json::to_vec(&json).unwrap()),
            Value::String(json.to_string()),
        ] {
            assert_eq!(value.json_at_path([] as [&str; 0]), Some(json.clone()));
            for key in ["integer", "string", "null", "array"] {
                assert_eq!(value.json_at_path(["nested", key]), Some(json["nested"][key].clone()));
            }
            assert_eq!(value.json_at_path(["missing"]), None);
            assert_eq!(value.json_at_path(["nested", "integer", "child"]), None);
        }
        assert_eq!(Value::String("not json".into()).json_at_path(["field"]), None);
        assert_eq!(Value::I64(42).json_at_path([] as [&str; 0]), None);
    }

    #[test]
    fn ordered_json_paths_match_postgresql_objects_and_arrays() {
        let json = serde_json::json!({
            "items": [{"rank": 2}, {"rank": 10}, null],
            "0": "literal", "-1": "negative key", "$": {"quoted\"key\\": 42}
        });
        for value in [
            Value::Json(json.clone()),
            Value::Binary(serde_json::to_vec(&json).unwrap()),
            Value::Object(serde_json::to_vec(&json).unwrap()),
            Value::String(json.to_string()),
        ] {
            assert_eq!(value.json_order_at_path([] as [&str; 0]), Some(json.clone()));
            for index in ["0", "+0", "-0", "00", " \t0", "-3"] {
                assert_eq!(value.json_order_at_path(["items", index, "rank"]), Some(serde_json::json!(2)), "{index}");
            }
            assert_eq!(value.json_order_at_path(["items", "-2", "rank"]), Some(serde_json::json!(10)));
            assert_eq!(value.json_order_at_path(["items", "-1"]), Some(serde_json::Value::Null));
            for index in ["3", "-4", "2147483648", "-2147483648", "0 ", "1.0", "", "+", "rank"] {
                assert_eq!(value.json_order_at_path(["items", index]), None, "{index}");
            }
            assert_eq!(value.json_order_at_path(["0"]), Some(serde_json::json!("literal")));
            assert_eq!(value.json_order_at_path(["-1"]), Some(serde_json::json!("negative key")));
            assert_eq!(value.json_order_at_path(["$", "quoted\"key\\"]), Some(serde_json::json!(42)));
            assert_eq!(value.json_order_at_path(["missing"]), None);
            assert_eq!(value.json_order_at_path(["items", "-1", "field"]), None);
            assert_eq!(value.json_at_path(["items", "0"]), None, "predicate traversal remains unchanged");
        }
    }
}
