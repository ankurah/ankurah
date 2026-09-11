use ankurah_core_types::Value;

/// Core-side operations on shared values.
pub trait ValueExt {
    /// Read a JSON subvalue without converting its scalar representation.
    /// Binary, object, and string values are interpreted as JSON.
    fn json_at_path(&self, path: impl IntoIterator<Item = impl AsRef<str>>) -> Option<serde_json::Value>;
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
}
