//! SQLite value type conversions

use ankurah_core::value::Value;

/// SQLite value wrapper for type mapping
#[derive(Debug, Clone)]
pub enum SqliteValue {
    /// TEXT type for strings
    Text(String),
    /// INTEGER type for small integers
    Integer(i64),
    /// REAL type for floating point
    Real(f64),
    /// BLOB type for binary data
    Blob(Vec<u8>),
    /// JSONB type for JSON (stored as BLOB, queried via -> operator)
    /// Note: SQLite JSONB is different from PostgreSQL JSONB but provides
    /// similar benefits: pre-parsed binary format for faster queries.
    Jsonb(serde_json::Value),
    /// NULL
    Null,
}

impl SqliteValue {
    /// Get the SQLite type name for column creation
    pub fn sqlite_type(&self) -> &'static str {
        match self {
            SqliteValue::Text(_) => "TEXT",
            SqliteValue::Integer(_) => "INTEGER",
            SqliteValue::Real(_) => "REAL",
            SqliteValue::Blob(_) => "BLOB",
            // JSONB is stored as BLOB in SQLite, but we use BLOB type
            // and rely on jsonb() function for conversion
            SqliteValue::Jsonb(_) => "BLOB",
            SqliteValue::Null => "TEXT", // Default to TEXT for NULL
        }
    }

    /// Check if this value is a JSONB type that needs special SQL handling
    pub fn is_jsonb(&self) -> bool { matches!(self, SqliteValue::Jsonb(_)) }

    /// Get the JSON string representation (for use with jsonb() function)
    pub fn as_json_string(&self) -> Option<String> {
        match self {
            SqliteValue::Jsonb(j) => Some(j.to_string()),
            _ => None,
        }
    }

    /// Convert to a rusqlite parameter value
    /// Note: For JSONB, this returns the JSON text - the caller must wrap with jsonb()
    pub fn to_sql(&self) -> rusqlite::types::Value {
        match self {
            SqliteValue::Text(s) => rusqlite::types::Value::Text(s.clone()),
            SqliteValue::Integer(i) => rusqlite::types::Value::Integer(*i),
            SqliteValue::Real(f) => rusqlite::types::Value::Real(*f),
            SqliteValue::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
            // Return as text - the SQL query will wrap this with jsonb()
            SqliteValue::Jsonb(j) => rusqlite::types::Value::Text(j.to_string()),
            SqliteValue::Null => rusqlite::types::Value::Null,
        }
    }
}

impl From<Value> for SqliteValue {
    fn from(value: Value) -> Self {
        match value {
            Value::String(s) => SqliteValue::Text(s),
            Value::I16(i) => SqliteValue::Integer(i as i64),
            Value::I32(i) => SqliteValue::Integer(i as i64),
            Value::I64(i) => SqliteValue::Integer(i),
            Value::F64(f) => SqliteValue::Real(f),
            Value::Bool(b) => SqliteValue::Integer(if b { 1 } else { 0 }),
            Value::EntityId(id) => SqliteValue::Text(id.to_base64()),
            Value::Object(bytes) => SqliteValue::Blob(bytes),
            Value::Binary(bytes) => SqliteValue::Blob(bytes),
            Value::Json(json) => SqliteValue::Jsonb(json),
        }
    }
}

impl From<Option<Value>> for SqliteValue {
    fn from(value: Option<Value>) -> Self {
        match value {
            Some(v) => v.into(),
            None => SqliteValue::Null,
        }
    }
}

/// Convert rusqlite Value to our SqliteValue
impl From<rusqlite::types::Value> for SqliteValue {
    fn from(value: rusqlite::types::Value) -> Self {
        match value {
            rusqlite::types::Value::Null => SqliteValue::Null,
            rusqlite::types::Value::Integer(i) => SqliteValue::Integer(i),
            rusqlite::types::Value::Real(f) => SqliteValue::Real(f),
            rusqlite::types::Value::Text(s) => SqliteValue::Text(s),
            rusqlite::types::Value::Blob(b) => SqliteValue::Blob(b),
        }
    }
}
