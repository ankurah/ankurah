// use tokio_postgres::types::ToSql;

use ankurah_core::value::Value;

#[derive(Debug)]
pub enum PGValue {
    Bytea(Vec<u8>),
    CharacterVarying(String),
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    DoublePrecision(f64),
    Boolean(bool),
    /// JSON value - stored as PostgreSQL's native jsonb type for query support.
    /// Uses serde_json::Value for proper type conversion via tokio-postgres.
    Jsonb(serde_json::Value),
}

impl PGValue {
    pub fn postgres_type(&self) -> &'static str {
        match *self {
            PGValue::CharacterVarying(_) => "varchar",
            PGValue::SmallInt(_) => "int2",
            PGValue::Integer(_) => "int4",
            PGValue::BigInt(_) => "int8",
            PGValue::DoublePrecision(_) => "float8",
            PGValue::Bytea(_) => "bytea",
            PGValue::Boolean(_) => "boolean",
            PGValue::Jsonb(_) => "jsonb",
        }
    }
}

impl From<Value> for PGValue {
    fn from(property: Value) -> Self {
        match property {
            Value::String(string) => PGValue::CharacterVarying(string),
            Value::I16(integer) => PGValue::SmallInt(integer),
            Value::I32(integer) => PGValue::Integer(integer),
            Value::I64(integer) => PGValue::BigInt(integer),
            Value::F64(float) => PGValue::DoublePrecision(float),
            Value::Bool(bool) => PGValue::Boolean(bool),
            Value::EntityId(entity_id) => PGValue::CharacterVarying(entity_id.to_base64()),
            Value::Object(items) => PGValue::Bytea(items),
            Value::Binary(items) => PGValue::Bytea(items),
            // Value::Json already contains serde_json::Value
            Value::Json(json) => PGValue::Jsonb(json),
        }
    }
}

// impl ToSql for PGValue {
//     fn to_sql(
//         &self,
//         ty: &tokio_postgres::types::Type,
//         out: &mut tokio_postgres::types::private::BytesMut,
//     ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
//     where
//         Self: Sized,
//     {
//         match self {
//             PGValue::CharacterVarying(v) => v.to_sql(ty, out),
//             PGValue::BigInt(v) => v.to_sql(ty, out),
//             PGValue::Bytea(v) => v.to_sql(ty, out),
//         }
//     }

//     fn accepts(ty: &tokio_postgres::types::Type) -> bool
//     where Self: Sized {
//         match self {
//             PGValue::CharacterVarying(v) => v.accepts(ty),
//             PGValue::BigInt(v) => v.accepts(ty),
//             PGValue::Bytea(v) => v.accepts(ty),
//         }
//     }

//     fn to_sql_checked(
//         &self,
//         ty: &tokio_postgres::types::Type,
//         out: &mut tokio_postgres::types::private::BytesMut,
//     ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
//         todo!()
//     }
// }
