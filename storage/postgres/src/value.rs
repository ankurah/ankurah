// use tokio_postgres::types::ToSql;

use ankurah_core::property::PropertyValue;

#[derive(Debug)]
pub enum PGValue {
    // Boolean(bool),
    Bytea(Vec<u8>),
    CharacterVarying(String),
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Boolean(bool),
    // Text(String),
    // Timestamp(chrono::DateTime<chrono::Utc>),
}

impl PGValue {
    pub fn postgres_type(&self) -> &'static str {
        match *self {
            PGValue::CharacterVarying(_) => "varchar",
            PGValue::SmallInt(_) => "int2",
            PGValue::Integer(_) => "int4",
            PGValue::BigInt(_) => "int8",
            PGValue::Bytea(_) => "bytea",
            PGValue::Boolean(_) => "boolean",
        }
    }
}

impl From<PropertyValue> for PGValue {
    fn from(property: PropertyValue) -> Self {
        match property {
            PropertyValue::String(string) => PGValue::CharacterVarying(string),
            PropertyValue::I16(integer) => PGValue::SmallInt(integer),
            PropertyValue::I32(integer) => PGValue::Integer(integer),
            PropertyValue::I64(integer) => PGValue::BigInt(integer),
            PropertyValue::Bool(bool) => PGValue::Boolean(bool),
            PropertyValue::Object(items) => PGValue::Bytea(items),
            PropertyValue::Binary(items) => PGValue::Bytea(items),
            PropertyValue::EntityId(id) => PGValue::CharacterVarying(id.to_base64()),
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
