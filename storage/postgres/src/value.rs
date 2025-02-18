// use tokio_postgres::types::ToSql;

#[derive(Debug)]
pub enum PGValue {
    BigInt(Option<i64>),
    // Boolean(bool),
    Bytea(Option<Vec<u8>>),
    CharacterVarying(Option<String>),
    // Integer(i32),
    // Text(String),
    // Timestamp(chrono::DateTime<chrono::Utc>),
}

impl PGValue {
    pub fn postgres_type(&self) -> &'static str {
        match self {
            PGValue::CharacterVarying(_) => "varchar",
            PGValue::BigInt(_) => "int8",
            PGValue::Bytea(_) => "bytea",
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
