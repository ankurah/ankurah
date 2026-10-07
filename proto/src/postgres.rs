use base64::engine::general_purpose;
use base64::write::EncoderWriter;
use postgres_types::{to_sql_checked, FromSql, IsNull, ToSql, Type};

use crate::{Clock, DecodeError, EventBody, EventId};
use bytes::{BufMut, BytesMut};
use std::error::Error;
use std::io::Write;

// EventID implementation
impl ToSql for EventId {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let mut enc = EncoderWriter::new(out.writer(), &general_purpose::URL_SAFE_NO_PAD);
        enc.write_all(self.as_bytes())?;
        enc.finish()?;
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match ty.name() {
            "character" => true,
            "bpchar" => true,
            _ => false,
        }
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for EventId {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let s = std::str::from_utf8(raw).map_err(|e| Box::new(e) as Box<dyn Error + Sync + Send>)?;
        Self::from_base64(s).map_err(|e| Box::new(e) as Box<dyn Error + Sync + Send>)
    }

    fn accepts(ty: &Type) -> bool {
        match ty.name() {
            "character" => true,
            "bpchar" => true,
            _ => false,
        }
    }
}

impl ToSql for Clock {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(&bincode::serialize(self)?);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool { *ty == Type::BYTEA }
    to_sql_checked!();
}

impl<'a> FromSql<'a> for Clock {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> { Ok(bincode::deserialize(raw)?) }
    fn accepts(ty: &Type) -> bool { *ty == Type::BYTEA }
}

// use bytea and bincode to serialize and deserialize the event body - do not base64 encode the bytea
impl ToSql for EventBody {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where Self: Sized {
        if ty.name() != "bytea" {
            return Err("expected bytea type".into());
        }
        out.put_slice(&bincode::serialize(self).map_err(|_| DecodeError::InvalidFormat)?);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool
    where Self: Sized {
        // bytea
        match ty.name() {
            "bytea" => true,
            _ => false,
        }
    }

    to_sql_checked!();
}
impl<'a> FromSql<'a> for EventBody {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if ty.name() != "bytea" {
            return Err("expected bytea type".into());
        }
        let body: EventBody = bincode::deserialize(raw).map_err(|_| DecodeError::InvalidFormat)?;
        Ok(body)
    }

    fn accepts(ty: &Type) -> bool {
        // bytea
        match ty.name() {
            "bytea" => true,
            _ => false,
        }
    }
}
