use base64::engine::{general_purpose, Engine};
use base64::write::EncoderWriter;
use postgres_protocol::types;
use postgres_types::{to_sql_checked, FromSql, IsNull, Kind, ToSql, Type};

use crate::{Clock, EntityId, EventId};
use bytes::{BufMut, BytesMut};
use std::collections::BTreeSet;
use std::error::Error;
use std::io::Write;

// EntityID implementation
impl ToSql for EntityId {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let mut enc = EncoderWriter::new(out.writer(), &general_purpose::URL_SAFE_NO_PAD);
        enc.write_all(self.0.to_bytes().as_slice())?;
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

impl<'a> FromSql<'a> for EntityId {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> { Ok(EntityId::from_base64(raw)?) }

    fn accepts(ty: &Type) -> bool {
        match ty.name() {
            "character" => true,
            "bpchar" => true,
            _ => false,
        }
    }
}

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

// Clock implementation
impl ToSql for Clock {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let member_type = match *ty.kind() {
            Kind::Array(ref member) => member,
            _ => panic!("expected array type"),
        };

        let dimension =
            postgres_protocol::types::ArrayDimension { len: self.len().try_into().map_err(|_| "array too large")?, lower_bound: 1 };

        postgres_protocol::types::array_to_sql(
            Some(dimension),
            member_type.oid(),
            self.iter(),
            |e, w| match e.to_sql(member_type, w)? {
                IsNull::No => Ok(postgres_protocol::IsNull::No),
                IsNull::Yes => Ok(postgres_protocol::IsNull::Yes),
            },
            out,
        )?;
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match ty.kind() {
            Kind::Array(inner) => match inner.name() {
                "character" => true,
                "bpchar" => true,
                _ => false,
            },
            _ => false,
        }
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for Clock {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let member_type = match *ty.kind() {
            Kind::Array(ref member) => member,
            _ => panic!("expected array type"),
        };
        use fallible_iterator::FallibleIterator; // 0.2.0

        let array = types::array_from_sql(raw)?;
        if array.dimensions().count()? > 1 {
            return Err("array contains too many dimensions".into());
        }

        let mut event_ids = BTreeSet::new();
        let mut values = array.values();
        while let Some(v) = values.next()? {
            if let Some(v) = v {
                event_ids.insert(EventId::from_sql(member_type, v)?);
            }
        }

        Ok(Clock(event_ids))
    }

    fn accepts(ty: &Type) -> bool {
        match ty.kind() {
            Kind::Array(inner) => match inner.name() {
                "character" => true,
                "bpchar" => true,
                _ => false,
            },
            _ => false,
        }
    }
}
