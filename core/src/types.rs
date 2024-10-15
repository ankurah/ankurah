use ulid::Ulid;

pub struct ID(Ulid);
pub struct EnumValue<T>(T);
pub struct StringValue(String);
