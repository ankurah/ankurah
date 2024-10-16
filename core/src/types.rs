use ulid::Ulid;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct ID(Ulid);
pub struct EnumValue<T>(T);
pub struct StringValue(String);
