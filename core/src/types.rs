use ulid::Ulid;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct ID(pub Ulid);


pub struct EnumValue<T>(T);
pub struct StringValue(String);
