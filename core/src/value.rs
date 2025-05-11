use crate::collation::Collatable;

/// A dynamically typed value
/// This is a short term expedience. Ideally we would NOT have one canonical set of types, but rather a pairwise mapping between the
/// storage engine types and the backend types.
///
/// TODO: Consolidate this with PropertyValue and stop using String for all matching
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
}

impl Collatable for Value {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            Value::String(s) => s.as_bytes().to_vec(),
            Value::Integer(i) => i.to_be_bytes().to_vec(),
            Value::Float(f) => {
                let bits = if f.is_nan() {
                    u64::MAX // NaN sorts last
                } else {
                    let bits = f.to_bits();
                    if *f >= 0.0 {
                        bits ^ (1 << 63) // Flip sign bit for positive numbers
                    } else {
                        !bits // Flip all bits for negative numbers
                    }
                };
                bits.to_be_bytes().to_vec()
            }
            Value::Boolean(b) => vec![*b as u8],
        }
    }

    fn successor_bytes(&self) -> Option<Vec<u8>> {
        match self {
            Value::String(s) => {
                if s.is_empty() {
                    let mut bytes = s.as_bytes().to_vec();
                    bytes.push(0);
                    Some(bytes)
                } else {
                    let mut bytes = s.as_bytes().to_vec();
                    bytes.push(0);
                    Some(bytes)
                }
            }
            Value::Integer(i) => {
                if *i == i64::MAX {
                    None
                } else {
                    Some((i + 1).to_be_bytes().to_vec())
                }
            }
            Value::Float(f) => {
                if f.is_nan() || (f.is_infinite() && *f > 0.0) {
                    None
                } else {
                    let bits = if *f >= 0.0 { f.to_bits() ^ (1 << 63) } else { !f.to_bits() };
                    let next_bits = bits + 1;
                    Some(next_bits.to_be_bytes().to_vec())
                }
            }
            Value::Boolean(b) => {
                if *b {
                    None
                } else {
                    Some(vec![1])
                }
            }
        }
    }

    fn predecessor_bytes(&self) -> Option<Vec<u8>> {
        match self {
            Value::String(s) => {
                if s.is_empty() {
                    None
                } else {
                    let bytes = s.as_bytes();
                    Some(bytes[..bytes.len() - 1].to_vec())
                }
            }
            Value::Integer(i) => {
                if *i == i64::MIN {
                    None
                } else {
                    Some((i - 1).to_be_bytes().to_vec())
                }
            }
            Value::Float(f) => {
                if f.is_nan() || (f.is_infinite() && *f < 0.0) {
                    None
                } else {
                    let bits = if *f >= 0.0 { f.to_bits() ^ (1 << 63) } else { !f.to_bits() };
                    let prev_bits = bits - 1;
                    Some(prev_bits.to_be_bytes().to_vec())
                }
            }
            Value::Boolean(b) => {
                if *b {
                    Some(vec![0])
                } else {
                    None
                }
            }
        }
    }

    fn is_minimum(&self) -> bool {
        match self {
            Value::String(s) => s.is_empty(),
            Value::Integer(i) => *i == i64::MIN,
            Value::Float(f) => *f == f64::NEG_INFINITY,
            Value::Boolean(b) => !b,
        }
    }

    fn is_maximum(&self) -> bool {
        match self {
            Value::String(_) => false, // Strings have no theoretical maximum
            Value::Integer(i) => *i == i64::MAX,
            Value::Float(f) => *f == f64::INFINITY,
            Value::Boolean(b) => *b,
        }
    }
}
