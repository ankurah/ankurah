use std::cmp::Ordering;

use ankql::ast;
use ankurah_proto::EntityId;
/// Represents a bound in a range query
#[derive(Debug, Clone, PartialEq)]
pub enum RangeBound<T> {
    Included(T),
    Excluded(T),
    Unbounded,
}

/// Trait for types that support collation operations
pub trait Collatable {
    /// Convert the value to its binary representation for collation
    fn to_bytes(&self) -> Vec<u8>;

    /// Returns the immediate successor's binary representation if one exists
    fn successor_bytes(&self) -> Option<Vec<u8>>;

    /// Returns the immediate predecessor's binary representation if one exists
    fn predecessor_bytes(&self) -> Option<Vec<u8>>;

    /// Returns true if this value represents a minimum bound in its domain
    fn is_minimum(&self) -> bool;

    /// Returns true if this value represents a maximum bound in its domain
    fn is_maximum(&self) -> bool;

    /// Compare two values in the collation order
    fn compare(&self, other: &Self) -> Ordering { self.to_bytes().cmp(&other.to_bytes()) }

    fn is_in_range(&self, lower: RangeBound<&Self>, upper: RangeBound<&Self>) -> bool {
        match (lower, upper) {
            (RangeBound::Included(l), RangeBound::Included(u)) => self.compare(l) != Ordering::Less && self.compare(u) != Ordering::Greater,
            (RangeBound::Included(l), RangeBound::Excluded(u)) => self.compare(l) != Ordering::Less && self.compare(u) == Ordering::Less,
            (RangeBound::Excluded(l), RangeBound::Included(u)) => {
                self.compare(l) == Ordering::Greater && self.compare(u) != Ordering::Greater
            }
            (RangeBound::Excluded(l), RangeBound::Excluded(u)) => self.compare(l) == Ordering::Greater && self.compare(u) == Ordering::Less,
            (RangeBound::Unbounded, RangeBound::Included(u)) => self.compare(u) != Ordering::Greater,
            (RangeBound::Unbounded, RangeBound::Excluded(u)) => self.compare(u) == Ordering::Less,
            (RangeBound::Included(l), RangeBound::Unbounded) => self.compare(l) != Ordering::Less,
            (RangeBound::Excluded(l), RangeBound::Unbounded) => self.compare(l) == Ordering::Greater,
            (RangeBound::Unbounded, RangeBound::Unbounded) => true,
        }
    }
}

impl Collatable for ast::Literal {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            ast::Literal::String(s) => s.as_bytes().to_vec(),
            ast::Literal::I16(i) => i.to_be_bytes().to_vec(),
            ast::Literal::I32(i) => i.to_be_bytes().to_vec(),
            ast::Literal::I64(i) => i.to_be_bytes().to_vec(),
            ast::Literal::F64(f) => {
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
            ast::Literal::Bool(b) => vec![*b as u8],
            ast::Literal::EntityId(ulid) => ulid.to_bytes().to_vec(),
            ast::Literal::Object(bytes) => bytes.clone(),
            ast::Literal::Binary(bytes) => bytes.clone(),
        }
    }

    fn successor_bytes(&self) -> Option<Vec<u8>> {
        match self {
            ast::Literal::String(s) => {
                if s.is_empty() {
                    let mut bytes = s.as_bytes().to_vec();
                    // TODO - I think this is wrong. We shouldn't just push a byte. We should increment by one bit perhaps?
                    // It also occurs to me that we need a fixed length for strings in order collate properly.
                    bytes.push(0);
                    Some(bytes)
                } else {
                    let mut bytes = s.as_bytes().to_vec();
                    // TODO - I think this is wrong
                    bytes.push(0);
                    Some(bytes)
                }
            }
            ast::Literal::I16(i) => {
                if *i == i16::MAX {
                    None
                } else {
                    Some((i + 1).to_be_bytes().to_vec())
                }
            }
            ast::Literal::I32(i) => {
                if *i == i32::MAX {
                    None
                } else {
                    Some((i + 1).to_be_bytes().to_vec())
                }
            }
            ast::Literal::I64(i) => {
                if *i == i64::MAX {
                    None
                } else {
                    Some((i + 1).to_be_bytes().to_vec())
                }
            }
            ast::Literal::F64(f) => {
                if f.is_nan() || (f.is_infinite() && *f > 0.0) {
                    None
                } else {
                    let bits = if *f >= 0.0 { f.to_bits() ^ (1 << 63) } else { !f.to_bits() };
                    let next_bits = bits + 1;
                    Some(next_bits.to_be_bytes().to_vec())
                }
            }
            ast::Literal::Bool(b) => {
                if !b {
                    None
                } else {
                    Some(vec![1])
                }
            }
            ast::Literal::EntityId(ulid) => {
                let mut bytes = ulid.to_bytes();
                // Find the rightmost byte that can be incremented
                for i in (0..bytes.len()).rev() {
                    if bytes[i] < 255 {
                        bytes[i] += 1;
                        // Zero out all bytes to the right
                        for j in (i + 1)..bytes.len() {
                            bytes[j] = 0;
                        }
                        return Some(bytes.to_vec());
                    }
                }
                None // All bytes are 255, no successor
            }
            ast::Literal::Object(bytes) | ast::Literal::Binary(bytes) => {
                let mut bytes = bytes.clone();
                // Find the rightmost byte that can be incremented
                for i in (0..bytes.len()).rev() {
                    if bytes[i] < 255 {
                        bytes[i] += 1;
                        // Zero out all bytes to the right
                        for j in (i + 1)..bytes.len() {
                            bytes[j] = 0;
                        }
                        return Some(bytes);
                    }
                }
                // All bytes are 255, append a zero byte
                bytes.push(0);
                Some(bytes)
            }
        }
    }

    fn predecessor_bytes(&self) -> Option<Vec<u8>> {
        match self {
            ast::Literal::String(s) => {
                if s.is_empty() {
                    None
                } else {
                    let bytes = s.as_bytes();
                    Some(bytes[..bytes.len() - 1].to_vec())
                }
            }
            ast::Literal::I16(i) => {
                if *i == i16::MIN {
                    None
                } else {
                    Some((i - 1).to_be_bytes().to_vec())
                }
            }
            ast::Literal::I32(i) => {
                if *i == i32::MIN {
                    None
                } else {
                    Some((i - 1).to_be_bytes().to_vec())
                }
            }
            ast::Literal::I64(i) => {
                if *i == i64::MIN {
                    None
                } else {
                    Some((i - 1).to_be_bytes().to_vec())
                }
            }
            ast::Literal::F64(f) => {
                if f.is_nan() || (f.is_infinite() && *f < 0.0) {
                    None
                } else {
                    let bits = if *f >= 0.0 { f.to_bits() ^ (1 << 63) } else { !f.to_bits() };
                    let prev_bits = bits - 1;
                    Some(prev_bits.to_be_bytes().to_vec())
                }
            }
            ast::Literal::Bool(b) => {
                if *b {
                    Some(vec![0])
                } else {
                    None
                }
            }
            ast::Literal::EntityId(ulid) => {
                let mut bytes = ulid.to_bytes();
                // Find the rightmost byte that can be decremented
                for i in (0..bytes.len()).rev() {
                    if bytes[i] > 0 {
                        bytes[i] -= 1;
                        // Set all bytes to the right to 255
                        for j in (i + 1)..bytes.len() {
                            bytes[j] = 255;
                        }
                        return Some(bytes.to_vec());
                    }
                }
                None // All bytes are 0, no predecessor
            }
            ast::Literal::Object(bytes) | ast::Literal::Binary(bytes) => {
                if bytes.is_empty() {
                    None
                } else {
                    let mut bytes = bytes.clone();
                    // Find the rightmost byte that can be decremented
                    for i in (0..bytes.len()).rev() {
                        if bytes[i] > 0 {
                            bytes[i] -= 1;
                            // Set all bytes to the right to 255
                            for j in (i + 1)..bytes.len() {
                                bytes[j] = 255;
                            }
                            return Some(bytes);
                        }
                    }
                    // All bytes are 0, remove the last byte
                    if bytes.len() > 1 {
                        bytes.pop();
                        Some(bytes)
                    } else {
                        None
                    }
                }
            }
        }
    }

    fn is_minimum(&self) -> bool {
        match self {
            ast::Literal::String(s) => s.is_empty(),
            ast::Literal::I16(i) => *i == i16::MIN,
            ast::Literal::I32(i) => *i == i32::MIN,
            ast::Literal::I64(i) => *i == i64::MIN,
            ast::Literal::F64(f) => *f == f64::NEG_INFINITY,
            ast::Literal::Bool(b) => !b,
            ast::Literal::EntityId(ulid) => ulid.to_bytes().iter().all(|&b| b == 0),
            ast::Literal::Object(bytes) | ast::Literal::Binary(bytes) => bytes.is_empty(),
        }
    }

    fn is_maximum(&self) -> bool {
        match self {
            ast::Literal::String(_) => false, // Strings have no theoretical maximum
            ast::Literal::I16(i) => *i == i16::MAX,
            ast::Literal::I32(i) => *i == i32::MAX,
            ast::Literal::I64(i) => *i == i64::MAX,
            ast::Literal::F64(f) => *f == f64::INFINITY,
            ast::Literal::Bool(b) => *b,
            ast::Literal::EntityId(ulid) => ulid.to_bytes().iter().all(|&b| b == 255),
            ast::Literal::Object(_) | ast::Literal::Binary(_) => false, // No theoretical maximum
        }
    }
}

// // Implementation for strings
impl Collatable for &str {
    fn to_bytes(&self) -> Vec<u8> { self.as_bytes().to_vec() }

    fn successor_bytes(&self) -> Option<Vec<u8>> {
        if self.is_maximum() {
            None
        } else {
            let mut bytes = self.as_bytes().to_vec();
            bytes.push(0);
            Some(bytes)
        }
    }

    fn predecessor_bytes(&self) -> Option<Vec<u8>> {
        if self.is_minimum() {
            None
        } else {
            let bytes = self.as_bytes();
            if bytes.is_empty() {
                None
            } else {
                Some(bytes[..bytes.len() - 1].to_vec())
            }
        }
    }

    fn is_minimum(&self) -> bool { self.is_empty() }

    fn is_maximum(&self) -> bool {
        false // Strings have no theoretical maximum
    }
}

// Implementation for integers
impl Collatable for i64 {
    fn to_bytes(&self) -> Vec<u8> {
        // Use big-endian encoding to preserve ordering
        self.to_be_bytes().to_vec()
    }

    fn successor_bytes(&self) -> Option<Vec<u8>> {
        if self == &i64::MAX {
            None
        } else {
            Some((self + 1).to_be_bytes().to_vec())
        }
    }

    fn predecessor_bytes(&self) -> Option<Vec<u8>> {
        if self == &i64::MIN {
            None
        } else {
            Some((self - 1).to_be_bytes().to_vec())
        }
    }

    fn is_minimum(&self) -> bool { *self == i64::MIN }

    fn is_maximum(&self) -> bool { *self == i64::MAX }
}

// Implementation for floats
impl Collatable for f64 {
    fn to_bytes(&self) -> Vec<u8> {
        let bits = if self.is_nan() {
            u64::MAX // NaN sorts last
        } else {
            let bits = self.to_bits();
            if *self >= 0.0 {
                bits ^ (1 << 63) // Flip sign bit for positive numbers
            } else {
                !bits // Flip all bits for negative numbers
            }
        };
        bits.to_be_bytes().to_vec()
    }

    fn successor_bytes(&self) -> Option<Vec<u8>> {
        if self.is_nan() || (self.is_infinite() && *self > 0.0) {
            None
        } else {
            let bits = if *self >= 0.0 {
                self.to_bits() ^ (1 << 63) // Apply same sign bit flip as to_bytes
            } else {
                !self.to_bits() // Apply same bit inversion as to_bytes
            };
            let next_bits = bits + 1;
            Some(next_bits.to_be_bytes().to_vec())
        }
    }

    fn predecessor_bytes(&self) -> Option<Vec<u8>> {
        if self.is_nan() || (self.is_infinite() && *self < 0.0) {
            None
        } else {
            let bits = if *self >= 0.0 {
                self.to_bits() ^ (1 << 63) // Apply same sign bit flip as to_bytes
            } else {
                !self.to_bits() // Apply same bit inversion as to_bytes
            };
            let prev_bits = bits - 1;
            Some(prev_bits.to_be_bytes().to_vec())
        }
    }

    fn is_minimum(&self) -> bool { *self == f64::NEG_INFINITY }

    fn is_maximum(&self) -> bool { *self == f64::INFINITY }
}

// Implementation for EntityId (ULIDs have natural lexicographic ordering)
impl Collatable for EntityId {
    fn to_bytes(&self) -> Vec<u8> {
        // EntityId is based on ULID which has lexicographic ordering
        self.to_bytes().to_vec()
    }

    fn successor_bytes(&self) -> Option<Vec<u8>> {
        if self.is_maximum() {
            None
        } else {
            let mut bytes = self.to_bytes();
            // Find the rightmost byte that can be incremented
            for i in (0..bytes.len()).rev() {
                if bytes[i] < 255 {
                    bytes[i] += 1;
                    // Zero out all bytes to the right
                    for j in (i + 1)..bytes.len() {
                        bytes[j] = 0;
                    }
                    return Some(bytes.to_vec());
                }
            }
            None // All bytes are 255, no successor
        }
    }

    fn predecessor_bytes(&self) -> Option<Vec<u8>> {
        if self.is_minimum() {
            None
        } else {
            let mut bytes = self.to_bytes();
            // Find the rightmost byte that can be decremented
            for i in (0..bytes.len()).rev() {
                if bytes[i] > 0 {
                    bytes[i] -= 1;
                    // Set all bytes to the right to 255
                    for j in (i + 1)..bytes.len() {
                        bytes[j] = 255;
                    }
                    return Some(bytes.to_vec());
                }
            }
            None // All bytes are 0, no predecessor
        }
    }

    fn is_minimum(&self) -> bool { self.to_bytes().iter().all(|&b| b == 0) }

    fn is_maximum(&self) -> bool { self.to_bytes().iter().all(|&b| b == 255) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_collation() {
        let s = "hello";
        assert!(s.successor_bytes().unwrap() > s.to_bytes());
        assert!(s.predecessor_bytes().unwrap() < s.to_bytes());
        assert!(!s.is_minimum());
        assert!(!s.is_maximum());

        let empty = "";
        assert!(empty.is_minimum());
        assert!(empty.predecessor_bytes().is_none());
    }

    #[test]
    fn test_integer_collation() {
        let n = 42i64;
        assert_eq!(i64::from_be_bytes(n.successor_bytes().unwrap().try_into().unwrap()), 43);
        assert_eq!(i64::from_be_bytes(n.predecessor_bytes().unwrap().try_into().unwrap()), 41);
        assert!(!n.is_minimum());
        assert!(!n.is_maximum());

        assert!(i64::MAX.successor_bytes().is_none());
        assert!(i64::MIN.predecessor_bytes().is_none());
        assert!(i64::MAX.is_maximum());
        assert!(i64::MIN.is_minimum());
    }

    #[test]
    fn test_float_collation() {
        let f = 1.0f64;
        assert!(f.successor_bytes().unwrap() > f.to_bytes());
        assert!(f.predecessor_bytes().unwrap() < f.to_bytes());
        assert!(!f.is_minimum());
        assert!(!f.is_maximum());

        assert!(f64::INFINITY.is_maximum());
        assert!(f64::NEG_INFINITY.is_minimum());
        assert!(f64::INFINITY.successor_bytes().is_none());
        assert!(f64::NEG_INFINITY.predecessor_bytes().is_none());

        let nan = f64::NAN;
        assert!(nan.successor_bytes().is_none());
        assert!(nan.predecessor_bytes().is_none());
    }

    #[test]
    fn test_range_bounds() {
        let n = 42i64;

        // Test inclusive bounds
        assert!(n.is_in_range(RangeBound::Included(&40), RangeBound::Included(&45)));
        assert!(n.is_in_range(RangeBound::Included(&42), RangeBound::Included(&45)));
        assert!(n.is_in_range(RangeBound::Included(&40), RangeBound::Included(&42)));

        // Test exclusive bounds
        assert!(n.is_in_range(RangeBound::Excluded(&40), RangeBound::Excluded(&43)));
        assert!(!n.is_in_range(RangeBound::Excluded(&42), RangeBound::Excluded(&43)));

        // Test mixed bounds
        assert!(n.is_in_range(RangeBound::Included(&42), RangeBound::Excluded(&43)));
        assert!(!n.is_in_range(RangeBound::Excluded(&41), RangeBound::Excluded(&42)));

        // Test unbounded
        assert!(n.is_in_range(RangeBound::Unbounded, RangeBound::Included(&45)));
        assert!(n.is_in_range(RangeBound::Included(&40), RangeBound::Unbounded));
        assert!(n.is_in_range(RangeBound::Unbounded, RangeBound::Unbounded));
    }

    #[test]
    fn test_literal_i16_collation() {
        let lit = ast::Literal::I16(100);
        assert!(lit.successor_bytes().unwrap() > lit.to_bytes());
        assert!(lit.predecessor_bytes().unwrap() < lit.to_bytes());
        assert!(!lit.is_minimum());
        assert!(!lit.is_maximum());

        let max_lit = ast::Literal::I16(i16::MAX);
        let min_lit = ast::Literal::I16(i16::MIN);
        assert!(max_lit.successor_bytes().is_none());
        assert!(min_lit.predecessor_bytes().is_none());
        assert!(max_lit.is_maximum());
        assert!(min_lit.is_minimum());
    }

    #[test]
    fn test_literal_i32_collation() {
        let lit = ast::Literal::I32(1000);
        assert!(lit.successor_bytes().unwrap() > lit.to_bytes());
        assert!(lit.predecessor_bytes().unwrap() < lit.to_bytes());
        assert!(!lit.is_minimum());
        assert!(!lit.is_maximum());

        let max_lit = ast::Literal::I32(i32::MAX);
        let min_lit = ast::Literal::I32(i32::MIN);
        assert!(max_lit.successor_bytes().is_none());
        assert!(min_lit.predecessor_bytes().is_none());
        assert!(max_lit.is_maximum());
        assert!(min_lit.is_minimum());
    }

    #[test]
    fn test_literal_entity_id_collation() {
        use ulid::Ulid;
        let ulid = Ulid::new();
        let lit = ast::Literal::EntityId(ulid);

        // Test basic operations
        assert!(!lit.is_minimum());
        assert!(!lit.is_maximum());

        // Test minimum ULID (all zeros)
        let min_ulid = Ulid::from_bytes([0; 16]);
        let min_lit = ast::Literal::EntityId(min_ulid);
        assert!(min_lit.is_minimum());
        assert!(min_lit.predecessor_bytes().is_none());

        // Test maximum ULID (all 255s)
        let max_ulid = Ulid::from_bytes([255; 16]);
        let max_lit = ast::Literal::EntityId(max_ulid);
        assert!(max_lit.is_maximum());
        assert!(max_lit.successor_bytes().is_none());
    }

    #[test]
    fn test_literal_binary_collation() {
        let lit = ast::Literal::Binary(vec![1, 2, 3]);
        assert!(lit.successor_bytes().unwrap() > lit.to_bytes());
        assert!(lit.predecessor_bytes().unwrap() < lit.to_bytes());
        assert!(!lit.is_minimum());
        assert!(!lit.is_maximum());

        let empty_lit = ast::Literal::Binary(vec![]);
        assert!(empty_lit.is_minimum());
        assert!(empty_lit.predecessor_bytes().is_none());
        assert!(!empty_lit.is_maximum());
    }

    #[test]
    fn test_literal_object_collation() {
        let lit = ast::Literal::Object(vec![10, 20, 30]);
        assert!(lit.successor_bytes().unwrap() > lit.to_bytes());
        assert!(lit.predecessor_bytes().unwrap() < lit.to_bytes());
        assert!(!lit.is_minimum());
        assert!(!lit.is_maximum());

        let empty_lit = ast::Literal::Object(vec![]);
        assert!(empty_lit.is_minimum());
        assert!(empty_lit.predecessor_bytes().is_none());
        assert!(!empty_lit.is_maximum());
    }
}
