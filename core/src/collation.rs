use std::cmp::Ordering;

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
        // Flip the sign bit so big-endian byte order matches signed numeric order.
        ((*self as u64) ^ (1 << 63)).to_be_bytes().to_vec()
    }

    fn successor_bytes(&self) -> Option<Vec<u8>> {
        if self == &i64::MAX {
            None
        } else {
            Some((self + 1).to_bytes())
        }
    }

    fn predecessor_bytes(&self) -> Option<Vec<u8>> {
        if self == &i64::MIN {
            None
        } else {
            Some((self - 1).to_bytes())
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
            float_key_bits(*self)
        };
        bits.to_be_bytes().to_vec()
    }

    fn successor_bytes(&self) -> Option<Vec<u8>> {
        if self.is_nan() || (self.is_infinite() && *self > 0.0) {
            None
        } else {
            let bits = float_key_bits(*self);
            let next_bits = bits + 1;
            Some(next_bits.to_be_bytes().to_vec())
        }
    }

    fn predecessor_bytes(&self) -> Option<Vec<u8>> {
        if self.is_nan() || (self.is_infinite() && *self < 0.0) {
            None
        } else {
            let bits = float_key_bits(*self);
            let prev_bits = bits - 1;
            Some(prev_bits.to_be_bytes().to_vec())
        }
    }

    fn is_minimum(&self) -> bool { *self == f64::NEG_INFINITY }

    fn is_maximum(&self) -> bool { *self == f64::INFINITY }
}

/// A non-NaN float's bits arranged so that unsigned order matches numeric order.
fn float_key_bits(f: f64) -> u64 {
    // Both zeros compare equal, so negative zero takes positive zero's key.
    let f = if f == 0.0 { 0.0 } else { f };
    if f >= 0.0 {
        f.to_bits() ^ (1 << 63) // Flip sign bit for positive numbers
    } else {
        !f.to_bits() // Flip all bits for negative numbers
    }
}

// Implementation for EntityId: the raw hash bytes order lexicographically.
impl Collatable for EntityId {
    fn to_bytes(&self) -> Vec<u8> { self.to_bytes().to_vec() }

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
        assert_eq!(n.successor_bytes(), Some(43i64.to_bytes()));
        assert_eq!(n.predecessor_bytes(), Some(41i64.to_bytes()));
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
}
