use std::cmp::Ordering;

/// Represents a bound in a range query
#[derive(Debug, Clone, PartialEq)]
pub enum RangeBound<T> {
    Included(T),
    Excluded(T),
    Unbounded,
}

/// Trait for types that support collation operations
pub trait Collation: Sized {
    /// Returns the immediate successor of the current value if one exists
    fn successor(&self) -> Option<Self>;

    /// Returns the immediate predecessor of the current value if one exists
    fn predecessor(&self) -> Option<Self>;

    /// Returns true if this value represents a minimum bound in its domain
    fn is_minimum(&self) -> bool;

    /// Returns true if this value represents a maximum bound in its domain
    fn is_maximum(&self) -> bool;

    /// Compare two values in the collation order
    fn compare(&self, other: &Self) -> Ordering;

    /// Returns true if this value is within the given range
    fn is_in_range(&self, lower: RangeBound<&Self>, upper: RangeBound<&Self>) -> bool {
        match (&lower, &upper) {
            (RangeBound::Unbounded, RangeBound::Unbounded) => true,
            (RangeBound::Unbounded, RangeBound::Included(upper)) => {
                self.compare(upper) != Ordering::Greater
            }
            (RangeBound::Unbounded, RangeBound::Excluded(upper)) => {
                self.compare(upper) == Ordering::Less
            }
            (RangeBound::Included(lower), RangeBound::Unbounded) => {
                self.compare(lower) != Ordering::Less
            }
            (RangeBound::Excluded(lower), RangeBound::Unbounded) => {
                self.compare(lower) == Ordering::Greater
            }
            (RangeBound::Included(lower), RangeBound::Included(upper)) => {
                self.compare(lower) != Ordering::Less && self.compare(upper) != Ordering::Greater
            }
            (RangeBound::Included(lower), RangeBound::Excluded(upper)) => {
                self.compare(lower) != Ordering::Less && self.compare(upper) == Ordering::Less
            }
            (RangeBound::Excluded(lower), RangeBound::Included(upper)) => {
                self.compare(lower) == Ordering::Greater && self.compare(upper) != Ordering::Greater
            }
            (RangeBound::Excluded(lower), RangeBound::Excluded(upper)) => {
                self.compare(lower) == Ordering::Greater && self.compare(upper) == Ordering::Less
            }
        }
    }

    /// Returns the next value after the given bound (useful for converting inclusive to exclusive bounds)
    fn after_bound(bound: RangeBound<Self>) -> RangeBound<Self> {
        match bound {
            RangeBound::Included(value) => {
                if let Some(next) = value.successor() {
                    RangeBound::Excluded(next)
                } else {
                    RangeBound::Unbounded
                }
            }
            other => other,
        }
    }

    /// Returns the previous value before the given bound (useful for converting inclusive to exclusive bounds)
    fn before_bound(bound: RangeBound<Self>) -> RangeBound<Self> {
        match bound {
            RangeBound::Included(value) => {
                if let Some(prev) = value.predecessor() {
                    RangeBound::Excluded(prev)
                } else {
                    RangeBound::Unbounded
                }
            }
            other => other,
        }
    }
}

// Implementation for strings
impl Collation for String {
    fn successor(&self) -> Option<Self> {
        if self.is_maximum() {
            None
        } else {
            let mut bytes = self.as_bytes().to_vec();
            bytes.push(0);
            Some(String::from_utf8(bytes).unwrap())
        }
    }

    fn predecessor(&self) -> Option<Self> {
        if self.is_minimum() {
            None
        } else {
            let bytes = self.as_bytes();
            if bytes.is_empty() {
                None
            } else {
                Some(String::from_utf8(bytes[..bytes.len() - 1].to_vec()).unwrap())
            }
        }
    }

    fn is_minimum(&self) -> bool {
        self.is_empty()
    }

    fn is_maximum(&self) -> bool {
        false // Strings have no theoretical maximum
    }

    fn compare(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }
}

// Implementation for integers
impl Collation for i64 {
    fn successor(&self) -> Option<Self> {
        if self == &i64::MAX {
            None
        } else {
            Some(self + 1)
        }
    }

    fn predecessor(&self) -> Option<Self> {
        if self == &i64::MIN {
            None
        } else {
            Some(self - 1)
        }
    }

    fn is_minimum(&self) -> bool {
        *self == i64::MIN
    }

    fn is_maximum(&self) -> bool {
        *self == i64::MAX
    }

    fn compare(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }
}

// Implementation for floats
impl Collation for f64 {
    fn successor(&self) -> Option<Self> {
        if self.is_nan() || (self.is_infinite() && *self > 0.0) {
            None
        } else {
            // Use bit manipulation for next representable float
            let bits = self.to_bits();
            let next_bits = if *self >= 0.0 { bits + 1 } else { bits - 1 };
            Some(f64::from_bits(next_bits))
        }
    }

    fn predecessor(&self) -> Option<Self> {
        if self.is_nan() || (self.is_infinite() && *self < 0.0) {
            None
        } else {
            // Use bit manipulation for previous representable float
            let bits = self.to_bits();
            let prev_bits = if *self > 0.0 { bits - 1 } else { bits + 1 };
            Some(f64::from_bits(prev_bits))
        }
    }

    fn is_minimum(&self) -> bool {
        *self == f64::NEG_INFINITY
    }

    fn is_maximum(&self) -> bool {
        *self == f64::INFINITY
    }

    fn compare(&self, other: &Self) -> Ordering {
        if self.is_nan() || other.is_nan() {
            Ordering::Equal // NaN is considered equal to itself for collation purposes
        } else {
            self.partial_cmp(other).unwrap_or(Ordering::Equal)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_collation() {
        let s = String::from("hello");
        assert!(s.successor().unwrap().compare(&s) == Ordering::Greater);
        assert!(s.predecessor().unwrap().compare(&s) == Ordering::Less);
        assert!(!s.is_minimum());
        assert!(!s.is_maximum());

        let empty = String::new();
        assert!(empty.is_minimum());
        assert!(empty.predecessor().is_none());
    }

    #[test]
    fn test_integer_collation() {
        let n = 42i64;
        assert_eq!(n.successor().unwrap(), 43);
        assert_eq!(n.predecessor().unwrap(), 41);
        assert!(!n.is_minimum());
        assert!(!n.is_maximum());

        assert!(i64::MAX.successor().is_none());
        assert!(i64::MIN.predecessor().is_none());
        assert!(i64::MAX.is_maximum());
        assert!(i64::MIN.is_minimum());
    }

    #[test]
    fn test_float_collation() {
        let f = 1.0f64;
        assert!(f.successor().unwrap().compare(&f) == Ordering::Greater);
        assert!(f.predecessor().unwrap().compare(&f) == Ordering::Less);
        assert!(!f.is_minimum());
        assert!(!f.is_maximum());

        assert!(f64::INFINITY.is_maximum());
        assert!(f64::NEG_INFINITY.is_minimum());
        assert!(f64::INFINITY.successor().is_none());
        assert!(f64::NEG_INFINITY.predecessor().is_none());

        let nan = f64::NAN;
        assert!(nan.successor().is_none());
        assert!(nan.predecessor().is_none());
        assert!(nan.compare(&nan) == Ordering::Equal); // NaN equals NaN for collation
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
