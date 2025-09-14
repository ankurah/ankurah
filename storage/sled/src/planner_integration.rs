use ankurah_core::collation::Collatable;
use ankurah_core::property::PropertyValue;
use ankurah_storage_common::{CanonicalRange, Endpoint, IndexBounds, KeyDatum};

/// Normalize IndexBounds to CanonicalRange (reuse the same logic shape as IndexedDB)
pub fn normalize(bounds: &IndexBounds) -> (CanonicalRange, usize, Vec<PropertyValue>) {
    let mut lower_tuple = Vec::new();
    let mut upper_tuple = Vec::new();
    let mut lower_open = false;
    let mut upper_open = false;
    let mut eq_prefix_len = 0;
    let mut eq_prefix_values = Vec::new();

    for bound in &bounds.keyparts {
        // Equality check (low==high, both inclusive)
        if let (Endpoint::Value { datum: low_datum, inclusive: low_incl }, Endpoint::Value { datum: high_datum, inclusive: high_incl }) =
            (&bound.low, &bound.high)
        {
            if let (KeyDatum::Val(low_val), KeyDatum::Val(high_val)) = (low_datum, high_datum) {
                if low_val == high_val && *low_incl && *high_incl {
                    lower_tuple.push(low_val.clone());
                    upper_tuple.push(high_val.clone());
                    eq_prefix_values.push(low_val.clone());
                    eq_prefix_len += 1;
                    continue;
                }
            }
        }

        // Lower side
        match &bound.low {
            Endpoint::Value { datum: KeyDatum::Val(val), inclusive } => {
                lower_tuple.push(val.clone());
                lower_open = !inclusive;
            }
            Endpoint::UnboundedLow(_) => {}
            _ => break,
        }

        // Upper side
        match &bound.high {
            Endpoint::Value { datum: KeyDatum::Val(val), inclusive } => {
                upper_tuple.push(val.clone());
                upper_open = !inclusive;
            }
            Endpoint::UnboundedHigh(_) => {
                return (CanonicalRange { lower: Some((lower_tuple, lower_open)), upper: None }, eq_prefix_len, eq_prefix_values);
            }
            _ => {
                return (CanonicalRange { lower: Some((lower_tuple, lower_open)), upper: None }, eq_prefix_len, eq_prefix_values);
            }
        }

        break; // stop after first non-equality column
    }

    if eq_prefix_len == bounds.keyparts.len() && eq_prefix_len == 1 {
        return (CanonicalRange { lower: Some((lower_tuple, lower_open)), upper: None }, eq_prefix_len, eq_prefix_values);
    }

    let canonical_range = CanonicalRange {
        lower: if lower_tuple.is_empty() { None } else { Some((lower_tuple, lower_open)) },
        upper: if upper_tuple.is_empty() { None } else { Some((upper_tuple, upper_open)) },
    };

    (canonical_range, eq_prefix_len, eq_prefix_values)
}

/// Encode a tuple of PropertyValue into a composite key (type/tag prefixed length segments)
/// Rationale: type tags + length delimiters preserve lexicographic ordering and make tuples unambiguous.
pub fn encode_tuple_for_sled(parts: &[PropertyValue]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in parts {
        let (tag, body) = match p {
            PropertyValue::String(_) => (0x10u8, p.to_bytes()),
            PropertyValue::I16(_) | PropertyValue::I32(_) | PropertyValue::I64(_) => (0x20u8, p.to_bytes()),
            PropertyValue::Bool(_) => (0x40u8, p.to_bytes()),
            PropertyValue::Object(_) | PropertyValue::Binary(_) => (0x50u8, p.to_bytes()),
        };
        // type tag then length header then body
        out.push(tag);
        let len = (body.len() as u32).to_be_bytes();
        out.extend_from_slice(&len);
        out.extend_from_slice(&body);
    }
    out
}

/// Compute the lexicographic successor of a composite tuple encoding
pub fn lex_successor(mut key: Vec<u8>) -> Option<Vec<u8>> {
    // Simple successor: append a null byte; relies on non-zero terminator between tuple and entity id
    key.push(0);
    Some(key)
}
