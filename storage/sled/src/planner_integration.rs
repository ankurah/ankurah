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
    // True bytewise successor: increment with carry; if overflow (all 0xFF), no successor
    for i in (0..key.len()).rev() {
        if key[i] != 0xFF {
            key[i] += 1;
            for j in (i + 1)..key.len() {
                key[j] = 0x00;
            }
            return Some(key);
        }
    }
    None
}

/// Convert canonical range and eq-prefix into sled key bounds over the composite tuple
/// Returns: (start_full, end_full_opt, upper_open_ended, eq_prefix_bytes)
pub fn bounds_to_sled_range(
    canonical: &CanonicalRange,
    eq_prefix_len: usize,
    eq_prefix_values: &[PropertyValue],
) -> (Vec<u8>, Option<Vec<u8>>, bool, Vec<u8>) {
    let mut upper_open_ended = false;
    let start_full = match &canonical.lower {
        Some((vals, lower_open)) => {
            let mut start = encode_tuple_for_sled(vals);
            if *lower_open {
                start = match lex_successor(start) {
                    Some(s) => s,
                    None => Vec::new(), // will indicate empty range to caller if needed
                };
            }
            start.push(0);
            start
        }
        None => {
            // Unbounded low: start at minimal tuple (empty) which is before any encoded tuple
            let mut start = Vec::new();
            start.push(0);
            start
        }
    };

    let end_full_opt = match &canonical.upper {
        Some((vals, upper_open)) => {
            let mut end = encode_tuple_for_sled(vals);
            if !*upper_open {
                if let Some(s) = lex_successor(end) {
                    end = s;
                } else {
                    // No successor → treat as unbounded-high
                    upper_open_ended = true;
                    return (
                        start_full,
                        None,
                        upper_open_ended,
                        if eq_prefix_len > 0 { encode_tuple_for_sled(eq_prefix_values) } else { Vec::new() },
                    );
                }
            }
            end.push(0);
            Some(end)
        }
        None => {
            upper_open_ended = true;
            None
        }
    };

    let eq_prefix_bytes = if eq_prefix_len > 0 { encode_tuple_for_sled(eq_prefix_values) } else { Vec::new() };
    (start_full, end_full_opt, upper_open_ended, eq_prefix_bytes)
}
