use crate::{CanonicalRange, Endpoint, KeyBounds, KeyDatum};
use ankurah_core::value::Value;

/// Normalize IndexBounds to a CanonicalRange shape shared across KV engines
pub fn normalize(bounds: &KeyBounds) -> (CanonicalRange, usize, Vec<Value>) {
    let mut lower_tuple = Vec::new();
    let mut upper_tuple = Vec::new();
    let mut lower_open = false;
    let mut upper_open = false;
    let mut eq_prefix_len = 0;
    let mut eq_prefix_values = Vec::new();

    for bound in &bounds.keyparts {
        if let (Endpoint::Value { datum: low_datum, inclusive: low_incl }, Endpoint::Value { datum: high_datum, inclusive: high_incl }) =
            (&bound.low, &bound.high)
            && let (KeyDatum::Val(low_val), KeyDatum::Val(high_val)) = (low_datum, high_datum)
            && low_val == high_val
            && *low_incl
            && *high_incl
        {
            lower_tuple.push(low_val.clone());
            upper_tuple.push(high_val.clone());
            eq_prefix_values.push(low_val.clone());
            eq_prefix_len += 1;
            continue;
        }

        match &bound.low {
            Endpoint::Value { datum: KeyDatum::Val(val), inclusive } => {
                lower_tuple.push(val.clone());
                lower_open = !inclusive;
            }
            Endpoint::UnboundedLow(_) => {}
            _ => break,
        }

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

        break;
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
