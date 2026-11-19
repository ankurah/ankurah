//! JSONB's comparator visits container sizes first, then their contents.
//! Object contents are visited in PostgreSQL's storage order (key byte length,
//! then bytes), although each visited key compares in ordinary C string order.
//!
//! Reference: PostgreSQL `compareJsonbContainers` and
//! `lengthCompareJsonbString` in src/backend/utils/adt/jsonb_util.c.

use serde_json::Value;

use super::frame;

pub(super) fn bytes(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    // PostgreSQL stores a root scalar as a one-element pseudo-array. Its array
    // length comparison makes a root empty array precede every scalar. Nested
    // empty arrays are ordinary arrays and retain their type rank.
    if matches!(value, Value::Array(values) if values.is_empty()) {
        return vec![0];
    }
    append(value, &mut out);
    out
}

fn append(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.push(1),
        Value::String(value) => {
            out.push(2);
            frame(out, value.as_bytes());
        }
        Value::Number(value) => {
            out.push(3);
            number(&value.to_string(), out);
        }
        Value::Bool(value) => out.extend([4, u8::from(*value)]),
        Value::Array(values) => {
            out.push(5);
            out.extend_from_slice(&(values.len() as u64).to_be_bytes());
            for value in values {
                append(value, out);
            }
        }
        Value::Object(values) => {
            out.push(6);
            out.extend_from_slice(&(values.len() as u64).to_be_bytes());
            let mut pairs: Vec<_> = values.iter().collect();
            pairs
                .sort_unstable_by(|(left, _), (right, _)| left.len().cmp(&right.len()).then_with(|| left.as_bytes().cmp(right.as_bytes())));
            for (key, value) in pairs {
                frame(out, key.as_bytes());
                append(value, out);
            }
        }
    }
}

fn number(value: &str, out: &mut Vec<u8>) {
    let (negative, value) = value.strip_prefix('-').map_or((false, value), |value| (true, value));
    let (mantissa, exponent) = value.split_once(['e', 'E']).unwrap_or((value, "0"));
    let decimal = mantissa.find('.').unwrap_or(mantissa.len());
    let digits: Vec<_> = mantissa.bytes().filter(|byte| *byte != b'.').collect();
    let Some(first) = digits.iter().position(|byte| *byte != b'0') else {
        out.push(1);
        return;
    };
    let end = digits.iter().rposition(|byte| *byte != b'0').unwrap() + 1;
    out.push(if negative { 0 } else { 2 });
    let start = out.len();
    // Decimal exponents, not f64 approximations: 2 == 2.0 and adjacent large
    // integers remain distinct. Arbitrary exponent arithmetic also avoids a
    // panic on JSON numbers outside PostgreSQL numeric's supported range.
    let (exponent_negative, exponent_digits) = adjusted_exponent(exponent, decimal as i128 - first as i128);
    out.push(if exponent_negative { 0 } else { 1 });
    let exponent_start = out.len();
    out.extend_from_slice(&(exponent_digits.len() as u64).to_be_bytes());
    out.extend_from_slice(&exponent_digits);
    if exponent_negative {
        out[exponent_start..].iter_mut().for_each(|byte| *byte = !*byte);
    }
    out.extend_from_slice(&digits[first..end]);
    out.push(0);
    if negative {
        out[start..].iter_mut().for_each(|byte| *byte = !*byte);
    }
}

/// Add the mantissa's decimal shift without narrowing an arbitrary JSON exponent.
fn adjusted_exponent(exponent: &str, shift: i128) -> (bool, Vec<u8>) {
    let negative = exponent.starts_with('-');
    let digits = exponent.trim_start_matches(['-', '+']).trim_start_matches('0').as_bytes();
    let shift_digits = shift.unsigned_abs().to_string();
    let shift_digits = shift_digits.trim_start_matches('0').as_bytes();
    let mut left = digits.to_vec();
    let mut right = shift_digits.to_vec();
    let mut sign = negative;
    if negative == (shift < 0) {
        let mut carry = 0;
        let mut result = Vec::new();
        for index in 0..left.len().max(right.len()) {
            let a = left.len().checked_sub(index + 1).map_or(0, |at| left[at] - b'0');
            let b = right.len().checked_sub(index + 1).map_or(0, |at| right[at] - b'0');
            let sum = a + b + carry;
            result.push(b'0' + sum % 10);
            carry = sum / 10;
        }
        if carry != 0 {
            result.push(b'0' + carry);
        }
        result.reverse();
        return (negative && !result.is_empty(), result);
    }
    if (left.len(), &left) < (right.len(), &right) {
        std::mem::swap(&mut left, &mut right);
        sign = shift < 0;
    }
    let mut borrow = 0;
    for index in 0..left.len() {
        let at = left.len() - 1 - index;
        let right = right.len().checked_sub(index + 1).map_or(0, |at| right[at] - b'0');
        let difference = 10 + left[at] - b'0' - right - borrow;
        left[at] = b'0' + difference % 10;
        borrow = u8::from(difference < 10);
    }
    let first = left.iter().position(|byte| *byte != b'0').unwrap_or(left.len());
    left.drain(..first);
    (sign && !left.is_empty(), left)
}
