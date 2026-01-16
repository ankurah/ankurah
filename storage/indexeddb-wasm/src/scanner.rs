//! IndexedDB cursor scanner for iterating over index records.
//!
//! `IdbIndexScanner` encapsulates the logic for opening an IndexedDB cursor
//! and iterating over records, including prefix guard termination for
//! open-ended ranges with equality prefixes.

use crate::util::cb_stream::cb_stream;
use crate::util::object::Object;
use ankurah_core::error::RetrievalError;
use futures::stream::{Stream, StreamExt};
use wasm_bindgen::prelude::*;

/// Configuration for an IndexedDB index scan
pub struct IdbIndexScanner {
    index: web_sys::IdbIndex,
    key_range: Option<web_sys::IdbKeyRange>,
    cursor_direction: web_sys::IdbCursorDirection,
    eq_prefix_len: usize,
    eq_prefix_js: Vec<JsValue>,
}

impl IdbIndexScanner {
    /// Create a new scanner configuration
    pub fn new(
        index: web_sys::IdbIndex,
        key_range: Option<web_sys::IdbKeyRange>,
        cursor_direction: web_sys::IdbCursorDirection,
        eq_prefix_len: usize,
        eq_prefix_values: Vec<ankurah_core::value::Value>,
    ) -> Self {
        // Convert equality prefix values to JsValues for comparison
        let eq_prefix_js: Vec<JsValue> = eq_prefix_values
            .iter()
            .map(|v| {
                let js_val: JsValue = crate::idb_value::IdbValue::from(v).into();
                js_val
            })
            .collect();

        Self { index, key_range, cursor_direction, eq_prefix_len, eq_prefix_js }
    }

    /// Scan the index, yielding JS Objects for each record.
    ///
    /// The scan handles:
    /// - Opening the cursor with the configured range and direction
    /// - Prefix guard termination for open-ended ranges
    /// - Cursor advancement via continue_()
    ///
    /// Returns a stream of `Result<Object, RetrievalError>` for each record.
    pub fn scan(self) -> impl Stream<Item = Result<Object, RetrievalError>> {
        futures::stream::unfold(Some(ScanState::Initial(self)), |state| async move {
            let state = state?;

            match state {
                ScanState::Initial(scanner) => {
                    // Open the cursor
                    let cursor_request = if let Some(range) = &scanner.key_range {
                        scanner.index.open_cursor_with_range_and_direction(range.as_ref(), scanner.cursor_direction)
                    } else {
                        scanner.index.open_cursor_with_range_and_direction(&JsValue::NULL, scanner.cursor_direction)
                    };

                    let cursor_request = match cursor_request {
                        Ok(req) => req,
                        Err(e) => return Some((Err(RetrievalError::StorageError(format!("Failed to open cursor: {:?}", e).into())), None)),
                    };

                    let stream = cb_stream(&cursor_request, "success", "error");
                    let state = ScanState::Scanning {
                        stream: Box::pin(stream),
                        eq_prefix_len: scanner.eq_prefix_len,
                        eq_prefix_js: scanner.eq_prefix_js,
                    };

                    // Recursively get the first item
                    Box::pin(async move { get_next_record(state).await }).await
                }
                ScanState::Scanning { .. } => get_next_record(state).await,
            }
        })
    }
}

enum ScanState {
    Initial(IdbIndexScanner),
    Scanning { stream: std::pin::Pin<Box<crate::util::cb_stream::CBStream>>, eq_prefix_len: usize, eq_prefix_js: Vec<JsValue> },
}

async fn get_next_record(state: ScanState) -> Option<(Result<Object, RetrievalError>, Option<ScanState>)> {
    let ScanState::Scanning { mut stream, eq_prefix_len, eq_prefix_js } = state else {
        return None;
    };

    // Get next cursor result
    let result = stream.next().await?;

    let cursor_result = match result {
        Ok(val) => val,
        Err(e) => return Some((Err(RetrievalError::StorageError(format!("Cursor error: {:?}", e).into())), None)),
    };

    // Check for end of cursor
    if cursor_result.is_null() || cursor_result.is_undefined() {
        return None;
    }

    let cursor: web_sys::IdbCursorWithValue = match cursor_result.dyn_into() {
        Ok(c) => c,
        Err(e) => return Some((Err(RetrievalError::StorageError(format!("Failed to cast cursor: {:?}", e).into())), None)),
    };

    // Apply prefix guard if needed
    if eq_prefix_len > 0 {
        if let Ok(key_js) = cursor.key() {
            if !key_js.is_undefined() && !key_js.is_null() {
                let key_arr = js_sys::Array::from(&key_js);
                for i in 0..(eq_prefix_len as u32) {
                    let lhs = key_arr.get(i);
                    let rhs = &eq_prefix_js[i as usize];
                    if !js_sys::Object::is(&lhs, rhs) {
                        // Prefix doesn't match - end of range
                        return None;
                    }
                }
            }
        }
    }

    // Get the record value
    let value = match cursor.value() {
        Ok(v) => v,
        Err(e) => return Some((Err(RetrievalError::StorageError(format!("Failed to get cursor value: {:?}", e).into())), None)),
    };

    let entity_obj = Object::new(value);

    // Advance cursor for next iteration
    if let Err(e) = cursor.continue_() {
        return Some((Err(RetrievalError::StorageError(format!("Failed to advance cursor: {:?}", e).into())), None));
    }

    // Return the record and continue scanning
    Some((Ok(entity_obj), Some(ScanState::Scanning { stream, eq_prefix_len, eq_prefix_js })))
}
