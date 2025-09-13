Here’s a compact, end-to-end playbook for lowering your new IR to IndexedDB without ever tripping the “lower key is greater than the upper key” error.

0. What you’re aiming for

Keep the Postgres-style IR you just defined (per-column bounds with typed ±∞).

Run a normalization pass to get one canonical lexicographic interval (possibly open-ended).

For IndexedDB:

If upper is open-ended → use lowerBound(…, open?) and stop the cursor when the equality prefix changes (no \uFFFF hacks).

If lower is unbounded because of trailing −∞ → shorten the lower tuple (shorter sorts first).

If both finite → use bound(lower, upper, lowerOpen, upperOpen).

If empty → emit no scan.

1. Normalization (IR → CanonicalRange)

Input: IndexBounds { keyparts: Vec<IndexColumnBound> }
Output: CanonicalRange { lower: Option<(Vec<PropertyValue>, bool)>, upper: Option<(Vec<PropertyValue>, bool)> }

Algorithm (left→right over keyparts):

Accumulate equality prefix. While low and high are both Value{x, inclusive:true} and equal, push x to both sides.

At the first non-equality column:

Lower side

UnboundedLow(\_) → stop adding (shorten lower tuple), lower_open = false.

Value{x, inclusive} → push x, set lower_open = !inclusive.

Upper side

UnboundedHigh(\_) → upper = None (open-ended).

Value{y, inclusive} → push y, set upper_open = !inclusive.

Ignore subsequent columns (can’t further tighten a lexicographic interval).

Collapse stacked infinities automatically by step (2).

Empty check (lexicographic compare honoring open/closed). If empty → no scan.

This yields at most:

lower = Some((lower_tuple, lower_open)) (or None if truly unbounded; rare for IndexedDB).

upper = None (open-ended) or Some((upper_tuple, upper_open)).

Tip: store also the prefix length (eq_prefix_len) = number of keyparts consumed in step (1). You’ll use it for the cursor prefix-guard.

2. Mapping CanonicalRange → IdbKeyRange

You’ll create JS keys as Array of JS values.

2.1 Build JS key tuples
fn idb_key_tuple(parts: &[PropertyValue]) -> Result<JsValue, Error> {
use js_sys::{Array, Uint8Array};
let arr = Array::new();
for p in parts {
match p {
PropertyValue::I16(x) => arr.push(&JsValue::from_f64(*x as f64)),
PropertyValue::I32(x) => arr.push(&JsValue::from_f64(*x as f64)),
PropertyValue::I64(x) => arr.push(&encode_i64_for_idb(*x)?), // see §4.2
PropertyValue::Bool(b) => arr.push(&JsValue::from_f64(if *b {1.0} else {0.0})), // see §4.1
PropertyValue::String(s) => arr.push(&JsValue::from_str(s)),
PropertyValue::Object(bytes) | PropertyValue::Binary(bytes) => {
let u8 = unsafe { Uint8Array::view(bytes) }; // or new_with_length + copy
arr.push(&u8.buffer()) // ArrayBuffer as key component
}
}
}
Ok(arr.into())
}

2.2 Produce IdbKeyRange

Upper = None (open-ended)

let lower_js = idb_key_tuple(&lower_tuple)?;
let range = web_sys::IdbKeyRange::lower_bound(&lower_js, lower_open)?;
// Remember eq_prefix_len for the cursor guard (see §3)

Finite lower & upper

let lower_js = idb_key_tuple(&lower_tuple)?;
let upper_js = idb_key_tuple(&upper_tuple)?;
let range = web_sys::IdbKeyRange::bound(&lower_js, &upper_js, lower_open, upper_open)?;

Exact equality (optional micro-opt): if both sides equal & closed → IdbKeyRange::only(key).

Safety: you already pre-checked lower ≤ upper during normalization, so no DataError here.

3. Cursor execution (prefix guard, direction, limit)
   let direction = match scan_direction {
   ScanDirection::Forward => web_sys::IdbCursorDirection::Next,
   ScanDirection::Reverse => web_sys::IdbCursorDirection::Prev,
   };

let req = index.open_cursor_with_range_and_direction(&range, direction)?;
let eq_prefix_len = …; // from normalization
let onsuccess = Closure::wrap(Box::new(move |e: web_sys::Event| {
let cursor = e.target().unwrap()
.unchecked_into<web_sys::IdbRequest>()
.result()
.dyn_into::<web_sys::IdbCursorWithValue>()
.ok();

    if let Some(c) = cursor {
        // ---- prefix guard for open-ended upper ----
        if upper_is_open_ended {
            let key = c.key().unwrap(); // JsValue: Array
            if !prefix_matches(&key, &eq_prefix_expected_prefix, eq_prefix_len) {
                return; // stop scan — we’ve left the prefix
            }
        }

        // consume row...
        c.continue_().ok();
    }

}) as Box<dyn FnMut(\_)>);
req.set_onsuccess(Some(onsuccess.as_ref().unchecked_ref()));
onsuccess.forget();

Prefix guard logic:

eq_prefix_expected_prefix = the equality prefix values you materialized in step (1) of normalization.

Compare only the first eq_prefix_len components of the cursor key with the expected values; if any differs, break.

ORDER BY DESC: use direction = Prev (IndexedDB has only ASC indexes; DESC is simulated by reverse cursor).

Limit/offset:

Offset k ⇒ on first cursor, call cursor.advance(k).

Limit n ⇒ count rows you yield; stop when reached.

4. Type mapping caveats (important!)
   4.1 Booleans

IndexedDB keys do not support boolean directly. Map to a stable key type:

Recommended: Number 0/1 (shown above).

Or "0"/"1" strings—just keep it consistent across reads/writes.

4.2 64-bit integers

IndexedDB keys store Number (IEEE-754 double); safe integer range is [-2^53, 2^53].

If your i64 may exceed this, encode as order-preserving string:

fn encode_i64_for_idb(x: i64) -> Result<JsValue, Error> {
const BIAS: i128 = (i64::MAX as i128) - (i64::MIN as i128); // use 128 math
let u = (x as i128) - (i64::MIN as i128); // [0 .. 2^64-1] as i128
Ok(JsValue::from_str(&format!("{:020}", u))) // zero-padded => lex order == numeric
}

Be consistent everywhere (both index writes and range bounds).

If you know values fit in i53, you can use plain Number (faster).

4.3 Binary/Object

Keys may be ArrayBuffer/TypedArray; ordering is lexicographic by byte.

For “Object(Vec<u8>)”, ensure it’s your canonical encoding (e.g., CBOR/MsgPack), not Rust’s serde_json string, if you want binary ordering.

4.4 Strings

Never use "\uFFFF" as a sentinel. Use open-ended scans + prefix guard instead.

Empty string "" is fine; the shorter lower tuple (e.g., ["album"]) correctly includes ["album",""].

5. Edge cases & correctness gates

Contradictory predicates (e.g., year > "2010" AND year < "2005"): your normalization must detect lower ≥ upper ⇒ no scan.

Multiple inequalities same column: tighten to the max lower and min upper before building IndexColumnBound.

Stacked ±∞ in IR: accepted, but collapse at the first occurrence during normalization.

Type mismatches in a keypart: assert early (e.g., KeyDatum::ty() must match on both sides).

6. Optional micro-optimizations

openKeyCursor when you don’t need values (only keys) for pre-filters.

IDBKeyRange.only(eq_tuple) for exact matches.

Reverse scan early stop: with finite upper, in Prev direction you can start from upper directly.

7. Minimal test plan

Equality-only: ["album"], ["album","Alice"] → only.

Open upper: \_\_collection="album" AND age>25 → lowerBound(["album", 25], open=true); prefix guard on ["album"].

Open lower: \_\_collection="album" AND year<"2005" → bound(["album"], ["album","2005"), closed/open.

Empty: year > "2010" AND year < "2005" → no scan.

Empty string: equality on "" and ranges around it.

Bool: verify 0/1 mapping remains ordered across compound keys.

i64 big: include values around ±2^53 and far beyond; confirm string encoding preserves order.

8. Drop-in lowering helpers (signatures)
   // 1) IR → CanonicalRange (+ eq_prefix_len metadata)
   fn normalize(bounds: &IndexBounds) -> (CanonicalRange, usize /_eq_prefix_len_/, Vec<PropertyValue> /_eq_prefix_values_/);

// 2) CanonicalRange → (IdbKeyRange, UpperOpenEndedFlag)
fn to_idb_keyrange(
index: &web_sys::IdbIndex,
cr: &CanonicalRange
) -> Result<(web_sys::IdbKeyRange, bool /_upper_open_/), Error>;

// 3) Cursor runner (prefix guard, direction, limit/offset)
async fn run_cursor(
index: &web_sys::IdbIndex,
keyrange: &web_sys::IdbKeyRange,
dir: ScanDirection,
upper_open: bool,
eq_prefix_len: usize,
eq_prefix_vals: &[PropertyValue],
limit: Option<usize>,
) -> Result<Vec<Row>, Error>;

That’s the whole recipe: normalize → materialize → scan with a prefix guard. No \uFFFF, no inverted bounds, and it aligns with how “real” planners behave while fitting IndexedDB’s API limits.
