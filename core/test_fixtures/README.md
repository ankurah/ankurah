# core property-backend fixtures

`proto/test_fixtures` pins the envelope. At the proto layer, `StateBuffers` values and
`Operation.diff` are opaque `Vec<u8>` — the wire carries them, nothing checks what is
in them. These fixtures pin what is inside: the encodings each property backend
produces, and the proto `EntityState` and `Event` assembled from them.

Read `proto/test_fixtures/README.md` first; the bincode conventions, the sidecar
rationale, and the regeneration mechanics are described there and apply here too.

## Backends covered

`core/src/property/backend/mod.rs` registers exactly two backends, `lww` and `yrs`.

- **`lww`** — fixtures below.
- **`yrs`** — fixtures live in `proto/test_fixtures/yrs_v2/`, because `YrsBackend::new`
  goes through `yrs::Doc::new`, which draws a random client id, and no constructor
  takes a fixed one. That directory drives a raw `yrs::Doc` with a pinned client id
  through the identical calls (`encode_state_as_update_v2` for the state buffer,
  `encode_diff_v2` for operations), so the bytes are the ones `YrsBackend` produces.
- **`pn_counter`** — `core/src/property/backend/pn_counter.rs` exists on disk, but its
  `mod` declaration is commented out and its `PropertyBackend` impl no longer matches
  the trait (`fork` returns `Box`, `to_operations` returns a bare `Vec`). It cannot be
  constructed, so it gets no fixtures. If it is revived, it needs them.

## The LWW encodings

Two distinct encodings, both bincode, and the operation one nests the other.

**State buffer** — `LWWBackend::to_state_buffer()`:

```
bincode(BTreeMap<String, Option<Value>>)
```

Keys are ordered by UTF-8 bytes, not insertion order. A key present with value `None`
is not the same as an absent key: the first is a property that has been cleared, the
second a property that was never set.

**Operation diff** — `LWWBackend::to_operations()[i].diff`:

```
bincode(LWWDiff { version: u8, data: Vec<u8> })
    where data = bincode(BTreeMap<String, Option<Value>>)   // only the CHANGED keys
```

Byte layout, spelled out because this is the sharpest integer-width trap in the system:

| offset | width | field |
| --- | --- | --- |
| 0 | **1** | `version`, currently `1` |
| 1 | 8 | `u64` byte length of `data` |
| 9 | *len* | `data`, itself a complete bincode buffer |

`version` is a bare `u8` — one byte. A port that writes it as a `u32` produces a buffer
three bytes longer and shifts everything after it. `lww/op_first_set.bin` starts
`01 3c 00 00 00 00 00 00 00`: version 1, then a 60-byte payload.

The changed-key map holds **only what changed since the last `to_operations()` call**.
LWW tracks a per-entry committed flag, so a second call with no intervening `set`
returns `None` — never an operation carrying an empty map. It tracks the flag, not
value equality, so re-setting a property to the value it already holds *does* produce
an operation (`lww/op_idempotent_set.bin`).

## Sidecar shape

One buffer per file, so the sidecar describes one value rather than a sequence:

```json
{
  "fixture": "lww/op_first_set.bin",
  "encoding": "bincode 1.3 `serialize` defaults: …",
  "produced_by": "LWWBackend::to_operations()[0].diff",
  "scenario": ["LWWBackend::new()", "set(\"name\", …)", "to_operations()"],
  "total_len": 69,
  "envelope": { "version": 1, "version_offset": 0, "version_width_bytes": 1,
                "data_length_prefix_offset": 1, "data_length_prefix_width_bytes": 8,
                "data_offset": 9, "data_len": 60 },
  "decoded_data": { "count": {"I32": 1}, "name": {"String": "Alice"} }
}
```

- **`scenario`** is the exact call sequence that produced the bytes, so a port can
  replay it.
- **`decoded`** (state buffers) / **`decoded_data`** (operations) is the property map a
  correct decoder must recover.
- **`envelope`** appears on operation fixtures only, and is the byte-level breakdown
  above computed from the actual bytes.

All expected values are `serde_json` — every type here has a faithful JSON form, so
no `debug` fallback is needed in this directory. Two things to know when reading them:

- `Value` is externally tagged, so `{"I32": 1}`, `{"String": "Alice"}`, and a cleared
  property is a bare `null`.
- `Value::Json` carries `#[serde(with = "json_as_bytes")]`, so both on the wire and in
  the sidecar it is the UTF-8 **bytes** of the serialized JSON behind a length prefix,
  not the JSON document. `lww/all_value_types.bin` is where that shows.

## Regenerating

```bash
OVERWRITE_FIXTURES=1 cargo test -p ankurah-core --test backend_fixtures
```

Without the flag the same command verifies both the `.bin` and the `.json` and fails on
drift. Every fixture is reproducible — no clock, no RNG, no environment reads: the
entity id is a fixed byte pattern, the LWW backend has no hidden state, and the yrs
backend (which does) is deliberately not driven from here.

## Inventory

| Fixture | bytes | Produced by | What it pins |
| --- | --- | --- | --- |
| `lww/empty_state.bin` | 8 | `to_state_buffer()` on a fresh backend | an empty `BTreeMap` is a bare `u64` zero — eight bytes, not zero and not four |
| `lww/state_after_first_set.bin` | 60 | `to_state_buffer()` | two properties, `String` and `I32` |
| `lww/op_first_set.bin` | 69 | `to_operations()[0].diff` | the `LWWDiff` envelope; both properties in the change map |
| `lww/state_after_second_set.bin` | 76 | `to_state_buffer()` | one property changed, one cleared to `None`, one untouched |
| `lww/op_second_set.bin` | 55 | `to_operations()[0].diff` | carries **only** the two changed keys — `name` was committed by the previous call and is absent. The test also asserts `state_1 + op_2 == state_2` byte for byte |
| `lww/op_idempotent_set.bin` | 35 | `to_operations()[0].diff` | re-setting a property to its current value still produces an operation; a port that suppresses no-op writes emits nothing and diverges |
| `lww/all_value_types.bin` | 694 | `to_state_buffer()` | every `core::value::Value` variant plus a `None`: `I16`/`I32`/`I64` at `MIN` and `MAX`, `I64` at 2^53+1 (past JS `Number.MAX_SAFE_INTEGER`), `F64` at `0.0`/`-0.0`/`0.1+0.2`/`MIN`/`MAX`, both `Bool`s, empty and non-empty `String`, `EntityId`, empty and non-empty `Object`, `Binary`, and `Json` through `json_as_bytes`. Keys are chosen so sorted order differs from insertion order |
| `lww/non_ascii.bin` | 154 | `to_state_buffer()` | non-ASCII property *names* and values: 3-byte (`名前`), 2-byte (`café`), 4-byte (`🚀`), an interior NUL, and a precomposed key paired with a decomposed value — a port that normalizes either produces different bytes |
| `entity_state.bin` | 184 | `bincode(Entity::to_entity_state())` | the join between layers: a real `Entity` with an LWW backend reached through `get_backend`, serialized as a proto `EntityState`. The LWW buffer appears nested under `state_buffers["lww"]`, and the sidecar decodes it in `state_buffers_decoded`. Head is empty (a freshly created entity), and only the backend actually touched is present |
| `event.bin` | 143 | `bincode(Event)` assembled from `to_operations()` | a proto `Event` built the way `Entity::generate_commit_event` builds it — `OperationSet` keyed by backend name, `parent` from the entity head. The sidecar carries `event_id_base64`, the SHA-256 `EventId::from_parts` derives from the bincode of `entity_id`, `operations` and `parent`; matching it proves all three encodings at once |

`Entity::generate_commit_event` is `pub(crate)`, so `event.bin` mirrors what it does
rather than calling it: collect each backend's `to_operations()` into an `OperationSet`
keyed by backend name, pair with the entity's current head as `parent`. The one
behaviour it does not mirror is the early return — `generate_commit_event` yields `None`
when no backend produced operations.
