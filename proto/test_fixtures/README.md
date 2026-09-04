# proto wire-format fixtures

These files exist so a port of ankurah to another language can prove it speaks the
same wire format, byte for byte, without a running Rust node to talk to. They are
generated from `ankurah-proto` types on this branch and consumed by the TypeScript
port's tests.

Every fixture is a pair:

- **`<name>.bin`** — the bytes. For proto fixtures this is the bincode encoding of a
  *sequence* of values concatenated with no framing, so a consumer decodes them in
  order from a single reader.
- **`<name>.json`** — the sidecar: what those bytes must decode to.

## Why the sidecar exists

Comparing bytes alone cannot catch a *symmetric* decode bug. A port that swaps two
adjacent same-typed fields on both encode and decode round-trips its own output
perfectly, and byte-equality against a fixture passes, while every value it hands to
the application is wrong. The sidecar closes that hole: it names each value, records
what it must decode to, and records the byte `offset` and `len` it must occupy — so a
consumer can also assert its reader sits at the right position after each value, which
catches a decoder that gets the values right but consumes the wrong number of bytes.

### Sidecar shape (proto fixtures)

```json
{
  "fixture": "ids.bin",
  "encoding": "bincode 1.3 `serialize` defaults: …",
  "total_len": 207,
  "items": [
    { "label": "entity_id", "type": "EntityId", "offset": 0, "len": 16,
      "json": "AAECAwQFBgcICQoLDA0ODw" }
  ]
}
```

Each item carries the expected value under exactly one of two keys:

- **`json`** — `serde_json::to_value` of the same value. Used for everything that has
  a faithful JSON form. Note that `EntityId` and `EventId` serialize *differently* in
  JSON than in bincode (base64 string versus raw bytes) because both implement
  `is_human_readable()`-aware `Serialize`; the sidecar shows the JSON form, the `.bin`
  holds the bincode form, and a port must implement both.
- **`debug`** — the Rust `{:?}` rendering. Used only where JSON would lie: the three
  non-finite `f64` cases in `ankql_ast.bin`, which `serde_json` silently turns into
  `null`.

Sidecar object keys are sorted alphabetically (serde_json's default `Map` is
BTreeMap-backed). That is *not* the field order on the wire — bincode writes struct
fields in Rust declaration order. Read the `.bin` for layout, the sidecar for values.

## The encoding

Every `.bin` under this directory except `yrs_v2/` is `bincode` 1.3 with the defaults
`bincode::serialize` applies:

| Thing | Encoding |
| --- | --- |
| integers | fixed width, little-endian — a `u8` is one byte, never widened |
| `Vec`, `String`, `BTreeMap` | `u64` element/byte count, then the elements |
| enum variant | `u32` tag, then the payload |
| `Option` | one byte, `0x00` or `0x01`, then the payload if present |
| `[u8; N]` | N raw bytes, **no length prefix** |
| `bool` | one byte |
| tuple, struct | fields back to back in declaration order, no framing |

Two traps this format sets, both deliberately exercised below:

- **Fixed arrays carry no length prefix.** `EntityId` is `[u8; 16]` → 16 bytes;
  `Operation.diff` is `Vec<u8>` → 8 length bytes then the content. Same content,
  different byte count.
- **Ulid-backed ids are strings, not 16 bytes.** `TransactionId`, `RequestId`,
  `QueryId` and `UpdateId` wrap `ulid::Ulid`, whose `Serialize` writes its 26-character
  Crockford Base32 rendering — so on the wire each is `26u64` followed by 26 ASCII
  bytes. `EntityId` is the exception: it has a hand-written `Serialize` that writes the
  raw 16 bytes.

## Regenerating

```bash
# proto (bincode) fixtures
OVERWRITE_FIXTURES=1 cargo test -p ankurah-proto --test bincode_fixtures

# proto/yrs_v2 (Yrs lib0-v2) fixtures
OVERWRITE_FIXTURES=1 cargo test -p ankurah-proto --test yrs_v2_fixtures

# core property-backend fixtures (written to ../../core/test_fixtures)
OVERWRITE_FIXTURES=1 cargo test -p ankurah-core --test backend_fixtures
```

Without `OVERWRITE_FIXTURES`, the same commands *verify* instead: they compare both the
`.bin` and the `.json` against what the current code produces, and fail on any drift. A
fixture that does not exist yet is written on a plain run, so adding a test generates
its files the first time.

Every fixture here is reproducible: regenerating twice yields identical bytes. Nothing
in the generation path reads the clock, the RNG, or the environment — all ids are
constructed from fixed byte patterns or fixed ULID strings, and the yrs docs below pin
their client ids explicitly.

## Inventory — bincode fixtures

`items` is the number of values concatenated in the `.bin`.

| Fixture | items | What it encodes | Edge cases it pins |
| --- | --- | --- | --- |
| `ids.bin` | 7 | `EntityId`, `EventId`, `TransactionId`, `RequestId`, `QueryId`, `UpdateId`, `CollectionId` | fixed `[u8;16]`/`[u8;32]` with no prefix, next to four ULID-as-26-char-string ids |
| `clock.bin` | 3 | `Clock` | empty, one event, three events |
| `auth.bin` | 6 | `AuthData`, `Attestation`, `AttestationSet`, `Attested<EntityState>` | empty and non-empty byte vectors; empty and two-element attestation sets |
| `data.bin` | 8 | `Operation`, `OperationSet`, `StateBuffers`, `State`, `StateFragment`, `Event`, `EventFragment`, `EntityState` | `BTreeMap` key ordering; nested fragments |
| `request.bin` | 6 | `NodeRequest` and all five `NodeRequestBody` variants | variant tags 0–4 |
| `response.bin` | 8 | `NodeResponse` and all seven `NodeResponseBody` variants | variant tags 0–6, including the unit `Success` |
| `causal.bin` | 8 | all seven `CausalRelation` variants, `CausalAssertionFragment` | `Disjoint.gca` as both `Some` and `None` |
| `causal_assertion.bin` | 2 | `CausalAssertion` | empty clocks on both sides |
| `delta.bin` | 7 | all three `DeltaContent` variants, `EntityDelta`, `KnownEntity` | — |
| `update.bin` | 10 | `NodeUpdate`, `SubscriptionUpdateItem`, both `UpdateContent` variants, all three `MembershipChange`, `NodeUpdateAck`, both ack bodies | tuple-variant `StateAndEvent(_, _)` |
| `message.bin` | 7 | both `Message` variants, all five `NodeMessage` variants | `auth` vector empty and non-empty |
| `presence.bin` | 2 | `Presence` | `durable` both ways; `system_root` `None` and `Some` |
| `system.bin` | 2 | `sys::Item::SysRoot`, `sys::Item::Collection` | — |
| `principal.bin` | 1 | `Principal` | **zero bytes** — an empty struct encodes to nothing |
| `attested_event.bin` | 2 | `Attested<Event>` | empty and two-element attestation sets |

### Gap fixtures

Added later; kept as separate files so every fixture above stays byte-identical for
consumers that already read them.

| Fixture | items | What it encodes | Edge cases it pins |
| --- | --- | --- | --- |
| `system_other.bin` | 3 | `sys::Item::Other` and two `Collection` names | the `#[serde(other)]` catch-all variant (tag 2); non-ASCII name; empty name |
| `integer_widths.bin` | 26 | every integer width the wire format uses, each in the type that carries it | `bool`; `u32` at 0/1/`MAX`/high-byte-only; `u64` limit at 0/1/`u32::MAX+1`/`MAX`; `i16`/`i32`/`i64` at `MIN`/`-1`/`MAX`; `i64` at 2^53+1 (past JS `Number.MAX_SAFE_INTEGER`); `[u8;16]` vs `Vec<u8>` of the same 16 bytes, and the same for 32; enum tag 6 |
| `unicode.bin` | 13 | non-ASCII text in every string-bearing position | 2-, 3- and 4-byte UTF-8 sequences; a combining mark; RTL script; an interior NUL; the empty string; non-ASCII `BTreeMap` keys in `OperationSet` and `StateBuffers`; a non-ASCII multi-step `PathExpr` |
| `empty_collections.bin` | 13 | every container the format lets you empty | empty `Clock`, `Operation.diff`, `OperationSet`, `StateBuffers`, `AttestationSet`, `CollectionId`; and *empty-inside-non-empty*: a backend key mapping to an empty operation list, a state-buffer key mapping to zero bytes |
| `ankql_ast.bin` | 58 | every variant of every `ankql::ast` enum | all 10 `Literal` (including three non-finite `f64` under `debug`, `-0.0`, `Literal::EntityId` as a ULID *string*, and `Json` through `json_as_bytes`); all 6 `Expr`; all 7 `Predicate` plus a nested tree; all 8 `ComparisonOperator`; all 4 `InfixOperator`; `OrderDirection`; multi-step `PathExpr`; `Selection` with `order_by` `None` vs `Some(vec![])` vs two items, and `limit` set |
| `request_edge.bin` | 7 | `NodeRequestBody` at its boundaries | empty `events`/`ids`/`event_ids`/`known_matches`; two events in one commit; `version: u32::MAX`; `limit` above `u32::MAX`; a two-conjunct selection with `ORDER BY` |
| `response_edge.bin` | 7 | `NodeResponseBody` at its boundaries | empty `Fetch`/`Get`/`GetEvents`/`QuerySubscribed` vectors; three deltas covering all three `DeltaContent` variants in one response; error string empty and non-ASCII |
| `update_edge.bin` | 6 | update path at its boundaries | empty `items`; empty `EventOnly`; `StateAndEvent` with no events; empty and three-entry `predicate_relevance`; non-ASCII ack error |
| `event_id_derivation.bin` | 8 | four `EventId`s from `Event::id()`, then the four `Event`s they came from | `EventId::from_parts` is SHA-256 over the bincode of `entity_id`, `operations` and `parent` in that order — matching it proves all three encodings at once, and cannot be faked field by field. Includes a genesis event (empty parent), non-ASCII backend keys, and a pair proving `collection` is **not** hashed: `standard_event` and `collection_is_not_hashed` differ only in collection and must produce the same id |

## Inventory — `yrs_v2/` fixtures

These are Yrs 0.24 documents encoded with the lib0 **v2** update format, produced by
`encode_state_as_update_v2` (full state) or `encode_diff_v2` (diff). They are the same
bytes `ankurah_core::property::backend::YrsBackend` produces: its `to_state_buffer` is
the former against an empty state vector, its `to_operations` the latter against the
previously-seen state vector.

They are generated from a raw `yrs::Doc` with an explicitly fixed `client_id`, not from
a `YrsBackend`, because `YrsBackend::new` goes through `yrs::Doc::new`, which draws a
random client id — no constructor takes a fixed one, so a fixture built through the
backend would not be reproducible.

**Text indices are UTF-8 byte offsets.** `yrs::Options::default()` sets
`OffsetKind::Bytes`, so `Text::insert` and `Text::remove_range` position by byte, not
by UTF-16 code unit and not by Unicode scalar. A port that indexes by JavaScript
`string.length` lands inside a multi-byte sequence. `unicode_text.bin` is the fixture
that catches it.

The sidecars here have a different shape from the bincode ones — the bytes are one
document, not a sequence of values:

```json
{
  "fixture": "unicode_text.bin",
  "encoding": "yrs 0.24.0 update, lib0 v2 encoding …",
  "offset_kind": "Bytes (yrs Options default): …",
  "applies_on_top_of": null,
  "text_fields": { "content": "café 日本 🚀" },
  "state_vector": { "6": 11 },
  "note": "…"
}
```

`text_fields` and `state_vector` describe the document **after** the update is applied;
for a diff, that means after applying it on top of the fixture named by
`applies_on_top_of`.

| Fixture | client id(s) | What it encodes | Edge cases it pins |
| --- | --- | --- | --- |
| `empty_doc.bin` | 1 | a document with no roots and no edits | the 13-byte "nothing here" encoding |
| `simple_text.bin` | 1 | one root text, one ASCII insert | baseline |
| `multifield.bin` | 2 | two root texts written in one transaction | root order in the encoding is insertion order, not alphabetical |
| `text_with_edits.bin` | 3 | insert, delete-then-insert, append across three transactions | a non-empty delete set and split blocks — the rendered string is shorter than the block content |
| `incremental_base.bin` | 4 | `"Hello"` | base for the diff below |
| `incremental_diff.bin` | 4 | the `", World!"` appended after `incremental_base` | diff-only encoding; the `to_operations` shape |
| `concurrent_merge.bin` | 10, 20 | two clients insert at index 0 without seeing each other, then merge | the merged string pins the conflict-resolution rule (order by client id), not just the layout |
| `empty_update.bin` | — | `yrs::Update::EMPTY_V2` | the exact 13-byte sentinel `YrsBackend::to_operations` compares against to decide a backend produced nothing. Byte-identical to `empty_doc.bin`, and that equality is itself the invariant |
| `unicode_text.bin` | 6 | inserts and a delete positioned by **byte** offset across 2-, 3- and 4-byte sequences | final string is `café 日本 🚀`; a UTF-16-indexed port computes different positions and produces different text |
| `new_field_base.bin` | 7 | one root text | base for the diff below |
| `new_field_diff.bin` | 7 | a diff that introduces a root field absent from the base | the `to_operations` shape for "a new property appeared", which `incremental_diff` does not cover |
| `fully_deleted_text.bin` | 8 | text inserted then entirely deleted | rendered string is `""` but the encoding is nothing like the empty doc — the root and the delete set survive |

`yrs_v2/metadata.json` predates the per-fixture sidecars and is hand-maintained rather
than generated. It already omits `concurrent_merge` and every fixture added since, and
nothing reads it. The generated `<name>.json` sidecars carry everything it held plus
the state vector, so treat it as superseded.

## Related

`core/test_fixtures/README.md` covers what lives *inside* the opaque `Vec<u8>` fields
here: `StateBuffers` values and `Operation.diff` payloads are byte vectors at the proto
layer and structured encodings at the core layer.
