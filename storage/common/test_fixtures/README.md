# planner fixtures

`plans.json` records what `Planner::plan` and `bounds::normalize` produce for a corpus
of queries and hand-built bound sets.

Nothing in `storage/common` implements `Serialize`. The planner IR never crosses a wire
or a storage boundary, so there is no encoding to prove. What these fixtures prove is
**planner agreement**: given the same query, primary key and engine capability config,
does a port choose the same plans, in the same order?

Order is part of the contract. `Planner::plan` returns a deduplicated candidate list
with the table scan appended last as a fallback, and callers pick from that list. A port
that produces the same set in a different order is not equivalent.

## Regenerating

```bash
OVERWRITE_FIXTURES=1 cargo test -p ankurah-storage-common --test planner_fixtures
```

Without the flag the same command verifies and fails on drift.

`serde_json` was added to `storage/common`'s **dev**-dependencies to write the file.
Nothing else changed; the projection below is written by hand in
`storage/common/tests/planner_fixtures.rs`.

## Why a hand-written projection

A `{:?}` snapshot of `Plan` would be a contract on Rust's `Debug` derive: it changes
whenever a field is added or reordered, and it asks a TypeScript port to reproduce
Rust's debug rendering, which is not a thing a port should ever have to do. So every
node below `Plan` is projected explicitly, in the `proj_*` functions of the test file,
into the schema documented here. Changing one of those functions changes the published
contract, which is the point.

Three types are *not* hand-projected, because they already have a canonical JSON shape
used elsewhere in this fixture suite and one shape per type is worth more than
uniformity of method:

- `ankql::ast::Predicate` and `ankql::ast::OrderByItem` — their real `Serialize`, the
  same shape as `proto/test_fixtures/ankql_ast.json` and `ankql/test_fixtures/parse_cases.json`.
- `ankurah_core::value::Value` and `ValueType` — their real `Serialize`, the same shape
  as `core/test_fixtures`. `Value` is externally tagged: `{"I32": 5}`, `{"String": "x"}`.

## Schema

```json
{
  "fixture": "plans.json",
  "produced_by": "…",
  "schema": "…",
  "plan_case_count": 26,
  "bounds_case_count": 10,
  "plan_cases": [ … ],
  "bounds_cases": [ … ]
}
```

### `plan_cases[]`

| field | type | meaning |
| --- | --- | --- |
| `query` | string | the ankql text, parsed with `parse_selection` before planning. What that parser accepts is pinned separately in `ankql/test_fixtures/parse_cases.json`. |
| `primary_key` | string | the primary key column passed to `plan` |
| `config.name` | `"indexeddb"` \| `"full_support"` | which `PlannerConfig` constructor |
| `config.supports_desc_indexes` | bool | the only field the config carries |
| `note` | string | what the case is probing |
| `selection_ast` | object | the parsed `Selection`, so a port can skip its own parser if it wants to test the planner in isolation |
| `plan_count` | number | `plans.len()` |
| `plans` | array | the candidate plans, **in order** |

### `plans[]` — one of three shapes, discriminated by `kind`

**`{"kind": "Index", …}`**

| field | type |
| --- | --- |
| `index_spec` | array of *keypart* (below). The `KeySpec` wrapper carries no other data, so it is projected as a bare array. |
| `scan_direction` | `"Forward"` \| `"Reverse"` |
| `bounds` | array of *key bound* (below). Same, `KeyBounds` is projected as a bare array. |
| `remaining_predicate` | ankql `Predicate` JSON — the residual quals the engine must still evaluate |
| `order_by_spill` | *order-by components* (below) |

**`{"kind": "TableScan", …}`** — identical minus `index_spec`: `bounds` (primary key
bounds, empty array when unconstrained), `scan_direction`, `remaining_predicate`,
`order_by_spill`.

**`{"kind": "EmptyScan"}`** — no other fields. The query can never match.

### *keypart* (`IndexKeyPart`)

| field | type | notes |
| --- | --- | --- |
| `column` | string | first path step for a dotted path |
| `sub_path` | array of string, or `null` | remaining path steps; `["territory"]` for `licensing.territory` |
| `direction` | `"Asc"` \| `"Desc"` | |
| `value_type` | `ValueType` string, e.g. `"String"`, `"I32"` | see the caveat below |
| `nulls` | `"First"` \| `"Last"` \| `null` | never populated by the current planner |
| `collation` | string or `null` | never populated by the current planner |

### *key bound* (`KeyBoundComponent`)

| field | type |
| --- | --- |
| `column` | string |
| `low` | *endpoint* |
| `high` | *endpoint* |

### *endpoint* (`Endpoint`) — discriminated by `kind`

- `{"kind": "UnboundedLow",  "value_type": <ValueType>}`
- `{"kind": "UnboundedHigh", "value_type": <ValueType>}`
- `{"kind": "Value", "datum": <key datum>, "inclusive": bool}`

### *key datum* (`KeyDatum`) — discriminated by `kind`

- `{"kind": "Val",         "value": <Value>}`
- `{"kind": "NegInfinity", "value_type": <ValueType>}`
- `{"kind": "PosInfinity", "value_type": <ValueType>}`

### *order-by components* (`OrderByComponents`)

| field | type | meaning |
| --- | --- | --- |
| `presort` | array of ankql `OrderByItem` | ordering the index scan already delivers; these define partition boundaries |
| `spill` | array of ankql `OrderByItem` | ordering the engine must apply in memory, within each partition |
| `is_satisfied` | bool | `spill.is_empty()` — included so a port can assert the derived predicate too, not just the arrays |
| `is_global_spill` | bool | `presort.is_empty() && !spill.is_empty()` |

### `bounds_cases[]`

| field | meaning |
| --- | --- |
| `label` | case name |
| `note` | what it probes |
| `bounds` | the input, as an array of *key bound* |
| `normalized.canonical_range.lower` / `.upper` | `null`, or `{"tuple": [<Value>…], "open": bool}` where `open` means exclusive |
| `normalized.eq_prefix_len` | number of leading columns that were pure equalities |
| `normalized.eq_prefix_values` | those columns' values, in order |

## Behaviour worth knowing before porting

All of this was found by running the corpus.

### ORDER BY keyparts are always typed `String`

`build_order_first_plan` appends ORDER BY columns as
`IndexKeyPart::asc(name, ValueType::String)` — the type is hardcoded, not derived from
the column. Equality keyparts, by contrast, use `ValueType::of(value)`. So in
`status = 'active' ORDER BY score DESC` the `score` keypart has `value_type: "String"`,
while in `score = 5` it would be `"I32"`. A port that infers the type from the column
will disagree. Recorded as-is; the fixture is a statement of what this planner does, not
of what it should do.

### The two configs differ only where directions are mixed

| query | `indexeddb` | `full_support` |
| --- | --- | --- |
| `ORDER BY score DESC` | keypart `score` **Asc**, `scan_direction: Reverse`, nothing spills | keypart `score` **Desc**, `scan_direction: Forward`, nothing spills |
| `ORDER BY a DESC, b DESC` | both keyparts **Asc**, `Reverse`, nothing spills | both **Desc**, `Forward`, nothing spills |
| `ORDER BY a ASC, b DESC` | keeps `a` only, `Forward`, **`b` spills** | `a` Asc + `b` Desc, `Forward`, nothing spills |

A uniform-direction ORDER BY costs IndexedDB nothing — reversing the scan covers it. It
is only the *change* of direction partway through that forces a spill, and only the
longest same-direction prefix survives into the index.

### `remaining_predicate` is the residual, not the whole predicate

For `score > 5 AND rank < 10` the planner emits two index plans; the `score` plan's
`remaining_predicate` is `rank < 10` and the `rank` plan's is `score > 5`. Each plan
subtracts what its own index already enforces.

### OR is opaque to the planner

`ConjunctFinder` stops at `Or` and hands the whole `Or` node back as a single conjunct,
which is neither an equality nor a range. So `status = 'a' OR status = 'b'` produces a
bare table scan with the entire predicate residual, while
`status = 'active' AND (a = 1 OR b = 2)` still gets an index on `status` and leaves the
`Or` in `remaining_predicate`.

### Primary-key-only queries short-circuit

`id = 'x'`, `id > 5` and `id > 5 ORDER BY id DESC` each return **exactly one** plan, a
table scan, with no index candidates generated at all. Add any other meaningful
predicate and the short-circuit stops applying: `status = 'active' ORDER BY id DESC`
returns an index plan plus the table scan.

### `EmptyScan` suppresses the fallback

`score > 10 AND score < 5` returns a single `EmptyScan` and **no** table scan. Everywhere
else the table scan is appended last.

### `LIMIT` does not reach the planner

`status = 'active' LIMIT 10` produces byte-identical plans to `status = 'active'`. The
limit rides along on the `Selection` and is the engine's problem.

### `bounds::normalize` — three shapes that are not the obvious ones

1. **A lone equality keypart yields an open-ended range.** `single_keypart_equality`
   normalizes to `lower: {tuple: ["active"], open: false}`, `upper: **null**` — not the
   closed `["active"] … ["active"]` you would write by hand. There is an explicit special
   case at the end of `normalize` for `eq_prefix_len == keyparts.len() && eq_prefix_len == 1`.
   Two equality keyparts (`two_keypart_equality`) *do* produce a closed range on both
   sides. The asymmetry is real and a port that implements the general rule only will
   diverge on every single-column equality.

2. **Unbounded on both sides is an empty tuple, not `null`.** `both_unbounded` yields
   `lower: {tuple: [], open: false}`, `upper: null`. The `UnboundedLow` arm pushes
   nothing, then the `UnboundedHigh` arm returns early with whatever the lower tuple
   currently is — which is empty. Compare `empty` (no keyparts at all), which yields
   `lower: null, upper: null`. Two different representations of "no constraint".

3. **`KeyDatum` infinity sentinels are ignored.** `normalize` matches only
   `Endpoint::Value { datum: KeyDatum::Val(_), .. }`, so an
   `Endpoint::Value { datum: KeyDatum::NegInfinity(..) }` falls to the `_ => break` arm
   and the whole normalization returns `lower: null, upper: null`. The sentinels are
   only meaningful as `Endpoint::UnboundedLow`/`UnboundedHigh`.

## Not covered

- `filtering.rs` and `sorting.rs` — the in-memory residual filter and the spill
  comparator. They are the natural next fixture set: given a plan's
  `remaining_predicate` and a set of rows, which rows survive and in what order.
- `PlannerConfig` values other than the two constructors. The struct has one field, so
  the two constructors are exhaustive today.
- Queries the ankql parser rejects — negative and fractional literals, `BETWEEN`,
  single-element `IN`. See `ankql/test_fixtures/README.md`. Planner behaviour on the
  ASTs those would produce cannot be reached from query text, and building them by hand
  would test a shape the system cannot currently produce.
