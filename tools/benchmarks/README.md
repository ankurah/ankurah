# Benchmark history

The performance dashboard reads
`https://raw.githubusercontent.com/ankurah/ankurah/benchmark-data/history.json`.
The `benchmark-data` branch is a durable data store; the site needs no publishing
credentials. GitHub's 30-day artifacts are transport and diagnostics, not history.
Nothing in these files activates a runner or publishes data before the changes land.

## Measurements and comparability

`config.json` pins the compiler, sampling defaults and legacy workload IDs.
For current main, Cargo metadata automatically discovers all registered benchmark
targets. Explicit Criterion targets (`harness=false`) are compiled and enumerated
with `--list`; every returned workload is measured without a UI/config list edit.
The current instrument includes DAG comparison/layer preparation with a borrowed fixture,
clock/topological ordering, and the Sled/local-process lanes. Fixture setup remains outside each lane's measured
region; final-state, stored-event and delivery-path assertions remain enabled.
The lane API repair unwraps fallible reads/property access and awaits context
readiness during setup. It changes no production behavior.

The extractor copies Criterion's `median.point_estimate` and the median's own
95% bootstrap confidence interval from `new/estimates.json`. It retains all
`benchmark.json`, `estimates.json` and `sample.json` values in raw JSON. A slope
estimate or the center of a CLI interval is never substituted for the median.
Missing workloads, malformed samples, failures and incompatible harnesses produce
no partial target point. A target that completed remains available when another
target fails; retries run only incomplete targets. Publication deduplicates each
workload and preserves all raw attempts. A fresh isolated build target prevents reused executables or
stale samples from masquerading as measurements.

Each measurement has an explicit `series` incorporating the workload contract,
lane (`local`/`hosted`/`stable`), machine identity/OS/hardware, compiler/target,
profile and build configuration, measurement parameters and operator series
generation. `semantics.py` explicitly audits known instrument bytes against a
shared semantic version: seeded DAG shape, borrowed versus cloned fixture, timed async
operation, result assertions, and drain versus prepare boundary. API-only fixture
adapters can share a series after that audit. Unknown instrument bytes default to
an exact harness hash, so changed workloads never silently join old lines. Exact
harness/adapter hashes always remain provenance and part of source deduplication.
Borrowed DAG timings form separate series from the clone-inclusive history.
Different series must not be connected. Source lockfile and manifest
hashes remain provenance and part of exact-source deduplication, but dependency
updates do not split a trend: dependencies are part of the changing library.

Hosted measurements remain a separate noisy evidence class. They are not a
stable-runner trend and impose no timing regression gate. Existing PR smoke
artifacts also remain separate and are never published to benchmark-data.

## Durable format (schema version 1)

`history.json` contains `runs`, `attempts`, `coverage` and `benchmarks` arrays. A successful run
has `id`, `source` (exact SHA, repository, requested ref/label, source commit date),
`recorded_at` (measurement date), `kind`, `markers`, runner/compiler/harness
provenance, profile/lockfile/build/profile hashes, parameters, comparison key,
workflow/raw URLs and measurements. Each measurement has workload, unit, median,
lower/upper confidence limits, confidence level, sample count, parameters and
explicit series. Current generated units are nanoseconds; the site's local
historical evidence can separately carry exact event-read counts.

`attempts` contain `status: failed|unsupported`, an exact source when resolvable,
reason, workflow URL, timestamp and raw diagnostics. `coverage` inventories every
discovered merged PR and crate release by source SHA and lane, with markers,
source commit date and `pending|requested|measured|unsupported|failed` status.
Unmapped Cargo publications use `source.sha: null` and a unique `source.label`,
with a reason rather than an invented commit/date. Crates.io publication dates
are retained separately from commit and measurement dates.

`benchmarks` is a durable target catalog. Each entry has `id` (`package/target`),
`package`, `target`, `status` (`registered|discovered|unsupported|failed`), optional
reason, and `workloads: [{id, original_workload, unit}]`. Registered targets appear
before any samples exist. Compilation/listing failures retain an entry and reason;
custom/non-Criterion targets require a runner adapter. Listing timeouts remain
retryable. Unknown workload IDs are namespaced by package/target, while existing
configured IDs retain their historical spelling. Removed targets/workloads stay
in history. The site unions catalog entries with observed measurements.

Permanent files are append-only:

- `runs/<attempt-id>/run.json` and `raw.json`: immutable result and raw evidence.
- `requests/<request-id>.json`: compact immutable identity/result receipt, including repeats; raw samples are stored only once.
- `catalogs/<request-id>.json` and `*-benchmarks.json`: immutable coverage and target inventory snapshots.
- `runs/<attempt-id>/adapter.json` and `diagnostics.json`: applied instrument surface/lock provenance and per-target failure logs, when present.
- `history.json`: a regenerated, versioned index with accumulated release/PR markers.

Publishing the same attempt is idempotent. A later tag of the same SHA/configuration
reuses its measurement and adds a marker. The publisher also reconciles races:
if two ordinary requests measured the same configuration concurrently, only the
first is plotted, while the second's raw files and request remain permanent and
`history.duplicates` records which point it reused. `repeat=true` deliberately
retains another plotted measurement. Optimistic push retries reread the latest
branch and merge without force-pushing or dropping unrelated runs. Generated data
commits use the `Ankurah benchmarks` service identity and its own `Signed-off-by`
trailer; they never attribute the automated attestation to a human reviewer.

## Historical backfill

Discovery checks both repository version tags and the crates.io publications of
the current publishable Ankurah/AnkQL workspace crates. It compares each candidate
tag's actual Cargo manifest version/name before attributing a publication to that
SHA. An umbrella `vVERSION` tag is not sufficient on its own. Unpublished package
404 responses mean no publications; other API errors fail discovery explicitly.
Merged PRs come from GitHub's paginated merged-main PR metadata. Releases and PRs
sharing one SHA retain all labels. Old packages no longer in today's workspace
can be included through the saved `--crates-json` input; this is the current
inventory boundary, not a claim that none existed.

Historical backfill selects each reviewed main revision's native full DAG harness
when available. Before that registration, the July 6 full suite
`893721a97551e379b2db6c45de472cfa0b99b04f` adapts the original Event API; later
Event/Clock representations use an appropriate reviewed harness. An explicit
`harness_ref` still selects a reviewed main-history revision. Historical runs keep
the audited comparison/layer subset; old end-to-end lanes need their own adapter.
`layers/diamond_chain_drain` and `layers/diamond_chain_prepare` never share a series.

`historical.py` installs only missing benchmark integration: the feature and
Criterion target/dependency, a feature-gated wrapper, its historical EventLayers
import, a wrapper constant, and relation enum visibility inside a private module.
It changes no comparison/layer algorithm. It retains the complete applied patch,
overlay sources, original/effective lockfile provenance, and verifies every
original dependency name/version/source survived instrument dependency resolution.
Result assertions are active in the overlaid DAG instrument.

Missing benchmark registration is not the cutoff. The implementation being
measured first appears at `abee31cb3351bc398e32539986a4c43c728525b0` (July 4).
Earlier algorithms need a separately audited workload contract. The current local
backfill has genuinely measured the 0.9.0 and 0.9.3 releases with the surface
adapter, plus later main merges; its raw evidence remains outside this repository
until reviewed. No timing point is invented for a registration/API gap.
Official release branches outside main (including release/0.9) have a narrower,
**hosted-only** execution path. The planner verifies all of: an official current
workspace crate name; its actual crates.io publication; a matching repository
release tag; that tag's Cargo package name/version; and the published `.crate`'s
clean `.cargo_vcs_info.json` SHA equaling the requested source SHA. The archive
is never extracted or executed during verification. Missing, dirty or disagreeing
VCS proof yields an explicit authorization gap, not a semantic-comparability claim.
Every authorization stores its evidence on the source record. These revisions
cannot use the stable runner even when `lane=stable` was requested; the planner
outputs `hosted` before measurement scheduling. Their series remains distinct.
Existing July baselines are historical evidence, not a same-machine backfill.
Historical end-to-end points need a separate audited lane adapter.

Read-only inventory (requires Python 3.11+, Git, network and an optional read-only
`GH_TOKEN` for API quota):

```sh
python3 tools/benchmarks/backfill.py --kind stable --output /tmp/backfill-plan.json
```

The JSON plan is inspectable before dispatch. `--dispatch --limit 10` submits only
pending entries to `benchmark-history.yml` on main; `--resume` continues the saved
plan after interruption. Requested entries are not blindly resubmitted. Fresh
discovery reconciles permanent history; the dispatcher checks queued/running jobs
before submitting. `--vcs-json` supplies saved publication metadata for reproducible
offline authorization of non-main releases. At-least-once dispatch is safe because measurement and publishing
both deduplicate. `--pulls-json`, `--crates-json` and `--history-json` support offline,
reproducible inventory. Transient failures retain their diagnostics and become
pending again with exponential backoff (2 hours initially, capped at 24 hours).
Unsupported API results are retried when the harness/generation changes or an
operator explicitly dispatches them again. Only structured compiler API errors
whose primary spans are in the overlaid benchmark, or Cargo's exact diagnostic
that a declared harness feature is absent, are classified unsupported;
network errors, timeouts and other build failures remain retryable. This proves
incompatibility with this instrument/compiler, not that an equivalent adapter is impossible.

The initial 0.9.3 compile smoke exposed missing `bench-internals`. The subsequent
surface adapter compiled and produced all fourteen real Criterion measurements,
preserving the original locked dependency versions. The production adapter's
applied source files were checked byte-for-byte against that measured adapter.

For a chosen historical revision, use the workflow's `ref` and `harness_ref`
inputs. `catalog=true` refreshes durable coverage. The hourly reconciliation job
fills gaps (including intermediate merges in a multi-commit main push), at most
ten missing sources per run, and requests a fresh permanent catalog. It is disabled
until `BENCHMARK_BACKFILL_ENABLED=true`; it does not silently declare backlog complete.

## Activation and security

1. Merge/review these controls on main. Permit the workflow's `GITHUB_TOKEN` to
   create/update only the intended `benchmark-data` branch through repository
   rules. Protect it from human force-push/deletion. No PAT or cross-repository
   credentials are needed. Allow Actions dispatch permissions for reconciliation.
2. For stable results, provision the `ankurah-benchmarks` runner group with the
   `ankurah-benchmark` label, restricted to this workflow on main/protected release
   tags. Use one isolated, quiet machine class and one job at a time. Use disposable
   one-job runner registration with a clean filesystem and no SSH keys, cloud
   credentials, repository write tokens or access to production services. Never
   share this group with PR workflows. Restrict creation of release tags to trusted
   maintainers. A label alone is not an access-control boundary.
3. Set `BENCHMARK_RUNNER_ID` to a permanent logical machine identity and
   `BENCHMARK_HARDWARE` to an accurate CPU/RAM description. Set
   `BENCHMARK_STABLE_ENABLED=true` only after the isolation/queueing checks pass.
   Until then main/tag runs use the visibly separate hosted lane.
4. Set `BENCHMARK_SERIES_GENERATION` (default `1`). **Bump it when changing the
   machine, OS, compiler installation or profile policy**, even when retaining
   runner labels. This explicitly resets historical coverage and schedules the
   new series; the detector cannot inspect an offline dedicated machine. Compiler
   or benchmark edits in config also change the harness hash automatically.
5. Enable `BENCHMARK_BACKFILL_ENABLED=true` when ready to spend runner time filling
   history. Monitor the first unsupported/failed records and adapt only where
   workload meaning remains comparable.

The planner runs on a hosted runner and resolves harness references against trusted
main history. Sources must be main ancestors or independently verified official
published releases; the latter are forced onto disposable hosted runners. Manual workflow dispatch
must target main. Measurement has read-only repository permission, does not persist
checkout credentials, and removes token/secret/password environment variables from
Cargo subprocesses. Main-history source is the trust boundary; there is no
`pull_request_target`, `workflow_run`, privileged PR checkout, source-controlled
shell interpolation or shared benchmark build cache. The publisher is a separate
fresh hosted job with write permission. It executes only the trusted controller,
validates JSON identity/provenance and raw-summary agreement, rejects path traversal
and symlinks, and never builds or executes benchmark/source artifacts.

There is intentionally no workflow-level concurrency group: replacement of a
pending run can lose merge points. A single stable runner queues work; concurrent
publishers use non-force push retries. Cancelled jobs may have no raw samples;
when planning succeeded the publisher records a missing-result failure. An entirely
cancelled workflow can prevent publication, so reconciliation rediscovers that gap.

These choices follow GitHub's official documentation on
[secure workflow use](https://docs.github.com/en/actions/reference/security/secure-use),
[job permissions and workflow syntax](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax),
[event triggers](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows),
and [checkout credential persistence](https://github.com/actions/checkout).
The three action release SHAs were verified against their upstream tags.

## Validation

```sh
python3 -m unittest discover -s tools/benchmarks -v
actionlint .github/workflows/benchmark-history.yml .github/workflows/benchmark-backfill.yml
cargo +nightly-2026-04-27 check --locked -p ankurah-tests --bench lane_perf
```

Tests cover median-versus-slope extraction, raw samples, missing/nonfinite results,
compatibility boundaries, publication retries/races, explicit repeats, release
marker reuse, immutable evidence, unsupported coverage, historical timing boundaries,
crate/tag verification and malicious ref/path/source inputs. Python checks also run
in the hosted PR smoke workflow. No timing threshold is an acceptance gate.
