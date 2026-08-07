# Ankurah CLI

`ankurah-cli` installs the `ak` command. In Ankurah 0.9 it provides portable
logical dump and load tooling for PostgreSQL, SQLite, and Sled. IndexedDB is
not supported.

The current loader is intentionally a 0.9-to-0.9 recovery and migration tool.
The dump uses an explicit, versioned representation so a future 0.10 loader
can translate it, but that translator does not exist yet; this release should
not be represented as a complete 0.9-to-0.10 upgrade path.

The dump contains raw events, raw state buffers, causal clocks, and
attestations. It deliberately excludes PostgreSQL and SQLite materialized
columns and Sled materialization/index values. Loading the raw states through
the destination engine rebuilds those derived values for that engine.

## Install

Install the published command:

```sh
cargo install ankurah-cli
```

Or install the current source from an Ankurah 0.9 checkout:

```sh
cargo install --locked --path cli
```

## Dump

Stop every durable Ankurah node that uses the source store before dumping.
The destination must not already exist and is installed atomically only after
the dump has been written successfully.

```sh
# Set DATABASE_URL through your normal secret-management mechanism.
ak dump --all-durable-nodes-stopped --file backup.akdump postgres

ak dump --all-durable-nodes-stopped --file backup.akdump sqlite \
  --path ankurah.sqlite

ak dump --all-durable-nodes-stopped --file backup.akdump sled \
  --path .ankurah
```

`--database-url` is also available, but command-line arguments may be visible
in shell history and process listings. Prefer `DATABASE_URL` or another
environment-injection mechanism for credentials.

## Load

Stop every durable node that could use the target and load into a new or
logically empty store:

```sh
ak load --all-durable-nodes-stopped --file backup.akdump sqlite \
  --path restored.sqlite
```

The complete dump is copied to private temporary storage and validated
before the target is inspected or written. Validation covers the format
version, checksum, record counts, event IDs, entity/collection ownership,
backend state buffers, and causal references. Events are restored before
states, which causes destination-specific materializations to be rebuilt from
the raw state buffers.

That validation copy is plaintext and is removed when the command exits. Make
sure the host's temporary filesystem has appropriate access controls and disk
encryption for your data.

The 0.9 storage interface cannot provide one atomic transaction for the whole
load. A failed or interrupted load can therefore leave a partial target.
Discard or clear that target and start again; never send application traffic
to it.

This logical dump supplements normal database backups rather than replacing
them. Treat it as sensitive: it contains the full logical contents of the
store, including attestations and application data. Encrypt and control access
to dump files as you would the source database. The SHA-256 footer detects
accidental corruption; it is not a signature, and anyone who can modify the
dump can recompute it. Use trusted storage or a separately protected
signature when authenticity matters.

The 0.9 implementation keeps causal metadata for the complete dump in memory
while storage records themselves are read in cursor pages. Measure validation
memory use against the largest production store before relying on it for a
very large migration.
