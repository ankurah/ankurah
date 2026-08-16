# Releasing Ankurah

Ankurah releases are prepared and published only from a maintenance branch
named `release/<major>.<minor>`. Merging ordinary work to `main` never
publishes crates.

`main` carries the next development minor version as soon as work for that
series begins (for example, `0.10.0` while 0.10 is under development). That
version does not imply publish intent and does not need a `RELEASES` entry on
`main`; publishing begins only after a matching release branch exists and its
release PR changes `RELEASES`.

The lockstep versions in the workspace `Cargo.toml` files are the release
version's source of truth. `RELEASES` is a human changelog and the change that
signals publish intent; scripts never infer a version from its first line.

## Prepare a release

For the first release of a minor series, create the matching branch from
`main`, such as `release/0.10`. The workspace is already `0.10.0`, so do not
run the bump script: add exactly one `0.10.0` entry to `RELEASES`, run
`release/validate-release.sh`, and review and commit that changelog change.

For a later patch release on an existing release branch:

1. Add exactly one `RELEASES` entry for the intended version. Entries are
   conventionally newest-first, but their order has no machine significance.
2. Run the bump with an explicit stable version:

   ```sh
   release/bump-version.sh 0.9.1
   ```

   The requested version must be greater than the workspace's current
   version and its major/minor pair must match the release branch.

3. Review the Cargo manifests, `Cargo.lock`, and changelog together. Open a PR
   to the same release branch and let its normal CI complete.

Merging the release PR pushes the `RELEASES` change to the matching release
branch, which starts the publish workflow. The workflow independently verifies
that every workspace package has the branch's version, the publish list is
exhaustive, the lockfile is current, and the changelog contains exactly one
entry for that version.

## Publishing and retrying

Crates publish in dependency order with `cargo publish --locked`. Each
successful crate gets a tag such as `ankurah-core-v0.9.1` at the release
commit. An existing tag is accepted only when it already points to that exact
commit; a same-named tag on another commit aborts publishing instead of being
silently ignored.

If a run stops after publishing only some crates, use GitHub Actions' **Re-run
failed jobs** action on the same push. Already-published crates are accepted,
their tags are repaired or verified, and publishing resumes in dependency
order.
