# Releasing Ankurah

Ankurah releases are prepared and published only from a maintenance branch
named `release/<major>.<minor>`. Merging ordinary work to `main` never
publishes crates.

The lockstep versions in the workspace `Cargo.toml` files are the release
version's source of truth. `RELEASES` is a human changelog and the change that
signals publish intent; scripts never infer a version from its first line.

## Prepare a release

1. Work on the matching release branch, such as `release/0.9`.
2. Add exactly one `RELEASES` entry for the intended version. Entries are
   conventionally newest-first, but their order has no machine significance.
3. Run the bump with an explicit stable version:

   ```sh
   .release/bump-version.sh 0.9.1
   ```

   The requested version must be greater than the workspace's current
   version and its major/minor pair must match the release branch.

4. Review the Cargo manifests, `Cargo.lock`, and changelog together. Open a PR
   to the same release branch and let its normal CI complete.

Merging that PR pushes the `RELEASES` change to `release/0.9`, which starts the
publish workflow. The workflow independently verifies that every workspace
package has the same `0.9.1` version, the publish list names real packages,
the lockfile is current, and the changelog contains exactly one 0.9.1 entry.

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
