#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "$#" != "1" ]]; then
    echo "usage: .release/bump-version.sh <major.minor.patch>" >&2
    exit 2
fi

version="$1"
version_major="${version%%.*}"
version_rest="${version#*.}"
version_minor="${version_rest%%.*}"
dependency_series="${version_major}-${version_minor}"

# shellcheck source=.release/release-context.sh
source .release/release-context.sh

branch="$(release_branch_name)"
validate_release_version "$version" "$branch"
validate_release_notes "$version"
current="$(workspace_release_version)"
validate_version_advance "$current" "$version"

echo "Bumping $branch workspace crates from $current to $version"

# Step 1: Set all package versions using cargo-edit
cargo set-version --workspace --offline "$version"

# Step 2: Get workspace crate names and manifest paths
crates=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[].name')
manifests=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[].manifest_path')

# Step 3: For each manifest, update workspace dependency versions to exact match (=x.y.z)
echo "Updating dependency versions to exact match..."
for manifest in $manifests; do
    for crate in $crates; do
        # Update direct dependency keys and aliases for the release series.
        # An alias for an older series, such as ankurah-core-0-9, remains
        # pinned so one binary can intentionally depend on both generations.
        RELEASE_CRATE="$crate" RELEASE_VERSION="$version" RELEASE_SERIES="$dependency_series" perl -0pi -e '
            BEGIN {
                $crate = $ENV{"RELEASE_CRATE"};
                $version = $ENV{"RELEASE_VERSION"};
                $series = $ENV{"RELEASE_SERIES"};
            }
            s!
                (\b([A-Za-z0-9_-]+)\s*=\s*\{)
                ([^}]*)
                \}
            !
                my ($open, $key, $body) = ($1, $2, $3);
                my $direct = $key eq $crate;
                my $series_alias = $key =~ /-\Q$series\E\z/
                    && $body =~ /\bpackage\s*=\s*"\Q$crate\E"/;
                if ($direct || $series_alias) {
                    $body =~ s/(\bversion\s*=\s*")[^"]+"/$1=$version"/;
                }
                "$open$body}"
            !gex;
        ' "$manifest"
    done
done

# Refresh Cargo.lock, then prove the result is internally consistent.
cargo metadata --no-deps --format-version=1 >/dev/null
validate_lockfile
metadata="$(workspace_metadata)"
actual="$(workspace_release_version "$metadata")"
if [[ "$actual" != "$version" ]]; then
    release_error "version bump produced $actual, expected $version"
    exit 1
fi
validate_published_crates "$version" .release/published_crates "$metadata"

echo "Version $version prepared. Review and commit the Cargo and RELEASES changes together."
