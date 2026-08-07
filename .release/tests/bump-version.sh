#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
sandbox="$(mktemp -d)"
cleanup() {
    case "${sandbox:?}" in
        /tmp/* | /private/tmp/* | /var/folders/* | /private/var/folders/*) rm -rf -- "$sandbox" ;;
        *) echo "refusing to remove unexpected temporary path: $sandbox" >&2 ;;
    esac
}
trap cleanup EXIT

mkdir -p "$sandbox/.release" "$sandbox/base" "$sandbox/app"
cp "$root/.release/bump-version.sh" "$root/.release/release-context.sh" "$sandbox/.release/"

printf '%s\n' \
    '[workspace]' \
    'members = ["base", "app"]' \
    'resolver = "2"' > "$sandbox/Cargo.toml"

printf '%s\n' \
    '[package]' \
    'name = "release-test-base"' \
    'version = "0.10.0"' \
    'edition = "2021"' > "$sandbox/base/Cargo.toml"
printf '' > "$sandbox/base/lib.rs"
printf '%s\n' \
    '[lib]' \
    'path = "lib.rs"' >> "$sandbox/base/Cargo.toml"

printf '%s\n' \
    '[package]' \
    'name = "release-test-app"' \
    'version = "0.10.0"' \
    'edition = "2021"' \
    '' \
    '[dependencies]' \
    'release-test-base = { path = "../base", version = "=0.10.0" }' \
    '' \
    '[package.metadata.release-dependency-fixtures]' \
    'release-test-base-0-9 = { package = "release-test-base", path = "../base", version = "=0.9.1" }' \
    'release-test-base-0-10 = { package = "release-test-base", path = "../base", version = "=0.10.0" }' \
    '' \
    '[lib]' \
    'path = "lib.rs"' > "$sandbox/app/Cargo.toml"
printf '' > "$sandbox/app/lib.rs"

printf '%s\n' \
    '0.10.1 synthetic maintenance release' \
    '0.10.0 prior release' > "$sandbox/RELEASES"
printf '%s\n' 'release-test-base' 'release-test-app' > "$sandbox/.release/published_crates"

(cd "$sandbox" && cargo generate-lockfile --offline)

if (cd "$sandbox" && RELEASE_BRANCH=release/0.10 ./.release/bump-version.sh >/dev/null 2>&1); then
    echo "bump-version accepted an implicit version" >&2
    exit 1
fi
if (cd "$sandbox" && RELEASE_BRANCH=release/0.10 ./.release/bump-version.sh 0.9.1 >/dev/null 2>&1); then
    echo "bump-version accepted a version from another release series" >&2
    exit 1
fi

(cd "$sandbox" && RELEASE_BRANCH=release/0.10 ./.release/bump-version.sh 0.10.1)

versions="$(cd "$sandbox" && cargo metadata --locked --no-deps --format-version=1 | jq -r '[.packages[].version] | unique | .[]')"
if [[ "$versions" != "0.10.1" ]]; then
    echo "unexpected bumped versions: $versions" >&2
    exit 1
fi
if ! grep -Fq 'release-test-base = { path = "../base", version = "=0.10.1" }' "$sandbox/app/Cargo.toml"; then
    echo "internal dependency pin was not bumped" >&2
    exit 1
fi
if ! grep -Fq 'release-test-base-0-10 = { package = "release-test-base", path = "../base", version = "=0.10.1" }' "$sandbox/app/Cargo.toml"; then
    echo "current-series dependency alias was not bumped" >&2
    exit 1
fi
if ! grep -Fq 'release-test-base-0-9 = { package = "release-test-base", path = "../base", version = "=0.9.1" }' "$sandbox/app/Cargo.toml"; then
    echo "prior-series dependency alias was changed" >&2
    exit 1
fi

echo "explicit version bump checks passed"
