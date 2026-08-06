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

repo="$sandbox/repo"
remote="$sandbox/origin.git"
fake_bin="$sandbox/bin"
publish_log="$sandbox/published"
real_cargo="$(command -v cargo)"

mkdir -p "$repo/.release" "$repo/base" "$repo/app" "$fake_bin"
cp "$root/.release/publish.sh" "$root/.release/release-context.sh" "$repo/.release/"

printf '%s\n' \
    '[workspace]' \
    'members = ["base", "app"]' \
    'resolver = "2"' > "$repo/Cargo.toml"
printf '%s\n' \
    '[package]' \
    'name = "release-test-base"' \
    'version = "0.9.1"' \
    'edition = "2021"' \
    '' \
    '[lib]' \
    'path = "lib.rs"' > "$repo/base/Cargo.toml"
printf '' > "$repo/base/lib.rs"
printf '%s\n' \
    '[package]' \
    'name = "release-test-app"' \
    'version = "0.9.1"' \
    'edition = "2021"' \
    '' \
    '[dependencies]' \
    'release-test-base = { path = "../base", version = "=0.9.1" }' \
    '' \
    '[lib]' \
    'path = "lib.rs"' > "$repo/app/Cargo.toml"
printf '' > "$repo/app/lib.rs"
printf '%s\n' '0.9.1 synthetic maintenance release' > "$repo/RELEASES"
printf '%s\n' 'release-test-base' 'release-test-app' > "$repo/.release/published_crates"

# The single-quoted lines are the literal body of the fake cargo executable.
# shellcheck disable=SC2016
printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -euo pipefail' \
    'if [[ "${1:-}" == "metadata" ]]; then' \
    '    exec "${RELEASE_TEST_REAL_CARGO:?}" "$@"' \
    'fi' \
    'if [[ "${1:-}" != "publish" ]]; then' \
    '    echo "unexpected cargo command: $*" >&2' \
    '    exit 1' \
    'fi' \
    'shift' \
    'package=""' \
    'locked=0' \
    'while [[ "$#" -gt 0 ]]; do' \
    '    case "$1" in' \
    '        --locked) locked=1 ;;' \
    '        --package) shift; package="${1:-}" ;;' \
    '    esac' \
    '    shift' \
    'done' \
    '[[ "$locked" == "1" && -n "$package" ]] || exit 1' \
    'printf "%s\n" "$package" >> "${RELEASE_TEST_PUBLISH_LOG:?}"' \
    'if [[ "${RELEASE_TEST_ALREADY_EXISTS:-0}" == "1" ]]; then' \
    '    echo "crate $package already exists on crates.io" >&2' \
    '    exit 1' \
    'fi' > "$fake_bin/cargo"
chmod +x "$fake_bin/cargo"

(cd "$repo" && "$real_cargo" generate-lockfile --offline)
git -C "$repo" init -qb release/0.9
git -C "$repo" config user.name release-test
git -C "$repo" config user.email release-test@example.com
git -C "$repo" add .
git -C "$repo" commit -qm release
git init -q --bare "$remote"
git -C "$repo" remote add origin "$remote"
git -C "$repo" push -qu origin release/0.9

PATH="$fake_bin:$PATH" \
    RELEASE_BRANCH=release/0.9 \
    RELEASE_TEST_REAL_CARGO="$real_cargo" \
    RELEASE_TEST_PUBLISH_LOG="$publish_log" \
    "$repo/.release/publish.sh"

release_commit="$(git -C "$repo" rev-parse HEAD)"
for crate in release-test-base release-test-app; do
    tag="${crate}-v0.9.1"
    if [[ "$(git --git-dir="$remote" rev-parse "refs/tags/$tag")" != "$release_commit" ]]; then
        echo "remote tag $tag does not point to the release commit" >&2
        exit 1
    fi
done

# A partial-run retry must accept published crates and preserve exact tags.
PATH="$fake_bin:$PATH" \
    RELEASE_BRANCH=release/0.9 \
    RELEASE_TEST_REAL_CARGO="$real_cargo" \
    RELEASE_TEST_PUBLISH_LOG="$publish_log" \
    RELEASE_TEST_ALREADY_EXISTS=1 \
    "$repo/.release/publish.sh"

if [[ "$(wc -l < "$publish_log" | tr -d ' ')" != "4" ]]; then
    echo "publish retry did not visit every crate" >&2
    exit 1
fi

echo "publish and retry checks passed"
