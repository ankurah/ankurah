#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"

# shellcheck source=.release/release-context.sh
source "$root/.release/release-context.sh"

expect_failure() {
    if "$@" >/dev/null 2>&1; then
        echo "expected failure: $*" >&2
        exit 1
    fi
}

validate_release_version "0.9.1" "release/0.9"
validate_release_version "0.10.0" "release/0.10"
expect_failure validate_release_version "0.9.1" "main"
expect_failure validate_release_version "0.10.0" "release/0.9"
expect_failure validate_release_version "0.9" "release/0.9"
expect_failure validate_release_version "0.9.1-alpha.1" "release/0.9"
validate_version_advance "0.9.0" "0.9.1"
validate_version_advance "0.9.9" "0.10.0"
expect_failure validate_version_advance "0.9.1" "0.9.1"
expect_failure validate_version_advance "0.9.1" "0.9.0"

workspace_version="$(workspace_release_version)"
workspace_series="${workspace_version%.*}"
validate_release_version "$workspace_version" "release/$workspace_series"
validate_published_crates "$workspace_version" "$root/.release/published_crates"
expect_failure workspace_release_version '{"packages":[{"version":"0.9.0"},{"version":"0.9.1"}]}'

# RELEASES is a changelog, not an implicit version source. A matching entry can
# appear anywhere in the file.
notes="$(mktemp)"
printf '1.0.0 newest\n0.9.1 maintenance\n' > "$notes"
validate_release_notes "0.9.1" "$notes"
expect_failure validate_release_notes "0.9.2" "$notes"

duplicate_crates="$(mktemp)"
printf 'ankql\nankql\n' > "$duplicate_crates"
expect_failure validate_published_crates "$workspace_version" "$duplicate_crates"

malformed_crates="$(mktemp)"
printf '  # indented comment\nankql trailing-token\n' > "$malformed_crates"
expect_failure validate_published_crates "$workspace_version" "$malformed_crates"

incomplete_crates="$(mktemp)"
awk 'BEGIN { removed = 0 } /^#/ || NF == 0 { print; next } !removed { removed = 1; next } { print }' \
    "$root/.release/published_crates" > "$incomplete_crates"
expect_failure validate_published_crates "$workspace_version" "$incomplete_crates"

tag_repo="$(mktemp -d)"
cleanup() {
    rm -f -- "${notes:?}" "${duplicate_crates:?}" "${malformed_crates:?}" "${incomplete_crates:?}"
    case "${tag_repo:?}" in
        /tmp/* | /private/tmp/* | /var/folders/* | /private/var/folders/*) rm -rf -- "$tag_repo" ;;
        *) echo "refusing to remove unexpected temporary path: $tag_repo" >&2 ;;
    esac
}
trap cleanup EXIT
git -C "$tag_repo" init -q
git -C "$tag_repo" config user.name release-test
git -C "$tag_repo" config user.email release-test@example.com
printf 'first\n' > "$tag_repo/value"
git -C "$tag_repo" add value
git -C "$tag_repo" commit -qm first
first_commit="$(git -C "$tag_repo" rev-parse HEAD)"
git -C "$tag_repo" tag example-v0.9.1
printf 'second\n' > "$tag_repo/value"
git -C "$tag_repo" commit -qam second
second_commit="$(git -C "$tag_repo" rev-parse HEAD)"
(cd "$tag_repo" && validate_release_tag example-v0.9.1 "$first_commit")
expect_failure bash -c "cd '$tag_repo' && source '$root/.release/release-context.sh' && validate_release_tag example-v0.9.1 '$second_commit'"

expect_failure env ALLOW_DIRTY_RELEASE=1 RELEASE_BRANCH=main "$root/.release/validate-release.sh"

bash -n "$root/.release/release-context.sh"
bash -n "$root/.release/validate-release.sh"
bash -n "$root/.release/bump-version.sh"
bash -n "$root/.release/publish.sh"

echo "release script checks passed"
