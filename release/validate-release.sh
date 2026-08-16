#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

# shellcheck source=release/release-context.sh
source release/release-context.sh

metadata="$(workspace_metadata)"
version="$(workspace_release_version "$metadata")"
branch="$(release_branch_name)"

validate_lockfile
validate_release_version "$version" "$branch"
validate_release_notes "$version"
validate_published_crates "$version" release/published_crates "$metadata"

if [[ -n "$(git status --porcelain)" && "${ALLOW_DIRTY_RELEASE:-0}" != "1" ]]; then
    release_error "publishing requires a clean working tree"
    exit 1
fi

echo "release validated: $version from $branch"
