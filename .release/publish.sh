#!/bin/bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

# shellcheck source=.release/release-context.sh
source .release/release-context.sh

PUBLISHED_CRATES_FILE=".release/published_crates"
metadata="$(workspace_metadata)"
version="$(workspace_release_version "$metadata")"
branch="$(release_branch_name)"

validate_release_version "$version" "$branch"
validate_release_notes "$version"
validate_published_crates "$version" "$PUBLISHED_CRATES_FILE" "$metadata"

if [[ -n "$(git status --porcelain)" ]]; then
    release_error "publishing requires a clean working tree"
    exit 1
fi

release_commit="$(git rev-parse HEAD)"
git fetch --tags origin

# Reject every tag collision before the first irreversible crate publication.
while IFS= read -r crate || [[ -n "$crate" ]]; do
    case "$crate" in
        "" | \#*) continue ;;
    esac
    validate_release_tag "${crate}-v${version}" "$release_commit"
done < "$PUBLISHED_CRATES_FILE"

ensure_release_tag() {
    local tag="$1"
    local tagged_commit
    validate_release_tag "$tag" "$release_commit"
    tagged_commit="$(git rev-parse -q --verify "refs/tags/$tag^{commit}" 2>/dev/null || true)"

    if [[ -z "$tagged_commit" ]]; then
        git tag "$tag" "$release_commit"
    fi
    git push origin "refs/tags/$tag"
}

# Publish crates in dependency order and create tags
while IFS= read -r crate || [[ -n "$crate" ]]; do
    # Skip comments and empty lines
    case "$crate" in
        "" | \#*) continue ;;
    esac

    echo
    echo "📦 Publishing $crate..."

    # Publish crate (treat "already exists" as success)
    if output=$(cargo publish --locked --package "$crate" 2>&1); then
        echo "   ✅  $version Published successfully"
    elif echo "$output" | grep -q "already exists on crates.io"; then
        echo "   ℹ️  $version already exists on crates.io"
    else
        echo "   ❌ Failed to publish"
        echo "$output"
        exit 1
    fi

    tag="${crate}-v${version}"
    echo "   🏷️  Ensuring $tag points to $release_commit..."
    ensure_release_tag "$tag"
done < "$PUBLISHED_CRATES_FILE"

echo
echo "🎉 Done!"
