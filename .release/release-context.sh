#!/usr/bin/env bash

# Shared validation for release preparation and publishing. This file is
# sourced by scripts which enable `set -euo pipefail` themselves.

release_error() {
    echo "release error: $*" >&2
}

release_branch_name() {
    local branch="${RELEASE_BRANCH:-${GITHUB_REF_NAME:-}}"
    if [[ -z "$branch" ]]; then
        branch="$(git branch --show-current)"
    fi
    if [[ -z "$branch" ]]; then
        release_error "cannot determine the current branch; set RELEASE_BRANCH explicitly"
        return 1
    fi
    printf '%s\n' "$branch"
}

validate_release_version() {
    local version="$1"
    local branch="${2:-$(release_branch_name)}"

    if [[ ! "$branch" =~ ^release/([0-9]+)\.([0-9]+)$ ]]; then
        release_error "publishing requires a branch named release/<major>.<minor>; got $branch"
        return 1
    fi
    local branch_major="${BASH_REMATCH[1]}"
    local branch_minor="${BASH_REMATCH[2]}"

    if [[ ! "$version" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
        release_error "version must be an explicit stable <major>.<minor>.<patch>; got $version"
        return 1
    fi
    local version_major="${BASH_REMATCH[1]}"
    local version_minor="${BASH_REMATCH[2]}"

    if [[ "$version_major" != "$branch_major" || "$version_minor" != "$branch_minor" ]]; then
        release_error "version $version does not belong on $branch"
        return 1
    fi
}

validate_version_advance() {
    local current="$1"
    local next="$2"
    local current_major current_minor current_patch
    local next_major next_minor next_patch

    if [[ ! "$current" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
        release_error "current workspace version is not stable semver: $current"
        return 1
    fi
    current_major="${BASH_REMATCH[1]}"
    current_minor="${BASH_REMATCH[2]}"
    current_patch="${BASH_REMATCH[3]}"
    if [[ ! "$next" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
        release_error "next workspace version is not stable semver: $next"
        return 1
    fi
    next_major="${BASH_REMATCH[1]}"
    next_minor="${BASH_REMATCH[2]}"
    next_patch="${BASH_REMATCH[3]}"

    if ((10#$next_major < 10#$current_major)) ||
        ((10#$next_major == 10#$current_major && 10#$next_minor < 10#$current_minor)) ||
        ((10#$next_major == 10#$current_major && 10#$next_minor == 10#$current_minor && 10#$next_patch <= 10#$current_patch)); then
        release_error "version must advance from $current; got $next"
        return 1
    fi
}

workspace_metadata() {
    cargo metadata --locked --no-deps --format-version=1
}

workspace_release_version() {
    local metadata="${1:-$(workspace_metadata)}"
    local versions
    versions="$(jq -r '[.packages[].version] | unique | .[]' <<<"$metadata")"
    local count
    count="$(wc -l <<<"$versions" | tr -d ' ')"
    if [[ "$count" != "1" || -z "$versions" ]]; then
        release_error "workspace packages must have one lockstep version; found: ${versions//$'\n'/, }"
        return 1
    fi
    printf '%s\n' "$versions"
}

validate_release_notes() {
    local version="$1"
    local notes_file="${2:-RELEASES}"
    local count
    count="$(awk -v version="$version" '$1 == version { count += 1 } END { print count + 0 }' "$notes_file")"
    if [[ "$count" != "1" ]]; then
        release_error "$notes_file must contain exactly one changelog entry for $version; found $count"
        return 1
    fi
}

validate_published_crates() {
    local version="$1"
    local crates_file="${2:-.release/published_crates}"
    local metadata="${3:-$(workspace_metadata)}"
    local crate_count=0
    local crate
    local duplicates

    duplicates="$(awk 'NF && $1 !~ /^#/ { print $1 }' "$crates_file" | sort | uniq -d)"
    if [[ -n "$duplicates" ]]; then
        release_error "$crates_file contains duplicate crates: ${duplicates//$'\n'/, }"
        return 1
    fi

    while IFS= read -r crate || [[ -n "$crate" ]]; do
        [[ -z "$crate" || "$crate" == \#* ]] && continue
        crate_count=$((crate_count + 1))

        local matches
        matches="$(jq -r --arg name "$crate" '[.packages[] | select(.name == $name)] | length' <<<"$metadata")"
        if [[ "$matches" != "1" ]]; then
            release_error "$crates_file names $crate $matches times in workspace metadata"
            return 1
        fi

        local crate_version
        crate_version="$(jq -r --arg name "$crate" '.packages[] | select(.name == $name) | .version' <<<"$metadata")"
        if [[ "$crate_version" != "$version" ]]; then
            release_error "$crate is version $crate_version, expected $version"
            return 1
        fi
    done < "$crates_file"

    if [[ "$crate_count" == "0" ]]; then
        release_error "$crates_file contains no crates"
        return 1
    fi
}

validate_release_tag() {
    local tag="$1"
    local release_commit="$2"
    local tagged_commit
    tagged_commit="$(git rev-parse -q --verify "refs/tags/$tag^{commit}" 2>/dev/null || true)"

    if [[ -n "$tagged_commit" && "$tagged_commit" != "$release_commit" ]]; then
        release_error "tag $tag already points to $tagged_commit, not release commit $release_commit"
        return 1
    fi
}
