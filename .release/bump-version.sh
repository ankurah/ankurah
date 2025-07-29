#!/bin/bash
set -euo pipefail

# Ankurah Version Bump Script
# Bumps crate versions based on the top entry in the RELEASES file

RELEASES_FILE="RELEASES"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Read the top line of RELEASES file and extract version
get_version_from_releases() {
    if [ ! -f "$RELEASES_FILE" ]; then
        log_error "$RELEASES_FILE not found"
        exit 1
    fi
    
    local top_line
    top_line=$(head -n 1 "$RELEASES_FILE")
    
    if [ -z "$top_line" ]; then
        log_error "$RELEASES_FILE is empty"
        exit 1
    fi
    
    # Extract version (everything before the first space)
    local version
    version=$(echo "$top_line" | cut -d' ' -f1)
    
    if [ -z "$version" ]; then
        log_error "Could not extract version from: $top_line"
        exit 1
    fi
    
    # Validate version format (basic semver check)
    if ! echo "$version" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.-]+)?$'; then
        log_error "Invalid version format: $version (expected semver like 1.2.3)"
        exit 1
    fi
    
    echo "$version"
}

# Check if workspace crates already have the target version
check_versions_already_bumped() {
    local target_version=$1
    
    log_info "Checking if versions are already bumped to $target_version..."
    
    # Get list of workspace crates and their current versions
    local workspace_info
    workspace_info=$(cargo metadata --no-deps --format-version 1 2>/dev/null || {
        log_error "Failed to get workspace metadata"
        exit 1
    })
    
    # Extract workspace member versions using jq (fallback to grep if jq not available)
    local current_versions
    if command -v jq >/dev/null 2>&1; then
        current_versions=$(echo "$workspace_info" | jq -r '.workspace_members[] as $member | .packages[] | select(.id == $member) | "\(.name) \(.version)"')
    else
        # Fallback parsing (less robust but works without jq)
        current_versions=$(echo "$workspace_info" | grep -o '"name":"[^"]*"[^}]*"version":"[^"]*"' | sed 's/"name":"\([^"]*\)".*"version":"\([^"]*\)"/\1 \2/')
    fi
    
    if [ -z "$current_versions" ]; then
        log_warn "Could not determine current workspace versions"
        return 1  # Assume not bumped if we can't check
    fi
    
    log_info "Current workspace versions:"
    echo "$current_versions"
    
    # Check if all workspace crates are already at target version
    local all_match=true
    while read -r crate_name crate_version; do
        if [ "$crate_version" != "$target_version" ]; then
            log_info "$crate_name is at $crate_version (target: $target_version)"
            all_match=false
        fi
    done <<< "$current_versions"
    
    if [ "$all_match" = true ]; then
        log_info "✓ All workspace crates are already at version $target_version"
        return 0  # Already bumped
    else
        log_info "Some crates need version bumping"
        return 1  # Need to bump
    fi
}

# Run cargo release to bump versions
bump_versions() {
    local version=$1
    
    log_info "Bumping versions to $version..."
    
    # Use --no-confirm to avoid interactive prompts
    # Note: cargo release version only supports basic flags
    if ! cargo release version "$version" --execute --no-confirm --workspace; then
        log_error "Failed to bump versions"
        exit 1
    fi
    
    log_info "✓ Version bump completed"
}

# Check if there are any changes to commit
has_uncommitted_changes() {
    ! git diff --quiet || ! git diff --cached --quiet
}

# Commit the version changes
commit_version_bump() {
    local version=$1
    
    if ! has_uncommitted_changes; then
        log_info "No changes to commit (versions already up to date)"
        return 0
    fi
    
    log_info "Committing version bump..."
    
    # Add only Cargo.toml files that were modified by cargo release
    find . -name "Cargo.toml" -exec git add {} \;
    git commit -m "Bump version to $version

Auto-committed by version bump script based on RELEASES file"
    
    log_info "✓ Version bump committed"
}

# Check if version has already been published (tag exists)
check_already_published() {
    local version=$1
    local tag="v$version"
    
    log_info "Checking if version $version has already been published..."
    
    if git tag -l | grep -q "^$tag$"; then
        log_info "✓ Tag $tag already exists. This version has already been published."
        return 0  # Return zero to indicate already published
    fi
    
    log_info "Version $version has not been published yet"
    return 1  # Return non-zero to indicate not published
}

# Main execution
main() {
    log_info "Starting version bump process..."
    
    # Get target version from RELEASES file
    local version
    version=$(get_version_from_releases)
    log_info "Target version: $version"
    
    # Check if version has already been published
    if check_already_published "$version"; then
        log_info "Version $version has already been published. Nothing to do."
        return 0
    fi
    
    # Check if versions are already bumped
    if check_versions_already_bumped "$version"; then
        log_info "✓ All versions are already at $version. Nothing to do."
        return 0
    fi
    
    # Bump versions
    bump_versions "$version"
    
    # Commit changes (if any)
    commit_version_bump "$version"
    
    log_info "✓ Version bump process completed for version $version"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            echo "Usage: $0"
            echo ""
            echo "Bumps workspace crate versions based on the top entry in the RELEASES file."
            echo "This script is designed to be run in a PR context and will commit changes."
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Run main workflow
main "$@" 