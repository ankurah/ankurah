#!/bin/bash
set -euo pipefail

# Ankurah Publishing Script
# Publishes crates based on the top entry in the RELEASES file

RELEASES_FILE="RELEASES"
PUBLISHED_CRATES_FILE=".release/published_crates"
ALLOW_EXISTING=false

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

# Safety check: ensure only RELEASES file was changed in the last commit
check_commit_safety() {
    log_info "Checking commit safety..."
    
    local changed_files
    changed_files=$(git diff --name-only HEAD~1 HEAD)
    
    if [ "$changed_files" != "$RELEASES_FILE" ]; then
        log_error "Commit contains changes to files other than $RELEASES_FILE:"
        echo "$changed_files"
        log_error "Publishing can only be triggered by commits that change only the $RELEASES_FILE"
        exit 1
    fi
    
    log_info "✓ Commit safety check passed"
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

# Check if version has already been published (tag exists)
check_already_published() {
    local version=$1
    local tag="v$version"
    
    log_info "Checking if version $version has already been published..."
    
    if git tag -l | grep -q "^$tag$"; then
        log_warn "Tag $tag already exists. This version has already been published."
        return 1  # Return non-zero to indicate already published
    fi
    
    log_info "✓ Version $version has not been published yet"
    return 0  # Return zero to indicate ready to publish
}

# Validate that we can parse the version from RELEASES file
validate_releases_format() {
    log_info "Validating RELEASES file format..."
    
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
    
    # Try to extract version to validate format
    local version
    version=$(echo "$top_line" | cut -d' ' -f1)
    
    if [ -z "$version" ]; then
        log_error "Could not extract version from top line: $top_line"
        exit 1
    fi
    
    log_info "✓ RELEASES file format validation passed"
}

# Run cargo release to bump versions
bump_versions() {
    local version=$1
    
    log_info "Bumping versions to $version..."
    
    if ! cargo release version "$version" --execute --no-confirm --workspace; then
        log_error "Failed to bump versions"
        exit 1
    fi
    
    log_info "✓ Version bump completed"
}

# Commit the version changes
commit_version_bump() {
    local version=$1
    
    log_info "Committing version bump..."
    
    git add .
    git commit -m "Bump version to $version"
    
    log_info "✓ Version bump committed"
}

# Publish crates using cargo publish
publish_crates() {
    local version=$1
    
    log_info "Publishing crates..."
    
    if [ ! -f "$PUBLISHED_CRATES_FILE" ]; then
        log_error "$PUBLISHED_CRATES_FILE not found"
        exit 1
    fi
    
    local crates
    crates=$(grep -v '^#' "$PUBLISHED_CRATES_FILE" | grep -v '^$' | tr '\n' ' ')
    
    log_info "Publishing crates: $crates"
    
    # Use cargo publish with workspace support (available in recent Cargo versions)
    for crate in $crates; do
        log_info "Publishing $crate..."
        
        # Capture output to check for "already exists" error
        local output
        if output=$(cargo publish --package "$crate" 2>&1); then
            log_info "✓ $crate published successfully"
        else
            # Check if it failed because the version already exists
            if echo "$output" | grep -q "already exists on crates.io"; then
                if [ "$ALLOW_EXISTING" = true ]; then
                    log_warn "✓ $crate version $version already exists on crates.io, skipping"
                else
                    log_error "$crate version $version already exists on crates.io"
                    log_error "Use --allow-existing flag to continue with already-published crates"
                    exit 1
                fi
            else
                log_error "Failed to publish $crate"
                echo "$output"
                exit 1
            fi
        fi
    done
    
    log_info "✓ All crates published successfully"
}

# Create and push git tag
create_and_push_tag() {
    local version=$1
    local tag="v$version"
    
    log_info "Creating and pushing tag $tag..."
    
    # Extract release text from RELEASES file (everything after the first space)
    local release_text
    release_text=$(head -n 1 "$RELEASES_FILE" | cut -d' ' -f2-)
    
    # Create annotated tag with release text
    git tag -a "$tag" -m "$release_text"
    
    # Push the tag and current branch
    git push origin HEAD
    git push origin "$tag"
    
    log_info "✓ Tag $tag created and pushed"
}

# Main execution
main() {
    log_info "Starting Ankurah publishing process..."
    
    # Safety checks
    check_commit_safety
    validate_releases_format
    
    # Get version and validate
    local version
    version=$(get_version_from_releases)
    log_info "Target version: $version"
    
    if ! check_already_published "$version"; then
        log_info "Version $version has already been published. Nothing to do."
        return 0
    fi
    
    # Publishing workflow
    bump_versions "$version"
    commit_version_bump "$version"
    publish_crates "$version"
    create_and_push_tag "$version"
    
    log_info "🎉 Publishing completed successfully for version $version!"
}

# Parse command line arguments
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --allow-existing)
            ALLOW_EXISTING=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "OPTIONS:"
            echo "  --dry-run         Validate setup without actually publishing"
            echo "  --allow-existing  Continue when crate versions already exist on crates.io"
            echo "  -h, --help        Show this help message"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Handle dry-run mode
if [ "$DRY_RUN" = true ]; then
    log_warn "DRY RUN MODE - No actual publishing will occur"
    
    # Just validate and show what would happen
    check_commit_safety
    validate_releases_format
    
    version=$(get_version_from_releases)
    log_info "Would publish version: $version"
    
    if ! check_already_published "$version"; then
        log_info "Version $version has already been published. Nothing would be done."
        exit 0
    fi
    
    crates=$(grep -v '^#' "$PUBLISHED_CRATES_FILE" 2>/dev/null | grep -v '^$' | tr '\n' ' ' || echo "ERROR: $PUBLISHED_CRATES_FILE not found")
    log_info "Would publish crates: $crates"
    
    log_info "✓ Dry run completed - everything looks good!"
    exit 0
fi

# Run main workflow
main "$@" 