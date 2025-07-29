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
    local tag="v$1"
    if git tag -l | grep -q "^$tag$"; then
        log_info "Tag $tag already exists. Nothing to do."
        return 1  # Already published
    fi
    return 0  # Ready to publish
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
    
    # Get version (validates RELEASES format)
    
    local version
    version=$(get_version_from_releases)
    log_info "Target version: $version"
    
    if ! check_already_published "$version"; then
        return 0
    fi
    
    # Show what we would publish
    local crates
    if [ -f "$PUBLISHED_CRATES_FILE" ]; then
        crates=$(grep -v '^#' "$PUBLISHED_CRATES_FILE" | grep -v '^$' | tr '\n' ' ')
        log_info "Target crates: $crates"
    else
        log_error "$PUBLISHED_CRATES_FILE not found"
        exit 1
    fi
    
    # DRY_RUN gates the actual publishing actions
    if [ "$DRY_RUN" = true ]; then
        log_warn "DRY RUN MODE - No actual publishing will occur"
        log_info "✓ Dry run completed - everything looks good!"
        return 0
    fi
    
    # Actually publish and tag
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

# Run main workflow
main "$@" 