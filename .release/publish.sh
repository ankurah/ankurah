#!/bin/bash
set -euo pipefail

# Ankurah Publishing Script
# Publishes crates based on the top entry in the RELEASES file

PUBLISHED_CRATES_FILE=".release/published_crates"

# Publish crates in dependency order and create tags
while read -r crate; do
    # Skip comments and empty lines
    [[ "$crate" =~ ^#.*$ ]] || [[ -z "$crate" ]] && continue
    
    echo
    echo "📦 Publishing $crate..."
    
    # Get the actual version of this crate using cargo metadata + jq
    version=$(cargo metadata --no-deps --format-version=1 2>/dev/null | jq -r --arg name "$crate" '.packages[] | select(.name==$name) | .version')
    
    # Publish crate (treat "already exists" as success)
    if output=$(cargo publish --package "$crate" 2>&1); then
        echo "   ✅  $version Published successfully"
        # Create and push tag for this crate
        tag="${crate}-v${version}"
        echo "   🏷️  Creating tag $tag..."
        git tag "$tag" || echo "   Tag $tag already exists"
        git push origin "$tag" || echo "   Tag $tag already pushed"

    elif echo "$output" | grep -q "already exists on crates.io"; then
        echo "   ℹ️  $version already exists on crates.io"

    else
        echo "   ❌ Failed to publish"
        echo "$output"
        exit 1
    fi
    
done < "$PUBLISHED_CRATES_FILE"

echo
echo "🎉 Done!" 