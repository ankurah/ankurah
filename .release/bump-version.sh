#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."

# Get version from top line of RELEASES file
version=$(head -n 1 RELEASES | cut -d' ' -f1)

echo "Bumping all workspace crates to version $version"

# Step 1: Set all package versions using cargo-edit
cargo set-version --workspace "$version"

# Step 2: Get workspace crate names and manifest paths
crates=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[].name')
manifests=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[].manifest_path')

# Step 3: For each manifest, update workspace dependency versions to exact match (=x.y.z)
echo "Updating dependency versions to exact match..."
for manifest in $manifests; do
    for crate in $crates; do
        # Escape hyphens for sed regex
        crate_pattern=$(echo "$crate" | sed 's/-/\\-/g')
        
        # Update table version: crate = { version = "^x.y.z" } -> crate = { version = "=x.y.z" }
        sed -i '' -E "s/(${crate_pattern}[[:space:]]*=[[:space:]]*\{[^}]*version[[:space:]]*=[[:space:]]*\")[^\"]+\"/\1=$version\"/g" "$manifest"
    done
done

echo "Done. Run 'cargo check' to verify."
