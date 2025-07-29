#!/bin/bash
set -euo pipefail

# Get version from top line of RELEASES file
version=$(head -n 1 RELEASES | cut -d' ' -f1)

# Bump versions using cargo release
cargo release version "$version" --execute --no-confirm --workspace 